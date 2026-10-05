"""quotes_hub 内存真相源与降采样测试。

零连库零外网：flush 路径全部打桩（仿 test_alert_api fake 模式）。@author yys
"""
import asyncio
from datetime import datetime, timezone

import quotes_hub as qh


def test_sampler_one_second_last_value():
    """同秒高频 tick 只保留末值，换秒价变才出队。"""
    s = qh.Sampler()
    t0 = 1000.0
    s.push("QQQ", 100.0, t0)
    s.push("QQQ", 100.2, t0 + 0.4)     # 同秒 → 取末值
    s.push("QQQ", 100.5, t0 + 1.2)     # 新秒 + 价变
    rows = s.drain()
    assert rows == [("QQQ", 100.2), ("QQQ", 100.5)]


def test_sampler_price_change_only():
    """价未变不产生出队点；drain 带出当前窗口末值（首次落库）。"""
    s = qh.Sampler()
    t0 = 2000.0
    s.push("QQQ", 100.0, t0)
    s.push("QQQ", 100.0, t0 + 2.0)     # 价未变 → 不落
    assert s.drain() == [("QQQ", 100.0)]


def test_sampler_drain_no_repeat_after_flush():
    """已 flush 的窗口值不重复出队：价格停滞后连续 drain 不放大行数。"""
    s = qh.Sampler()
    t0 = 3000.0
    s.push("QQQ", 100.0, t0)
    s.push("QQQ", 101.0, t0 + 1.0)
    s.push("QQQ", 102.0, t0 + 2.0)
    assert len(s.drain()) == 3
    assert s.drain() == []             # 已 flush 不重复
    s.push("QQQ", 102.0, t0 + 3.0)     # 价未变不产生新点
    assert s.drain() == []


def test_update_quotes_live():
    qh.quotes_live.clear()
    qh.update("QQQ", 101.0, source="lp_push", bid=100.9, ask=101.1,
              ts=datetime(2026, 10, 4, 12, tzinfo=timezone.utc))
    snap = qh.snapshot()
    assert snap[0]["symbol"] == "QQQ" and snap[0]["price"] == 101.0
    assert snap[0]["source"] == "lp_push"


def test_ring_buffer_capped(monkeypatch):
    """环形缓冲封顶 RING_MAX，monkeypatch 还原避免污染其他用例。"""
    monkeypatch.setattr(qh, "RING_MAX", 5)
    qh.ring.clear()
    for i in range(10):
        qh._ring_push("QQQ", float(i))
    assert len(qh.ring["QQQ"]) == 5


def test_flush_once_batches_and_staggers_same_symbol(monkeypatch):
    """flush_once 批量落库；同 symbol 多条错开 ts（uk_sym_ts 幂等下防 IGNORE 丢点）。"""
    import database as db
    captured = {}

    async def fake_batch(rows):
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(db, "save_market_ticks_batch", fake_batch)
    monkeypatch.setattr(qh, "_sampler", qh.Sampler())  # 隔离模块级采样器
    qh.push_tick("QQQ", 100.0, now=4000.0)
    qh.push_tick("QQQ", 100.5, now=4000.4)             # 同秒取末值
    qh.push_tick("QQQ", 101.0, now=4001.2)             # 换秒价变
    n = asyncio.run(qh.flush_once())
    assert n == 2
    rows = captured["rows"]
    assert rows[-1][0] == "QQQ" and rows[-1][2] == 101.0
    # 同 symbol 的多条 ts 必须互不相同，否则撞 uk_sym_ts 被 INSERT IGNORE 丢弃
    assert len(rows) == len({(sym, ts) for sym, ts, _ in rows})


def test_drain_then_new_price_same_second():
    """drain 后同秒来新价：flushed 重置，再 drain 带出新价（兜住最后一个价变）。"""
    s = qh.Sampler()
    t0 = 7000.0
    s.push("QQQ", 100.0, t0)
    assert s.drain() == [("QQQ", 100.0)]   # 首次落库，窗口置 flushed
    s.push("QQQ", 100.7, t0 + 0.3)         # 同秒新价 → flushed 重置
    assert s.drain() == [("QQQ", 100.7)]


def test_flush_failure_requeue(monkeypatch):
    """落库失败回灌队头：flush_once 抛出且数据可再次 drain；恢复后重放成功。"""
    import database as db

    async def failing_batch(rows):
        raise RuntimeError("db down")

    monkeypatch.setattr(db, "save_market_ticks_batch", failing_batch)
    monkeypatch.setattr(qh, "_sampler", qh.Sampler())
    qh.push_tick("QQQ", 100.0, now=5000.0)
    qh.push_tick("QQQ", 101.0, now=5001.0)
    try:
        asyncio.run(qh.flush_once())
        assert False, "落库失败应向上抛出"
    except RuntimeError:
        pass
    # 回灌后原序在队头待重放（窗口已全 flushed，drain 即原样返回 _out，
    # 此处直查 _out 避免把数据消费掉）
    assert qh._sampler._out == [("QQQ", 100.0), ("QQQ", 101.0)]

    # DB 恢复后重放成功（INSERT IGNORE 幂等保证重放安全）
    captured = {}

    async def ok_batch(rows):
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(db, "save_market_ticks_batch", ok_batch)
    n = asyncio.run(qh.flush_once())
    assert n == 2
    assert [(r[0], r[2]) for r in captured["rows"]] == [("QQQ", 100.0), ("QQQ", 101.0)]


def test_max_queue_truncate(monkeypatch):
    """max_queue 截断只保留最新 max_queue 条（丢最旧）。"""
    import database as db
    captured = {}

    async def fake_batch(rows):
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(db, "save_market_ticks_batch", fake_batch)
    s = qh.Sampler()
    t0 = 6000.0
    s.push("QQQ", 1.0, t0)
    s.push("MU", 2.0, t0)
    s.push("QQQ", 3.0, t0 + 1.0)
    monkeypatch.setattr(qh, "_sampler", s)
    n = asyncio.run(qh.flush_once(max_queue=2))
    assert n == 2
    # 最旧 ("QQQ", 1.0) 被丢；保尾部即保最新（drain 序：定格点在前、窗口末值在后）
    assert [(r[0], r[2]) for r in captured["rows"]] == [("QQQ", 3.0), ("MU", 2.0)]


def test_save_market_ticks_batch_executemany(monkeypatch):
    """database 层批量写走 cursor 上下文 executemany（aiomysql Connection 无 execute）。"""
    import database as db
    store = {}

    class _Cur:
        def __init__(self):
            self.rowcount = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def executemany(self, sql, seq):
            store["sql"] = sql
            store["rows"] = list(seq)
            self.rowcount = len(store["rows"])

    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        def cursor(self):
            return _Cur()

    class _Pool:
        def acquire(self):
            return _Conn()

    async def fake_get_pool():
        return _Pool()

    monkeypatch.setattr(db, "get_pool", fake_get_pool)
    n = asyncio.run(db.save_market_ticks_batch(
        [("QQQ", datetime(2026, 10, 4, 12, 0, 0), 100.5),
         ("MU", datetime(2026, 10, 4, 12, 0, 1), 88.0)]))
    assert n == 2
    assert "INSERT IGNORE INTO market_ticks" in store["sql"]
    assert len(store["rows"]) == 2
