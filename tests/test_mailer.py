"""邮件通道聚合节流测试。@author yys"""
import asyncio
import time

import mailer


def _ev(sym="NVDA", rule="atr_stop", level="critical", title="止损"):
    return {"symbol": sym, "rule": rule, "level": level, "title": title,
            "body": {"price": 149.0}}


def test_info_events_aggregate_in_window(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="info", sym="NVDA", rule="price_plan", title="加仓档"))
    box.push(_ev(level="info", sym="MU", rule="extreme_guard", title="超卖"))
    mailer.flush(box, force=False, now=time.time() + 301)
    assert len(sent) == 1                     # 5 分钟窗口合并一封
    assert "NVDA" in sent[0][1] and "MU" in sent[0][1]


def test_critical_sends_immediately(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="critical"))
    mailer.flush(box, force=True, now=time.time())
    assert len(sent) == 1 and "止损" in sent[0][0]


def test_unconfigured_mail_logs_only(monkeypatch):
    async def fake_cfg(key):
        return None
    import database as db
    monkeypatch.setattr(db, "get_config", fake_cfg)
    # 重置配置缓存（monkeypatch 化，避免其它用例/历史缓存污染判定）
    monkeypatch.setattr(mailer, "_cfg_cache", {"at": 0.0, "addr": "", "code": ""})
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    ok = asyncio.run(mailer.try_send("s", "t"))
    assert ok is False and sent == []


def test_flush_skips_within_window(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="info"))
    mailer.flush(box, force=False, now=time.time() + 100)   # 窗口内不 flush
    assert sent == []


def test_flush_retains_infos_until_due(monkeypatch):
    """未到期 info 必须留在 pending，到期后随下一轮刷出（事件不丢）。"""
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="info", sym="NVDA", rule="price_plan", title="加仓档"))
    t0 = box.window_start
    assert mailer.flush(box, force=False, now=t0 + 100) == 0   # 未到期
    assert len(box.pending) == 1                               # 保留
    box.push(_ev(level="info", sym="MU", rule="extreme_guard", title="超卖"))
    assert mailer.flush(box, force=False, now=t0 + 301) == 1   # 到期一封
    assert box.pending == [] and box.window_start == 0.0       # 清空归零


def test_mail_worker_delivers_via_try_send(monkeypatch):
    """生产路径回归：enqueue → 全局队列 → _drain 入箱 → flush → _emit 调 try_send。"""
    sent = []

    async def fake_try_send(s, t):
        sent.append((s, t))
        return True

    monkeypatch.setattr(mailer, "try_send", fake_try_send)

    async def main():
        worker = asyncio.create_task(mailer.mail_worker(interval=0.05))
        await asyncio.sleep(0.02)            # 等 worker 初始化全局 _queue
        mailer.enqueue(_ev(level="critical"))
        await asyncio.sleep(0.2)             # 等一轮 flush + 发送任务调度
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

    asyncio.run(main())
    assert len(sent) == 1 and "止损" in sent[0][0]


def test_emit_schedules_task_when_loop_running(monkeypatch):
    """_emit 在运行中的事件循环内必须走 create_task(try_send) 分支。"""
    called = []

    async def fake_send(s, t):
        called.append((s, t))

    monkeypatch.setattr(mailer, "try_send", fake_send)

    async def main():
        mailer._emit("s", "t")
        await asyncio.sleep(0)               # 让出控制权，调度被创建的任务

    asyncio.run(main())
    assert called == [("s", "t")]
