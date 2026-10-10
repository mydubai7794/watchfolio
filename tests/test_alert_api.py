"""提醒模块数据库 CRUD 与 API 测试。

@author yys
"""
import asyncio
import json
from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient


class _FakeCursor:
    def __init__(self, store):
        self.store = store          # 共享 dict 模拟表
        self.sql = None
        self.args = None
        self.rowcount = 0
        self._result = []
        self.sqls = []              # execute/executemany 调用历史（供同游标断言）

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, args=None):
        self.sql, self.args = sql, args
        self.sqls.append(sql)
        if "INSERT INTO alerts" in sql:
            self._result = [(42,)]  # 模拟 LAST_INSERT_ID() 查询结果
        self.rowcount = 1

    async def executemany(self, sql, seq):
        self.sql, self.args = sql, list(seq)
        self.sqls.append(sql)
        self.rowcount = len(self.args)
        if "INSERT IGNORE INTO daily_bars" in sql:
            for row in self.args:
                self.store["daily_bars"].setdefault(row[0], {})
                self.store["daily_bars"][row[0]][str(row[1])] = row

    async def fetchone(self):
        return self._result.pop(0) if self._result else None

    async def fetchall(self):
        return self._result


class _FakeConn:
    """模拟 aiomysql Connection：async with 得连接自身，cursor() 再给游标，begin/commit/rollback 记录调用。"""

    def __init__(self, store):
        self.store = store
        self.cursors = []           # 本连接打开过的游标（供同游标断言）
        self.tx_calls = []          # begin/commit/rollback 调用记录（供事务断言）
        store.setdefault("conns", []).append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def begin(self):
        self.tx_calls.append("begin")

    async def commit(self):
        self.tx_calls.append("commit")

    async def rollback(self):
        self.tx_calls.append("rollback")

    def cursor(self, *args):
        # 接受 aiomysql.DictCursor 等游标类型参数（execute_dict 会传），忽略类型差异
        cur = _FakeCursor(self.store)
        self.cursors.append(cur)
        return cur


class _FakePool:
    def __init__(self, store):
        self.store = store

    def acquire(self):
        return _FakeConn(self.store)


@pytest.fixture
def fake_store(monkeypatch):
    store = {"daily_bars": {}, "alert_rules": {}, "config": {}}
    import database as db

    async def fake_get_pool():
        return _FakePool(store)  # 模仿生产 async get_pool 形态

    monkeypatch.setattr(db, "get_pool", fake_get_pool)
    return store


def test_save_daily_bars_executemany(fake_store):
    import database as db

    rows = [(date(2026, 10, 1), 1.0, 2.0, 0.5, 1.5, 100),
            (date(2026, 10, 2), 1.5, 1.8, 1.2, 1.4, 90)]
    asyncio.run(db.save_daily_bars("QQQ", rows))
    assert len(fake_store["daily_bars"]["QQQ"]) == 2


def test_upsert_and_get_alert_rule_roundtrip(monkeypatch):
    import database as db

    rows = []

    async def fake_exec_dict(sql, args=None, _pool=None):
        return rows

    async def fake_execute(sql, args=None, _pool=None):
        rows.clear()
        rows.append({"symbol": args[0], "params": json.loads(args[1]),
                     "fired_state": json.loads(args[2]), "enabled": 1})

    monkeypatch.setattr(db, "execute", fake_execute)
    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    asyncio.run(db.upsert_alert_rule("NVDA", {"atr_stop": {"enabled": True}},
                                     {"atr_stop": {"cooldown_until": 0}}))
    rule = asyncio.run(db.get_alert_rule("NVDA"))
    assert rule["params"] == {"atr_stop": {"enabled": True}}
    assert rule["enabled"] == 1


def test_save_alert_same_conn_last_insert_id(fake_store):
    import database as db

    n = asyncio.run(db.save_alert({
        "symbol": "NVDA", "rule": "atr_stop", "level": "info",
        "title": "ATR 止损触发", "body": {"price": 1.0},
    }))
    assert n == 42
    # INSERT 与 LAST_INSERT_ID 查询必须同连接同游标（池多连接下取错会话值会拿到 0/错 id）
    assert len(fake_store["conns"]) == 1
    conn = fake_store["conns"][0]
    assert len(conn.cursors) == 1
    sqls = conn.cursors[0].sqls
    assert any("INSERT INTO alerts" in s for s in sqls)
    assert any("LAST_INSERT_ID" in s for s in sqls)
    assert len(sqls) >= 2


# ---------------------------------------------------------------------------
# API 端点（TestClient 不用 with 上下文避免 lifespan；db 调用全部打桩零连库，
# 同 test_ledger 模式）
# ---------------------------------------------------------------------------
import time

import server


def _client(monkeypatch, username="alert-user"):
    async def fake_get_session(t):
        # expires 必须为 float：鉴权中间件 _check_session 会做数值过期比较
        return {"username": username, "expires": time.time() + 3600}
    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    client = TestClient(server.app)
    client.cookies.set("session_token", "tok")
    return client


def test_kline_endpoint(monkeypatch, fake_store):
    import daily_bars as dbar
    client = _client(monkeypatch)

    async def fake_bars(sym, limit=None):
        return [{"date": date(2026, 10, 1), "open": 1, "high": 2, "low": 0.5,
                 "close": 1.5, "volume": 10}]

    async def fake_rule(sym):
        return None

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(server.db, "get_holdings",
                        lambda enabled_only=True: asyncio.sleep(
                            0, result=[{"symbol": "QQQ", "market": "US",
                                        "cost_price": 100.0}]))
    # setitem 注入：测试结束自动还原，不污染全局 INDICATORS（别的用例还在跑）
    monkeypatch.setitem(dbar.INDICATORS, "QQQ",
                        {"ma50": 1.4, "ma200": 1.3, "rsi14": 55.0,
                         "stop_level": None, "hhv22": 2.0, "atr22": 0.1,
                         "enough_history": False, "asof": "2026-10-01"})
    r = client.get("/api/kline/QQQ?range=90")
    assert r.status_code == 200
    data = r.json()
    assert data["bars"][0]["close"] == 1.5
    assert data["marks"]["cost"] == 100.0
    assert data["indicators"]["rsi14"] == 55.0
    assert data["rsi_series"] == [None]   # 1 根 K 线不足以算 RSI，应为 None 占位


def test_alerts_read_endpoints(monkeypatch):
    client = _client(monkeypatch)
    stored = [{"id": 1, "symbol": "QQQ", "rule": "atr_stop", "level": "critical",
               "title": "t", "body": {}, "is_read": 0,
               "triggered_at": "2026-10-04 10:00:00"}]

    async def fake_get_alerts(unread_only=False, limit=50):
        return [a for a in stored if not unread_only or not a["is_read"]]

    async def fake_mark(aid):
        for a in stored:
            if a["id"] == aid:
                a["is_read"] = 1

    async def fake_mark_all():
        for a in stored:
            a["is_read"] = 1

    monkeypatch.setattr(server.db, "get_alerts", fake_get_alerts)
    monkeypatch.setattr(server.db, "mark_alert_read", fake_mark)
    monkeypatch.setattr(server.db, "mark_all_alerts_read", fake_mark_all)
    assert client.get("/api/alerts?unread=1").json()["total"] == 1
    client.post("/api/alerts/1/read")
    assert client.get("/api/alerts?unread=1").json()["total"] == 0
    client.post("/api/alerts/read_all")


def test_rules_put_get(monkeypatch, fake_store):
    client = _client(monkeypatch)
    body = {"params": {"atr_stop": {"enabled": False}}, "enabled": 1}
    r = client.put("/api/alert/rules/QQQ", json=body)
    assert r.status_code == 200

    async def fake_rules():
        return [{"symbol": "QQQ",
                 "params": {"atr_stop": {"enabled": False}},
                 "fired_state": {}, "enabled": 1}]

    monkeypatch.setattr(server.db, "get_alert_rules", fake_rules)
    data = client.get("/api/alert/rules").json()
    assert data["rules"][0]["symbol"] == "QQQ"
    assert data["defaults"]["atr_stop"]["enabled"] is True


def test_rules_put_preserves_fired_state(monkeypatch):
    """PUT 只更新 params/enabled：走 update_alert_rule_params，fired_state 无被触碰路径。"""
    client = _client(monkeypatch)
    got = {}

    async def fake_update(symbol, params, enabled=1):
        got.update(symbol=symbol, params=params, enabled=enabled)
        return 1

    async def fake_upsert(*a, **kw):
        raise AssertionError("PUT 不应调用 upsert_alert_rule（会覆盖 fired_state）")

    monkeypatch.setattr(server.db, "update_alert_rule_params", fake_update)
    monkeypatch.setattr(server.db, "upsert_alert_rule", fake_upsert)
    r = client.put("/api/alert/rules/QQQ", json={
        "params": {"atr_stop": {"enabled": False}}, "enabled": 0})
    assert r.status_code == 200
    assert got["symbol"] == "QQQ"
    assert got["params"] == {"atr_stop": {"enabled": False}}
    assert got["enabled"] == 0


def test_rules_put_rejects_bad_params(monkeypatch):
    """畸形策略包（非 dict 值）入库前 400 拦截：坏包会让 kline 500/引擎静默失效。"""
    client = _client(monkeypatch)
    r = client.put("/api/alert/rules/QQQ", json={"params": {"price_plan": None}})
    assert r.status_code == 400
    assert r.json()["ok"] is False
    # 未知规则键同样拦截
    r2 = client.put("/api/alert/rules/QQQ", json={"params": {"foo": {"a": 1}}})
    assert r2.status_code == 400


def test_rules_put_rejects_string_levels(monkeypatch):
    """levels 字符串数字 "8.0" 拒绝 400：float() 能过但下游 p/100.0 会 TypeError。"""
    client = _client(monkeypatch)
    r = client.put("/api/alert/rules/QQQ", json={
        "params": {"price_plan": {"levels": ["8.0"]}}})
    assert r.status_code == 400
    assert "levels" in r.json()["message"]
    # 布尔是 int 子类，同样拒绝；数值型放行由 test_rules_put_get 覆盖
    r2 = client.put("/api/alert/rules/QQQ", json={
        "params": {"price_plan": {"levels": [True]}}})
    assert r2.status_code == 400


def test_rules_put_first_time_creates_row(monkeypatch):
    """首次 PUT（库内无行）返回 200 且走 update_alert_rule_params（首存自动建行）。"""
    client = _client(monkeypatch)
    got = {}

    async def fake_update(symbol, params, enabled=1):
        got.update(symbol=symbol, params=params, enabled=enabled)
        return 1

    monkeypatch.setattr(server.db, "update_alert_rule_params", fake_update)
    r = client.put("/api/alert/rules/MU", json={
        "params": {"atr_stop": {"enabled": True}}, "enabled": 1})
    assert r.status_code == 200
    assert got["symbol"] == "MU"
    assert got["params"] == {"atr_stop": {"enabled": True}}
    assert got["enabled"] == 1


def test_rule_save_accepts_v2_keys(monkeypatch):
    """_RULE_KEYS 白名单放行 vix_take_profit/deep_dip 新键（V2 双规则）。"""
    client = _client(monkeypatch)
    got = {}

    async def fake_update(symbol, params, enabled=1):
        got.update(symbol=symbol, params=params, enabled=enabled)
        return 1

    monkeypatch.setattr(server.db, "update_alert_rule_params", fake_update)
    r = client.put("/api/alert/rules/QQQ", json={
        "params": {"vix_take_profit": {"enabled": True, "profit_pct": 25.0,
                                       "vix_pctile_max": 25.0},
                   "deep_dip": {"enabled": True, "dip_pct": 18.0}},
        "enabled": True})
    assert r.status_code == 200 and r.json()["ok"] is True
    assert got["symbol"] == "QQQ"
    assert got["params"]["vix_take_profit"]["profit_pct"] == 25.0
    assert got["params"]["deep_dip"]["dip_pct"] == 18.0
    # 未知键 400 回归（白名单拦截依旧生效）
    r2 = client.put("/api/alert/rules/QQQ", json={
        "params": {"vix_take_profit2": {"enabled": True}}, "enabled": True})
    assert r2.status_code == 400
    # 数值键畸形配置 400：字符串数字会让引擎 float() 抛异常 → 引擎静默失效
    r3 = client.put("/api/alert/rules/QQQ", json={
        "params": {"vix_take_profit": {"profit_pct": "abc"}}, "enabled": True})
    assert r3.status_code == 400
    assert "profit_pct" in r3.json()["message"]
    # 范围越界 400（消息含实际值）：VIX 分位上界须落在 0~100
    r4 = client.put("/api/alert/rules/QQQ", json={
        "params": {"vix_take_profit": {"vix_pctile_max": 150}}, "enabled": True})
    assert r4.status_code == 400
    assert "150" in r4.json()["message"]


# ---------------------------------------------------------------------------
# 二期集成（Task 12）：SSE quote 节流广播 + live 事件接线 + lifespan 挂载
# ---------------------------------------------------------------------------

import longport_push as lpp
import quotes_hub as qh


def test_sse_quote_loop_throttled_broadcast(monkeypatch):
    """_sse_quote_loop：1s 节流批量广播 quotes_hub 快照（quote 事件 items 形态）。

    快照为空不广播；CancelledError 原样上抛（任务关闭语义）。
    """
    sent = []
    qh.quotes_live.clear()

    async def fake_broadcast(event, data):
        sent.append((event, data))

    monkeypatch.setattr(server, "_broadcast", fake_broadcast)
    # 注入假 SSE 客户端：循环对 clients 为空短路（无观者不序列化），广播路径需非空
    monkeypatch.setattr(server, "clients", [object()])
    sleeps = []

    async def fake_sleep(sec):
        sleeps.append(sec)
        # 三拍：第 1 拍返回后快照仍在（QQQ）→ 广播；第 2 拍返回前清空 →
        # 第 2 拍广播空快照跳过；第 3 拍取消退出
        if len(sleeps) >= 3:
            raise asyncio.CancelledError
        if len(sleeps) == 2:
            qh.quotes_live.clear()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    qh.update("QQQ", 100.5, source="lp_push", bid=100.4, ask=100.6)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._sse_quote_loop(interval=1.0))
    assert sleeps == [1.0, 1.0, 1.0]
    assert len(sent) == 1                # 第一拍有快照广播，第二拍空快照跳过
    event, data = sent[0]
    assert event == "quote"
    item = data["items"][0]
    assert item["symbol"] == "QQQ" and item["price"] == 100.5
    assert item["source"] == "lp_push"
    assert item["bid"] == 100.4 and item["ask"] == 100.6
    assert item["ts"]                    # ts 字符串化透传
    qh.quotes_live.clear()


def test_sse_quote_loop_swallows_broadcast_error(monkeypatch):
    """广播异常仅告警不炸循环（下一节拍继续）。"""
    qh.quotes_live.clear()
    calls = {"n": 0}

    async def bad_broadcast(event, data):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("queue gone")

    monkeypatch.setattr(server, "_broadcast", bad_broadcast)
    # 注入假 SSE 客户端（同上：clients 为空会短路跳过广播，异常路径无法触达）
    monkeypatch.setattr(server, "clients", [object()])

    async def fake_sleep(_sec):
        qh.quotes_live["MU"] = {"price": 1.0, "bid": None, "ask": None,
                                "ts": "t", "source": "lp_push"}
        if calls["n"] >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._sse_quote_loop(interval=1.0))
    assert calls["n"] == 2               # 首拍失败后第二拍继续广播
    qh.quotes_live.clear()


def test_sse_quote_loop_skips_when_no_clients(monkeypatch):
    """无 SSE 客户端时空转短路：不取快照不广播（少算不放大）。"""
    qh.quotes_live.clear()
    sent = []

    async def fake_broadcast(event, data):
        sent.append((event, data))

    monkeypatch.setattr(server, "_broadcast", fake_broadcast)
    monkeypatch.setattr(server, "clients", [])   # 无观者
    sleeps = []

    async def fake_sleep(_sec):
        sleeps.append(1)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    qh.update("QQQ", 100.5, source="lp_push")
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._sse_quote_loop(interval=1.0))
    assert sent == []                    # 全程零广播（即使快照非空）
    qh.quotes_live.clear()


# ---------------- SSE overview 推送价重建（_live_overview_payload） ----------------

def _setup_pf_ctx(monkeypatch, quotes):
    """注入 _pf_ctx/portfolio_cache/market 假快照（_live_overview_payload 消费）。"""
    holdings = [{"symbol": "QQQ", "market": "US", "quantity": 10.0,
                 "cost_price": 100.0, "currency": "USD", "name": "", "enabled": 1}]
    monkeypatch.setattr(server, "_pf_ctx", {
        "holdings": holdings, "quotes": quotes, "cash_usd": 1000.0,
        "fx": 7.8, "base": {"QQQ": 100.0}, "close": {"QQQ": 101.0}, "at": 1.0})
    monkeypatch.setattr(server, "portfolio_cache", {
        "ok": True, "session": "regular", "positions": [], "summary": {},
        "cash": [], "errors": []})
    monkeypatch.setattr(server, "fund_nav_cache", None)
    monkeypatch.setattr(server, "market_cache",
                        [{"key": "USDCNH", "price": 7.2}])
    monkeypatch.setattr(server, "fx_spark", [])
    monkeypatch.setattr(server, "_cny_cfg", {"invested": 100000.0, "expense": 0.0})


def test_live_overview_overrides_with_fresh_lp_price(monkeypatch):
    """新鲜 lp_push 价覆盖美股现价并重算汇总（总资产含现金、pct 分母含现金）。"""
    _setup_pf_ctx(monkeypatch, {"QQQ": {"price": 100.5, "pre_close": 100.0,
                                        "name": "QQQ"}})
    monkeypatch.setattr(server, "_fresh_lp_price", lambda s: 103.0 if s == "QQQ" else None)
    data = server._live_overview_payload()
    assert data is not None
    pos = data["positions"][0]
    assert pos["price"] == 103.0                     # 推送价生效
    assert pos["today_pnl"] == pytest.approx(30.0)   # (103-100场基准)×10
    s = data["summary"]
    assert s["total_value"] == pytest.approx(1030.0 + 1000.0)   # 含现金
    # cny：value=总资产×USDCNH，pnl=value−净投入
    assert data["cny"]["value"] == pytest.approx(2030.0 * 7.2, rel=1e-6)
    assert data["cny"]["pnl"] == pytest.approx(2030.0 * 7.2 - 100000.0, rel=1e-6)


def test_live_overview_skips_stale_and_non_lp(monkeypatch):
    """陈旧/非 lp_push 价不覆盖（_fresh_lp_price 返回 None 走轮询价）。"""
    _setup_pf_ctx(monkeypatch, {"QQQ": {"price": 100.5, "pre_close": 100.0,
                                        "name": "QQQ"}})
    monkeypatch.setattr(server, "_fresh_lp_price", lambda s: None)
    data = server._live_overview_payload()
    assert data["positions"][0]["price"] == 100.5    # 轮询价原样


def test_live_overview_empty_ctx_returns_none(monkeypatch):
    """上轮快照缺失（冷启动/行情源不可用）→ None 不广播。"""
    monkeypatch.setattr(server, "_pf_ctx", {"holdings": [], "quotes": {},
                                            "cash_usd": 0.0, "fx": 0.0,
                                            "base": {}, "close": {}, "at": 0.0})
    monkeypatch.setattr(server, "portfolio_cache", {"ok": False})
    assert server._live_overview_payload() is None


def test_fresh_lp_price_window(monkeypatch):
    """新鲜度窗口：lp_push 价 90s 内有效、超窗/非 lp_push/坏 ts 均为 None。"""
    from datetime import datetime as dt, timezone as tz, timedelta
    qh.quotes_live.clear()
    fresh = dt.now(tz.utc).isoformat()
    stale = dt.now(tz.utc) - timedelta(seconds=120)
    qh.quotes_live["A"] = {"price": 10.0, "ts": fresh, "source": "lp_push"}
    qh.quotes_live["B"] = {"price": 10.0, "ts": stale.isoformat(), "source": "lp_push"}
    qh.quotes_live["C"] = {"price": 10.0, "ts": fresh, "source": "poll"}
    qh.quotes_live["D"] = {"price": 10.0, "ts": "garbage", "source": "lp_push"}
    assert server._fresh_lp_price("A") == 10.0
    assert server._fresh_lp_price("B") is None
    assert server._fresh_lp_price("C") is None
    assert server._fresh_lp_price("D") is None
    qh.quotes_live.clear()


def test_sse_overview_loop_broadcast_and_skip(monkeypatch):
    """_sse_overview_loop：有客户端广播推送价重建载荷；无客户端空转零广播。"""
    sent = []

    async def fake_broadcast(event, data):
        sent.append((event, data))

    monkeypatch.setattr(server, "_broadcast", fake_broadcast)
    _setup_pf_ctx(monkeypatch, {"QQQ": {"price": 100.5, "pre_close": 100.0,
                                        "name": "QQQ"}})
    monkeypatch.setattr(server, "_fresh_lp_price", lambda s: 101.0 if s == "QQQ" else None)
    sleeps = []
    fake_clients = [object()]
    monkeypatch.setattr(server, "clients", fake_clients)

    async def fake_sleep(_sec):
        sleeps.append(1)
        if len(sleeps) == 2:
            fake_clients.clear()             # 第二拍起无观者（同一对象清空）
        if len(sleeps) >= 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._sse_overview_loop(interval=2.0))
    assert len(sent) == 1                    # 第一拍有观者有载荷 → 广播一次
    assert sent[0][0] == "overview"
    assert sent[0][1]["positions"][0]["price"] == 101.0


def test_sse_overview_loop_swallows_payload_error(monkeypatch):
    """载荷构建异常仅告警不炸循环（下一节拍继续）。

    _live_overview_payload 为同步函数，打桩须同步签名——async 桩会返回
    coroutine（恒真值）直接进广播 JSON 序列化，且计数器永不递增导致
    循环无法退出（曾致本用例无限空转刷日志）。
    """
    calls = {"n": 0}

    def bad_payload():
        calls["n"] += 1
        raise RuntimeError("ctx gone")

    monkeypatch.setattr(server, "_live_overview_payload", bad_payload)
    monkeypatch.setattr(server, "clients", [object()])

    async def fake_sleep(_sec):
        if calls["n"] >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._sse_overview_loop(interval=2.0))
    assert calls["n"] == 2                   # 首拍失败后第二拍继续


def test_lp_on_events_same_shape_as_poll_side(monkeypatch):
    """live 推送事件接线：邮件入队（完整事件含 body）+ SSE 广播剥离 body，
    payload 与 _portfolio_loop 轮询侧同款 {id,symbol,rule,level,title}。"""
    enqueued = []
    broadcasted = []

    monkeypatch.setattr(server.mailer, "enqueue", lambda ev: enqueued.append(ev))

    async def fake_broadcast(event, data):
        broadcasted.append((event, data))

    monkeypatch.setattr(server, "_broadcast", fake_broadcast)
    ev = {"id": 9, "symbol": "MU", "rule": "atr_stop", "level": "critical",
          "title": "ATR 止损触发", "body": {"price": 99.0}}
    asyncio.run(server._lp_on_events([ev]))
    assert enqueued == [ev]              # 邮件走完整事件
    assert broadcasted == [("alert", {   # SSE 剥离 body（无鉴权通道不推详情）
        "id": 9, "symbol": "MU", "rule": "atr_stop",
        "level": "critical", "title": "ATR 止损触发"})]


def test_lifespan_mounts_phase2_tasks(monkeypatch):
    """lifespan 任务挂载 + 对称取消（全流程打桩：零连库、全部后台协程
    挂起不执行——create_task 捕获后即 close）。

    验证：二期三任务（push_loop/flush_loop/_sse_quote_loop）被挂载、
    on_events 接线生效、shutdown 后无 task 泄漏。
    """
    async def _noop(*a, **kw):
        return None

    # lifespan 直接 await 的连库入口全部打桩
    monkeypatch.setattr(server.db, "init_tables", _noop)
    monkeypatch.setattr(server.db, "close_pool", _noop)
    monkeypatch.setattr(server.db, "get_latest_fund_nav", _noop)
    monkeypatch.setattr(server, "_daily_init", _noop)

    created = []
    real_create = asyncio.create_task

    def fake_create(coro, **kw):
        created.append(coro)
        coro.close()                     # 挂载验证只看协程身份，不真跑循环
        return real_create(_hang())

    async def _hang():
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "create_task", fake_create)

    async def drive():
        cm = server.lifespan(server.app)
        await cm.__aenter__()
        await cm.__aexit__(None, None, None)
        # shutdown cancel 后回收全部挂起 task，防泄漏告警
        pending = [t for t in asyncio.all_tasks()
                   if t is not asyncio.current_task()]
        await asyncio.gather(*pending, return_exceptions=True)

    asyncio.run(drive())
    names = sorted(c.__name__ for c in created)
    # 二期三任务挂载
    assert "push_loop" in names
    assert "flush_loop" in names
    assert "_sse_quote_loop" in names
    # 一期/既有任务同轮挂载（对称性抽查）
    assert "_portfolio_loop" in names and "_sse_quote_loop" in names
    # live 事件接线：钩子指向 server 侧分发协程
    assert lpp.on_events is server._lp_on_events


# ---------------------------------------------------------------------------
# kline tf 扩展（周/月聚合 + 分时聚合 + 60s 缓存，K线弹层增强 Task 2）
# ---------------------------------------------------------------------------

def _ag(bars, limit):
    """get_daily_bars 的 limit 语义模拟（None=全量）。"""
    return bars if limit is None else bars[-limit:]


async def _ar(sym):
    """无规则（get_alert_rule 打桩，_merged_params 走默认合成）。"""
    return None


def _fake_daily_bars_five():
    """2026-09-28(周一)~10-02 五根日K（跨 ISO 周 40 与两个自然月）。"""
    days = ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]
    return [{"date": d, "open": 10.0 + i, "high": 12.0 + i, "low": 9.0 + i,
             "close": 11.0 + i, "volume": 100} for i, d in enumerate(days)]


def test_kline_tf_week_month(monkeypatch):
    """week 聚 1 根 / month 聚 2 根；series 与 bars 等长；tf=day 原样。"""
    async def fake_bars(sym, limit=None):
        return _ag(_fake_daily_bars_five(), limit)

    async def fake_holdings(enabled_only=True):
        return []

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", _ar)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    for tf, n in (("week", 1), ("month", 2), ("day", 5)):
        r = client.get(f"/api/kline/QQQ?tf={tf}&range=1000")
        assert r.status_code == 200
        d = r.json()
        assert len(d["bars"]) == n
        assert len(d["rsi_series"]) == n
        assert set(d["macd_series"]) == {"dif", "dea", "hist"}
        assert set(d["kdj_series"]) == {"k", "d", "j"}
        assert d["volume_mode"] == "volume"


def test_kline_tf_5m_ticks(monkeypatch):
    """5m：tick 聚合 OHLC/计数正确；无 ticks 返回空 bars（前端隐藏分时入口）。"""
    from datetime import timedelta

    t0 = datetime(2026, 10, 2, 13, 30, 0)
    rows = [{"ts": t0 + timedelta(seconds=s), "price": p}
            for s, p in ((0, 100.0), (120, 101.0), (299, 99.0),
                         (300, 98.0))]

    async def fake_ticks(sym, limit=5000):
        return rows

    async def fake_holdings(enabled_only=True):
        return []

    monkeypatch.setattr(server.db, "get_market_ticks_range", fake_ticks)
    monkeypatch.setattr(server.db, "get_alert_rule", _ar)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    d = client.get("/api/kline/QQQ?tf=5m&range=1000").json()
    assert len(d["bars"]) == 2
    assert d["bars"][0]["open"] == 100.0 and d["bars"][0]["close"] == 99.0
    assert d["bars"][0]["volume"] == 3
    assert d["volume_mode"] == "ticks"

    # 空 ticks → 空 bars（端点对 get_market_ticks_range 二次打桩）
    async def fake_ticks_empty(sym, limit=5000):
        return []

    monkeypatch.setattr(server.db, "get_market_ticks_range", fake_ticks_empty)
    server._KLINE_CACHE.clear()
    d = client.get("/api/kline/QQQ?tf=5m&range=1000").json()
    assert d["ok"] is True and d["bars"] == []


def test_kline_tf_invalid_and_cache(monkeypatch):
    """非法 tf → 400；60s 缓存命中不重查库。"""
    calls = {"n": 0}

    async def fake_bars(sym, limit=None):
        calls["n"] += 1
        return _ag(_fake_daily_bars_five(), limit)

    async def fake_holdings(enabled_only=True):
        return []

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", _ar)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    assert client.get("/api/kline/QQQ?tf=hour").status_code == 400
    r1 = client.get("/api/kline/QQQ?tf=week&range=1000")
    assert r1.status_code == 200
    r2 = client.get("/api/kline/QQQ?tf=week&range=1000")
    assert r2.status_code == 200
    assert calls["n"] == 1          # 第二次走缓存


def test_kline_range_truncate(monkeypatch):
    """range 截断：bars 与 series 同步截尾对齐；缓存期内不同 range 不重查库。"""
    calls = {"n": 0}

    async def fake_bars(sym, limit=None):
        calls["n"] += 1
        return _ag(_fake_daily_bars_five(), limit)

    async def fake_holdings(enabled_only=True):
        return []

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", _ar)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    d = client.get("/api/kline/QQQ?tf=month&range=1").json()
    assert len(d["bars"]) == 1 and d["bars"][0]["date"] == "2026-10-01"
    assert len(d["rsi_series"]) == 1
    # 缓存 hit 路径：同 tf 不同 range 二连请求（不清缓存），第二次从缓存截尾
    d2 = client.get("/api/kline/QQQ?tf=month&range=2").json()
    assert len(d2["bars"]) == 2 and d2["bars"][-1]["date"] == "2026-10-01"
    assert len(d2["rsi_series"]) == 2
    assert calls["n"] == 1          # 全程只查库一次（首次回填缓存）


def test_rule_params_v3_new_keys_validation():
    """v3 新键数值规格：band_pct 开区间 >0 上界 20；depth 含 0.3~0.9。@author yys"""
    # 新键数值规格：band_pct 开区间 >0 上界 20；depth 含 0.3 下界上界 0.9
    assert server._validate_rule_params(
        {"ma200_zone": {"bias_low": -12.0, "band_pct": 3.0},
         "fib_retrace": {"depth": 0.618}}) is None
    assert "band_pct" in server._validate_rule_params({"ma200_zone": {"band_pct": 0}})
    assert "band_pct" in server._validate_rule_params({"ma200_zone": {"band_pct": 25}})
    assert "depth" in server._validate_rule_params({"fib_retrace": {"depth": 0.2}})
    assert "depth" in server._validate_rule_params({"fib_retrace": {"depth": 0.95}})
    # 新规则键已进白名单（_RULE_KEYS 从 DEFAULT_PARAMS 自动派生）
    assert server._validate_rule_params({"ma200_zone": {}}) is None
    assert server._validate_rule_params({"fib_retrace": {}}) is None


def test_kline_overlays_day_and_truncation(monkeypatch):
    """day 周期 overlays：MA50/MA200 序列（不足窗口 None 占位）+ fib 档位；
    range 截尾时均线序列同步截、fib 横线保留；非 day 周期 overlays 为空。@author yys"""
    import daily_bars as dbar
    client = _client(monkeypatch)
    bars = [{"date": date(2026, 9, d), "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 5} for d in range(1, 11)]

    async def fake_bars(sym, limit=None):
        return bars

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule",
                        lambda s: asyncio.sleep(0, result=None))
    monkeypatch.setattr(server.db, "get_holdings",
                        lambda enabled_only=True: asyncio.sleep(0, result=[]))
    monkeypatch.setitem(dbar.INDICATORS, "QQQ",
                        {"ma50": 10.4, "ma200": 10.0, "rsi14": 50.0, "stop_level": None,
                         "hhv22": 11.0, "atr22": 0.2, "enough_history": True,
                         "asof": "2026-09-10",
                         "fib": {"high": 11.0, "low": 9.0, "depth": 0.4,
                                 "levels": {"382": 10.24, "500": 10.0, "618": 9.76}}})
    server._KLINE_CACHE.clear()          # 防前序用例同 key 缓存污染
    d = client.get("/api/kline/QQQ?tf=day&range=1000").json()
    assert len(d["overlays"]["ma50_series"]) == 10
    assert d["overlays"]["ma50_series"][0] is None          # 10 根不足 50，None 占位
    assert d["overlays"]["fib"]["levels"]["500"] == 10.0
    assert d["overlays"]["fib"]["depth"] == 0.4
    d4 = client.get("/api/kline/QQQ?tf=day&range=4").json()  # 走缓存截尾分支
    assert len(d4["overlays"]["ma50_series"]) == 4
    assert d4["overlays"]["fib"]["levels"]["500"] == 10.0    # fib 横线不随截尾切
    server._KLINE_CACHE.clear()
    dw = client.get("/api/kline/QQQ?tf=week&range=1000").json()
    assert dw["overlays"] == {}                               # 非 day 周期为空 dict


# ---------------------------------------------------------------------------
# 宏观估值指标（macro_indicators 日频底座，市场底部信号 Task 5）
# ---------------------------------------------------------------------------

def test_macro_indicator_crud(monkeypatch):
    """写入走 ON DUPLICATE KEY 幂等 upsert；最新行 join MAX(asof) 子查询；
    历史查询按 name 过滤升序。@author yys"""
    import database as db
    captured = []

    async def fake_execute(sql, args=None, _pool=None):
        captured.append((sql, args))

    async def fake_exec_dict(sql, args=None, _pool=None):
        if "MAX(asof)" in sql:
            return [{"name": "cape", "asof": date(2026, 10, 6),
                     "value": 28.1, "extra": None}]
        return [{"asof": date(2026, 10, 6), "value": 28.1}]

    monkeypatch.setattr(db, "execute", fake_execute)
    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    asyncio.run(db.upsert_macro_indicator("cape", "2026-10-06", 28.1, {"rating": "x"}))
    sql, args = captured[0]
    assert "ON DUPLICATE KEY" in sql
    assert args[0] == "cape" and args[1] == "2026-10-06"
    latest = asyncio.run(db.get_macro_latest())
    assert latest[0]["name"] == "cape" and latest[0]["value"] == 28.1
    hist = asyncio.run(db.get_macro_history("cape", days=365))
    assert hist[0]["value"] == 28.1


def test_macro_endpoint(monkeypatch):
    """/api/macro：最新值+近一年历史+状态判定；extra JSON 串解析为对象。@author yys"""
    client = _client(monkeypatch)

    async def fake_latest():
        return [{"name": "cape", "asof": date(2026, 10, 6), "value": 28.1,
                 "extra": None},
                {"name": "fear_greed", "asof": date(2026, 10, 7), "value": 22.0,
                 "extra": json.dumps({"source": "cnn"})}]

    async def fake_hist(name, days=365):
        return [{"asof": date(2026, 10, 6), "value": 28.1},
                {"asof": date(2026, 10, 7), "value": 28.3}]

    monkeypatch.setattr(server.db, "get_macro_latest", fake_latest)
    monkeypatch.setattr(server.db, "get_macro_history", fake_hist)
    d = client.get("/api/macro").json()
    assert [i["name"] for i in d["indicators"]] == ["cape", "fear_greed"]
    assert d["status"]["cape"] == "expensive"
    assert d["status"]["fear_greed"] == "cheap"
    assert len(d["indicators"][0]["history"]) == 2
    assert d["indicators"][1]["extra"] == {"source": "cnn"}
