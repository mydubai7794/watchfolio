"""server 鉴权与持仓 API 离线测试。

不触发 lifespan（不连 DB、不起后台任务）、不发真实外网请求：
- TestClient 不用 with 上下文，避免 lifespan 执行
- 涉及 DB 的分支用 monkeypatch 桩掉
"""
import asyncio
import time
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import server

# 不使用 with：构造即用，避免 lifespan 连接数据库/启动后台循环
client = TestClient(server.app)


@pytest.fixture(autouse=True)
def _clean_state():
    """每个用例结束后清理测试污染的全局状态。"""
    yield
    server._captchas.clear()
    server._captcha_attempts.clear()
    server._login_attempts.clear()


def test_captcha_ok():
    """GET /api/captcha 返回 200，含 captcha_id 与 base64 PNG，且缓存可见。"""
    r = client.get("/api/captcha")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["captcha_id"]
    assert data["image"].startswith("data:image/png;base64,")
    assert data["captcha_id"] in server._captchas
    assert "answer" in server._captchas[data["captcha_id"]]


def test_captcha_one_shot(monkeypatch):
    """验证码一次性：答案正确可过验证码关（密码错仍 401），同 id 复用报验证码错误。"""

    async def fake_get_user(username):
        # 桩掉 DB 查询，返回固定密码以便走"密码错误"分支
        return {"username": username, "password": "right-pass"}

    monkeypatch.setattr(server.db, "get_user", fake_get_user)

    r = client.get("/api/captcha")
    captcha_id = r.json()["captcha_id"]
    answer = server._captchas[captcha_id]["answer"]

    payload = {"username": "u1", "password": "wrong",
               "captcha_id": captcha_id, "captcha_answer": str(answer)}
    r1 = client.post("/api/login", json=payload)
    assert r1.status_code == 401
    assert "用户名或密码" in r1.json()["message"]

    # 同一 captcha_id 已被 pop，二次使用应报验证码错误
    r2 = client.post("/api/login", json=payload)
    assert r2.status_code == 401
    assert "验证码" in r2.json()["message"]


def test_captcha_rate_limit():
    """每 IP 每分钟 30 次下发限制：第 31 次返回 429。"""
    for i in range(30):
        r = client.get("/api/captcha")
        assert r.status_code == 200, f"第 {i + 1} 次下发不应被限速"
    r31 = client.get("/api/captcha")
    assert r31.status_code == 429
    assert "频繁" in r31.json()["message"]


def _fake_valid_session(token: str = "tester"):
    """构造 DB 会话查询桩：返回有效会话。"""
    async def fake_get_session(t):
        return {"username": token, "expires": time.time() + 3600}
    return fake_get_session


def test_market_validation(monkeypatch):
    """持仓新增 market 非法值 → 400，不入库。"""
    monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
    client.cookies.set("session_token", "test-token-market")
    try:
        r = client.post("/api/portfolio/holding", json={"symbol": "AAPL", "market": "xxx"})
        assert r.status_code == 400
        assert "US、HK 或 HKFUND" in r.json()["message"]
    finally:
        client.cookies.clear()


class TestNextFundNavRun:
    """_next_fund_nav_run 边界：14:00/20:00 双目标点，严格大于顺延。"""

    def test_before_14(self):
        assert server._next_fund_nav_run(datetime(2026, 10, 1, 13, 59)) == datetime(2026, 10, 1, 14, 0)

    def test_at_14_exact(self):
        # 14:00 整点已过（严格大于），顺延到当天 20:00
        assert server._next_fund_nav_run(datetime(2026, 10, 1, 14, 0, 0)) == datetime(2026, 10, 1, 20, 0)

    def test_after_14(self):
        assert server._next_fund_nav_run(datetime(2026, 10, 1, 15, 0)) == datetime(2026, 10, 1, 20, 0)

    def test_at_20_exact(self):
        # 20:00 整点已过，顺延次日 14:00
        assert server._next_fund_nav_run(datetime(2026, 10, 1, 20, 0, 0)) == datetime(2026, 10, 2, 14, 0)

    def test_late_night(self):
        assert server._next_fund_nav_run(datetime(2026, 10, 1, 23, 59)) == datetime(2026, 10, 2, 14, 0)


def _login_as(monkeypatch, token: str):
    """测试辅助：mock DB 会话查询返回有效会话，并设置 cookie。"""
    monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
    client.cookies.set("session_token", token)


def test_cash_update_invalid_currency(monkeypatch):
    """PUT /api/portfolio/cash 非法币种 → 400。"""
    _login_as(monkeypatch, "test-token-cash-bad")
    try:
        r = client.put("/api/portfolio/cash", json={"currency": "XXX", "amount": 100})
        assert r.status_code == 400
        assert "USD 或 HKD" in r.json()["message"]
    finally:
        client.cookies.clear()


def test_intraday_hours_window(monkeypatch):
    """GET /api/portfolio/intraday?hours=24 → 走最近24h窗口查询：window=24h，ts 带日期跨日可读。"""
    captured = {}

    async def fake_recent(hours):
        captured["recent_hours"] = hours
        return [{"ts": datetime(2026, 10, 1, 21, 30, 0), "total_value_usd": 100.5, "day_pnl_usd": 1.5}]

    async def fake_intraday(trade_date):
        captured["day_trade_date"] = trade_date
        return []

    monkeypatch.setattr(server.db, "get_portfolio_recent", fake_recent)
    monkeypatch.setattr(server.db, "get_portfolio_intraday", fake_intraday)
    _login_as(monkeypatch, "test-token-intraday-24h")
    try:
        r = client.get("/api/portfolio/intraday", params={"hours": 24})
        assert r.status_code == 200
        data = r.json()
        assert data["window"] == "24h"
        assert captured["recent_hours"] == 24
        # hours>0 不得触发 trade_date 当日分支
        assert "day_trade_date" not in captured
        assert "trade_date" not in data
        # ts 带 %m-%d %H:%M 日期，跨日分时可读
        assert data["points"][0]["ts"] == "10-01 21:30"
        assert data["points"][0]["v"] == 100.5
        assert data["points"][0]["p"] == 1.5
    finally:
        client.cookies.clear()


def test_intraday_default_day(monkeypatch):
    """GET /api/portfolio/intraday 不带 hours → 保持原当日逻辑：window=day，ts 仅时分秒。"""

    async def fake_recent(hours):
        raise AssertionError("hours=0 不应触发 recent 窗口查询")

    async def fake_intraday(trade_date):
        return [{"ts": datetime(2026, 10, 1, 10, 0, 0), "total_value_usd": 99.0, "day_pnl_usd": 0.5}]

    monkeypatch.setattr(server.db, "get_portfolio_recent", fake_recent)
    monkeypatch.setattr(server.db, "get_portfolio_intraday", fake_intraday)
    _login_as(monkeypatch, "test-token-intraday-day")
    try:
        r = client.get("/api/portfolio/intraday")
        assert r.status_code == 200
        data = r.json()
        assert data["window"] == "day"
        assert data["trade_date"]
        assert data["points"][0]["ts"] == "10:00:00"
    finally:
        client.cookies.clear()


def test_cash_update_ok(monkeypatch):
    """PUT /api/portfolio/cash 合法币种 → 200，set_cash 收到规整大写币种。"""
    calls = {}

    async def fake_set_cash(currency, amount):
        calls["args"] = (currency, amount)
        return 1

    monkeypatch.setattr(server.db, "set_cash", fake_set_cash)
    _login_as(monkeypatch, "test-token-cash-ok")
    try:
        r = client.put("/api/portfolio/cash", json={"currency": "usd", "amount": 100})
        assert r.status_code == 200
        assert r.json()["ok"] is True
        assert calls["args"] == ("USD", 100)
    finally:
        client.cookies.clear()


def test_holding_add_duplicate_deduct(monkeypatch):
    """勾选扣现录入已存在 symbol → 400，且不执行 upsert（防二次扣现）。"""

    async def fake_execute_dict(sql, args=None):
        if "FROM holdings" in sql:
            return [{"id": 1}]  # symbol 已存在
        return []

    state = {"upsert_called": False}

    async def fake_upsert(*a, **kw):
        state["upsert_called"] = True
        return 1

    monkeypatch.setattr(server.db, "execute_dict", fake_execute_dict)
    monkeypatch.setattr(server.db, "upsert_holding", fake_upsert)
    _login_as(monkeypatch, "test-token-dup-deduct")
    try:
        r = client.post("/api/portfolio/holding",
                        json={"symbol": "MU", "market": "US", "quantity": 1,
                              "cost_price": 100, "currency": "USD", "deduct_cash": True})
        assert r.status_code == 400
        assert "已存在" in r.json()["message"]
        assert state["upsert_called"] is False
    finally:
        client.cookies.clear()


def test_holding_add_deduct(monkeypatch):
    """新 symbol + 扣现 → 200，响应含 cash_balance（扣减后余额）。"""

    async def fake_execute_dict(sql, args=None):
        return []  # 无已存在持仓

    async def fake_upsert(*a, **kw):
        return 1

    async def fake_deduct(currency, amount):
        assert currency == "USD" and amount == 100.0  # 2 × 50
        return 88.88

    monkeypatch.setattr(server.db, "execute_dict", fake_execute_dict)
    monkeypatch.setattr(server.db, "upsert_holding", fake_upsert)
    monkeypatch.setattr(server.db, "deduct_cash", fake_deduct)
    _login_as(monkeypatch, "test-token-add-deduct")
    try:
        r = client.post("/api/portfolio/holding",
                        json={"symbol": "NEW1", "market": "US", "quantity": 2,
                              "cost_price": 50, "currency": "USD", "deduct_cash": True})
        assert r.status_code == 200
        assert r.json()["cash_balance"] == 88.88
    finally:
        client.cookies.clear()


def test_session_persisted_via_db(monkeypatch):
    """登录成功后会话写入 DB 而非内存字典。"""
    calls = []

    async def fake_create(token, username, ttl):
        calls.append((token, username, ttl))

    monkeypatch.setattr(server.db, "create_session", fake_create)

    async def fake_get_user(username):
        # 桩掉 DB 用户查询，保持测试离线不连库
        return {"username": username, "password": "hello9987"}

    monkeypatch.setattr(server.db, "get_user", fake_get_user)
    # 预置验证码后走完整登录流程
    server._captchas["cid"] = {"answer": 7, "expires": time.time() + 60}
    r = client.post("/api/login", json={
        "username": "admin", "password": "hello9987",
        "captcha_id": "cid", "captcha_answer": "7"})
    assert r.status_code == 200
    assert len(calls) == 1 and calls[0][1] == "admin"
    assert calls[0][2] == server.SESSION_TTL
    assert not hasattr(server, "_sessions") or not server._sessions


def test_expired_token_rejected(monkeypatch):
    """过期 token 401：cookie 携带 token，DB 层返回 float 过去时刻 → _check_session 兜底拦截。"""
    calls = []

    async def fake_get(token):
        calls.append(token)
        return {"username": "t", "expires": time.time() - 1}

    monkeypatch.setattr(server.db, "get_session", fake_get)
    client.cookies.set("session_token", "tok")
    try:
        r = client.get("/api/portfolio/overview")
        assert r.status_code == 401
        # 鉴权必须真实查库且收到 cookie 中的 token（Authorization 头不在鉴权链路）
        assert calls == ["tok"]
    finally:
        client.cookies.clear()


def test_expired_token_datetime_rejected(monkeypatch):
    """过期 token 401：DB 返回 datetime 形式的过去时刻，覆盖 _is_expired 的 datetime 分支。"""
    async def fake_get(token):
        return {"username": "t", "expires": datetime.now() - timedelta(seconds=1)}

    monkeypatch.setattr(server.db, "get_session", fake_get)
    client.cookies.set("session_token", "tok")
    try:
        r = client.get("/api/portfolio/overview")
        assert r.status_code == 401
    finally:
        client.cookies.clear()


def test_overview_fx_spark(monkeypatch):
    """overview 顶层下发 fx_spark（USD/HKD 近5日走势，前端拼 USDHKD 方块用）。"""
    monkeypatch.setattr(server.db, "get_session", _fake_valid_session("fx-user"))
    server.fx_spark = [7.81, 7.82, 7.80]
    client.cookies.set("session_token", "tok")
    try:
        r = client.get("/api/portfolio/overview")
        assert r.status_code == 200
        assert r.json()["fx_spark"] == [7.81, 7.82, 7.80]
    finally:
        client.cookies.clear()
        server.fx_spark = []


def test_refresh_fx_spark(monkeypatch):
    """_refresh_fx_spark：成功写入全局；异常置空数组不抛出（不致命）。"""
    async def fake_ok(proxy_urls=None):
        assert proxy_urls == ["p1"]
        return [7.80, 7.81]

    monkeypatch.setattr(server.pf, "fetch_fx_spark", fake_ok)
    server.fx_spark = []
    asyncio.run(server._refresh_fx_spark(["p1"]))
    assert server.fx_spark == [7.80, 7.81]

    async def fake_fail(proxy_urls=None):
        raise RuntimeError("yahoo down")

    monkeypatch.setattr(server.pf, "fetch_fx_spark", fake_fail)
    asyncio.run(server._refresh_fx_spark([]))
    assert server.fx_spark == []
    server.fx_spark = []


def test_due_minute_keys_ttl():
    """分时拉取 TTL 节流（纯函数）：到期项入选、未到期跳过、首次(缺 key)视为到期。"""
    keys = ["QQQ", "SPY", "HSI"]
    # 冷启动：minute_due 空 → 全部到期
    assert server.due_minute_keys({}, 1000.0, keys, 180.0) == ["QQQ", "SPY", "HSI"]
    due = {"QQQ": 1000.0, "SPY": 900.0, "HSI": 830.0}
    # now=1000：三项距上次分别 0s/100s/170s，均 <180s → 全部跳过
    assert server.due_minute_keys(due, 1000.0, keys, 180.0) == []
    # now=1010：仅 HSI 距上次 180s 恰好到期
    assert server.due_minute_keys(due, 1010.0, keys, 180.0) == ["HSI"]
    # now=1190：三项距上次 ≥190s → 全部到期
    assert server.due_minute_keys(due, 1190.0, keys, 180.0) == ["QQQ", "SPY", "HSI"]


class TestRootRedirect:
    """root 跳转回归：已登录 / → 302 持仓页（首选入口）；未登录 / → 跳登录页；
    已登录经 middleware 访问受保护路径正常到达目标（无二次重定向）。

    实现事实：middleware 对 path=="/" 且已登录短路 302；未登录放行到 root()，
    root() 的 RedirectResponse 未显式指定状态码（Starlette 默认 307）。
    """

    def test_root_logged_in_redirects_portfolio(self, monkeypatch):
        """已登录 GET / → middleware 短路 302 → /static/portfolio.html。"""
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session("root-user"))
        client.cookies.set("session_token", "tok-root-in")
        try:
            r = client.get("/", follow_redirects=False)
            assert r.status_code == 302
            assert r.headers["location"] == "/static/portfolio.html"
        finally:
            client.cookies.clear()

    def test_root_anonymous_redirects_login(self):
        """未登录 GET / → root() 跳登录页（307 + Location=/static/login.html）。"""
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/static/login.html"

    def test_protected_path_logged_in_reaches_target(self, monkeypatch):
        """已登录访问受保护路径 /static/portfolio.html → 200 直达持仓页内容。"""
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session("root-user"))
        client.cookies.set("session_token", "tok-root-prot")
        try:
            r = client.get("/static/portfolio.html", follow_redirects=False)
            assert r.status_code == 200
            assert "portfolio" in r.text
        finally:
            client.cookies.clear()
