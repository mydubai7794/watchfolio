"""server 鉴权与持仓 API 离线测试。

不触发 lifespan（不连 DB、不起后台任务）、不发真实外网请求：
- TestClient 不用 with 上下文，避免 lifespan 执行
- 涉及 DB 的分支用 monkeypatch 桩掉
"""
import time
from datetime import datetime

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
    server._sessions.clear()


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


def test_market_validation():
    """持仓新增 market 非法值 → 400，不入库。"""
    token = "test-token-market"
    server._sessions[token] = {"username": "tester", "expires": time.time() + 3600}
    client.cookies.set("session_token", token)
    try:
        r = client.post("/api/portfolio/holding", json={"symbol": "AAPL", "market": "xxx"})
        assert r.status_code == 400
        assert "US 或 HKFUND" in r.json()["message"]
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
