"""sys_config 配置 API 离线测试。

参照 test_server_auth.py 模式：TestClient 不用 with 上下文（不触发 lifespan，
不连 DB、不起后台任务），DB 层用 monkeypatch 桩掉。

覆盖：
- GET /api/config 敏感键掩码 + masked 标记
- GET 普通键明文返回
- PUT 提交掩码形态/空串 → 跳过 set_config（留空=不修改语义）
- PUT 正常值 → set_config 收到原值
- 掩码纯函数长短值两形态 + 敏感键判定
"""
import pytest
from fastapi.testclient import TestClient

import server

# 不使用 with：构造即用，避免 lifespan 连接数据库/启动后台循环
client = TestClient(server.app)


def _login_as(monkeypatch, token: str):
    """测试辅助：mock DB 会话查询返回有效会话，并设置 cookie。"""

    async def fake_get_session(t):
        return {"username": "tester", "expires": __import__("time").time() + 3600}

    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    client.cookies.set("session_token", token)


def test_get_masks_sensitive_keys(monkeypatch):
    """GET /api/config：敏感键值掩码返回且 masked=true。"""

    async def fake_get_configs():
        return [
            {"key": "LONGPORT_ACCESS_TOKEN", "value": "abcdef1234567890",
             "remark": "长桥访问令牌", "updated_at": "2026-10-09 12:00:00"},
            {"key": "MAIL_AUTH_CODE", "value": "short", "remark": "邮箱授权码"},
        ]

    monkeypatch.setattr(server.db, "get_configs", fake_get_configs)
    _login_as(monkeypatch, "tok-cfg-mask")
    try:
        r = client.get("/api/config")
        assert r.status_code == 200
        rows = {row["key"]: row for row in r.json()}
        # 长敏感值：前2 + **** + 后2
        assert rows["LONGPORT_ACCESS_TOKEN"]["value"] == "ab****90"
        assert rows["LONGPORT_ACCESS_TOKEN"]["masked"] is True
        # 短敏感值：全掩码
        assert rows["MAIL_AUTH_CODE"]["value"] == "****"
        assert rows["MAIL_AUTH_CODE"]["masked"] is True
    finally:
        client.cookies.clear()


def test_get_plain_keys_plaintext(monkeypatch):
    """GET /api/config：普通键明文返回且 masked=false。"""

    async def fake_get_configs():
        return [
            {"key": "INVESTED_CNY", "value": "100000", "remark": "总投入"},
            {"key": "NAV_BASE_DATE", "value": "2026-10-09", "remark": None},
        ]

    monkeypatch.setattr(server.db, "get_configs", fake_get_configs)
    _login_as(monkeypatch, "tok-cfg-plain")
    try:
        r = client.get("/api/config")
        assert r.status_code == 200
        rows = {row["key"]: row for row in r.json()}
        assert rows["INVESTED_CNY"]["value"] == "100000"
        assert rows["INVESTED_CNY"]["masked"] is False
        assert rows["NAV_BASE_DATE"]["value"] == "2026-10-09"
        assert rows["NAV_BASE_DATE"]["masked"] is False
    finally:
        client.cookies.clear()


def test_put_masked_value_skips_set(monkeypatch):
    """PUT 提交值等于当前掩码形态 → 不调用 set_config（未改动不落库）。"""
    calls = []

    async def fake_get_config(key):
        return "abcdef1234567890"  # 库内真值

    async def fake_set_config(key, value, remark=""):
        calls.append((key, value))

    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    monkeypatch.setattr(server.db, "set_config", fake_set_config)
    _login_as(monkeypatch, "tok-cfg-skip")
    try:
        r = client.put("/api/config/LONGPORT_ACCESS_TOKEN",
                       json={"value": "ab****90"})
        assert r.status_code == 200
        assert r.json()["ok"] is True
        assert r.json()["skipped"] is True
        assert calls == []
    finally:
        client.cookies.clear()


def test_put_empty_value_skips_set(monkeypatch):
    """PUT 提交空串 → 留空=不修改，不查库不落库。"""
    calls = []

    async def fake_get_config(key):
        raise AssertionError("空串跳过不应回查当前值")

    async def fake_set_config(key, value, remark=""):
        calls.append((key, value))

    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    monkeypatch.setattr(server.db, "set_config", fake_set_config)
    _login_as(monkeypatch, "tok-cfg-empty")
    try:
        r = client.put("/api/config/INVESTED_CNY", json={"value": ""})
        assert r.status_code == 200
        assert r.json()["skipped"] is True
        assert calls == []
    finally:
        client.cookies.clear()


def test_put_normal_value_calls_set(monkeypatch):
    """PUT 提交正常值 → set_config 收到 (key, value) 原样落库。"""
    calls = []

    async def fake_get_config(key):
        raise AssertionError("非敏感键不应回查当前值")

    async def fake_set_config(key, value, remark=""):
        calls.append((key, value))
        return 1

    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    monkeypatch.setattr(server.db, "set_config", fake_set_config)
    _login_as(monkeypatch, "tok-cfg-set")
    try:
        r = client.put("/api/config/INVESTED_CNY", json={"value": "200000"})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        assert calls == [("INVESTED_CNY", "200000")]
    finally:
        client.cookies.clear()


def test_put_sensitive_new_value_calls_set(monkeypatch):
    """PUT 敏感键提交非掩码新值 → 正常落库（轮换密钥场景）。"""
    calls = []

    async def fake_get_config(key):
        return "old-secret-value"

    async def fake_set_config(key, value, remark=""):
        calls.append((key, value))
        return 1

    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    monkeypatch.setattr(server.db, "set_config", fake_set_config)
    _login_as(monkeypatch, "tok-cfg-rotate")
    try:
        r = client.put("/api/config/AGENT_API_KEY", json={"value": "brand-new-key"})
        assert r.status_code == 200
        assert r.json() == {"ok": True}
        assert calls == [("AGENT_API_KEY", "brand-new-key")]
    finally:
        client.cookies.clear()


class TestMaskHelpers:
    """掩码纯函数：长短值两形态 + 敏感键片段判定。"""

    def test_mask_long_value(self):
        assert server.mask_secret("abcdef1234567890") == "ab****90"
        assert server.mask_secret("abcdefghi") == "ab****hi"  # 恰好 9 字符

    def test_mask_short_value(self):
        assert server.mask_secret("short") == "****"
        assert server.mask_secret("12345678") == "****"  # 恰好 8 字符不长于 8
        assert server.mask_secret("9-char!") == "****"   # 7 字符

    def test_mask_empty_and_none(self):
        assert server.mask_secret("") == "****"
        assert server.mask_secret(None) == "****"

    def test_is_sensitive_key(self):
        assert server.is_sensitive_key("LONGPORT_APP_KEY") is True
        assert server.is_sensitive_key("LONGPORT_APP_SECRET") is True
        assert server.is_sensitive_key("LONGPORT_ACCESS_TOKEN") is True
        assert server.is_sensitive_key("MAIL_AUTH_CODE") is True
        assert server.is_sensitive_key("DB_PASSWORD") is True
        assert server.is_sensitive_key("INVESTED_CNY") is False
        assert server.is_sensitive_key("NAV_BASE_DATE") is False
        assert server.is_sensitive_key("") is False

    def test_put_unauthenticated_rejected(self):
        """未登录访问配置 API → 401（middleware 全局拦截回归）。"""
        r = client.get("/api/config")
        assert r.status_code == 401
