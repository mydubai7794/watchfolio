"""决策上下文聚合 + 双端点注入测试（db 全打桩零连库）。@author yys"""
import asyncio
import json
import time
from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient


def _bars(n, base=100.0):
    """n 根日K，close 递增 1%。"""
    return [{"date": date(2026, 9, 1 + i % 28), "open": base, "high": base,
             "low": base, "close": round(base * (1.01 ** i), 4), "volume": 1}
            for i in range(n)]


def _env(monkeypatch):
    import server
    import daily_bars as dbar

    server._agent_ctx_cache.update(ts=0.0, ctx=None)

    async def fake_bars(sym, limit=None):
        return {"SPY": _bars(30, 500.0), "RSP": _bars(30, 100.0),
                "MU": _bars(30, 900.0), "BABA": _bars(30, 100.0),
                "SMH": _bars(30, 200.0)}.get(sym, [])

    async def fake_holdings(enabled_only=True):
        return [
            {"symbol": "MU", "market": "US", "asset_type": "stock"},
            {"symbol": "BABA", "market": "US", "asset_type": "fpi"},
            {"symbol": "SMH", "market": "US", "asset_type": "etf"},
            {"symbol": "HK0000740297", "market": "HKFUND", "asset_type": None},
        ]

    async def fake_insider(sym, days=90):
        if sym != "MU":
            return []
        return [
            {"filed_date": date(2026, 10, 1), "action": "A", "shares": 141000,
             "value_usd": 19035000.0},
            {"filed_date": date(2026, 9, 20), "action": "D", "shares": 5000,
             "value_usd": 700000.0},
        ]

    async def fake_macro_latest():
        return [
            {"name": "credit_spread_hy", "value": 3.18, "asof": date(2026, 10, 10),
             "extra": "{}"},
            {"name": "cpi_yoy", "value": 2.9, "asof": date(2026, 9, 30),
             "extra": "{}"},
            {"name": "yield_2s10s", "value": 0.15, "asof": date(2026, 10, 10),
             "extra": json.dumps({"dgs10": 4.05, "dgs2": 3.9})},
        ]

    async def fake_cfg(key):
        if key == "EDGAR_LAST_OK":
            return (datetime.now() - timedelta(hours=1)) \
                .strftime("%Y-%m-%d %H:%M:%S")
        return None

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_insider_tx", fake_insider)
    monkeypatch.setattr(server.db, "get_macro_latest", fake_macro_latest)
    monkeypatch.setattr(server.db, "get_config", fake_cfg)
    monkeypatch.setattr(server, "market_cache",
                        [{"key": "US10Y", "price": 4.04}])
    monkeypatch.setitem(dbar.INDICATORS, "MU",
                        {"ma50": 1.0, "ma200": 1.0, "rsi14": 62.3,
                         "stop_level": None, "hhv22": 2.0, "atr22": 0.1,
                         "enough_history": True, "asof": "2026-10-10"})
    return server


def test_build_agent_context(monkeypatch):
    server = _env(monkeypatch)
    ctx = asyncio.run(server._build_agent_context())
    # SPY 块
    assert ctx["spy"]["close"] == round(500.0 * 1.01 ** 29, 4)
    assert ctx["spy"]["chg_pct_1d"] == 1.0
    # 广度：RSP/SPY 首日对齐比值
    assert ctx["breadth"]["rsp_spy_ratio"] == round(100.0 / 500.0, 6)
    assert ctx["breadth"]["chg_pct_5d"] is not None
    # US10Y 双源
    assert ctx["us10y"]["price"] == 4.04
    assert ctx["us10y"]["dgs10_fred"] == 4.05
    assert ctx["credit_spread_hy"] == {"value": 3.18, "asof": "2026-10-10"}
    assert ctx["cpi_yoy"] == {"value": 2.9, "asof": "2026-09-30"}
    # factors：个股三分类
    f_mu = ctx["factors"]["MU"]
    assert f_mu["rsi14"] == 62.3 and f_mu["asset_type"] == "stock"
    assert f_mu["spy_rel_20d_pct"] == 0.0          # 同涨幅 → 相对差 0
    assert f_mu["insider_90d"]["net_shares"] == 136000
    assert f_mu["insider_90d"]["net_value_usd"] == 18335000.0
    assert ctx["factors"]["BABA"]["insider_note"] == "fpi_no_form4"
    assert ctx["factors"]["SMH"]["insider_90d"] is None
    assert "HK0000740297" not in ctx["factors"]    # 港股基金不进 factors
    assert ctx["meta"]["edgar_ok"] is True


def test_build_agent_context_cache_60s(monkeypatch):
    server = _env(monkeypatch)
    calls = []

    async def fake_bars(sym, limit=None):
        calls.append(sym)
        return []

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    asyncio.run(server._build_agent_context())
    n1 = len(calls)
    asyncio.run(server._build_agent_context())     # 60s 内第二次 → 缓存命中
    assert len(calls) == n1


def test_agent_portfolio_endpoint_injects_context(monkeypatch):
    server = _env(monkeypatch)

    async def fake_ctx():
        return {"spy": {"close": 600.0}}

    monkeypatch.setattr(server, "_build_agent_context", fake_ctx)

    async def fake_cfg(key):
        return "test-key" if key == "AGENT_API_KEY" else None

    monkeypatch.setattr(server.db, "get_config", fake_cfg)
    client = TestClient(server.app)
    r = client.get("/api/agent/portfolio", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json()["context"] == {"spy": {"close": 600.0}}


def test_overview_endpoint_injects_context(monkeypatch):
    server = _env(monkeypatch)

    async def fake_ctx():
        return {"spy": {"close": 600.0}}

    monkeypatch.setattr(server, "_build_agent_context", fake_ctx)
    monkeypatch.setattr(server.db, "get_recent_trades", _async_ret([]))
    monkeypatch.setattr(server, "_get_closed_ctx", _async_ret(([], [])))

    async def fake_session(t):
        return {"username": "u", "expires": time.time() + 3600}

    monkeypatch.setattr(server.db, "get_session", fake_session)
    client = TestClient(server.app)
    client.cookies.set("session_token", "tok")
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    assert r.json()["context"] == {"spy": {"close": 600.0}}


def _async_ret(v):
    async def _f(*a, **kw):
        return v
    return _f
