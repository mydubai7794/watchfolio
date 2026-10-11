"""决策上下文聚合 + 双端点注入测试（db 全打桩零连库）。@author yys"""
import asyncio
import json
import time
from datetime import date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient


def _bars(n, base=100.0):
    """n 根日K（日期连续），close 递增 1%。"""
    return [{"date": date(2026, 9, 1) + timedelta(days=i), "open": base,
             "high": base, "low": base, "close": round(base * (1.01 ** i), 4),
             "volume": 1}
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
            {"filed_date": date.today() - timedelta(days=10), "action": "A",
             "tx_code": "P", "shares": 141000, "value_usd": 19035000.0},
            {"filed_date": date.today() - timedelta(days=21), "action": "D",
             "tx_code": "S", "shares": 5000, "value_usd": 700000.0},
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

    async def fake_vals():
        return {}          # 估值因子默认空（零连库；专用用例另行打桩）

    monkeypatch.setattr(server.db, "get_valuations", fake_vals)
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
    assert ctx["breadth"]["chg_pct_5d"] == 0.0        # 比值恒定 → 零变化
    assert ctx["breadth"]["chg_pct_20d"] == 0.0
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


def test_agent_context_valuation_injection(monkeypatch):
    """factors[symbol].valuation：pe/pb/peg 用现价实时算、reason 推导、
    ETF fund 门控、null 安全。@author yys"""
    server = _env(monkeypatch)

    async def fake_holdings(enabled_only=True):
        return [
            {"symbol": "NVDA", "market": "US", "asset_type": "stock"},
            {"symbol": "QQQ", "market": "US", "asset_type": "etf"},
            {"symbol": "BABA", "market": "US", "asset_type": "fpi"},
            {"symbol": "DRAM", "market": "US", "asset_type": None},
            {"symbol": "GOOGL", "market": "US", "asset_type": "stock"},
        ]

    async def fake_vals():
        return {
            "NVDA": {"eps_ttm": 8.0, "bps": 9.5, "growth_pct": 66.67,
                     "dividend_yield": 0.52,
                     "updated_at": "2026-10-11 16:00:00"},
            "QQQ": {"eps_ttm": None, "bps": None, "growth_pct": None,
                    "dividend_yield": None,
                    "updated_at": "2026-10-11 16:00:00"},
            "BABA": {"eps_ttm": 4.39, "bps": 64.37, "growth_pct": -17.91,
                     "dividend_yield": 1.03,
                     "updated_at": "2026-10-11 16:00:00"},
            # 长桥完全无数据标的：0 落库形态 → no_data（批2审查 Important-1）
            "DRAM": {"eps_ttm": 0.0, "bps": 0.0, "growth_pct": None,
                     "dividend_yield": 0.0,
                     "updated_at": "2026-10-11 16:00:00"},
            # eps/bps 有效但现价缺失：估值倍数 null、reason 不变（注入瞬态契约）
            "GOOGL": {"eps_ttm": 6.0, "bps": 20.0, "growth_pct": 10.0,
                      "dividend_yield": 0.0,
                      "updated_at": "2026-10-11 16:00:00"},
        }

    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_valuations", fake_vals)
    # 现价快照整体替换（参照 test_alert_api._setup_pf_ctx），teardown 自动还原
    monkeypatch.setattr(server, "_pf_ctx",
                        {"holdings": [], "quotes": {
                            "NVDA": {"price": 160.0},
                            "BABA": {"price": 140.0}},
                         "cash_usd": 0.0})
    ctx = asyncio.run(server._build_agent_context())
    # NVDA：pe=160/8=20、pb=160/9.5≈16.84、peg=20/66.67≈0.30（PEG=P/E÷增速百分数）
    v_nvda = ctx["factors"]["NVDA"]["valuation"]
    assert v_nvda["pe_ttm"] == pytest.approx(20.0, abs=0.01)
    assert v_nvda["pb"] == pytest.approx(16.84, abs=0.01)
    assert v_nvda["peg"] == pytest.approx(0.30, abs=0.01)
    assert v_nvda["eps_ttm"] == 8.0
    assert v_nvda["bps"] == pytest.approx(9.5, abs=0.01)
    assert v_nvda["growth_pct"] == pytest.approx(66.67, abs=0.01)
    # 股息率：落库为每股TTM股息USD（0.52=$0.52/股），注入÷现价×100 换算
    assert v_nvda["dividend_yield"] == pytest.approx(0.325, abs=0.01)
    assert v_nvda["reason"] is None
    assert v_nvda["updated_at"] == "2026-10-11 16:00:00"
    # QQQ：ETF fund 门控（数值/现价全缺 → pe/pb/peg 全 None）
    v_qqq = ctx["factors"]["QQQ"]["valuation"]
    assert v_qqq["pe_ttm"] is None
    assert v_qqq["pb"] is None
    assert v_qqq["peg"] is None
    assert v_qqq["reason"] == "fund"
    # BABA：growth≤0 → peg None、reason=no_growth；现价实时算 pe/pb
    v_baba = ctx["factors"]["BABA"]["valuation"]
    assert v_baba["pe_ttm"] == pytest.approx(31.89, abs=0.01)   # 140/4.39
    assert v_baba["pb"] == pytest.approx(2.17, abs=0.01)        # 140/64.37
    assert v_baba["peg"] is None
    assert v_baba["reason"] == "no_growth"
    # 股息率换算：1.03 USD/股 ÷ 140 现价 = 0.74%
    assert v_baba["dividend_yield"] == pytest.approx(0.74, abs=0.01)
    # fpi 分支 insider 注记不受 valuation 块影响
    assert ctx["factors"]["BABA"]["insider_note"] == "fpi_no_form4"
    # DRAM：长桥 0 落库（非 None）→ no_data 而非 no_growth（Important-1 锚）
    v_dram = ctx["factors"]["DRAM"]["valuation"]
    assert v_dram["pe_ttm"] is None and v_dram["pb"] is None
    assert v_dram["reason"] == "no_data"
    # GOOGL：eps/bps/growth 有效但无现价 → 倍数全 null、reason=None、键恒在
    v_g = ctx["factors"]["GOOGL"]["valuation"]
    assert v_g["pe_ttm"] is None and v_g["pb"] is None and v_g["peg"] is None
    assert v_g["eps_ttm"] == 6.0 and v_g["growth_pct"] == 10.0
    assert v_g["reason"] is None
    # dps=0 无股息 + 现价缺失 → 股息率 null
    assert v_g["dividend_yield"] is None


def _async_ret(v):
    async def _f(*a, **kw):
        return v
    return _f
