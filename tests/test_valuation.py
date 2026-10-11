"""估值因子层测试：纯函数 + DB CRUD + 轮编排（全部打桩零网络零 DB 连接）。@author yys"""
import asyncio
import json
import pytest
from types import SimpleNamespace

import database as db
import longport_fetcher
import edgar_fetcher


# ---------------------------------------------------------------------------
# DB CRUD（fake pool 模式参照 tests/test_edgar_db.py）
# ---------------------------------------------------------------------------

def test_upsert_valuation_static_and_get(monkeypatch):
    """日更字段 upsert 不触碰 growth 列；周更 upsert 仅更新 growth。"""
    calls = []

    async def fake_exec(sql, params=None):
        calls.append((sql, params))

    async def fake_dict(sql, params=None):
        return [{"symbol": "NVDA", "eps_ttm": 8.0, "bps": 9.5,
                 "growth_pct": 66.67, "dividend_yield": 0.52,
                 "updated_at": "2026-10-11 16:00:00"}]

    monkeypatch.setattr(db, "execute", fake_exec)
    monkeypatch.setattr(db, "execute_dict", fake_dict)
    asyncio.run(db.upsert_holding_valuation_static(
        "NVDA", 8.0, 9.5, 0.52))
    asyncio.run(db.upsert_holding_valuation_growth("NVDA", 66.67))
    # static upsert 只更新 eps/bps/dy 三列
    assert "eps_ttm=VALUES(eps_ttm)" in calls[0][0]
    assert "growth_pct" not in calls[0][0].split("ON DUPLICATE")[1]
    # growth upsert 只更新 growth 列
    assert "growth_pct=VALUES(growth_pct)" in calls[1][0]
    assert "eps_ttm" not in calls[1][0].split("ON DUPLICATE")[1]
    rows = asyncio.run(db.get_valuations())
    assert rows["NVDA"]["eps_ttm"] == 8.0
    assert rows["NVDA"]["growth_pct"] == 66.67
