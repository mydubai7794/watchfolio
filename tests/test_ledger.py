"""ledger 模块测试：CRUD SQL 形状 + 报表纯计算 + ECB 解析 + API。

依赖 monkeypatch 桩掉 database 层（同 test_email_trader.py 模式），不起服务不连 MySQL。
"""
import asyncio
from datetime import datetime

import database as db


def run(coro):
    return asyncio.run(coro)


class TestLedgerCrud:
    def test_upsert_ledger_event_sql(self, monkeypatch):
        captured = {}

        async def fake_execute(sql, args=None, _pool=None):
            captured["sql"], captured["args"] = sql, args
            return 1

        monkeypatch.setattr(db, "execute", fake_execute)
        run(db.upsert_ledger_event(
            event_date="2026-06-26", event_type="inflow", account="za_bank",
            channel="建行购汇汇出", direction="+", amount_ccy="CNY", amount=10000.0,
            recv_ccy="HKD", recv_amount=11503.90, usd_equivalent=1466.09,
            cny_amount=10000.0, mid_rate=None, note="", ref_no="V3-I01"))
        sql = captured["sql"]
        assert "INSERT INTO ledger_events" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql  # ref_no 幂等 upsert
        assert captured["args"][-1] == "V3-I01"  # ref_no 为最后一个绑定值

    def test_get_ledger_events_filters(self, monkeypatch):
        captured = {}

        async def fake_execute_dict(sql, args=None, _pool=None):
            captured["sql"], captured["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        run(db.get_ledger_events(event_type="fx", year=2026, account="hsbc"))
        assert "WHERE" in captured["sql"] and "event_type" in captured["sql"]
        assert 2026 in captured["args"] and "hsbc" in captured["args"]
        run(db.get_ledger_events())
        assert "WHERE" not in captured["sql"]  # 无筛选不过滤

    def test_upsert_ledger_fx_rate_and_latest(self, monkeypatch):
        cap = {}

        async def fake_execute(sql, args=None, _pool=None):
            cap["up"] = (sql, args)
            return 1

        async def fake_execute_dict(sql, args=None, _pool=None):
            cap["q"] = (sql, args)
            return [{"rate_date": "2026-06-18", "usd_cny": 6.7266, "usd_hkd": 7.8083}]

        monkeypatch.setattr(db, "execute", fake_execute)
        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        run(db.upsert_ledger_fx_rate("2026-06-18", 6.7266, 7.8083))
        assert "INSERT INTO ledger_fx_rates" in cap["up"][0]
        assert "ON DUPLICATE KEY UPDATE" in cap["up"][0]
        row = run(db.get_ledger_fx_for_date("2026-06-19"))
        assert "rate_date <= %s" in cap["q"][0]  # 缺当日时回退最近一条
        assert row["usd_cny"] == 6.7266

    def test_get_latest_portfolio_value(self, monkeypatch):
        async def fake_execute_dict(sql, args=None, _pool=None):
            assert "total_value_usd" in sql and "ORDER BY ts DESC LIMIT 1" in sql
            return [{"total_value_usd": 13651.98}]

        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        assert run(db.get_latest_portfolio_value()) == 13651.98
