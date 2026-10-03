"""ledger 模块测试：CRUD SQL 形状 + 报表纯计算 + ECB 解析 + API。

依赖 monkeypatch 桩掉 database 层（同 test_email_trader.py 模式），不起服务不连 MySQL。
"""
import asyncio
from datetime import datetime

import database as db
import ledger
from ledger_seed import SEED_EVENTS


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


class TestBuildReport:
    def _report(self, **kw):
        ctx = dict(total_assets_usd=13651.98, usdcnh=6.7061, cash_accounts=[])
        ctx.update(kw)
        return ledger.build_report(SEED_EVENTS, **ctx)

    def test_v3_baselines(self):
        r = self._report()
        s = r["summary"]
        assert s["total_cny_invested"] == 84850.0
        assert abs(s["usd_in"] - 12478.27) < 0.05
        assert abs(s["avg_cost"] - 6.7998) < 0.0005          # 勘误后: 84850/12478.27
        assert abs(s["fx_loss_usd"] - 43.32) < 0.15           # v3: 43.32(购汇30.19+账户内13.13)
        assert abs(s["fee_usd"] - 13.14) < 0.05
        assert abs(s["reward_usd"] - 163.01) < 0.5            # v3估算161,种子实算163.01
        assert abs(s["realized_usd"] - (-36.68)) < 0.05
        assert abs(s["expense_usd"] - 36.73) < 0.05
        assert abs(s["external_transfer_usd"] - 2.26) < 0.05
        assert s["pending_count"] == 0

    def test_v3_identity(self):
        r = self._report()
        i = r["identity"]
        assert abs(i["gain_usd"] - 1210.44) < 0.5             # 增值与投入口径无关
        assert abs(i["gain_pct"] - 9.73) < 0.05
        assert abs(i["fx_loss_cny"] - (-1169.5)) < 1.5        # 勘误后(v3的-589基于错误avg)
        assert abs(i["expense_cny"] - 246.4) < 0.5
        assert abs(i["net_cny"] - 6701.54) < 0.5              # 勘误后主口径精确
        assert abs(i["net_pct"] - 7.90) < 0.05
        assert abs(i["identity_check_cny"] - i["net_cny"]) < 1.0  # 分解式闭环±1

    def test_no_usdcnh_degrades(self):
        r = self._report(usdcnh=None)
        assert r["identity"]["usdcnh"] is None
        assert r["identity"]["net_cny"] is None               # 缺汇率时CNY恒等式置空不报错

    def test_channel_ranking(self):
        ch = {c["channel"]: c for c in self._report()["channels"]}
        assert abs(ch["汇丰柜台"]["wavg_spread_pct"] - 0.428) < 0.02   # v3: 0.41~0.43%最贵
        assert abs(ch["众安App换U"]["wavg_spread_pct"] - 0.117) < 0.01  # v3: 0.117%最优
        assert abs(ch["同事代换"]["wavg_spread_pct"] - (-1.80)) < 0.05  # 收益
        ranked = [c["channel"] for c in self._report()["channels"]]
        assert ranked[0] == "汇丰柜台"                          # 最贵排第一

    def test_balance_reconciliation(self):
        cash = [{"currency": "HKD", "amount": 52.72}, {"currency": "USD", "amount": 371.79}]
        r = self._report(cash_accounts=cash)
        b = r["balances"]
        assert abs(b["ledger"]["HKD"] - 52.72) < 0.01          # 众安32.52+汇丰20.20(各自最新快照合计)
        assert abs(b["ledger"]["USD"] - 371.79) < 0.01         # 汇丰10-02最新+致富9-30最新
        assert b["system"]["HKD"] == 52.72
        assert abs(b["diff"]["HKD"]) < 0.01 and abs(b["diff"]["USD"]) < 0.01

    def test_fx_row_derivation(self):
        rows = {row["ref_no"]: row for row in self._report()["fx_detail"]}
        f01 = rows["V3-F01"]
        assert abs(f01["theoretical"] - 11535.55) < 1          # 10000×1.153555
        assert abs(f01["spread_pct"] - 0.274) < 0.02           # v3 §三
        assert abs(f01["loss_usd"] - 4.04) < 0.05
        f11 = rows["V3-F11"]
        assert f11["loss_usd"] < 0                              # 同事代换=收益-2.25


class TestEcb:
    def test_parse_ecb_payload(self):
        data = {"rates": {
            "2026-06-18": {"CNY": 6.7266, "HKD": 7.8083},
            "2026-06-20": {"CNY": 6.72, "HKD": 7.80},          # 周末缺19日,ECB自然跳过
        }}
        rows = ledger.parse_ecb_payload(data)
        assert rows == [("2026-06-18", 6.7266, 7.8083),
                        ("2026-06-20", 6.72, 7.80)]

    def test_parse_ecb_bad_payload(self):
        assert ledger.parse_ecb_payload({}) == []
        assert ledger.parse_ecb_payload({"rates": {"d1": {}}}) == []
