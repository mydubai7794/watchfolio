"""ledger 模块测试：CRUD SQL 形状 + 报表纯计算 + ECB 解析 + API。

依赖 monkeypatch 桩掉 database 层（同 test_email_trader.py 模式），不起服务不连 MySQL。
"""
import asyncio
from datetime import datetime

import pytest

import database as db
import ledger

# ledger_seed.py 含真实记账流水，已 gitignore 不入库（隐私出库）；
# 缺失时（如公开仓库 clone 环境）跳过本文件全部实算测试。
pytest.importorskip(
    "ledger_seed",
    reason="本地 ledger_seed.py 未提供（真实流水不入库），跳过实算测试",
)
from ledger_seed import SEED_EVENTS


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 期望值独立重算辅助：从 SEED_EVENTS 自带字段独立累加，与 build_report
# 内部实现各自书写（期望不绑死任何具体种子数值，对任意种子数据成立）。
# 口径常数：HKD→USD 静态折算 7.846（系统口径契约，字面量保持独立性）。
# ---------------------------------------------------------------------------

HKD2USD = 7.846


def _seed_usd_sum(etype):
    """按事件类型独立累加 usd_equivalent。"""
    return sum(float(e.get("usd_equivalent") or 0)
               for e in SEED_EVENTS if e["event_type"] == etype)


def _seed_inflow_totals():
    """入金事件独立累加 → (总投入CNY, 美元入金USD)。"""
    inflows = [e for e in SEED_EVENTS if e["event_type"] == "inflow"]
    return (sum(float(e["cny_amount"] or 0) for e in inflows),
            sum(float(e.get("usd_equivalent") or 0) for e in inflows))


def _seed_fx_rows():
    """fx 事件逐笔独立重算 → {ref_no: (理论应得, 点差%, 损失USD)}。

    点差 = 理论应得(mid×付出金额) − 实际到账（正=少得）；
    收币为 HKD 时损失按 7.846 折 USD。
    """
    rows = {}
    for e in SEED_EVENTS:
        if e["event_type"] != "fx" or not e.get("mid_rate"):
            continue
        theo = float(e["amount"]) * float(e["mid_rate"])
        actual = e.get("recv_amount")
        if actual is None or theo <= 0:
            continue
        diff = theo - float(actual)
        loss = diff / HKD2USD if (e.get("recv_ccy") or "") == "HKD" else diff
        rows[e["ref_no"]] = (theo, diff / theo * 100, loss)
    return rows


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
        """summary 各分项 == 从 SEED_EVENTS 独立累加的期望值（不绑死种子数值）。"""
        r = self._report()
        s = r["summary"]
        gross_cny, usd_in = _seed_inflow_totals()
        fx_rows = _seed_fx_rows()
        assert s["total_cny_invested"] == pytest.approx(gross_cny, abs=0.01)
        assert s["usd_in"] == pytest.approx(usd_in, abs=0.01)
        assert s["avg_cost"] == pytest.approx(gross_cny / usd_in, abs=0.0005)  # 总投入/美元入金
        assert s["fx_loss_usd"] == pytest.approx(
            sum(v[2] for v in fx_rows.values()), abs=0.01)
        assert s["fee_usd"] == pytest.approx(_seed_usd_sum("fee"), abs=0.01)
        assert s["reward_usd"] == pytest.approx(_seed_usd_sum("reward"), abs=0.01)
        assert s["realized_usd"] == pytest.approx(_seed_usd_sum("realized"), abs=0.01)
        assert s["expense_usd"] == pytest.approx(_seed_usd_sum("expense"), abs=0.01)
        expected_ext = sum(float(e["usd_equivalent"]) for e in SEED_EVENTS
                           if e["event_type"] == "transfer"
                           and e.get("account") == "external"
                           and e.get("usd_equivalent") is not None)
        assert s["external_transfer_usd"] == pytest.approx(expected_ext, abs=0.01)
        pending = sum(1 for e in SEED_EVENTS
                      if e["event_type"] == "inflow" and e.get("cny_amount") is None)
        assert s["pending_count"] == pending

    def test_v3_identity(self):
        """恒等式分项 == 从 SEED_EVENTS+报表上下文独立重算（净投入口径）。"""
        r = self._report()
        i = r["identity"]
        total_assets, usdcnh = 13651.98, 6.7061
        gross_cny, usd_in = _seed_inflow_totals()
        expense_usd = _seed_usd_sum("expense")
        # 独立重算：净投入 = 总投入 − 消耗折CNY；net = 总资产×USDCNH − 净投入
        expense_cny = expense_usd * usdcnh
        invest_net = gross_cny - expense_cny
        gain_usd = total_assets + expense_usd - usd_in
        fx_loss_cny = usd_in * (usdcnh - gross_cny / usd_in)
        net_cny = total_assets * usdcnh - invest_net
        assert i["gain_usd"] == pytest.approx(gain_usd, abs=0.01)
        assert i["gain_pct"] == pytest.approx(
            gain_usd / (usd_in - expense_usd) * 100, abs=0.01)
        assert i["fx_loss_cny"] == pytest.approx(fx_loss_cny, abs=0.5)
        assert i["expense_cny"] == pytest.approx(expense_cny, abs=0.01)
        assert i["invest_net_cny"] == pytest.approx(invest_net, abs=0.01)
        assert i["net_cny"] == pytest.approx(net_cny, abs=0.01)
        assert i["net_pct"] == pytest.approx(net_cny / invest_net * 100, abs=0.01)
        assert i["gain_cny"] == pytest.approx(gain_usd * usdcnh, abs=0.01)
        # 结构断言（恒等式闭环，保留）：净 = 增值 + 汇损 闭环 ±1
        assert abs(i["identity_check_cny"] - i["net_cny"]) < 1.0

    def test_no_usdcnh_degrades(self):
        r = self._report(usdcnh=None)
        assert r["identity"]["usdcnh"] is None
        assert r["identity"]["net_cny"] is None               # 缺汇率时CNY恒等式置空不报错

    def test_channel_ranking(self):
        """通道加权点差 == 按 fx 事件自带字段独立重算；报表按点差降序排列。"""
        report = self._report()
        ch = {c["channel"]: c for c in report["channels"]}
        expect: dict[str, dict] = {}
        for ref, (theo, _pct, loss) in _seed_fx_rows().items():
            ev = next(e for e in SEED_EVENTS if e["ref_no"] == ref)
            theo_usd = theo / HKD2USD if (ev.get("recv_ccy") or "") == "HKD" else theo
            c = expect.setdefault(ev.get("channel") or "", {"wsum": 0.0, "loss": 0.0})
            c["wsum"] += theo_usd
            c["loss"] += loss
        assert set(ch) == set(expect)             # 有可推导换汇的通道全部入报表
        for name, agg in expect.items():
            assert ch[name]["wavg_spread_pct"] == pytest.approx(
                agg["loss"] / agg["wsum"] * 100, abs=0.01)
        spreads = [c["wavg_spread_pct"] for c in report["channels"]]
        assert spreads == sorted(spreads, reverse=True)       # 最贵排第一

    def test_balance_reconciliation(self):
        """对账：ledger 侧=每「账户×币种」最新 balance 快照独立合计；system 侧回显传入。"""
        latest: dict[tuple, dict] = {}
        for e in SEED_EVENTS:
            if e["event_type"] == "balance":
                key = (e.get("account"), e["amount_ccy"])
                if key not in latest or e["event_date"] >= latest[key]["event_date"]:
                    latest[key] = e
        expect: dict[str, float] = {}
        for (_acct, ccy), e in latest.items():
            expect[ccy] = round(expect.get(ccy, 0.0) + float(e["amount"]), 2)
        # system 侧传入值取独立合计（对账闭环）；无 balance 事件的种子给演示值
        cash = ([{"currency": c, "amount": v} for c, v in expect.items()]
                or [{"currency": "USD", "amount": 100.0}])
        r = self._report(cash_accounts=cash)
        b = r["balances"]
        for ccy, val in expect.items():
            assert b["ledger"][ccy] == pytest.approx(val, abs=0.01)
            assert b["system"][ccy] == val          # system 回显传入值
            assert abs(b["diff"][ccy]) < 0.01       # 对账闭环：差异为 0

    def test_fx_row_derivation(self):
        """fx 明细行 == 逐笔按事件自带 mid_rate+双边金额独立重算。"""
        rows = {row["ref_no"]: row for row in self._report()["fx_detail"]}
        expect = _seed_fx_rows()
        assert set(rows) == set(expect)           # 可推导的 fx 事件全部入明细
        for ref, (theo, spread_pct, loss) in expect.items():
            row = rows[ref]
            assert row["theoretical"] == pytest.approx(theo, abs=0.01)
            assert row["spread_pct"] == pytest.approx(spread_pct, abs=0.01)
            assert row["loss_usd"] == pytest.approx(loss, abs=0.01)


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


class TestLedgerFxLoop:
    def test_next_run_before_2235(self):
        import server
        assert server._next_ledger_fx_run(datetime(2026, 10, 3, 10, 0)) == \
               datetime(2026, 10, 3, 22, 35)

    def test_next_run_after_2235(self):
        import server
        assert server._next_ledger_fx_run(datetime(2026, 10, 3, 23, 0)) == \
               datetime(2026, 10, 4, 22, 35)

    def test_sync_once_backfills(self, monkeypatch):
        """循环体：库空 → 从 ledger 起点一次区间拉取 → 逐条 upsert。"""
        import server
        calls = {"fetch": 0, "upsert": 0}

        async def fake_fetch(start, end, proxy_urls=None):
            calls["fetch"] += 1
            assert start == server.ledger.LEDGER_FX_START  # 空库从起点回填
            return [("2026-10-02", 6.7044, 7.8463)]

        async def fake_upsert(d, cny, hkd):
            calls["upsert"] += 1
            assert (d, cny, hkd) == ("2026-10-02", 6.7044, 7.8463)

        async def fake_for_date(d):
            return None

        async def fake_proxies():
            return []

        monkeypatch.setattr(server.ledger, "fetch_ecb_rates", fake_fetch)
        monkeypatch.setattr(server.db, "upsert_ledger_fx_rate", fake_upsert)
        monkeypatch.setattr(server.db, "get_ledger_fx_for_date", fake_for_date)
        monkeypatch.setattr(server.db, "get_alive_proxies", fake_proxies)
        run(server._ledger_fx_sync_once())
        assert calls == {"fetch": 1, "upsert": 1}


# ---------------------------------------------------------------------------
# API 端点（TestClient 不用 with 上下文避免 lifespan，同 test_server_auth 模式）
# ---------------------------------------------------------------------------
import time

from fastapi.testclient import TestClient

import server

client = TestClient(server.app)


def _fake_valid_session(token: str = "tester"):
    async def fake_get_session(t):
        return {"username": token, "expires": time.time() + 3600}
    return fake_get_session


class TestLedgerApi:
    def test_post_events_upsert(self, monkeypatch):
        upserted = []

        async def fake_upsert(**ev):
            upserted.append(ev)
            return 1

        async def fake_sync():
            return {"before": 0.0, "after": 0.0}

        monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)
        monkeypatch.setattr(server.db, "sync_ledger_cny_invested", fake_sync)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
        client.cookies.set("session_token", "t-ledger-1")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-05", "event_type": "inflow", "account": "hsbc",
                 "direction": "+", "amount_ccy": "USD", "amount": 100, "ref_no": "TEST-1",
                 "usd_equivalent": 100, "cny_amount": 680}]})
            assert r.status_code == 200 and r.json()["ok"] is True
            assert r.json()["applied"] == 1
            assert upserted[0]["ref_no"] == "TEST-1"
        finally:
            client.cookies.clear()

    def test_post_events_bad_type_rejected(self, monkeypatch):
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
        client.cookies.set("session_token", "t-ledger-2")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-05", "event_type": "hack", "account": "hsbc",
                 "direction": "+", "amount_ccy": "USD", "amount": 1, "ref_no": "TEST-2"}]})
            assert r.status_code == 400
        finally:
            client.cookies.clear()

    def test_report_endpoint(self, monkeypatch):
        async def fake_events(event_type=None, year=None, account=None):
            return SEED_EVENTS

        async def fake_value():
            return 13651.98

        async def fake_cash():
            return [{"currency": "HKD", "amount": 100.0}, {"currency": "USD", "amount": 200.0}]

        monkeypatch.setattr(server.db, "get_ledger_events", fake_events)
        monkeypatch.setattr(server.db, "get_latest_portfolio_value", fake_value)
        monkeypatch.setattr(server.db, "get_cash_accounts", fake_cash)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
        monkeypatch.setattr(server, "market_cache",
                            [{"key": "USDCNH", "price": 6.7061}], raising=False)
        client.cookies.set("session_token", "t-ledger-3")
        try:
            r = client.get("/api/ledger/report")
            assert r.status_code == 200
            data = r.json()
            assert data["ok"] is True
            # 期望值从 SEED_EVENTS 独立重算（同 TestBuildReport 口径）
            gross_cny, _usd_in = _seed_inflow_totals()
            usdcnh = 6.7061
            invest_net = gross_cny - _seed_usd_sum("expense") * usdcnh
            expected_net = 13651.98 * usdcnh - invest_net
            assert data["summary"]["total_cny_invested"] == pytest.approx(gross_cny, abs=0.01)
            assert data["identity"]["net_cny"] == pytest.approx(expected_net, abs=0.5)
        finally:
            client.cookies.clear()

    def test_post_events_derived_sync(self, monkeypatch):
        async def fake_upsert(**ev):
            return 1

        async def fake_sync():
            return {"before": 500.0, "after": 1180.0}

        monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)
        monkeypatch.setattr(server.db, "sync_ledger_cny_invested", fake_sync)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
        monkeypatch.setattr(server, "_cny_cfg", {"invested": 500.0, "expense": 0.0})
        client.cookies.set("session_token", "t-ledger-4")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-08", "event_type": "inflow",
                 "account": "za_bank", "direction": "+", "amount_ccy": "CNY",
                 "amount": 10000, "ref_no": "TEST-SYNC-1",
                 "usd_equivalent": 1491.48, "cny_amount": 10000}]})
            assert r.status_code == 200
            data = r.json()
            assert data["ok"] is True and data["applied"] == 1
            assert data["derived_sync"]["cny_invested"] == {"before": 500.0, "after": 1180.0}
            assert server._cny_cfg["invested"] == 1180.0
        finally:
            client.cookies.clear()


# ---------------------------------------------------------------------------
# sync_ledger_cny_invested（cny_invested 服务端重算同步）
# ---------------------------------------------------------------------------

from decimal import Decimal


class TestSyncCnyInvested:
    def _patch(self, monkeypatch, sum_value, config_value):
        calls = {}

        async def fake_sum(*a, **k):
            return [{"s": sum_value}]

        async def fake_get(key):
            return config_value

        async def fake_set(key, value, remark=""):
            calls.update(key=key, value=value, remark=remark)
            return 1

        monkeypatch.setattr(db, "execute_dict", fake_sum)
        monkeypatch.setattr(db, "get_config", fake_get)
        monkeypatch.setattr(db, "set_config", fake_set)
        return calls

    def test_sync_writes_when_diff(self, monkeypatch):
        calls = self._patch(monkeypatch, Decimal("680.00"), "0")
        out = run(db.sync_ledger_cny_invested())
        assert out == {"before": 0.0, "after": 680.0}
        assert calls["key"] == "cny_invested"
        assert calls["value"] == "680.00"
        assert "自动同步" in calls["remark"]

    def test_sync_skips_when_equal(self, monkeypatch):
        calls = self._patch(monkeypatch, Decimal("680.00"), "680.00")
        out = run(db.sync_ledger_cny_invested())
        assert out == {"before": 680.0, "after": 680.0}
        assert calls == {}

    def test_sync_empty_table(self, monkeypatch):
        calls = self._patch(monkeypatch, None, "500")
        out = run(db.sync_ledger_cny_invested())
        assert out == {"before": 500.0, "after": 0.0}
        assert calls["value"] == "0.00"

    def test_sync_replays_converge(self, monkeypatch):
        # 重放修正：config 旧值 500，Σ 已变 1180 → 覆盖为新 Σ（幂等收敛）
        calls = self._patch(monkeypatch, Decimal("1180.00"), "500")
        out = run(db.sync_ledger_cny_invested())
        assert out == {"before": 500.0, "after": 1180.0}
        assert calls["value"] == "1180.00"

    def test_sync_zero_sum_decimal_falsy(self, monkeypatch):
        # Σ 恰为 Decimal("0.00"）时 or 0 短路 → after=0.0（负向冲正完全抵减场景）
        calls = self._patch(monkeypatch, Decimal("0.00"), "500")
        out = run(db.sync_ledger_cny_invested())
        assert out == {"before": 500.0, "after": 0.0}
        assert calls["value"] == "0.00"
