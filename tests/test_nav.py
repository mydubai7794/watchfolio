"""单位净值（TWR）纯函数 + API 端点测试：入金不虚增收益是核心断言。@author yys"""
import ledger
import server
from fastapi.testclient import TestClient

client = TestClient(server.app)


def D(date, value):
    return {"date": date, "total_value": value}


def E(date, etype, usd):
    return {"event_date": date, "event_type": etype, "usd_equivalent": usd}


class TestBuildNavSeries:
    def test_no_cashflow_returns_none(self):
        """无现金流事件 → None（净值无定义，宁缺毋错）。"""
        assert ledger.build_nav_series([D("2026-06-26", 1000)], []) is None

    def test_deposit_does_not_inflate_nav(self):
        """核心：入金当日 NAV 不跳变，其后的上涨才算收益。"""
        daily = [D("2026-06-25", 1000), D("2026-06-26", 2000), D("2026-06-27", 2200)]
        events = [E("2026-06-26", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events)
        # 基期 06-26：legacy=1000 + 入金 1000 → shares=2000，NAV=2000/2000=1.0
        assert out["series"] == [
            {"date": "2026-06-26", "nav": 1.0},
            {"date": "2026-06-27", "nav": 1.1}]
        assert out["twr_pct"] == 10.0
        assert out["ann_pct"] is None          # days=2 < 30 不年化
        assert out["days"] == 2

    def test_withdrawal_reduces_shares_not_nav(self):
        """转出按当时 NAV 减份额：转出本身不产生收益/亏损。"""
        daily = [D("2026-06-26", 2000), D("2026-06-27", 1500), D("2026-06-28", 1650)]
        events = [E("2026-06-26", "inflow", 2000), E("2026-06-27", "transfer", 500)]
        out = ledger.build_nav_series(daily, events)
        # 06-27: shares=2000-500/1.0=1500, NAV=1500/1500=1.0；06-28: NAV=1650/1500=1.1
        navs = [p["nav"] for p in out["series"]]
        assert abs(navs[0] - 1.0) < 1e-9
        assert abs(navs[1] - 1.0) < 1e-9
        assert abs(navs[2] - 1.1) < 1e-9

    def test_fee_and_expense_are_negative_cf(self):
        """fee/expense 计负现金流（减份额）。"""
        daily = [D("2026-06-26", 1000), D("2026-06-27", 950)]
        events = [E("2026-06-26", "inflow", 1000), E("2026-06-27", "fee", 50)]
        out = ledger.build_nav_series(daily, events)
        # 06-27: shares=1000-50/1.0=950, NAV=950/950=1.0（费用不算投资盈亏）
        assert abs(out["series"][1]["nav"] - 1.0) < 1e-9

    def test_reward_and_fx_not_cashflow(self):
        """reward 推高 NAV（无本金收益）；fx 组合内部结构不调份额。"""
        daily = [D("2026-06-26", 1000), D("2026-06-27", 1100)]
        events = [E("2026-06-26", "inflow", 1000),
                  E("2026-06-27", "reward", 100), E("2026-06-27", "fx", 999)]
        out = ledger.build_nav_series(daily, events)
        assert abs(out["series"][1]["nav"] - 1.1) < 1e-9  # reward 全部体现为净值上涨

    def test_realtime_total_value_overrides_last_point(self):
        """latest_total_value 以实时总资产覆盖末点（实时 TWR）。"""
        daily = [D("2026-06-26", 2000), D("2026-06-27", 2200)]
        events = [E("2026-06-26", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events, latest_total_value=2400)
        assert abs(out["series"][-1]["nav"] - 1.2) < 1e-9
        assert out["twr_pct"] == 20.0

    def test_annualized_when_days_ge_30(self):
        """days>=30 时给出年化（复利外推）。"""
        daily = [D("2026-01-01", 1000), D("2026-02-10", 1100)]
        events = [E("2026-01-01", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events)
        assert out["days"] == 41
        assert out["twr_pct"] == 10.0
        expected = ((1.1) ** (365 / 41) - 1) * 100
        assert abs(out["ann_pct"] - expected) < 0.01

    def test_nonpositive_shares_returns_none(self):
        """份额退化非正（数据异常）→ None。"""
        daily = [D("2026-06-26", 100)]
        events = [E("2026-06-26", "inflow", 100), E("2026-06-26", "transfer", 200)]
        assert ledger.build_nav_series(daily, events) is None

    def test_no_daily_after_base_returns_none(self):
        """基期后无日线点（空 ticks）→ None。"""
        assert ledger.build_nav_series([], [E("2026-06-26", "inflow", 1000)]) is None
        assert ledger.build_nav_series(
            [D("2026-06-25", 500)], [E("2026-06-26", "inflow", 1000)]) is None

    def test_tail_cashflow_folded_before_realtime_override(self):
        """尾部现金流（晚于最后日线）先折份额再实时覆盖：入金不虚增 TWR。"""
        daily = [D("2026-06-26", 1000), D("2026-06-27", 1150)]
        events = [E("2026-06-26", "inflow", 1000), E("2026-06-28", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events, latest_total_value=2150)
        # 06-27 原始 NAV=1.15；尾部入金折份额 shares=1000+1000/1.15≈1869.57
        # 实时 2150/1869.57≈1.15 → 与日线末点一致，尾部入金零影响
        assert abs(out["series"][-1]["nav"] - 1.15) < 1e-6
        assert out["twr_pct"] == 15.0

    def test_base_before_all_daily_starts_from_first_observation(self):
        """线上边界：首笔现金流早于全部日线——不可观测期间现金流视同期初投入，
        自首个市值观测日起建序列（首点归一 1.0）。"""
        daily = [D("2026-10-02", 13000), D("2026-10-03", 13650)]
        events = [E("2026-06-26", "inflow", 1000), E("2026-09-29", "inflow", 12000)]
        out = ledger.build_nav_series(daily, events)
        # 份额₀ = 1000+12000=13000（全部按 NAV=1.0 期初投入）
        # 10-02 首观测点 NAV=13000/13000=1.0；10-03 = 13650/13000 → 1.05
        assert out["series"] == [
            {"date": "2026-10-02", "nav": 1.0},
            {"date": "2026-10-03", "nav": round(13650 / 13000, 6)}]
        assert out["twr_pct"] == 5.0
        assert out["days"] == 2          # 自首观测日起计，不含不可观测区间

    def test_observation_start_excludes_unobservable_window_from_ann(self):
        """days 自首观测日起：基期到首观测日跨 100 天也不放大年化分母。"""
        daily = [D("2026-10-02", 1000), D("2026-10-09", 1100)]
        events = [E("2026-06-01", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events)
        assert out["days"] == 8          # 10-02 → 10-09
        assert out["ann_pct"] is None    # days < 30 不年化（若按基期 06-01 起算会 >30 天）

    def test_gap_day_cashflow_folded_by_range(self):
        """间隙日现金流（介于两日线之间的空洞日）按前一观测日 NAV 折份额。"""
        daily = [D("2026-10-02", 11000), D("2026-10-05", 11100)]
        events = [E("2026-10-02", "inflow", 11000), E("2026-10-04", "inflow", 1000)]
        out = ledger.build_nav_series(daily, events)
        # 10-04 入金按 10-02 原始 NAV（=1.0 归一前 11000/11000）折份额：
        # shares=11000+1000=12000 → 10-05 NAV=11100/12000 ≈ 0.925
        # 旧实现丢失 10-04 入金 → 10-05 NAV=11100/11000≈1.009 虚增收益
        assert out is not None and len(out["series"]) == 2
        nav2 = out["series"][1]["nav"]
        assert nav2 < 1.0            # 不再虚增为 1.009
        assert abs(nav2 - 11100 / 12000) < 1e-6

    def test_multiple_gap_cashflows(self):
        """多笔间隙现金流分布在不同空洞日，全部按各自前一观测日 NAV 折算。"""
        daily = [D("2026-10-02", 1000), D("2026-10-04", 1100), D("2026-10-08", 1210)]
        events = [E("2026-10-02", "inflow", 1000),
                  E("2026-10-03", "inflow", 100), E("2026-10-06", "inflow", 100)]
        out = ledger.build_nav_series(daily, events)
        # 10-03 入金按 10-02 NAV(1.0)：shares=1100 → 10-04 NAV=1100/1100=1.0
        # 10-06 入金按 10-04 原始 NAV(1.0)折：shares=1100+100=1200
        # → 10-08 NAV=1210/1200 → round(1210/1200,6)
        assert out["series"][1]["nav"] == 1.0
        assert out["series"][2]["nav"] == round(1210 / 1200, 6)


# ---------------- API 层（TestClient 不用 with，monkeypatch 桩 DB） ----------------

_DAILY = [{"date": "2026-06-26", "total_value": 2000},
          {"date": "2026-06-27", "total_value": 2200}]
_EVENTS = [{"event_date": "2026-06-26", "event_type": "inflow", "usd_equivalent": 1000}]


class TestNavApi:
    def setup_method(self):
        server._nav_ctx_cache.update(ts=0.0, daily=None, events=None)

    def teardown_method(self):
        server._nav_ctx_cache.update(ts=0.0, daily=None, events=None)
        server.portfolio_cache = {"ok": False, "positions": [], "summary": {}, "errors": []}
        client.cookies.delete("session_token")

    def _login(self, monkeypatch):
        async def fake_get_session(t):
            import time as _t
            return {"username": "t", "expires": _t.time() + 3600}
        monkeypatch.setattr(server.db, "get_session", fake_get_session)
        client.cookies.set("session_token", "nav-test")

    def _patch_db(self, monkeypatch, daily=_DAILY, events=_EVENTS):
        async def fake_daily(days):
            return daily
        async def fake_events(**kw):
            return events
        monkeypatch.setattr(server.db, "get_portfolio_daily", fake_daily)
        monkeypatch.setattr(server.db, "get_ledger_events", fake_events)

    def test_nav_requires_login(self):
        """未登录 → 401（auth_middleware 对 /api/* 生效）。"""
        r = client.get("/api/portfolio/nav")
        assert r.status_code == 401

    def test_nav_series_shape(self, monkeypatch):
        """登录后 200：series/twr_pct/ann_pct/days 结构与纯函数一致。"""
        self._login(monkeypatch)
        self._patch_db(monkeypatch)
        r = client.get("/api/portfolio/nav?range=30")
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["twr_pct"] == 10.0
        assert data["ann_pct"] is None
        assert data["days"] == 2
        assert data["series"][-1] == {"date": "2026-06-27", "nav": 1.1}

    def test_nav_no_cashflow_degrades(self, monkeypatch):
        """无现金流 → 空序列 + None 指标（前端降级），不报错。"""
        self._login(monkeypatch)
        self._patch_db(monkeypatch, daily=_DAILY, events=[])
        r = client.get("/api/portfolio/nav")
        assert r.status_code == 200
        data = r.json()
        assert data["series"] == [] and data["twr_pct"] is None

    def test_overview_nav_block_realtime(self, monkeypatch):
        """overview 顶层 nav 块：实时总资产覆盖末点 → twr=20%。"""
        self._login(monkeypatch)
        self._patch_db(monkeypatch)
        server.portfolio_cache = {
            "ok": True, "summary": {"total_value": 2400},
            "positions": [], "errors": []}
        r = client.get("/api/portfolio/overview")
        assert r.status_code == 200
        nav = r.json().get("nav")
        assert nav and nav["twr_pct"] == 20.0
