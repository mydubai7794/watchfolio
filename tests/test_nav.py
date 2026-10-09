"""单位净值（TWR）纯函数测试：入金不虚增收益是核心断言。@author yys"""
import ledger


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
