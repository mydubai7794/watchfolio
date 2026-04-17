"""scorer 模块单元测试"""
import pytest
from scorer import normalize, calc_score, apply_futures_kill, rank_etfs


class TestNormalize:
    def test_basic(self):
        assert normalize([1, 2, 3, 4, 5]) == [0.0, 0.25, 0.5, 0.75, 1.0]

    def test_single_value(self):
        assert normalize([42]) == [1.0]

    def test_all_same(self):
        assert normalize([3, 3, 3]) == [1.0, 1.0, 1.0]

    def test_empty(self):
        assert normalize([]) == []

    def test_with_zeros(self):
        result = normalize([0, 5, 10])
        assert result[0] == 0.0
        assert result[2] == 1.0


class TestCalcScore:
    def test_basic_weighted(self):
        assert calc_score(1.0, 0.0) == pytest.approx(0.7)

    def test_zero_inputs(self):
        assert calc_score(0.0, 0.0) == 0.0

    def test_max_ratio_min_premium(self):
        score = calc_score(1.0, 1.0)
        assert score == pytest.approx(0.4)

    def test_custom_weights(self):
        score = calc_score(1.0, 0.0, w_ratio=0.5, w_premium=0.5)
        assert score == pytest.approx(0.5)


class TestFuturesKill:
    def test_kill_when_diverging(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=True)
        assert score == 0.0

    def test_no_kill_when_not_diverging(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-0.5,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0

    def test_no_kill_when_etf_down(self):
        score = apply_futures_kill(85.0, etf_change_pct=-1.0, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0

    def test_no_kill_when_disabled(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=False)
        assert score == 85.0

    def test_no_kill_when_futures_none(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=None,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0


class TestRankEtfs:
    def test_ranking_order(self):
        etfs = [
            {"code": "A", "buy1_vol": 100, "sell1_vol": 50,
             "price": 1.5, "iopv": 1.5, "change_pct": 1.0, "futures_change": None},
            {"code": "B", "buy1_vol": 500, "sell1_vol": 10,
             "price": 1.2, "iopv": 1.25, "change_pct": 0.5, "futures_change": None},
        ]
        result = rank_etfs(etfs, w_ratio=0.7, w_premium=0.3, futures_kill_enabled=False,
                           futures_kill_threshold=-1.5)
        assert result[0]["code"] == "B"

    def test_buy1_zero_gets_zero_score(self):
        etfs = [
            {"code": "A", "buy1_vol": 0, "sell1_vol": 100,
             "price": 1.0, "iopv": 1.0, "change_pct": 0, "futures_change": None},
        ]
        result = rank_etfs(etfs)
        assert result[0]["score"] == 0.0
