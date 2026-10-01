"""测试4步条件分支决策模型。"""
import sys
sys.path.insert(0, "/root/workspace/project/qdii")
import scorer

# ---------------------------------------------------------------------------
# 模拟数据
# ---------------------------------------------------------------------------

BASE_ETF = {
    "code": "513100", "name": "纳指ETF", "futures_index": "NQ",
    "price": 1.50, "pre_close": 1.49, "iopv": 1.495,
    "change_pct": 0.67,  # open_gap ≈ +0.67%
    "premium_pct": 0.33,
    "buy1_vol": 2000, "sell1_vol": 500,
    "buy1_price": 1.499, "sell1_price": 1.501,
    "buy_total_vol": 8000, "sell_total_vol": 2500,
    "total_volume": 320000, "amount": 480000,
    "iopv_source": "IOPV", "weibi": 42.86,
    "outer_vol": 200000, "inner_vol": 80000,
}


def _make_etf(**overrides):
    etf = dict(BASE_ETF)
    etf.update(overrides)
    return etf


def _fut(raw_nq):
    return {"NQ": raw_nq}


OVERNIGHT = {"纳斯达克": {"close": 18000, "change_pct": 0.5}}


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------

def test_scenario_c_normal():
    """剧本C：常规震荡，全通过盘口，选最低gap"""
    etfs = [
        _make_etf(code="513100", name="纳指ETF", change_pct=0.5, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", name="纳指科技", change_pct=0.3, buy1_vol=2000, sell1_vol=400),
    ]
    r = scorer.run_decision_model(etfs, _fut(0.3), OVERNIGHT, None)
    assert r["scenario"] == "C"
    assert r["decision"] == "buy"
    assert r["target"]["code"] == "513050"  # 最低 gap
    print(f"  PASS scenario=C, target={r['target']['code']}, reason={r['reason']}")


def test_scenario_a_crash():
    """剧本A：期指大跌，只选极端错杀"""
    etfs = [
        _make_etf(code="513100", change_pct=-0.8, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", change_pct=-2.5, buy1_vol=2000, sell1_vol=400),  # 错杀
    ]
    raw_fut = -1.0  # adjusted = -1.0 * 1.15 = -1.15
    # threshold = -1.15 - 1.0 = -2.15
    # 513100: gap=-0.8 ≥ -2.15 → 淘汰
    # 513050: gap=-2.5 < -2.15 → 通过
    r = scorer.run_decision_model(etfs, _fut(raw_fut), OVERNIGHT, None)
    assert r["scenario"] == "A"
    assert r["decision"] == "buy"
    assert r["target"]["code"] == "513050"
    print(f"  PASS scenario=A, target={r['target']['code']}, reason={r['reason']}")


def test_scenario_a_no_overshot():
    """剧本A：期指大跌但没有错杀标的 → 空仓"""
    etfs = [
        _make_etf(code="513100", change_pct=-0.5, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", change_pct=-0.8, buy1_vol=2000, sell1_vol=400),
    ]
    raw_fut = -1.0  # adjusted = -1.15, threshold = -2.15
    r = scorer.run_decision_model(etfs, _fut(raw_fut), OVERNIGHT, None)
    assert r["scenario"] == "A"
    assert r["decision"] == "skip"
    print(f"  PASS scenario=A skip, reason={r['reason']}")


def test_scenario_b_reversal():
    """剧本B：昨夜跌但今朝修复，选落后最多的"""
    overnight = {"纳斯达克": {"close": 17800, "change_pct": -0.8}}
    etfs = [
        _make_etf(code="513100", change_pct=0.5, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", change_pct=0.0, buy1_vol=2000, sell1_vol=400),
    ]
    raw_fut = 0.5  # adjusted = 0.575
    # 513100: lag = 0.5 - 0.575 = -0.075, 不够
    # 513050: lag = 0.0 - 0.575 = -0.575 < -0.3 → 通过
    r = scorer.run_decision_model(etfs, _fut(raw_fut), overnight, None)
    assert r["scenario"] == "B"
    assert r["decision"] == "buy"
    assert r["target"]["code"] == "513050"
    print(f"  PASS scenario=B, target={r['target']['code']}, reason={r['reason']}")


def test_step3_veto():
    """Step 3: 盘口否决，买卖比不足 → 空仓"""
    etfs = [
        _make_etf(code="513100", change_pct=0.5, buy1_vol=300, sell1_vol=500),  # ratio=0.6 < 2.0
    ]
    r = scorer.run_decision_model(etfs, _fut(0.3), OVERNIGHT, None)
    assert r["scenario"] == "C"
    assert r["decision"] == "skip"
    assert "否决" in r["reason"]
    print(f"  PASS step3 veto, reason={r['reason']}")


def test_step1_premium_crushed_priority():
    """Step 4: 杀溢价标的优先"""
    etfs = [
        _make_etf(code="513100", change_pct=0.3, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", change_pct=0.1, buy1_vol=2000, sell1_vol=400),
    ]
    # 昨日513100涨了1.5%，513050涨了0.3%
    # 纳斯达克昨夜涨了0.5%
    # 513100 premium_change = 1.5 - 0.5 = 1.0 → 涨溢价
    # 513050 premium_change = 0.3 - 0.5 = -0.2 → 杀溢价
    prev_day = {"513100": 1.5, "513050": 0.3}
    r = scorer.run_decision_model(etfs, _fut(0.3), OVERNIGHT, prev_day)
    assert r["decision"] == "buy"
    # 杀溢价的513050应该被优先选中（即使gap不是最低）
    # 但513050 gap=0.1 < 513100 gap=0.3，它也是gap最低的
    assert r["target"]["premium_tag"] == "杀溢价"
    print(f"  PASS crushed priority, target={r['target']['code']}, tag={r['target']['premium_tag']}")


def test_score_assignment():
    """score 字段分配正确（连续加权评分）"""
    etfs = [
        _make_etf(code="513100", change_pct=0.5, buy1_vol=3000, sell1_vol=500),
        _make_etf(code="513050", change_pct=0.1, buy1_vol=2000, sell1_vol=400),
        _make_etf(code="159941", change_pct=0.3, buy1_vol=100, sell1_vol=500),  # 低买卖比
    ]
    r = scorer.run_decision_model(etfs, _fut(0.3), OVERNIGHT, None)
    target = r["target"]
    other_pass = [e for e in r["etf_data"] if e["step2_pass"] and e["step3_pass"] and not e.get("is_target")]
    failed = [e for e in r["etf_data"] if not e["step2_pass"] or not e["step3_pass"]]

    assert target["score"] == 1.0
    for e in other_pass:
        assert 0.50 <= e["score"] <= 0.95  # step2+step3 通过 → 连续评分区间
    for e in failed:
        assert e["score"] < 0.5
    print(f"  PASS scores: target={target['score']}, other={[e['score'] for e in other_pass]}, failed={[e['score'] for e in failed]}")


def test_empty_input():
    """空输入"""
    r = scorer.run_decision_model([], _fut(0.3), OVERNIGHT, None)
    assert r["decision"] == "skip"
    assert r["etf_data"] == []
    print(f"  PASS empty input")


def test_futures_beta_adjustment():
    """期指基差修正"""
    r = scorer.run_decision_model(
        [_make_etf()], _fut(-0.5), OVERNIGHT, None,
        futures_beta=1.15,
    )
    # adjusted = -0.5 * 1.15 = -0.575
    assert r["etf_data"][0]["adjusted_fut"] == -0.575
    print(f"  PASS beta adjustment: adjusted_fut={r['etf_data'][0]['adjusted_fut']}")


# ---------------------------------------------------------------------------
# 运行
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_scenario_c_normal,
        test_scenario_a_crash,
        test_scenario_a_no_overshot,
        test_scenario_b_reversal,
        test_step3_veto,
        test_step1_premium_crushed_priority,
        test_score_assignment,
        test_empty_input,
        test_futures_beta_adjustment,
    ]
    print("=== 4步决策模型测试 ===\n")
    for t in tests:
        print(f"• {t.__doc__.strip()}")
        t()
    print("\n=== 全部通过 ===")
