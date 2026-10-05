"""回测纯函数测试（构造已知序列）。@author yys"""
import alert_backtest as ab


def _mk(closes):
    bars = []
    for i, c in enumerate(closes):
        bars.append({"date": f"2026-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}",
                     "open": c, "high": c * 1.02, "low": c * 0.98,
                     "close": c, "volume": 0})
    return bars


def test_trend_upside_cross_detected():
    closes = [10.0] * 200 + [5.0] * 20 + [10.0] * 30   # 深跌后标准上穿
    bars = _mk(closes)
    res = ab.backtest_trend(bars)
    assert res["count"] >= 1
    # 触发价必在回升段（≈10.0），而非深跌段（5.0）
    assert any(abs(t["price"] - 10.0) < 1e-9 for t in res["triggers"])
    # 过滤回撤对照自 MA200 起算（头段无信号按空仓计，不污染对照）
    assert res["filtered_from"] == 200


def test_atr_stop_trades_complete():
    closes = [10.0] * 205 + [12.0] * 20 + [9.0] * 10   # 涨后深跌触发止损
    res = ab.backtest_atr(_mk(closes), k=3.0)
    assert res["trades"], "应有止损出场交易"
    t = res["trades"][0]
    assert set(t) >= {"entry", "exit", "ret"}
    assert t["ret"] < 0                                # 止损出场必亏损
    assert t["days"] > 0                               # 持仓天数为正
    assert res["open_position"] is None                # 已止损离场，无未平仓
    # 尾部未平仓：单边上涨不止损 → 披露 open_position（entry/days）
    res_up = ab.backtest_atr(_mk([10.0] * 210 + [12.0] * 40), k=3.0)
    op = res_up["open_position"]
    assert op is not None and op["days"] > 0


def test_levels_touch_probability_bounds():
    closes = [10.0 + (i % 3) for i in range(120)]      # 震荡 ±10%（确定序列）
    res = ab.backtest_levels(_mk(closes), [-5.0, 8.0], horizon=60)
    # 本地确定序列实测固化：round(-5.0 档,6)=0.655462 / round(8.0 档,6)=0.672269
    assert abs(res["-5.0"]["touch_prob"] - 0.6554621848739496) < 1e-9
    assert abs(res["8.0"]["touch_prob"] - 0.6722689075630253) < 1e-9


def test_run_dispatch_unknown_rule():
    res = ab.run("QQQ", "nope", {}, _mk([10.0] * 60))
    assert "error" in res


def test_run_params_dual_contract():
    """params 嵌套（与 alert_rules 存储同构）与平铺两种形态等价生效。"""
    bars = _mk([10.0] * 205 + [12.0] * 20 + [9.0] * 10)
    nested = ab.run("QQQ", "atr_stop", {"atr_stop": {"k": 99.0}}, bars)
    flat = ab.run("QQQ", "atr_stop", {"k": 99.0}, bars)
    # k=99 → 止损深度为负，深跌也不触发 → 无交易且披露未平仓（两种形态一致）
    assert nested["count"] == 0 and nested["open_position"] is not None
    assert flat["count"] == 0 and flat["open_position"] is not None
    # 无参数时默认值从 alert_engine.DEFAULT_PARAMS 同源取（k=3.0 正常触发）
    default = ab.run("QQQ", "atr_stop", {}, bars)
    assert default["count"] >= 1
    # levels 同理：嵌套取子包，平铺兜底整包；缺省回落 DEFAULT_PARAMS
    bars_l = _mk([10.0 + (i % 3) for i in range(120)])
    nested_l = ab.run("QQQ", "price_plan",
                      {"price_plan": {"levels": [-5.0, 8.0]}}, bars_l)
    flat_l = ab.run("QQQ", "price_plan", {"levels": [-5.0, 8.0]}, bars_l)
    assert set(nested_l) == {"-5.0", "8.0"} and set(flat_l) == {"-5.0", "8.0"}
    default_l = ab.run("QQQ", "price_plan", {}, bars_l)
    assert set(default_l) == {"-5.0", "-10.0", "8.0", "15.0"}
