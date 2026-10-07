"""提醒引擎规则与冷却状态机测试。@author yys"""
import asyncio
import json
import time

import pytest

import alert_engine as ae


@pytest.fixture
def engine_env(monkeypatch):
    """打桩：规则表/提醒表/持仓成本/指标缓存。"""
    saved = {}
    monkeypatch.setattr(ae.db, "get_alert_rule", lambda s: asyncio.sleep(0, result=None))

    async def fake_upsert(sym, params, state, enabled=1):
        saved["params"], saved["state"] = params, state
    monkeypatch.setattr(ae.db, "upsert_alert_rule", fake_upsert)

    async def fake_save(ev):
        saved["last_event"] = ev
        return 1
    monkeypatch.setattr(ae.db, "save_alert", fake_save)

    async def fake_holdings(enabled_only=True):
        return [{"symbol": "NVDA", "market": "US", "cost_price": 200.0,
                 "quantity": 100.0}]
    monkeypatch.setattr(ae.db, "get_holdings", fake_holdings)

    # evaluate_round 每轮从 sys_config 重载冷却时长，测试环境禁止连库 → 打桩返回 "0"
    async def fake_config(key):
        return "0"
    monkeypatch.setattr(ae.db, "get_config", fake_config)

    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 0.0)     # 测试即时过期
    # 同步直调 eval_* 时模块级持仓缓存为空 → 直接预写缓存绕过异步加载
    monkeypatch.setattr(ae, "_positions", {"NVDA": {"cost": 200.0, "qty": 100.0}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
    # 阈值起点显式归位（全局值会被 evaluate_round 刷新，见 _positions_env 注释）
    monkeypatch.setattr(ae, "MIN_MARKET_VALUE", 500.0)
    return saved


def _ind(stop=150.0, rsi=25.0, ma50=180.0, ma200=170.0, enough=True):
    return {"stop_level": stop, "rsi14": rsi, "ma50": ma50, "ma200": ma200,
            "hhv22": 160.0, "atr22": 3.3, "enough_history": enough}


def _rule_store(monkeypatch) -> dict:
    """内存版规则库 harness：get/upsert 打桩 + JSON 往返深拷贝，
    模拟真实落库/读回语义（内存对象与库内解耦）。"""
    store: dict = {}

    async def fake_rule(s):
        raw = store.get(s)
        return json.loads(json.dumps(raw)) if raw else None

    async def fake_upsert(sym, params, state, enabled=1):
        store[sym] = {"params": params,
                      "fired_state": json.loads(json.dumps(state)),
                      "enabled": enabled}

    monkeypatch.setattr(ae.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(ae.db, "upsert_alert_rule", fake_upsert)
    return store


def test_price_plan_triggers_and_resets(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind())
    state = {}
    # -5% 档目标 190：价格 189 触发
    evs = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert any(e["rule"] == "price_plan" and "-5" in e["title"] for e in evs)
    # 同档冷却内不重复
    evs2 = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert not evs2
    # 价格回到触发带上方 ±0.5%（>190.95）→ 重置
    ae.eval_price_plan("NVDA", 191.5, ae.DEFAULT_PARAMS, state, _ind())
    evs3 = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert any(e["rule"] == "price_plan" for e in evs3)


def test_atr_stop_critical_event(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    state = {}
    evs = ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state, _ind(stop=150.0))
    assert evs and evs[0]["level"] == "critical"
    assert evs[0]["body"]["stop_level"] == pytest.approx(150.0)
    # 价格回上止损位 0.5% → 冷却重置
    ae.eval_atr_stop("NVDA", 151.0, ae.DEFAULT_PARAMS, state, _ind(stop=150.0))
    assert ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state,
                            _ind(stop=150.0))


def test_extreme_guard_oversold_only(engine_env, monkeypatch):
    state = {}
    evs = ae.eval_extreme_guard("NVDA", 175.0, ae.DEFAULT_PARAMS, state, _ind(rsi=25.0))
    assert evs and "加仓" in evs[0]["title"]
    # 乖离 <= -8%（price 165 vs ma50 180 → -8.3%）同样触发
    evs2 = ae.eval_extreme_guard("NVDA", 165.0, ae.DEFAULT_PARAMS, state,
                                 _ind(rsi=50.0))
    assert evs2


def test_atr_stop_disabled_without_history(engine_env, monkeypatch):
    state = {}
    evs = ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state,
                           _ind(stop=None, enough=False))
    assert not evs


def test_evaluate_round_end_to_end(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators",
                        lambda s: _ind(stop=150.0, rsi=25.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)
    assert engine_env["last_event"]["symbol"] == "NVDA"


def test_price_plan_sell_tier(engine_env):
    state = {}
    # +8% 档目标 216：价格 216.5 ≥ 216 触发止盈档
    evs = ae.eval_price_plan("NVDA", 216.5, ae.DEFAULT_PARAMS, state, _ind())
    assert any("止盈" in e["title"] and "+8" in e["title"] for e in evs)
    # 回落至触发带下方（< 216×0.995=214.92）→ 重置后再触发
    ae.eval_price_plan("NVDA", 214.0, ae.DEFAULT_PARAMS, state, _ind())
    evs2 = ae.eval_price_plan("NVDA", 216.5, ae.DEFAULT_PARAMS, state, _ind())
    assert any(e["rule"] == "price_plan" for e in evs2)


def test_eval_trend_cross():
    # 210 根横盘 10 + 40 根拉升 12（确定性序列）：
    # ma200(末根) = (160×10 + 40×12)/200 = 10.4 < 12 → above
    # ma200(前根) = (161×10 + 39×12)/200 = 10.39 < 12 → 前根亦 above → 无金叉
    closes = [10.0] * 210 + [12.0] * 40
    bars = [{"close": c} for c in closes]
    res = ae.eval_trend(bars, ae.DEFAULT_PARAMS)
    assert res is not None
    assert res["state"] == "above"
    assert res["cross"] is None
    assert res["ma200"] == pytest.approx(10.4)
    # 不足 MA_SLOW+1 根 → 无法判定
    assert ae.eval_trend(bars[:200], ae.DEFAULT_PARAMS) is None


def test_merged_params_override():
    params = ae._merged_params({"params": {"atr_stop": {"k": 4.0}}})
    assert params["atr_stop"]["k"] == 4.0          # 覆盖生效
    assert params["atr_stop"]["enabled"] is True   # 同类其余键保留默认
    assert params["price_plan"]["levels"] == ae.DEFAULT_PARAMS["price_plan"]["levels"]
    assert ae._merged_params(None)["atr_stop"]["k"] == 3.0   # 未配置 → 全默认


def test_disabled_rule_returns_empty(engine_env, monkeypatch):
    async def fake_rule(s):
        return {"symbol": "NVDA", "params": None, "fired_state": None, "enabled": 0}
    monkeypatch.setattr(ae.db, "get_alert_rule", fake_rule)
    assert asyncio.run(ae.evaluate("NVDA", 149.0)) == []


def test_persistence_reset_survives_reload(engine_env, monkeypatch):
    """关键回归：重置轮（无事件）也必须持久化 state，否则下轮从库读回
    旧值、重置丢失 → 档位永久哑火/止损冷却失效。"""
    monkeypatch.setattr(ae.daily_bars, "get_indicators",
                        lambda s: _ind(rsi=50.0))    # rsi=50 避开 extreme_guard 干扰
    store = _rule_store(monkeypatch)
    asyncio.run(ae.evaluate("NVDA", 189.0))     # T1：触及 -5% 档 → 触发
    asyncio.run(ae.evaluate("NVDA", 191.5))     # T2：回上带 → 重置（本轮无事件）
    assert "-5" not in store["NVDA"]["fired_state"]["price_plan"]["fired"]
    evs = asyncio.run(ae.evaluate("NVDA", 189.0))   # T3：重置已持久化 → 应能再次触发
    assert any(e["rule"] == "price_plan" and "-5" in e["title"] for e in evs)


def test_atr_stop_cooldown_reset_persists(engine_env, monkeypatch):
    """回归：站回止损位上方 → 主冷却重置须落库（此前重置 pop 键路径错误为
    no-op、冷却残留 → 快照对比无变化不落库 → 提醒最长 4h 空窗）。"""
    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 3600.0)   # 冷却不过期，才能测出重置是否生效
    monkeypatch.setattr(ae.daily_bars, "get_indicators",
                        lambda s: _ind(rsi=50.0))    # rsi=50 避开 extreme_guard 干扰
    store = _rule_store(monkeypatch)
    evs1 = asyncio.run(ae.evaluate("NVDA", 149.0))   # T1：跌破止损 150 → critical
    assert any(e["rule"] == "atr_stop" and e["level"] == "critical" for e in evs1)
    evs2 = asyncio.run(ae.evaluate("NVDA", 151.0))   # T2：站回上方 → 冷却重置（无事件轮）
    assert not any(e["rule"] == "atr_stop" for e in evs2)
    assert "main" not in store["NVDA"]["fired_state"]["atr_stop"]   # 重置已落库
    evs3 = asyncio.run(ae.evaluate("NVDA", 149.0))   # T3：再跌破 → 应再次触发
    assert any(e["rule"] == "atr_stop" and e["level"] == "critical" for e in evs3)


def test_extreme_guard_reset_persists(engine_env, monkeypatch):
    """回归：离开超卖区 → 主冷却重置须落库，再超卖可再次提醒。"""
    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 3600.0)   # 冷却不过期，才能测出重置是否生效
    store = _rule_store(monkeypatch)
    oversold = lambda s: _ind(rsi=25.0)
    normal = lambda s: _ind(rsi=50.0)
    monkeypatch.setattr(ae.daily_bars, "get_indicators", oversold)
    evs1 = asyncio.run(ae.evaluate("NVDA", 175.0))   # T1：RSI 25 → 超卖触发
    assert any(e["rule"] == "extreme_guard" for e in evs1)
    monkeypatch.setattr(ae.daily_bars, "get_indicators", normal)
    evs2 = asyncio.run(ae.evaluate("NVDA", 190.0))   # T2：指标恢复 → 无事件轮落库重置
    assert not any(e["rule"] == "extreme_guard" for e in evs2)
    assert "main" not in store["NVDA"]["fired_state"]["extreme_guard"]   # 重置已落库
    monkeypatch.setattr(ae.daily_bars, "get_indicators", oversold)
    evs3 = asyncio.run(ae.evaluate("NVDA", 175.0))   # T3：再超卖 → 应再次触发
    assert any(e["rule"] == "extreme_guard" for e in evs3)


# ---------------- V2 双规则（仅 QQQ）：止盈 + 深跌回投 ----------------

def _qqq_positions(monkeypatch, cost=100.0, qty=100.0):
    """预写 QQQ 持仓缓存（覆盖 engine_env 的 NVDA 缓存，QQQ 专用用例用）。"""
    monkeypatch.setattr(ae, "_positions", {"QQQ": {"cost": cost, "qty": qty}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())


def test_vix_take_profit_triggers(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_positions(monkeypatch)
    state = {}
    evs = ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state,
                                  dict(_ind(), asof="2026-10-02"))
    assert len(evs) == 1 and evs[0]["rule"] == "vix_take_profit"
    assert evs[0]["level"] == "warning"
    assert evs[0]["body"]["profit_pct"] == 25.0
    assert evs[0]["body"]["vix_pctile"] == 15.0


def test_vix_take_profit_needs_both_conditions(engine_env, monkeypatch):
    state = {}
    # 浮盈达标但 VIX 分位高（市场未自满）→ 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 35.0)
    _qqq_positions(monkeypatch)
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state, _ind()) == []
    # VIX 分位低但浮盈不足 → 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    assert ae.eval_vix_take_profit("QQQ", 115.0, ae.DEFAULT_PARAMS, state, _ind()) == []


def test_vix_take_profit_missing_vix_and_gate(engine_env, monkeypatch):
    # VIX 分位缺失（拉取失败退化）→ 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: None)
    _qqq_positions(monkeypatch)
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, {}, _ind()) == []
    # 非 QQQ 门禁（报告结论不外推）
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    assert ae.eval_vix_take_profit("NVDA", 250.0, ae.DEFAULT_PARAMS, {}, _ind()) == []
    # 无成本 → 不触发
    monkeypatch.setattr(ae, "_positions", {})
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, {}, _ind()) == []


def test_vix_take_profit_resets_out_of_band(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_positions(monkeypatch)
    state = {}
    ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state, _ind())
    assert "main" in state["vix_take_profit"]
    # 浮盈回落到带外（<19.5%）→ 冷却清除，重新具备触发资格
    ae.eval_vix_take_profit("QQQ", 119.0, ae.DEFAULT_PARAMS, state, _ind())
    assert "main" not in state["vix_take_profit"]


def test_deep_dip_triggers_and_resets(engine_env, monkeypatch):
    ind = dict(_ind(), week52_high=200.0)
    state = {}
    evs = ae.eval_deep_dip("QQQ", 168.0, ae.DEFAULT_PARAMS, state, ind)
    assert len(evs) == 1 and evs[0]["rule"] == "deep_dip"
    assert evs[0]["level"] == "info"
    assert evs[0]["body"]["drawdown_pct"] == -16.0
    assert evs[0]["body"]["week52_high"] == 200.0
    # 回撤收窄到带外（>-14.5%）→ 冷却清除
    ae.eval_deep_dip("QQQ", 171.2, ae.DEFAULT_PARAMS, state, ind)
    assert "main" not in state["deep_dip"]


def test_deep_dip_gate_and_missing_week52(engine_env, monkeypatch):
    # week52_high 缺失（历史不足）→ 不触发
    assert ae.eval_deep_dip("QQQ", 100.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=None)) == []
    # 非 QQQ 门禁
    assert ae.eval_deep_dip("NVDA", 100.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=200.0)) == []
    # 回撤未达标（-14%）→ 不触发
    assert ae.eval_deep_dip("QQQ", 172.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=200.0)) == []


def test_evaluate_dispatches_v2_rules(engine_env, monkeypatch):
    """evaluate 接线验证：QQQ 一次求值两条新规则生效。"""
    ind = dict(_ind(), week52_high=100.0, asof="2026-10-02")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: ind)
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_positions(monkeypatch)
    evs = asyncio.run(ae.evaluate("QQQ", 60.0))
    rules = {e["rule"] for e in evs}
    assert "deep_dip" in rules            # 距 52 周高回撤 -40% 达标
    assert "vix_take_profit" not in rules  # 浮盈 -40% 未达标


def test_deep_dip_reset_persists(engine_env, monkeypatch):
    """回归：回撤收窄离开触发带 → 主冷却重置须落库，再深跌可再次提醒。

    重置轮（无事件也可能有 state 变化）不落库的话，下轮从库读回旧值、
    重置丢失 → 深跌提醒最长一个冷却期空窗。"""
    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 3600.0)   # 冷却不过期，才能测出重置是否生效
    ind = dict(_ind(rsi=50.0, ma50=170.0, ma200=165.0), week52_high=200.0)
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: ind)
    _qqq_positions(monkeypatch)
    store = _rule_store(monkeypatch)
    evs1 = asyncio.run(ae.evaluate("QQQ", 160.0))   # T1：距 52 周高 -20% → 触发
    assert any(e["rule"] == "deep_dip" for e in evs1)
    evs2 = asyncio.run(ae.evaluate("QQQ", 195.0))   # T2：回撤收窄 -2.5% → 重置
    assert not any(e["rule"] == "deep_dip" for e in evs2)
    assert "main" not in store["QQQ"]["fired_state"]["deep_dip"]   # 重置已落库
    evs3 = asyncio.run(ae.evaluate("QQQ", 160.0))   # T3：再深跌 → 应再次触发
    assert any(e["rule"] == "deep_dip" for e in evs3)


# ---------------- 市值阈值过滤（evaluate_round 收口） ----------------

def _positions_env(monkeypatch, positions, config_value="500"):
    """预写持仓缓存与 sys_config 桩（config_value 传 None 模拟配置缺失）。

    显式锁定阈值起点 500：MIN_MARKET_VALUE 提为全局后由 evaluate_round
    内 global 赋值刷新，monkeypatch 无法自动回收，不显式设起点会让
    "沿用上轮"语义把上一用例的残值带进本用例（用例间污染）。"""
    monkeypatch.setattr(ae, "_positions", positions)
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
    monkeypatch.setattr(ae, "MIN_MARKET_VALUE", 500.0)

    async def fake_config(key):
        return config_value
    monkeypatch.setattr(ae.db, "get_config", fake_config)


def test_round_filters_small_position(engine_env, monkeypatch):
    """市值 < 阈值 → 不评估不落库：2 股 × 149 = 298 < 500，跌破止损也无事件。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 2.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert evs == []
    assert "last_event" not in engine_env      # 不落库：提醒表零写入


def test_round_keeps_large_position(engine_env, monkeypatch):
    """市值 ≥ 阈值 → 正常评估：10 股 × 149 = 1490 ≥ 500，atr_stop 触发。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 10.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)


def test_round_threshold_boundary_equal(engine_env, monkeypatch):
    """恰好等于阈值 → 保留（≥ 判定）：1 股 × 500 = 500。
    500 触发 +8%=216 与 +15%=230 两档止盈（cost=200），atr_stop 不触发。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 1.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 500.0}))
    assert any(e["rule"] == "price_plan" for e in evs)


def test_round_zero_threshold_disables_filter(engine_env, monkeypatch):
    """阈值 0 → 不过滤：2 股 × 149 = 298 仍评估。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 2.0}}, "0")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)


def test_round_missing_config_defaults_500(engine_env, monkeypatch):
    """配置缺失 → 默认 500：3 股 × 149 = 447 < 500 被过滤。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 3.0}}, None)
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert evs == []


def test_round_negative_threshold_disables_filter(engine_env, monkeypatch):
    """阈值负值 → 不过滤（与 0 同语义）：2 股 × 149 = 298 仍评估。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 2.0}}, "-1")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)


def test_round_invalid_config_defaults_500(engine_env, monkeypatch):
    """配置非法（"abc"）→ float 抛 ValueError → 沿用默认 500：
    3 股 × 149 = 447 < 500 被过滤。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 3.0}}, "abc")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert evs == []


def test_round_failure_keeps_previous_threshold(engine_env, monkeypatch):
    """回归：配置读取失败 → 沿用上轮阈值（与冷却时长语义对称）。

    上轮已设 0（关闭过滤），DB 抖动轮内不得静默回退 500 重新开启过滤，
    否则关闭开关在 DB 异常期间失效、小市值提醒悄然丢失。"""
    monkeypatch.setattr(ae, "MIN_MARKET_VALUE", 0.0)

    async def broken_config(key):
        raise RuntimeError("db down")
    monkeypatch.setattr(ae.db, "get_config", broken_config)
    monkeypatch.setattr(ae, "_positions", {"NVDA": {"cost": 200.0, "qty": 2.0}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)   # 沿用 0 → 298 市值仍评估


# ---------------------------------------------------------------------------
# V3 新规则：ma200_zone（年线区域）/ fib_retrace（斐波那契回撤）
# ---------------------------------------------------------------------------

_IND_OK = {"enough_history": True, "ma200": 100.0,
           "fib": {"high": 120.0, "low": 80.0, "depth": 0.55,
                   "levels": {"382": 104.8, "500": 100.0, "618": 95.2}}}


def test_default_params_include_v3_rules():
    assert "ma200_zone" in ae.DEFAULT_PARAMS
    assert "fib_retrace" in ae.DEFAULT_PARAMS


def test_ma200_zone_deep_bias_fires():
    state = {}
    evs = ae.eval_ma200_zone("QQQ", 85.0, ae._merged_params(None), state, _IND_OK)
    assert len(evs) == 1
    assert evs[0]["rule"] == "ma200_zone" and evs[0]["level"] == "warning"
    assert evs[0]["body"]["bias_pct"] == -15.0


def test_ma200_zone_touch_fires():
    state = {}
    evs = ae.eval_ma200_zone("QQQ", 101.0, ae._merged_params(None), state, _IND_OK)
    assert len(evs) == 1 and evs[0]["level"] == "info"
    assert "贴近200日线" in evs[0]["title"]


def test_ma200_zone_cooldown_and_reset():
    params = ae._merged_params(None)
    state = {}
    assert len(ae.eval_ma200_zone("QQQ", 85.0, params, state, _IND_OK)) == 1
    assert ae.eval_ma200_zone("QQQ", 84.0, params, state, _IND_OK) == []   # 冷却内
    ae.eval_ma200_zone("QQQ", 99.0, params, state, _IND_OK)                # bias=-1 离开深乖离带
    assert "bias" not in state["ma200_zone"]                               # 冷却重置
    assert len(ae.eval_ma200_zone("QQQ", 85.0, params, state, _IND_OK)) == 1


def test_ma200_zone_gates():
    p = ae._merged_params(None)
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {},
                              {"enough_history": False, "ma200": 100.0}) == []
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {}, {"enough_history": True}) == []
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {}, None) == []


def test_ma200_zone_param_override():
    params = {k: dict(v) for k, v in ae.DEFAULT_PARAMS.items()}
    params["ma200_zone"]["bias_low"] = -20.0
    assert ae.eval_ma200_zone("QQQ", 85.0, params, {}, _IND_OK) == []      # -15 未达 -20


def test_fib_retrace_fires():
    evs = ae.eval_fib_retrace("QQQ", 98.0, ae._merged_params(None), {}, _IND_OK)
    assert len(evs) == 1 and evs[0]["rule"] == "fib_retrace"
    assert evs[0]["body"]["fib_500"] == 100.0
    assert evs[0]["body"]["depth"] == 0.55


def test_fib_retrace_cooldown_and_reset():
    p = ae._merged_params(None)
    state = {}
    assert len(ae.eval_fib_retrace("QQQ", 98.0, p, state, _IND_OK)) == 1
    assert ae.eval_fib_retrace("QQQ", 97.0, p, state, _IND_OK) == []       # 冷却内
    ind2 = dict(_IND_OK, fib=dict(_IND_OK["fib"], depth=0.42))             # 离开带 <0.45
    ae.eval_fib_retrace("QQQ", 97.0, p, state, ind2)
    assert "main" not in state["fib_retrace"]
    assert len(ae.eval_fib_retrace("QQQ", 98.0, p, state, _IND_OK)) == 1


def test_fib_retrace_gates():
    p = ae._merged_params(None)
    assert ae.eval_fib_retrace("QQQ", 98.0, p, {}, dict(_IND_OK, fib=None)) == []
    assert ae.eval_fib_retrace("QQQ", 98.0, p, {},
                               {"enough_history": False, "fib": _IND_OK["fib"]}) == []


def test_ma200_zone_touch_cooldown_and_reset():
    params = ae._merged_params(None)
    state = {}
    assert len(ae.eval_ma200_zone("QQQ", 101.0, params, state, _IND_OK)) == 1
    assert ae.eval_ma200_zone("QQQ", 102.0, params, state, _IND_OK) == []   # 冷却内
    ae.eval_ma200_zone("QQQ", 110.0, params, state, _IND_OK)                # |bias|=10 离开年线带
    assert "touch" not in state["ma200_zone"]                               # 冷却重置
    assert len(ae.eval_ma200_zone("QQQ", 101.0, params, state, _IND_OK)) == 1


def test_ma200_zone_band_override():
    params = {k: dict(v) for k, v in ae.DEFAULT_PARAMS.items()}
    params["ma200_zone"]["band_pct"] = 5.0
    assert len(ae.eval_ma200_zone("QQQ", 104.0, params, {}, _IND_OK)) == 1  # bias=4 在 5% 带内


def test_fib_retrace_depth_override():
    params = {k: dict(v) for k, v in ae.DEFAULT_PARAMS.items()}
    params["fib_retrace"]["depth"] = 0.6
    assert ae.eval_fib_retrace("QQQ", 98.0, params, {}, _IND_OK) == []      # 0.55 未达 0.6


def test_evaluate_dispatches_v3_rules(engine_env, monkeypatch):
    """evaluate() 接线冒烟：ma200_zone/fib_retrace 两规则被纳入单标的求值链。

    state 须跨轮持久化才能验证冷却（engine_env 桩每轮返回 None），
    故用 _rule_store + 冷却不过期，同 atr_stop 重置回归用例模式。"""
    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 3600.0)   # 冷却不过期，才能测出第二轮不再触发
    _rule_store(monkeypatch)
    monkeypatch.setitem(ae.daily_bars.INDICATORS, "QQQ", _IND_OK)
    monkeypatch.setattr(ae, "_positions", {"QQQ": {"cost": 100.0, "qty": 10.0}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
    evs = asyncio.run(ae.evaluate("QQQ", 85.0))
    rules = {e["rule"] for e in evs}
    assert "ma200_zone" in rules        # bias=-15 深乖离触发
    assert "fib_retrace" in rules       # depth 0.55 ≥ 0.5 触发
    rules2 = {e["rule"] for e in asyncio.run(ae.evaluate("QQQ", 85.0))}
    # 第二轮同价：冷却内不再触发（接线后冷却状态机同样生效）
    assert "ma200_zone" not in rules2
