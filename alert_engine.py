"""买卖点提醒引擎：四类规则、冷却状态机、判定落库。

实证依据（2026-10-04 回测，附件报告）：
主打 atr_stop(+3.13%/笔)、骨架 trend_filter(回撤减半)、辅助超卖加仓(66.9%)、
放弃追高侧；档位 +8%/-5% 主力。@author yys
"""
import asyncio
import json
import logging
import time

import database as db
import daily_bars

logger = logging.getLogger("qdii.alert")

COOLDOWN_SECONDS = 4 * 3600.0      # 同规则同方向冷却（sys_config ALERT_COOLDOWN_HOURS 可覆盖）
MIN_MARKET_VALUE = 500.0           # 市值阈值（USD，sys_config ALERT_MIN_MARKET_VALUE 每轮刷新；读取失败沿用上轮）
RESET_BAND = 0.005                  # 离开触发带 ±0.5% 重置冷却

DEFAULT_PARAMS = {
    "price_plan": {"enabled": True, "levels": [-5.0, -10.0, 8.0, 15.0]},
    "atr_stop": {"enabled": True, "k": 3.0},
    "trend_filter": {"enabled": True},
    "extreme_guard": {"enabled": True, "rsi_low": 30.0, "bias_low": -8.0},
    "vix_take_profit": {"enabled": True, "profit_pct": 20.0, "vix_pctile_max": 20.0},
    "deep_dip": {"enabled": True, "dip_pct": 15.0},
    "ma200_zone": {"enabled": True, "bias_low": -10.0, "band_pct": 2.0},
    "fib_retrace": {"enabled": True, "depth": 0.5},
}

_positions: dict[str, dict[str, float]] = {}   # symbol → {"cost": 成本价, "qty": 数量}
_positions_at = 0.0

# 同 (symbol, rule) 自然日封顶：与冷却状态机正交（冷却管带内不重复、离开
# 触发带重置可再触发，此处管全天封顶——当日已发过即不再发）。键集当日
# 首次求值从 alerts 预载（重启不重发）；预载失败沿用旧键集宁可少挡。
_daily_fired: set[tuple[str, str]] = set()
_daily_date: str = ""

# 全局锁串行化求值：读-改-写含 await 点，轮询/push 多入口并发会丢更新
_EVAL_LOCK = asyncio.Lock()


def _merged_params(rule: dict | None) -> dict:
    """库内单票配置覆盖默认值（浅合并每类规则的 dict）。"""
    params = {k: dict(v) for k, v in DEFAULT_PARAMS.items()}
    if rule and rule.get("params"):
        for k, v in rule["params"].items():
            # 防御畸形配置（如 {"price_plan": null}）：非 dict 值或未知键
            # 跳过合并并告警，避免产出坏包导致 kline 500 / 引擎静默失效
            if k not in params or not isinstance(v, dict):
                logger.warning("忽略非法规则配置 %s: %r（须为规则键白名单内的对象值）", k, v)
                continue
            params[k].update(v)
    return params


async def _load_positions() -> dict[str, dict]:
    """持仓成本+数量缓存（60s 刷新，供市值阈值过滤与成本锚定）。@author yys"""
    global _positions, _positions_at
    if time.monotonic() - _positions_at > 60 or not _positions:
        holdings = await db.get_holdings(enabled_only=True)
        _positions = {h["symbol"]: {"cost": float(h["cost_price"] or 0),
                                    "qty": float(h["quantity"] or 0)}
                      for h in holdings if h["market"] == "US"}
        _positions_at = time.monotonic()
    return _positions


def _cooldown_ok(state: dict, rule: str, now: float) -> bool:
    """rule 传规则内具名冷却键（单冷却规则的约定键为 "main"，档位类用 "level:xxx"）。"""
    return now >= state.setdefault(rule, {}).get("cooldown_until", 0)


def _set_cooldown(state: dict, rule: str, now: float, seconds: float):
    state.setdefault(rule, {})["cooldown_until"] = now + seconds


def _event(symbol: str, rule: str, title: str, level: str, body: dict) -> dict:
    return {"symbol": symbol, "rule": rule, "level": level, "title": title,
            "body": body, "triggered_at": None}


def eval_price_plan(symbol: str, price: float, params: dict, state: dict,
                    ind: dict | None) -> list[dict]:
    """成本价锚定档位：buy 档价格 ≤ 目标触发、sell 档价格 ≥ 目标触发；
    逐档一次，价格离开带 ±0.5% 重置。"""
    cfg = params.get("price_plan", {})
    if not cfg.get("enabled", True):
        return []
    cost = _positions.get(symbol, {}).get("cost")
    if not cost:
        return []
    now = time.time()
    st = state.setdefault("price_plan", {"fired": {}})
    fired: dict = st.setdefault("fired", {})
    events = []
    for pct in cfg.get("levels", []):
        target = cost * (1.0 + float(pct) / 100.0)
        key = f"{pct:+g}"
        side = "buy" if pct < 0 else "sell"
        hit = price <= target if side == "buy" else price >= target
        reset = price > target * (1 + RESET_BAND) if side == "buy" \
            else price < target * (1 - RESET_BAND)
        if key in fired and reset:
            # 价格离开触发带 → 判定状态与该档冷却一并重置（离开带说明
            # 边缘抖动已结束，60s 防抖冷却使命完成，可重新具备触发资格）
            del fired[key]
            st.pop(f"level:{key}", None)
            continue
        if hit and key not in fired and _cooldown_ok(st, f"level:{key}", now):
            events.append(_event(
                symbol, "price_plan",
                f"{symbol} 触及{'加仓' if side == 'buy' else '止盈'}档 {key}%",
                "info",
                {"pct": pct, "target": round(target, 2), "cost": cost,
                 "price": price}))
            fired[key] = True
            _set_cooldown(st, f"level:{key}", now, 60.0)
    return events


def eval_atr_stop(symbol: str, price: float, params: dict, state: dict,
                  ind: dict | None) -> list[dict]:
    """Chandelier 移动止损：实时价跌破 22日最高−k×ATR22 → critical。"""
    cfg = params.get("atr_stop", {})
    if not cfg.get("enabled", True) or not ind:
        return []
    stop = ind.get("stop_level")
    if stop is None or not ind.get("enough_history"):
        return []
    now = time.time()
    st = state.setdefault("atr_stop", {})
    if price > stop * (1 + RESET_BAND):
        st.pop("main", None)     # 站回止损位上方 → 主冷却重置（具名键与 _set_cooldown 对称）
        return []
    if price < stop and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "atr_stop", f"{symbol} 跌破移动止损位，建议减仓/卖出",
            "critical",
            {"stop_level": round(stop, 2), "price": price,
             "hhv22": ind.get("hhv22"), "atr22": ind.get("atr22")})]
    return []


def eval_extreme_guard(symbol: str, price: float, params: dict, state: dict,
                       ind: dict | None) -> list[dict]:
    """超卖加仓提醒（仅超卖侧，追高侧按回测实证放弃）。"""
    cfg = params.get("extreme_guard", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    rsi = ind.get("rsi14")
    ma50 = ind.get("ma50")
    bias = (price - ma50) / ma50 * 100.0 if ma50 else None
    reasons = []
    if rsi is not None and rsi <= float(cfg.get("rsi_low", 30.0)):
        reasons.append(f"RSI14={rsi:.1f}")
    if bias is not None and bias <= float(cfg.get("bias_low", -8.0)):
        reasons.append(f"对MA50乖离{bias:+.1f}%")
    if not reasons:
        # 离开超卖区 → 主冷却重置（具名键与 _set_cooldown 对称）
        state.setdefault("extreme_guard", {}).pop("main", None)
        return []
    now = time.time()
    st = state.setdefault("extreme_guard", {})
    if not _cooldown_ok(st, "main", now):
        return []
    _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
    return [_event(
        symbol, "extreme_guard",
        f"{symbol} 超卖（{' / '.join(reasons)}），可考虑分批加仓",
        "info", {"rsi14": rsi, "bias_pct": round(bias, 2) if bias is not None else None,
                 "price": price})]


def eval_vix_take_profit(symbol: str, price: float, params: dict, state: dict,
                         ind: dict | None) -> list[dict]:
    """V2 止盈（回测实证）：浮盈≥阈值 且 VIX 处 2 年低分位，双条件缺一不可。

    浮盈高+波动率奇低=涨幅大且市场自满，风险积累；VIX 分位是防卖飞过滤器。
    仅 QQQ 生效（回测结论不外推）；VIX 分位缺失（拉取失败/落后基准日）
    不触发（报告同款 NaN 语义）。@author yys"""
    if symbol != "QQQ":
        return []
    if not ind:
        return []      # 无指标快照 → asof 无从谈起，与其它指标依赖规则惯例一致
    cfg = params.get("vix_take_profit", {})
    if not cfg.get("enabled", True):
        return []
    cost = _positions.get(symbol, {}).get("cost")
    if not cost:
        return []
    vix_pctl = daily_bars.get_vix_percentile(ind.get("asof"))
    if vix_pctl is None:
        return []
    profit_pct = (price / cost - 1.0) * 100.0
    want = float(cfg.get("profit_pct", 20.0))
    now = time.time()
    st = state.setdefault("vix_take_profit", {})
    if profit_pct < want - 0.5:
        st.pop("main", None)     # 浮盈离开触发带 → 冷却重置（可再触发）
        return []
    if profit_pct >= want and vix_pctl <= float(cfg.get("vix_pctile_max", 20.0)) \
            and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "vix_take_profit",
            "QQQ 浮盈达标且 VIX 处 2 年低分位，建议止盈",
            "warning",
            {"profit_pct": round(profit_pct, 2), "vix_pctile": round(vix_pctl, 1),
             "cost": cost, "price": price})]
    return []


def eval_deep_dip(symbol: str, price: float, params: dict, state: dict,
                  ind: dict | None) -> list[dict]:
    """V2 深跌回投（回测实证）：距 52 周高回撤≥阈值 → 底部区域提示。

    仅 QQQ 生效；与 extreme_guard 口径独立（52 周高 vs RSI/乖离），
    同轮并存属设计现状。@author yys"""
    if symbol != "QQQ":
        return []
    cfg = params.get("deep_dip", {})
    if not cfg.get("enabled", True) or not ind:
        return []
    week52 = ind.get("week52_high")
    if week52 is None:
        return []
    dd = (price / week52 - 1.0) * 100.0
    dip = float(cfg.get("dip_pct", 15.0))
    now = time.time()
    st = state.setdefault("deep_dip", {})
    if dd > -(dip - 0.5):
        st.pop("main", None)     # 回撤收窄离开触发带 → 冷却重置
        return []
    if dd <= -dip and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "deep_dip",
            "QQQ 距 52 周高点回撤达标，深跌回投窗口",
            "info",
            {"drawdown_pct": round(dd, 2), "week52_high": round(week52, 2),
             "price": price})]
    return []


def eval_ma200_zone(symbol: str, price: float, params: dict, state: dict,
                    ind: dict | None) -> list[dict]:
    """年线区域提醒：深度乖离（bias 键）与贴近年线（touch 键）两个独立子信号。

    与 extreme_guard 的 MA50 乖离口径独立（本规则锚 200 日年线）；
    短历史（enough_history=False）静默禁用，同 atr_stop 惯例。@author yys"""
    cfg = params.get("ma200_zone", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    ma200 = ind.get("ma200")
    if not ma200:
        return []
    bias = (price - ma200) / ma200 * 100.0
    bias_low = float(cfg.get("bias_low", -10.0))
    band = float(cfg.get("band_pct", 2.0))
    events = []
    now = time.time()
    st = state.setdefault("ma200_zone", {})
    if bias > bias_low + 0.5:
        st.pop("bias", None)      # 离开深乖离带 → 冷却重置（0.5 个百分点边际）
    elif bias <= bias_low and _cooldown_ok(st, "bias", now):
        _set_cooldown(st, "bias", now, COOLDOWN_SECONDS)
        events.append(_event(
            symbol, "ma200_zone",
            f"{symbol} 现价较200日线乖离 {bias:+.1f}%，历史级低估区",
            "warning",
            {"bias_pct": round(bias, 2), "ma200": round(ma200, 2), "price": price}))
    if abs(bias) > band + 0.5:
        st.pop("touch", None)     # 离开年线带 → 冷却重置
    elif abs(bias) <= band and _cooldown_ok(st, "touch", now):
        _set_cooldown(st, "touch", now, COOLDOWN_SECONDS)
        events.append(_event(
            symbol, "ma200_zone",
            f"{symbol} 贴近200日线（乖离 {bias:+.1f}%），长线支撑测试",
            "info",
            {"bias_pct": round(bias, 2), "ma200": round(ma200, 2), "price": price}))
    return events


def eval_fib_retrace(symbol: str, price: float, params: dict, state: dict,
                     ind: dict | None) -> list[dict]:
    """斐波那契回撤提醒：252 日上涨波段回撤深度 ≥ 阈值 → 底部区域提示。

    波段口径见 daily_bars.compute_indicators（先低后高才定义回撤，
    下跌延续段 fib=None 静默禁用）。@author yys"""
    cfg = params.get("fib_retrace", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    fib = ind.get("fib")
    if not fib or fib.get("depth") is None:
        return []
    depth = float(fib["depth"])
    want = float(cfg.get("depth", 0.5))
    now = time.time()
    st = state.setdefault("fib_retrace", {})
    if depth < want - 0.05:
        st.pop("main", None)      # 回撤收窄离开触发带 → 冷却重置
        return []
    if depth >= want and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        lv = fib.get("levels") or {}
        return [_event(
            symbol, "fib_retrace",
            f"{symbol} 本轮涨幅已回撤 {depth * 100:.0f}%，斐波那契底部区域关注",
            "info",
            {"depth": round(depth, 3), "swing_high": fib.get("high"),
             "swing_low": fib.get("low"), "fib_382": lv.get("382"),
             "fib_500": lv.get("500"), "fib_618": lv.get("618"), "price": price})]
    return []


def eval_trend(bars: list[dict], params: dict) -> dict | None:
    """MA200 趋势状态（日收盘判定，供日K刷新后调用与指标接口展示）。"""
    if not params.get("trend_filter", {}).get("enabled", True):
        return None
    if len(bars) < daily_bars.MA_SLOW + 1:
        return None
    closes = [float(b["close"]) for b in bars]
    ma = daily_bars.sma(closes, daily_bars.MA_SLOW)
    prev_above = closes[-2] > ma[-2]
    now_above = closes[-1] > ma[-1]
    if prev_above == now_above:
        return {"state": "above" if now_above else "below",
                "ma200": ma[-1], "cross": None}
    return {"state": "above" if now_above else "below", "ma200": ma[-1],
            "cross": "up" if now_above else "down",
            "close": closes[-1]}


async def _daily_dedupe(events: list[dict]) -> list[dict]:
    """自然日去重：同 (symbol, rule) 当日只放行首条（含跨日重置预载）。"""
    global _daily_fired, _daily_date
    today = time.strftime("%Y-%m-%d")
    if _daily_date != today:
        try:
            _daily_fired = await db.get_today_alert_keys()
        except Exception as e:
            logger.warning("当日提醒键集预载失败（沿用旧键集）: %s", e)
        _daily_date = today
    out = []
    for ev in events:
        key = (ev["symbol"], ev["rule"])
        if key in _daily_fired:
            continue
        _daily_fired.add(key)
        out.append(ev)
    return out


async def evaluate(symbol: str, price: float) -> list[dict]:
    """单标的求值：规则评估 → fired_state/提醒双落库。

    全局锁串行化：轮询/push 多入口安全（读-改-写全程持 _EVAL_LOCK）。
    重置分支（无事件）同样落库：否则下轮从库读回旧 state，重置丢失。"""
    async with _EVAL_LOCK:
        rule = await db.get_alert_rule(symbol)
        if rule is not None and not rule.get("enabled", 1):
            return []
        params = _merged_params(rule)
        state = (rule or {}).get("fired_state") or {}
        snapshot = json.dumps(state, sort_keys=True, default=str)
        ind = daily_bars.get_indicators(symbol)
        events = []
        events += eval_price_plan(symbol, price, params, state, ind)
        events += eval_atr_stop(symbol, price, params, state, ind)
        events += eval_extreme_guard(symbol, price, params, state, ind)
        events += eval_vix_take_profit(symbol, price, params, state, ind)
        events += eval_deep_dip(symbol, price, params, state, ind)
        events += eval_ma200_zone(symbol, price, params, state, ind)
        events += eval_fib_retrace(symbol, price, params, state, ind)
        events = await _daily_dedupe(events)
        state_changed = json.dumps(state, sort_keys=True, default=str) != snapshot
        if events or state_changed:
            # 写回库内原始配置（非合并快照），避免固化默认值、
            # 后续调整 DEFAULT_PARAMS 对已触发过的 symbol 失效
            await db.upsert_alert_rule(symbol, (rule or {}).get("params") or {}, state)
            for ev in events:
                ev["id"] = await db.save_alert(ev)                  # 提醒留痕
        return events


async def evaluate_round(prices: dict[str, float]) -> list[dict]:
    """一轮全量求值（轮询循环/push 回调统一入口）。

    市值阈值过滤：持仓市值（现价×数量）低于 ALERT_MIN_MARKET_VALUE
    （sys_config，USD，默认 500）的标的跳过求值——不评估、不提醒、不落库；
    设 0 或负值关闭过滤。阈值读取失败沿用上轮值（与冷却时长语义对称，
    避免关闭过滤的配置在 DB 抖动期间被默认 500 静默覆盖）。@author yys"""
    global COOLDOWN_SECONDS, MIN_MARKET_VALUE
    try:
        hours = float(await db.get_config("ALERT_COOLDOWN_HOURS") or 4)
        COOLDOWN_SECONDS = hours * 3600.0
        MIN_MARKET_VALUE = float(await db.get_config("ALERT_MIN_MARKET_VALUE") or 500)
    except Exception:
        logger.warning("冷却/阈值配置加载失败，沿用当前 %sh / %.0f USD",
                       COOLDOWN_SECONDS / 3600, MIN_MARKET_VALUE)
    await _load_positions()
    events = []
    for sym, price in prices.items():
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        pos = _positions.get(sym)
        if pos is None:
            continue
        if MIN_MARKET_VALUE > 0 and price * pos["qty"] < MIN_MARKET_VALUE:
            logger.debug("市值过滤跳过 %s: 市值 %.0f < 阈值 %.0f",
                         sym, price * pos["qty"], MIN_MARKET_VALUE)
            continue
        try:
            events += await evaluate(sym, price)
        except Exception as e:
            logger.warning("提醒求值异常 %s: %s", sym, e)
    return events
