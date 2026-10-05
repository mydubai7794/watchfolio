"""四类规则历史回测：daily_bars 日K回放，触发统计与后视收益。

与 2026-10-04 实证回测同口径（附件报告）。@author yys
"""
import alert_engine
import daily_bars as dbar


def _fwd_ret(bars: list[dict], i: int, n: int) -> float | None:
    """i 日收盘后 n 个交易日收益。

    内部对 close 做 float() 归一：线上库列为 DECIMAL（返回 Decimal），
    Decimal 参与后续与 float 权益曲线混算会 TypeError。
    """
    j = i + n
    if j >= len(bars):
        return None
    return float(bars[j]["close"]) / float(bars[i]["close"]) - 1.0


def backtest_trend(bars: list[dict]) -> dict:
    """MA200 上穿买点：5/10/20 日胜率与均收 + MA200 过滤回撤对照。"""
    closes = [float(b["close"]) for b in bars]
    if len(closes) < dbar.MA_SLOW + 1:
        return {"triggers": [], "note": "insufficient_history"}
    ma = dbar.sma(closes, dbar.MA_SLOW)
    triggers, rets = [], {"5": [], "10": [], "20": []}
    for i in range(dbar.MA_SLOW, len(bars) - 1):
        if ma[i - 1] is None or ma[i] is None:
            continue
        if closes[i - 1] <= ma[i - 1] < closes[i] or \
           (closes[i] > ma[i] and closes[i - 1] <= ma[i - 1]):
            triggers.append({"date": str(bars[i]["date"])[:10], "price": closes[i]})
            for n, key in ((5, "5"), (10, "10"), (20, "20")):
                r = _fwd_ret(bars, i, n)
                if r is not None:
                    rets[key].append(r)
    # 回撤对照：全程持有 vs 仅收盘>MA200 持有
    def _max_dd(equity):
        peak, dd = equity[0], 0.0
        for v in equity:
            peak = max(peak, v)
            dd = max(dd, 1 - v / peak)
        return dd
    eq_all, eq_flt = [1.0], [1.0]
    for i in range(1, len(bars)):
        r = closes[i] / closes[i - 1] - 1.0
        eq_all.append(eq_all[-1] * (1 + r))
        # MA 未就绪头段不视为持有（None or 0 会恒真，污染回撤对照）
        held = ma[i - 1] is not None and closes[i - 1] > ma[i - 1]
        eq_flt.append(eq_flt[-1] * (1 + (r if held else 0.0)))
    return {
        "triggers": triggers, "count": len(triggers),
        **{f"win{k}": (sum(1 for r in v if r > 0) / len(v) if v else None) for k, v in rets.items()},
        **{f"avg{k}": (sum(v) / len(v) if v else None) for k, v in rets.items()},
        "max_dd_all": _max_dd(eq_all), "max_dd_filtered": _max_dd(eq_flt),
        # filtered 曲线自第 MA_SLOW 根起 ma[i-1] 才首个非 None，此前无信号按空仓计
        "filtered_from": dbar.MA_SLOW}


def backtest_atr(bars: list[dict], k: float = 3.0) -> dict:
    """Chandelier 止损：入场=MA200 上穿，出场=收盘跌破 HHV22−k×ATR22。"""
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    ma = dbar.sma(closes, dbar.MA_SLOW)
    atr = dbar.atr_wilder(bars)
    hh = dbar.hhv(highs)
    trades = []
    entry = None
    for i in range(dbar.MA_SLOW, len(bars)):
        stop = None if hh[i] is None else hh[i] - k * atr[i]
        if entry is None:
            if ma[i] is not None and ma[i - 1] is not None and \
               closes[i - 1] <= ma[i - 1] < closes[i]:
                entry = (i, closes[i])
        elif stop is not None and closes[i] < stop:
            ret = closes[i] / entry[1] - 1.0
            recov = max(highs[i + 1: i + 21], default=None)
            trades.append({"entry": str(bars[entry[0]]["date"])[:10],
                           "exit": str(bars[i]["date"])[:10],
                           "ret": round(ret, 4), "days": i - entry[0],
                           "false_stop": bool(recov and recov > highs[entry[0]])})
            entry = None
    rets = [t["ret"] for t in trades]
    # 尾部未平仓披露（循环结束 entry 非 None = 仍在持仓，未到止损）
    open_pos = None
    if entry is not None:
        open_pos = {"entry": str(bars[entry[0]]["date"])[:10],
                    "days": len(bars) - 1 - entry[0]}
    return {"trades": trades, "count": len(trades),
            "win_rate": sum(1 for r in rets if r > 0) / len(rets) if rets else None,
            "avg_ret": sum(rets) / len(rets) if rets else None,
            "avg_days": sum(t["days"] for t in trades) / len(trades) if trades else None,
            "open_position": open_pos}


def backtest_extreme(bars: list[dict]) -> dict:
    """RSI14 极值信号后 5/10/20 日收益（验证超卖加仓/追高无效）。"""
    closes = [float(b["close"]) for b in bars]
    rsi = dbar.rsi_wilder(closes)
    out = {"oversold": {"count": 0, "win5": None}, "overbought": {"count": 0, "win5": None}}
    for side, lo, hi in (("oversold", None, 30.0), ("overbought", 70.0, None)):
        rets = []
        for i in range(len(bars) - 5):
            v = rsi[i]
            if v is None:
                continue
            if (hi is not None and v <= hi) or (lo is not None and v >= lo):
                r = _fwd_ret(bars, i, 5)
                if r is not None:
                    rets.append(r)
        out[side] = {"count": len(rets),
                     "win5": sum(1 for r in rets if r > 0) / len(rets) if rets else None,
                     "avg5": sum(rets) / len(rets) if rets else None}
    return out


def backtest_levels(bars: list[dict], pcts: list[float],
                    horizon: int = 60) -> dict:
    """±档位 horizon 日内触及概率（用 low/high 判定）。

    key 取 str(pct)（如 "-5.0"/"8.0"），与档位配置原值对齐。
    """
    out = {}
    n = len(bars)
    for pct in pcts:
        touched = total = 0
        avg_days = []
        for i in range(n - 1):
            base = float(bars[i]["close"])
            target = base * (1 + pct / 100.0)
            window = bars[i + 1: i + 1 + horizon]
            if not window:
                break
            total += 1
            for j, b in enumerate(window):
                hit = float(b["low"]) <= target if pct < 0 else float(b["high"]) >= target
                if hit:
                    touched += 1
                    avg_days.append(j + 1)
                    break
        key = str(pct)
        out[key] = {"touch_prob": touched / total if total else None,
                    "avg_days": sum(avg_days) / len(avg_days) if avg_days else None}
    return out


def run(symbol: str, rule: str, params: dict, bars: list[dict],
        days: int = 750) -> dict:
    """API 分发入口。params 兼容嵌套（与 alert_rules 存储同构）与平铺两种形态。"""
    bars = bars[-days:]
    p = params or {}

    def _sub(key: str) -> dict:
        # 嵌套 {"atr_stop": {...}} 取子包；平铺（无该键或值非 dict）整包视为子包
        v = p.get(key)
        return v if isinstance(v, dict) else p

    if rule == "trend_filter":
        return backtest_trend(bars)
    if rule == "atr_stop":
        k = _sub("atr_stop").get("k")
        return backtest_atr(bars, k=float(k) if k is not None
                            else alert_engine.DEFAULT_PARAMS["atr_stop"]["k"])
    if rule == "extreme_guard":
        return backtest_extreme(bars)
    if rule == "price_plan":
        levels = _sub("price_plan").get("levels")
        return backtest_levels(bars, levels or
                               alert_engine.DEFAULT_PARAMS["price_plan"]["levels"])
    return {"error": f"unknown rule: {rule}"}
