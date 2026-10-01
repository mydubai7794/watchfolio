"""QDII ETF 条件分支决策模型

4步漏斗 + 加权评分：
  1. 前置状态确认 — 标记昨日溢价变动方向（仅标记，不淘汰）
  2. 宏观情景分类 — 确定剧本(A/B/C)并筛选候选
  3. 微观盘口否决 — 买卖比 < 阈值 → 一票否决
  4. 最终目标选择 — 加权得分最高者胜

加权评分三维度（step2_pass 标的进入评分）：
  - 落差得分 Gap Score   50%  (组均值 - 自身) * 100
  - 盘口得分 Ratio Score  30%  log(buy/sell)
  - 溢价回归 Premium     20%  杀溢价=1.0 持平=0.5 涨溢价=0
"""

import logging
import math
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 期指 → 隔夜指数映射
# ---------------------------------------------------------------------------

FUTURES_TO_OVERNIGHT = {
    "NQ": "纳斯达克",
    "ES": "标普500",
    "YM": "道琼斯",
}


# ---------------------------------------------------------------------------
# 核心决策模型
# ---------------------------------------------------------------------------

def run_decision_model(
    etf_data: list[dict],
    futures_data: dict[str, float | None],
    overnight_data: dict,
    prev_day_changes: dict[str, float] | None = None,
    *,
    futures_beta: float = 1.15,
    scenario_a_fut_drop: float = -0.6,
    scenario_b_ndx_drop: float = -0.5,
    scenario_b_fut_rise: float = 0.3,
    scenario_a_overshot: float = 1.0,
    scenario_b_lag: float = 0.3,
    buysell_ratio_threshold: float = 2.0,
) -> dict:
    """执行4步条件分支决策模型。

    Args:
        etf_data:        ETF 快照列表（含 futures_change 字段）
        futures_data:    {futures_index: change_pct}  如 {"NQ": 1.2}
        overnight_data:  {"纳斯达克": {"change_pct": 0.5, ...}, ...}
        prev_day_changes:{code: yesterday_change_pct}  可为 None
        其余为可配策略参数

    Returns:
        {
            "scenario":          "A"/"B"/"C",
            "scenario_desc":     str,
            "adjusted_fut_change": float | None,
            "etf_data":          list[dict],
            "target":            dict | None,
            "decision":          "buy" | "skip",
            "reason":            str,
        }
    """
    if not etf_data:
        return _empty_result("无ETF数据")

    # ---- 预计算 ratio / premium（存历史用）----
    for etf in etf_data:
        buy = etf.get("buy1_vol", 0)
        sell = max(etf.get("sell1_vol", 0), 1)
        etf["ratio"] = buy / sell if buy > 0 else 0
        price = etf.get("price", 0)
        iopv = etf.get("iopv", 0)
        etf["premium"] = (price - iopv) / iopv * 100 if iopv and price else 0

    # ---- 按 futures_index 分组 ----
    groups: dict[str, list] = defaultdict(list)
    for etf in etf_data:
        fi = etf.get("futures_index", "UNKNOWN")
        groups[fi].append(etf)

    primary_fi = Counter(groups.keys()).most_common(1)[0][0]
    primary_adjusted_fut = None

    for fi, group_etfs in groups.items():
        raw_fut = futures_data.get(fi)
        adjusted_fut = _adjust_futures(raw_fut, futures_beta)

        if fi == primary_fi:
            primary_adjusted_fut = adjusted_fut

        overnight_key = FUTURES_TO_OVERNIGHT.get(fi)
        ndx_change = (
            overnight_data.get(overnight_key, {}).get("change_pct")
            if overnight_key and overnight_data else None
        )

        # Step 2: 宏观情景分类
        scenario, scenario_desc = _classify_scenario(
            adjusted_fut, ndx_change,
            scenario_a_fut_drop, scenario_b_ndx_drop, scenario_b_fut_rise,
        )

        logger.info("期货组 %s: 剧本%s — %s (raw=%.2f%%, adj=%.2f%%)",
                    fi, scenario, scenario_desc,
                    raw_fut or 0, adjusted_fut or 0)

        for etf in group_etfs:
            etf["adjusted_fut"] = round(adjusted_fut, 4) if adjusted_fut is not None else None
            etf["scenario"] = scenario
            etf["scenario_desc"] = scenario_desc

            # ---- Step 1: 前置状态确认 ----
            yesterday_chg = (
                prev_day_changes.get(etf["code"]) if prev_day_changes else None
            )
            pc, tag = _compute_premium_change(yesterday_chg, ndx_change)
            etf["premium_change"] = pc
            etf["premium_tag"] = tag

            # ---- Step 2: 按剧本筛选 ----
            passed, reason = _filter_scenario(
                scenario, etf, adjusted_fut,
                scenario_a_overshot, scenario_b_lag,
            )
            etf["step2_pass"] = passed
            etf["step2_reason"] = reason

            # ---- Step 3: 盘口否决 ----
            buy = etf.get("buy1_vol", 0)
            sell = max(etf.get("sell1_vol", 0), 1)
            ratio = buy / sell if buy > 0 else 0
            etf["buy_sell_ratio"] = round(ratio, 2)
            etf["step3_pass"] = bool(passed) and ratio >= buysell_ratio_threshold

    # ---- Pre-Step 4: 计算加权得分（step2_pass 标的） ----
    qualified = [e for e in etf_data if e.get("step2_pass")]
    if qualified:
        _calc_weighted_scores(etf_data, qualified)

    # ---- Step 4: 最终目标选择（加权得分最高者胜） ----
    surviving = [e for e in etf_data if e.get("step2_pass") and e.get("step3_pass")]

    target = None
    decision = "skip"
    reason = ""

    if not surviving:
        step2_only = [e for e in etf_data if e.get("step2_pass")]
        if not step2_only:
            reason = f"Step2淘汰全部"
        else:
            reason = f"Step3盘口否决(买卖比均<{buysell_ratio_threshold})"
    else:
        target = max(surviving, key=lambda e: e.get("_weighted_score", 0))
        target["is_target"] = True
        decision = "buy"
        reason = (
            f"选中 {target['code']} {target.get('name', '')} "
            f"(gap={target.get('change_pct', 0):.2f}%, "
            f"买卖比={target.get('buy_sell_ratio', 0):.1f}, "
            f"加权={target.get('_weighted_score', 0):.4f})"
        )
        logger.info("Step 4: %s", reason)

    _assign_scores(etf_data, target["code"] if target else None)
    _calc_recommended_prices(etf_data)

    # 主剧本
    primary_scenario = etf_data[0].get("scenario", "C") if etf_data else "C"
    primary_desc = etf_data[0].get("scenario_desc", "") if etf_data else ""

    return _build_result(
        primary_scenario, primary_desc, primary_adjusted_fut,
        etf_data, target, decision, reason,
    )


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------

def _adjust_futures(raw: float | None, beta: float) -> float | None:
    """期指基差修正：adjusted = raw × beta"""
    if raw is None:
        return None
    return raw * beta


def _compute_premium_change(
    etf_yesterday_change: float | None,
    overnight_change: float | None,
) -> tuple[float | None, str]:
    """Step 1: 昨日溢价变动 = 昨日ETF涨跌幅 - 昨夜指数涨跌幅"""
    if etf_yesterday_change is None or overnight_change is None:
        return None, "数据不足"
    pc = round(etf_yesterday_change - overnight_change, 4)
    if pc > 0.03:
        return pc, "涨溢价"
    if pc < -0.03:
        return pc, "杀溢价"
    return pc, "持平"


def _classify_scenario(
    adjusted_fut: float | None,
    ndx_change: float | None,
    a_drop: float,
    b_ndx_drop: float,
    b_fut_rise: float,
) -> tuple[str, str]:
    """Step 2: 宏观情景分类"""
    if adjusted_fut is not None and adjusted_fut < a_drop:
        return "A", f"期指大跌破位(adjusted={adjusted_fut:.2f}%)"
    if (ndx_change is not None and ndx_change < b_ndx_drop
            and adjusted_fut is not None and adjusted_fut > b_fut_rise):
        return "B", f"日内反转(ndx={ndx_change:.2f}%, fut_adj={adjusted_fut:.2f}%)"
    return "C", "常规震荡/上涨"


def _filter_scenario(
    scenario: str,
    etf: dict,
    adjusted_fut: float | None,
    a_overshot: float,
    b_lag: float,
) -> tuple[bool, str]:
    """Step 2: 按剧本筛选单个 ETF"""
    # open_gap ≈ change_pct（集合竞价阶段即为开盘跳空）
    open_gap = etf.get("change_pct", 0)

    if scenario == "A":
        if adjusted_fut is None:
            return False, "剧本A: 期指数据缺失"
        threshold = adjusted_fut - a_overshot
        if open_gap < threshold:
            return True, f"极端错杀(gap={open_gap:.2f}%<thresh={threshold:.2f}%)"
        return False, f"未达错杀(gap={open_gap:.2f}%≥thresh={threshold:.2f}%)"

    if scenario == "B":
        if adjusted_fut is None:
            return True, "剧本B: 期指缺失,默认通过"
        lag = open_gap - adjusted_fut
        if lag < -b_lag:
            return True, f"落后修复(lag={lag:.2f}%<-{b_lag}%)"
        return False, f"未达落后(lag={lag:.2f}%≥-{b_lag}%)"

    # C: 全部通过
    return True, "常规模式"


def _calc_weighted_scores(
    all_etfs: list[dict], qualified: list[dict],
):
    """为 step2_pass 标的计算三维度加权吸引力指数。

    维度与权重：
      Gap Score   50%  (Group_Avg_Gap - Self_Gap) * 100  越掉队分越高
      Ratio Score 30%  log(buy1_vol / sell1_vol)          盘口厚度
      Premium     20%  杀溢价=1.0 持平=0.5 涨溢价=0       均值回归空间

    每个维度做 min-max 归一化到 [0,1] 后加权，结果 [0,1]。
    """
    if not qualified:
        return

    # 按 futures_index 计算组平均跳空（用全部标的，不仅 step2_pass）
    grp_gaps: dict[str, list[float]] = defaultdict(list)
    for etf in all_etfs:
        fi = etf.get("futures_index", "UNKNOWN")
        grp_gaps[fi].append(etf.get("change_pct", 0))

    grp_avg: dict[str, float] = {}
    for fi, gaps in grp_gaps.items():
        grp_avg[fi] = sum(gaps) / len(gaps)

    # 计算原始分
    for etf in qualified:
        fi = etf.get("futures_index", "UNKNOWN")
        avg_gap = grp_avg.get(fi, 0)
        self_gap = etf.get("change_pct", 0)

        etf["_raw_gap"] = (avg_gap - self_gap) * 100   # 正 = 掉队
        etf["_raw_ratio"] = math.log(max(etf.get("ratio", 0), 1))  # log(1)=0

        tag = etf.get("premium_tag", "")
        etf["_raw_premium"] = (
            1.0 if tag == "杀溢价" else (0.5 if tag == "持平" else 0.0)
        )

    # Min-max 归一化到 [0,1]
    def _normalize(items, key):
        vals = [e[key] for e in items]
        lo, hi = min(vals), max(vals)
        rng = hi - lo if hi != lo else 1.0
        for e in items:
            e[key + "_norm"] = (e[key] - lo) / rng

    _normalize(qualified, "_raw_gap")
    _normalize(qualified, "_raw_ratio")

    for etf in qualified:
        ws = (
            etf["_raw_gap_norm"] * 0.5
            + etf["_raw_ratio_norm"] * 0.3
            + etf["_raw_premium"] * 0.2
        )
        etf["_weighted_score"] = round(ws, 4)

        # 清理临时字段
        for k in ("_raw_gap", "_raw_ratio", "_raw_premium",
                   "_raw_gap_norm", "_raw_ratio_norm"):
            etf.pop(k, None)


def _assign_scores(etf_data: list[dict], target_code: str | None = None):
    """为 ETF 分配连续 score 字段。

    映射规则：
      target                → 1.0
      step2+step3 通过       → 0.50 + ws * 0.45  (范围 0.50-0.95)
      仅 step2 通过          → 0.10 + ws * 0.35  (范围 0.10-0.45)
      step2 淘汰             → 0.0
    """
    for etf in etf_data:
        ws = etf.pop("_weighted_score", None)
        if etf.get("code") == target_code:
            etf["score"] = 1.0
        elif etf.get("step2_pass") and etf.get("step3_pass"):
            etf["score"] = round(0.50 + (ws or 0) * 0.45, 4)
        elif etf.get("step2_pass"):
            etf["score"] = round(0.10 + (ws or 0) * 0.35, 4)
        else:
            etf["score"] = 0.0


def _calc_recommended_prices(etf_data: list[dict]):
    """计算推荐操作价位（Buy/Sell），精度统一 3 位小数。

    三档推荐：
      高分 (>0.8) → Buy 当前价 + 0.001（高挂抢筹）
      中分 (0.4~0.8) → Buy 当前价（随缘挂单）
      低分 (≤0.4) → Sell min(理论回归价, 机械止盈价)
    """
    if not etf_data:
        return

    avg_change = sum(e.get("change_pct", 0) for e in etf_data) / len(etf_data)

    for etf in etf_data:
        score = etf.get("score", 0)
        price = etf.get("price", 0)
        pre_close = etf.get("pre_close", 0)

        if score > 0.8:
            etf["rec_action"] = "Buy"
            etf["rec_price"] = round(price + 0.001, 3)
        elif score > 0.4:
            etf["rec_action"] = "Buy"
            etf["rec_price"] = round(price, 3)
        else:
            etf["rec_action"] = "Sell"
            target_price = pre_close * (1 + avg_change / 100)
            min_sell = price + 0.002
            etf["rec_price"] = round(min(target_price, min_sell), 3)


def _empty_result(reason: str) -> dict:
    return {
        "scenario": "C", "scenario_desc": "无数据",
        "adjusted_fut_change": None,
        "etf_data": [], "target": None,
        "decision": "skip", "reason": reason,
    }


def _build_result(scenario, scenario_desc, adjusted_fut,
                  etf_data, target, decision, reason):
    return {
        "scenario": scenario,
        "scenario_desc": scenario_desc,
        "adjusted_fut_change": round(adjusted_fut, 4) if adjusted_fut is not None else None,
        "etf_data": etf_data,
        "target": target,
        "decision": decision,
        "reason": reason,
    }
