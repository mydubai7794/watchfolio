"""得分计算模块：归一化、评分、期货杀跌、ETF 排名。"""


def normalize(values):
    """Min-Max 归一化到 [0, 1]。

    - 空列表返回 []
    - 所有值相同时返回 [1.0, ...]
    """
    if not values:
        return []
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [1.0] * len(values)
    return [(v - min_v) / (max_v - min_v) for v in values]


def calc_score(ratio_norm, premium_norm, w_ratio=0.7, w_premium=0.3):
    """加权评分：w_ratio * ratio_norm - w_premium * premium_norm"""
    return w_ratio * ratio_norm - w_premium * premium_norm


def apply_futures_kill(score, etf_change_pct, futures_change_pct, threshold, enabled):
    """期货杀跌逻辑。

    当 enabled=True 且 futures_change_pct < threshold 且 etf_change_pct > 0 时，
    返回 0.0（杀跌），否则原样返回 score。
    """
    if not enabled:
        return score
    if futures_change_pct is None:
        return score
    if etf_change_pct > 0 and futures_change_pct < threshold:
        return 0.0
    return score


def rank_etfs(etf_data, w_ratio=0.7, w_premium=0.3,
              futures_kill_enabled=False, futures_kill_threshold=-1.5):
    """对 ETF 列表计算评分并按分数降序排列。

    每条 ETF dict 需包含: code, buy1_vol, sell1_vol, price, iopv,
    change_pct, futures_change。

    返回排序列表，每项额外附加 score, ratio, premium 字段。
    """
    if not etf_data:
        return []

    # 计算每只 ETF 的 ratio 和 premium
    for etf in etf_data:
        buy = etf["buy1_vol"]
        sell = etf["sell1_vol"]
        price = etf["price"]
        iopv = etf["iopv"]

        # ratio = buy1_vol / max(sell1_vol, 1)，buy1_vol==0 则 ratio=0
        if buy == 0:
            etf["_ratio"] = 0
        else:
            etf["_ratio"] = buy / max(sell, 1)

        # premium = (price - iopv) / iopv * 100，iopv==0 或 price==0 则 premium=0
        if iopv == 0 or price == 0:
            etf["_premium"] = 0
        else:
            etf["_premium"] = (price - iopv) / iopv * 100

    # 归一化 ratio（正向：越大越好）
    ratios = [e["_ratio"] for e in etf_data]
    ratios_norm = normalize(ratios)

    # 归一化 inverted premium：premium 越小越好，所以取负值再归一化
    premiums = [e["_premium"] for e in etf_data]
    inv_premiums = [-p for p in premiums]
    inv_premiums_norm = normalize(inv_premiums)

    for i, etf in enumerate(etf_data):
        score = calc_score(ratios_norm[i], inv_premiums_norm[i],
                           w_ratio=w_ratio, w_premium=w_premium)

        # buy1_vol==0 强制 score=0
        if etf["buy1_vol"] == 0:
            score = 0.0

        # 期货杀跌
        score = apply_futures_kill(
            score,
            etf_change_pct=etf["change_pct"],
            futures_change_pct=etf["futures_change"],
            threshold=futures_kill_threshold,
            enabled=futures_kill_enabled,
        )

        etf["score"] = score
        etf["ratio"] = etf.pop("_ratio")
        etf["premium"] = etf.pop("_premium")

    # 按分数降序排列
    etf_data.sort(key=lambda e: e["score"], reverse=True)
    return etf_data
