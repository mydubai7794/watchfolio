"""估值因子层：长桥 static_info 日更（eps_ttm/bps/股息率）+ EDGAR
companyconcept 周更（EPS 同比增速）→ holding_valuations 落库。

口径契约（2026-10-11 实测锚定，详见 specs/2026-10-11-valuation-factors-design.md）：
- 长桥 eps_ttm/bps 为券商换算后 USD/ADS 口径（BABA 无需换算）；
- ETF 静态字段是噪声（QQQ eps=-88/SMH 全 0），asset_type 门控拦截；
- 增速取 companyconcept 主币种（行数最多 unit）最近两个完整财年同比，
  原币比值货币无关；pe/pb/peg 不落库，注入时用现价实时算。@author yys
"""
import asyncio
import json
import logging
from datetime import datetime

import database as db
import edgar_fetcher
import longport_fetcher

logger = logging.getLogger("qdii.valuation")

GROWTH_INTERVAL_SEC = 6 * 86400      # 增速周更节流（>6 天必跑）
_FY_FORMS = ("10-K", "20-F", "40-F")


def normalize_static_info(infos) -> dict:
    """长桥 SecurityStaticInfo 列表 → {sym: {eps_ttm, bps, dividend_yield}}。
    Decimal→float；0/负值保留原值（ETF 噪声门控在上层）。@author yys"""
    out = {}
    for q in infos or []:
        sym = str(getattr(q, "symbol", "")).replace(".US", "", 1)
        if not sym:
            continue

        def _f(name):
            v = getattr(q, name, None)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        out[sym] = {"eps_ttm": _f("eps_ttm"), "bps": _f("bps"),
                    "dividend_yield": _f("dividend_yield")}
    return out


def parse_eps_growth(text) -> float | None:
    """companyconcept JSON → 最近两个完整财年 EPS 同比%（主币种 unit）。

    行过滤：form ∈ {10-K,20-F,40-F} 且 fp=FY（fp 过滤季度/累计值混入）；
    同财年多行（重复申报）取 filed 最大行；财年不足两个 / 基期 0 /
    解析失败 → None。@author yys"""
    try:
        d = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    units = d.get("units") or {}
    if not units:
        return None
    rows = max(units.values(), key=len) or []
    fy: dict[tuple, tuple] = {}      # (start年,end年) -> (filed, val)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if (r.get("form") not in _FY_FORMS or r.get("fp") != "FY"
                or not r.get("start") or not r.get("end")
                or r.get("val") is None):
            continue
        key = (r["start"][:4], r["end"][:4])
        cand = (str(r.get("filed", "")), float(r["val"]))
        if key not in fy or cand[0] >= fy[key][0]:
            fy[key] = cand
    if len(fy) < 2:
        return None
    ordered = sorted(fy.items(), key=lambda kv: kv[0][1])
    v1 = ordered[-2][1][1]
    v2 = ordered[-1][1][1]
    if v1 == 0:
        return None
    return round((v2 / v1 - 1) * 100, 4)
