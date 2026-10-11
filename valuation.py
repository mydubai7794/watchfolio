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


async def run_valuation_round() -> dict:
    """估值轮：长桥批量静态信息（日更）+ EDGAR companyconcept 增速（周更
    门控：周一或距上次成功 >6 天或首跑）→ 双节奏 upsert。

    stats: rows=写入行数（US 标的，含门控行）/ fund=ETF 门控 /
    no_data=长桥数值缺失 / no_cik=无 CIK 映射 / growth_runs=本轮是否拉增速。
    @author yys"""
    holdings = await db.get_holdings(enabled_only=True)
    us = [h for h in holdings if h.get("market") == "US"
          and h["symbol"].upper() not in edgar_fetcher._EDGAR_EXCLUDE]
    stats = {"rows": 0, "fund": 0, "no_data": 0, "no_cik": 0,
             "growth_runs": False}
    infos = await longport_fetcher.fetch_static_info(
        [h["symbol"] for h in us])
    sinfo = normalize_static_info(infos)

    ts = await db.get_config("VALUATION_GROWTH_TS")
    run_growth = True
    if ts:
        try:
            last = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            age = (datetime.now() - last).total_seconds()
            run_growth = datetime.now().weekday() == 0 \
                or age > GROWTH_INTERVAL_SEC
        except ValueError:
            run_growth = True
    tmap = await edgar_fetcher.get_ticker_map() if run_growth else {}
    growth_written = False
    for h in us:
        sym = h["symbol"]
        try:
            s = sinfo.get(sym) or {}
            eps_ttm, bps = s.get("eps_ttm"), s.get("bps")
            dy = s.get("dividend_yield")
            if h.get("asset_type") == "etf":
                eps_ttm = bps = dy = None   # ETF 字段噪声，置空落库覆盖
                stats["fund"] += 1
            # 门控行同样落库（QQQ 置 None 覆盖历史脏值）；eps/bps 缺失或 0 计 no_data
            await db.upsert_holding_valuation_static(sym, eps_ttm, bps, dy)
            if not eps_ttm or not bps:
                stats["no_data"] += 1
            if run_growth and h.get("asset_type") in ("stock", "fpi") \
                    and eps_ttm and bps and eps_ttm > 0 and bps > 0:
                cik = tmap.get(sym.upper()) \
                    or edgar_fetcher._CIK_OVERRIDES.get(sym.upper())
                if cik is None:
                    stats["no_cik"] += 1
                else:
                    text = await edgar_fetcher.fetch_companyconcept(cik)
                    g = parse_eps_growth(text) if text else None
                    if g is not None:
                        await db.upsert_holding_valuation_growth(sym, g)
                        growth_written = True
            stats["rows"] += 1
        except Exception as e:
            logger.warning("估值处理 %s 失败（仅跳过该标的）: %s", sym, e)
            stats["rows"] += 1
    stats["growth_runs"] = run_growth
    if run_growth and growth_written:
        await db.set_config("VALUATION_GROWTH_TS",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "估值增速周更水位")
    return stats
