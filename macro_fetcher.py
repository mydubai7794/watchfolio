"""宏观估值指标日频抓取：巴菲特/席勒CAPE/美债2s10s利差/恐慌贪婪。

网络策略：直连优先 → 代理池前 2（latency 升序，同 Yahoo backfill 候选模式）
→ 全失败当日放弃（次日再试），展示沿用上次落库值，不放大请求。
落库幂等键 name+asof。@author yys
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta

import httpx

import database as db

logger = logging.getLogger("qdii.macro")

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"
MULTPL_CAPE_URL = "https://www.multpl.com/shiller-pe/table-json"
CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Accept": "application/json,text/csv,text/html,*/*",
}


async def _proxy_candidates() -> list[str | None]:
    """直连优先 + 代理池前 2。@author yys"""
    try:
        rows = await db.get_alive_proxies()
        urls = [r["url"] for r in rows if r.get("url")][:2]
    except Exception:
        urls = []
    return [None] + urls


async def _get_text(url: str) -> str | None:
    """直连→代理容错取响应文本；全失败返回 None。@author yys"""
    last_err = ""
    for proxy in await _proxy_candidates():
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy,
                                         follow_redirects=True,
                                         headers=UA_HEADERS) as client:
                resp = await client.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("macro 抓取失败 %s: %s", url, last_err)
    return None


def parse_fred_csv(text: str) -> tuple[str, float] | None:
    """FRED CSV → (最新日期, 值)；'.' 与空值行跳过（自后向前找最新有效行）。@author yys"""
    lines = [l.strip() for l in text.strip().splitlines()]
    for line in reversed(lines[1:]):          # 跳过表头
        parts = line.split(",")
        if len(parts) != 2:
            continue
        d, v = parts
        if v in (".", ""):
            continue
        try:
            return d, float(v)
        except ValueError:
            continue
    return None


def parse_multpl_cape(text: str) -> tuple[str, float] | None:
    """multpl CAPE JSON（[{date,value},...] 最近在前）→ 首条。@author yys"""
    try:
        arr = json.loads(text)
        item = arr[0]
        return str(item["date"])[:10], float(item["value"])
    except Exception:
        return None


def parse_cnn_fg(text: str) -> tuple[float, str] | None:
    """CNN graphdata JSON → (score, rating)。@author yys"""
    try:
        fg = (json.loads(text).get("fear_greed") or {})
        return float(fg["score"]), str(fg.get("rating", ""))
    except Exception:
        return None


async def fetch_buffett() -> dict | None:
    """巴菲特指标 = Wilshire5000 / 年化GDP ×100%（FRED 两 CSV）。@author yys"""
    w = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid="WILL5000PRFC")) or "")
    g = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid="GDP")) or "")
    if not w or not g or g[1] <= 0:
        return None
    return {"name": "buffett", "asof": w[0], "value": round(w[1] / g[1] * 100.0, 1),
            "extra": {"wilshire": w[1], "gdp": g[1],
                      "wilshire_asof": w[0], "gdp_asof": g[0]}}


async def fetch_cape() -> dict | None:
    """席勒 CAPE（multpl 主源）。@author yys"""
    got = parse_multpl_cape(await _get_text(MULTPL_CAPE_URL) or "")
    if not got:
        return None
    return {"name": "cape", "asof": got[0], "value": round(got[1], 2), "extra": {}}


async def fetch_yield_spread() -> dict | None:
    """美债 2s10s 利差 = DGS10 − DGS2（FRED 两 CSV，pct）。@author yys"""
    t10 = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid="DGS10")) or "")
    t2 = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid="DGS2")) or "")
    if not t10 or not t2:
        return None
    return {"name": "yield_2s10s", "asof": min(t10[0], t2[0]),
            "value": round(t10[1] - t2[1], 3),
            "extra": {"dgs10": t10[1], "dgs2": t2[1]}}


async def fetch_fear_greed() -> dict | None:
    """恐慌贪婪：CNN 主源；失败回退 VIX 2 年分位合成（extra.source 标注）。@author yys"""
    asof = datetime.now().strftime("%Y-%m-%d")
    got = parse_cnn_fg(await _get_text(CNN_FG_URL) or "")
    if got:
        return {"name": "fear_greed", "asof": asof, "value": round(got[0], 1),
                "extra": {"rating": got[1], "source": "cnn"}}
    import daily_bars
    pctl = daily_bars.get_vix_percentile()
    if pctl is None:
        return None
    return {"name": "fear_greed", "asof": asof, "value": round(100.0 - pctl, 1),
            "extra": {"source": "fallback_vix_pctl"}}


FETCHERS = (fetch_buffett, fetch_cape, fetch_yield_spread, fetch_fear_greed)


async def refresh_all():
    """四指标串行刷新落库（间隔 2s；单项失败不中断）。@author yys"""
    for i, fn in enumerate(FETCHERS):
        if i:
            await asyncio.sleep(2.0)
        try:
            r = await fn()
            if r:
                await db.upsert_macro_indicator(r["name"], r["asof"],
                                                r["value"], r["extra"])
                logger.info("macro %s = %.2f (%s)", r["name"], r["value"], r["asof"])
            else:
                logger.warning("macro %s 本轮无数据", fn.__name__)
        except Exception as e:
            logger.warning("macro %s 失败: %s", fn.__name__, e)


async def macro_daily_loop():
    """后台任务：启动即刷一次，此后每日北京时间 22:40（与 ledger_fx 22:35 错峰）。@author yys"""
    while True:
        try:
            await refresh_all()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("macro 循环异常: %s", e)
        now = datetime.now()
        target = now.replace(hour=22, minute=40, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
