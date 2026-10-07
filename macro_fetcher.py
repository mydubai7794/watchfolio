"""宏观估值指标日频抓取：巴菲特/席勒CAPE/美债2s10s利差/恐慌贪婪。

网络策略：直连优先 → 代理池前 2（latency 升序，同 Yahoo backfill 候选模式）
→ 全失败当日放弃（次日再试），展示沿用上次落库值，不放大请求。
落库幂等键 name+asof。@author yys
"""
import asyncio
import json
import logging
import re
from datetime import datetime, timedelta

import httpx

import database as db

logger = logging.getLogger("qdii.macro")

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"
MULTPL_CAPE_URL = "https://www.multpl.com/shiller-pe/table/by-year"
CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
# FRED 实测歧视「伪造浏览器 UA 缺配套指纹头」的请求（Chrome UA 挂起 ReadTimeout、
# 默认 python-httpx UA 直连 200），故 FRED 不配浏览器头、用默认 UA
FRED_HEADERS: dict = {}
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json,text/csv,text/html,*/*",
}
# CNN 短 UA 会得到 200+teapot 反爬文本，需完整 UA 串 + Referer 才返回真 JSON；
# 且境内代理中转出口会被 CNN 降级响应（score 缺失），故 CNN 只直连不走代理
CNN_HEADERS = {**BROWSER_HEADERS,
               "Referer": "https://edition.cnn.com/markets/fear-and-greed"}
# 巴菲特指标序列：原 Wilshire 系列（WILL5000PRFC 等）已从 FRED 下架（404），
# 改用非金融企业市值（NCBEILQ027S，单位百万$）÷ GDP（单位十亿$，差 1000 倍）
BUFFETT_MCAP_SID = "NCBEILQ027S"
BUFFETT_GDP_SID = "GDP"


async def _proxy_candidates() -> list[str | None]:
    """直连优先 + 代理池前 2。@author yys"""
    try:
        rows = await db.get_alive_proxies()
        urls = [r["url"] for r in rows if r.get("url")][:2]
    except Exception:
        urls = []
    return [None] + urls


async def _get_text(url: str, headers: dict | None = None,
                    allow_proxy: bool = True) -> str | None:
    """直连→代理容错取响应文本（headers=None 用 httpx 默认 UA；
    allow_proxy=False 仅直连，用于代理出口会被目标站降级的源）。@author yys"""
    last_err = ""
    candidates = await _proxy_candidates() if allow_proxy else [None]
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy,
                                         follow_redirects=True,
                                         headers=headers or {}) as client:
                resp = await client.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = repr(e)
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
    """multpl by-year 表格 HTML → (最新日期, 值)；首数据行即最新，值列含 &#x2002; 实体。@author yys"""
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S):
        cells = [re.sub(r"<[^>]+>|&#x?[0-9a-fA-F]+;|\s+", "", c)
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
        if len(cells) != 2:
            continue
        d, v = cells
        try:
            asof = datetime.strptime(d, "%b%d,%Y").strftime("%Y-%m-%d")
            return asof, float(v)
        except ValueError:
            continue
    return None


def parse_cnn_fg(text: str) -> tuple[float, str] | None:
    """CNN graphdata JSON → (score, rating)。顶层键必须是 fear_and_greed
    （曾误写 fear_greed 少 _and，对真实响应永远 miss 返回 None）。@author yys"""
    try:
        d = json.loads(text)
        fg = d.get("fear_and_greed") or {}
        return float(fg["score"]), str(fg.get("rating", ""))
    except Exception:
        return None


async def fetch_buffett() -> dict | None:
    """巴菲特指标 = 非金融企业市值(NCBEILQ027S,百万$) / GDP(十亿$) ×100%。
    Wilshire 系列已从 FRED 下架故改此口径；asof 取两序列较旧者。@author yys"""
    m = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid=BUFFETT_MCAP_SID)) or "")
    g = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid=BUFFETT_GDP_SID)) or "")
    if not m or not g or g[1] <= 0:
        return None
    return {"name": "buffett", "asof": min(m[0], g[0]),
            "value": round(m[1] / 1_000.0 / g[1] * 100.0, 1),
            "extra": {"mcap_musd": m[1], "gdp_busd": g[1],
                      "mcap_asof": m[0], "gdp_asof": g[0]}}


async def fetch_cape() -> dict | None:
    """席勒 CAPE（multpl 主源，by-year HTML 表格）。@author yys"""
    got = parse_multpl_cape(await _get_text(MULTPL_CAPE_URL, headers=BROWSER_HEADERS) or "")
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
    """恐慌贪婪：CNN 主源（仅直连，响应偶发降级缺失 score 时隔 2s 重试一次）；
    仍失败回退 VIX 2 年分位合成（extra.source 标注）。@author yys"""
    asof = datetime.now().strftime("%Y-%m-%d")
    got = parse_cnn_fg(await _get_text(CNN_FG_URL, headers=CNN_HEADERS,
                                       allow_proxy=False) or "")
    if not got:
        await asyncio.sleep(2.0)
        got = parse_cnn_fg(await _get_text(CNN_FG_URL, headers=CNN_HEADERS,
                                           allow_proxy=False) or "")
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
