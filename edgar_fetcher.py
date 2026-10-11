"""EDGAR 内部人交易（Form 4）日频抓取：探测分类 + 限流拉取 + 纯解析聚合。

数据源：SEC EDGAR 官方免费接口（company_tickers 映射 / submissions 索引 /
Form 4 XML）。限流契约（以稳为主）：全程串行、任意两请求间隔 ≥2s（官方上限
10 req/s 的 1/20）；429/503 指数退避 2s→4s→8s 最多重试 3 次；网络异常仅重试
1 次。@author yys
"""
import asyncio
import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import httpx

import database as db

logger = logging.getLogger("qdii.edgar")

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
EDGAR_MIN_INTERVAL = 2.0          # 请求最小间隔（秒）
EDGAR_BACKOFFS = (2.0, 4.0, 8.0)  # 429/503 退避序列
# SEC 公平访问政策要求 UA 声明联系方式（名称 + email）；默认值仅最后兜底，
# 生产 UA 经 sys_config.EDGAR_UA 配置（网络层热取，见 _resolve_ua）
DEFAULT_UA = "QDII Portfolio Monitor"


def parse_ticker_map(text: str) -> dict[str, int]:
    """company_tickers.json → {ticker大写: cik}；坏行跳过。@author yys"""
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        return {}
    out = {}
    for v in (d or {}).values():
        try:
            out[str(v["ticker"]).upper()] = int(v["cik_str"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def classify_asset_type(form_types: list[str]) -> str | None:
    """按申报类型集判定资产分类：20-F→fpi / 10-K,10-Q→stock / N系→etf。
    三类互斥（同一发行人不会同时提交），fpi 优先判定；都不含返回 None 下轮再试。@author yys"""
    s = set(form_types or [])
    if any((f or "").startswith("20-F") for f in s):
        return "fpi"
    if any((f or "").startswith("10-K") or (f or "").startswith("10-Q")
           for f in s):
        return "stock"
    if any((f or "").startswith("N-1A") or (f or "").startswith("N-CSR")
           or (f or "").startswith("NPORT") for f in s):
        return "etf"
    return None


def parse_submissions_recent(text: str, form_filter: str | None = None) -> list[dict]:
    """submissions JSON recent → [{form, filed, accession, primary_doc}]
    （form_filter 指定时仅保留该类型，如 "4"）。解析失败返回 []。@author yys"""
    try:
        d = json.loads(text)
        rec = (d.get("filings") or {}).get("recent") or {}
    except (json.JSONDecodeError, AttributeError):
        return []
    out = []
    for form, filed, acc, doc in zip(rec.get("formType") or [],
                                     rec.get("filingDate") or [],
                                     rec.get("accessionNumber") or [],
                                     rec.get("primaryDocument") or []):
        if form_filter is None or form == form_filter:
            out.append({"form": form, "filed": filed,
                        "accession": acc, "primary_doc": doc})
    return out


def form4_url(cik: int, accession: str, primary_doc: str) -> str:
    """Archives 原始 XML 地址（primary_doc 含 xsl 渲染前缀时取 basename）。@author yys"""
    doc = str(primary_doc or "").split("/")[-1] or "primary_doc.xml"
    return ARCHIVES_URL.format(cik=cik, acc=accession.replace("-", ""), doc=doc)


def parse_form4_xml(text: str, symbol: str, cik: str, accession: str,
                    filed_date: str) -> list[dict]:
    """Form 4 XML → 非衍生品交易行列表（一个申报可含多笔）。
    action A=买入 D=卖出；price/value 缺失为 None；shares≤0 或 AD 码异常行跳过；
    解析失败返回 []。@author yys"""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    owner = root.find("reportingOwner")
    rid = owner.find("reportingOwnerId") if owner is not None else None
    reporter = (rid.findtext("rptOwnerName", "").strip()
                if rid is not None else "") or "UNKNOWN"
    rel = owner.find("reportingOwnerRelationship") if owner is not None else None
    title = ""
    if rel is not None:
        if (rel.findtext("isOfficer") or "0") == "1":
            title = (rel.findtext("officerTitle") or "").strip() or "Officer"
        elif (rel.findtext("isDirector") or "0") == "1":
            title = "Director"
        elif (rel.findtext("isTenPercentOwner") or "0") == "1":
            title = "10% Owner"
    rows = []
    table = root.find("nonDerivativeTable")
    for txn in (table.findall("nonDerivativeTransaction")
                if table is not None else []):
        try:
            shares = int(float(txn.findtext(
                "transactionAmounts/transactionShares/value") or 0))
        except ValueError:
            continue
        price_txt = txn.findtext("transactionAmounts/transactionPricePerShare/value")
        try:
            price = float(price_txt) if price_txt not in (None, "") else None
        except ValueError:
            price = None
        action = (txn.findtext(
            "transactionAmounts/transactionAcquiredDisposedCode/value") or "").strip()
        if action not in ("A", "D") or shares <= 0:
            continue
        rows.append({
            "symbol": symbol, "cik": cik, "accession": accession,
            "filed_date": filed_date,
            "trade_date": txn.findtext("transactionDate/value") or None,
            "reporter": reporter, "title": title, "action": action,
            "tx_code": (txn.findtext("transactionCoding/transactionCode") or "").strip(),
            "shares": shares, "price": price,
            "value_usd": round(shares * price, 2) if price is not None else None,
        })
    return rows


def build_insider_summary(rows: list[dict], today: str) -> dict | None:
    """近 90 天净买入聚合（纯函数，rows 为 DB 行）：net_shares/net_value_usd/
    buys/sells/tx_count/since；value 缺失按 0 计；空窗口返回 None。@author yys"""
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=90)) \
        .strftime("%Y-%m-%d")
    sel = [r for r in rows if str(r.get("filed_date"))[:10] >= cutoff]
    if not sel:
        return None
    net_shares = 0
    net_value = 0.0
    buys = sells = 0
    for r in sel:
        sign = 1 if r["action"] == "A" else -1
        net_shares += sign * int(r["shares"])
        v = r.get("value_usd")
        if v is not None:
            net_value += sign * float(v)
        if r["action"] == "A":
            buys += 1
        else:
            sells += 1
    return {"net_shares": net_shares, "net_value_usd": round(net_value, 2),
            "buys": buys, "sells": sells, "tx_count": len(sel), "since": cutoff}


# ---------------------------------------------------------------------------
# 网络层：全局节流 + 退避 + 拉取函数（全程串行，以稳为主）
# ---------------------------------------------------------------------------

_last_req_ts = 0.0                # 模块级节流状态（monotonic）
_TICKER_MAP: dict[str, int] | None = None   # symbol(大写) -> cik
_EDGAR_LOCK = asyncio.Lock()      # 全局串行锁：多协程并发进 _edgar_get 时仍保证间隔


async def _resolve_ua() -> str:
    """UA 从 sys_config.EDGAR_UA 热取（缺省无域名占位；真实联系方式库配置，
    域名属敏感值禁入代码）。@author yys"""
    try:
        ua = await db.get_config("EDGAR_UA")
        if ua:
            return ua
    except Exception:
        pass
    return DEFAULT_UA


async def _edgar_get(url: str, ua: str) -> httpx.Response | None:
    """限流 GET（_EDGAR_LOCK 串行化）：距上次请求 ≥EDGAR_MIN_INTERVAL；429/503
    按 EDGAR_BACKOFFS 退避重试；网络异常仅重试 1 次；穷尽返回 None（调用方按
    数据缺处理）。@author yys"""
    global _last_req_ts
    last_err = ""
    net_retried = False
    async with _EDGAR_LOCK:
        for attempt in range(len(EDGAR_BACKOFFS) + 1):
            gap = _last_req_ts + EDGAR_MIN_INTERVAL - time.monotonic()
            if gap > 0:
                await asyncio.sleep(gap)
            _last_req_ts = time.monotonic()
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True,
                                             headers={"User-Agent": ua}) as client:
                    resp = await client.get(url)
                if resp.status_code not in (429, 503):
                    if resp.status_code != 200:
                        logger.warning("EDGAR 非200 %s: HTTP %s",
                                       url, resp.status_code)
                    return resp
                last_err = f"HTTP {resp.status_code}"
            except Exception as e:
                last_err = repr(e)
                if net_retried:
                    break
                net_retried = True
            if attempt < len(EDGAR_BACKOFFS):
                await asyncio.sleep(EDGAR_BACKOFFS[attempt])
    logger.warning("EDGAR GET 失败 %s: %s", url, last_err)
    return None


async def _map_fresh() -> bool:
    """sys_config.EDGAR_TICKER_MAP_TS 是否 7 天内。@author yys"""
    ts = await db.get_config("EDGAR_TICKER_MAP_TS")
    if not ts:
        return False
    try:
        return (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).days < 7
    except ValueError:
        return False


async def get_ticker_map() -> dict[str, int] | None:
    """ticker→CIK 全量映射：内存缓存 + 时间戳 7 天过期（重启重拉一次可接受）。
    拉取失败保留旧缓存兜底。@author yys"""
    global _TICKER_MAP
    if _TICKER_MAP is not None and await _map_fresh():
        return _TICKER_MAP
    resp = await _edgar_get(TICKER_MAP_URL, await _resolve_ua())
    if resp is not None and resp.status_code == 200:
        parsed = parse_ticker_map(resp.text)
        if parsed:                       # 空 map（坏 JSON/拦截页）不落缓存不锁 7 天
            _TICKER_MAP = parsed
            await db.set_config("EDGAR_TICKER_MAP_TS",
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "EDGAR ticker映射刷新时间")
        else:
            logger.warning("EDGAR ticker map 解析为空（%d 字节），保留旧缓存",
                           len(resp.text))
    return _TICKER_MAP


async def fetch_submissions(cik: int, ua: str) -> str | None:
    """公司申报索引 JSON 文本（非 200/网络穷尽 → None）。@author yys"""
    resp = await _edgar_get(SUBMISSIONS_URL.format(cik=cik), ua)
    if resp is None or resp.status_code != 200:
        return None
    return resp.text


async def fetch_form4_text(cik: int, accession: str, primary_doc: str,
                           ua: str) -> str | None:
    """Form 4 原始 XML 文本（404/失败 → None，数据缺不重试轰炸）。@author yys"""
    resp = await _edgar_get(form4_url(cik, accession, primary_doc), ua)
    if resp is None or resp.status_code != 200:
        return None
    return resp.text
