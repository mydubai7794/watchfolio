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
    if any(f.startswith("20-F") for f in s):
        return "fpi"
    if any(f.startswith("10-K") or f.startswith("10-Q") for f in s):
        return "stock"
    if any(f.startswith("N-1A") or f.startswith("N-CSR") or f.startswith("NPORT")
           for f in s):
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
    doc = primary_doc.split("/")[-1] or "primary_doc.xml"
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
