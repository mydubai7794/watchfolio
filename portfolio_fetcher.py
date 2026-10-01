"""持仓数据抓取模块：美股行情(腾讯主源/新浪扩展/东财兜底) + 基金净值(FT GraphQL) + 汇率。

数据源结论见 docs/us-hk-quote-api-research.md：
- 腾讯 us 前缀批量：常规时段行情（无盘前盘后）
- 新浪 gb_ 前缀批量：字段[21]-[24] 为盘前/盘后实时价
- 东财 ulist 批量：兜底（仅常规价，105=NASDAQ/107=AMEX，易封 IP，≤1次/分钟）
- FT 官网 GraphQL：基金官方日频净值（fundid=31402/shclcode=2B）
- 新浪 fx_susdhkd：USD/HKD 汇率（失败兜底 7.80）
"""

import logging
import re
import time
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

from zoneinfo import ZoneInfo

# 美东时区（DST 自动正确；缺失 tzdata 时 import 阶段直接报错，保持 fail-fast）
ET_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# 接口常量
# ---------------------------------------------------------------------------

TENCENT_US_URL = "http://qt.gtimg.cn/q={symbols}"
SINA_US_URL = "https://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}
SINA_FX_URL = "https://hq.sinajs.cn/list=fx_susdhkd"
EM_ULIST_URL = "http://push2.eastmoney.com/api/qt/ulist.np/get"
EM_FIELDS = "f2,f3,f12,f13,f14,f18,f124"
FT_PDS_URL = "https://www.franklintempleton.com.hk/api/pds/price-and-performance"
FT_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://www.franklintempleton.com.hk",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"),
}
FT_FUND_QUERY = (
    "query O($f:String!,$s:String!,$c:String!,$l:String!){Overview(fundid:$f,"
    "shareclasscode:$s,countrycode:$c,languagecode:$l){fundname shareclass{shclname "
    "nav{navvalue navdate navchngvalue navchngpct ytdtotretatnav}}}}"
)
FT_FUND_VARS = {"f": "31402", "s": "2B", "c": "HK", "l": "en_GB"}
FX_FALLBACK = 7.80

# 东财兜底市场号（105=NASDAQ 107=AMEX），未知符号默认 105
EM_MARKET_MAP = {"MU": "105", "SKHY": "105", "QQQM": "105", "SPYM": "107", "DRAM": "107", "DISK": "107"}
EM_DEFAULT_MARKET = "105"

# 东财兜底节流：主源连续失败>=3轮才启用，且间隔>=60s
_EM_FAIL_THRESHOLD = 3
_EM_MIN_INTERVAL_SEC = 60.0
_em_fail_streak = 0
_last_em_used = 0.0


# ---------------------------------------------------------------------------
# 美股时段判断（美东时钟，DST 自动正确）
# ---------------------------------------------------------------------------

def us_market_session(now_et: datetime | None = None) -> tuple[str, date, str]:
    """返回 (session, 美东日期, 美东时间串)。session ∈ pre/regular/post/closed。"""
    now = now_et or datetime.now(ET_TZ)
    session = "closed"
    if now.weekday() < 5:
        minutes = now.hour * 60 + now.minute
        if 4 * 60 <= minutes < 9 * 60 + 30:
            session = "pre"
        elif 9 * 60 + 30 <= minutes < 16 * 60:
            session = "regular"
        elif 16 * 60 <= minutes < 20 * 60:
            session = "post"
    return session, now.date(), now.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 解析纯函数
# ---------------------------------------------------------------------------

def _f(s: str | None, default: float = 0.0) -> float:
    """安全转 float。"""
    try:
        return float(s) if s not in (None, "") else default
    except (ValueError, TypeError):
        return default


def parse_tencent_us(raw: str, symbols: list[str]) -> dict[str, dict]:
    """解析腾讯美股批量响应（GBK 已解码文本），返回 {symbol: quote}。"""
    quotes: dict[str, dict] = {}
    for sym in symbols:
        m = re.search(rf'v_us{re.escape(sym)}="([^"]*)"', raw)
        if not m or not m.group(1):
            continue
        f = m.group(1).split("~")
        # 守卫阈值必须大于最大访问索引 37（currency=f[37]），否则截断行会抛 IndexError
        if len(f) < 38:
            continue
        price, pre_close = _f(f[3]), _f(f[4])
        change_pct = (price - pre_close) / pre_close * 100 if pre_close > 0 else 0.0
        quotes[sym] = {
            "symbol": sym,
            "name": f[1],
            "price": price,
            "pre_close": pre_close,
            "change_pct": round(change_pct, 4),
            "high": _f(f[35]),
            "low": _f(f[36]),
            "currency": f[37] or "USD",
            "quote_time": f[32] if len(f) > 32 else "",
            "ext_price": 0.0,
            "ext_time": "",
        }
    return quotes


def parse_sina_us(raw: str, symbols: list[str]) -> dict[str, dict]:
    """解析新浪美股批量响应，返回 {symbol: {ext_price, ext_time, pre_close, price}}。"""
    result: dict[str, dict] = {}
    for sym in symbols:
        m = re.search(rf'var hq_str_gb_{re.escape(sym.lower())}="([^"]*)"', raw)
        if not m or not m.group(1):
            continue
        f = m.group(1).split(",")
        if len(f) < 27:
            continue
        result[sym] = {
            "price": _f(f[1]),
            "pre_close": _f(f[26]),
            "ext_price": _f(f[21]) if len(f) > 21 else 0.0,
            "ext_pct": _f(f[22]) if len(f) > 22 else 0.0,
            "ext_time": f[24].strip() if len(f) > 24 else "",
        }
    return result


def parse_fx_usdhkd(raw: str) -> float:
    """解析新浪 fx_susdhkd，返回 USD/HKD 汇率；失败/越界返回 0.0。"""
    m = re.search(r'var hq_str_fx_susdhkd="([^"]*)"', raw or "")
    if not m:
        return 0.0
    f = m.group(1).split(",")
    if len(f) < 2:
        return 0.0
    v = _f(f[1])
    return v if 7.5 <= v <= 8.5 else 0.0


def parse_ft_nav(data: dict) -> dict | None:
    """解析 FT GraphQL Overview 响应，返回 {nav, nav_date, change_pct, ytd_pct}。"""
    try:
        nav = data["data"]["Overview"]["shareclass"][0]["nav"]
        val = float(str(nav["navvalue"]).replace("$", "").replace(",", ""))
        dd, mm, yy = str(nav["navdate"]).split("/")
        return {
            "nav": val,
            "nav_date": date(int(yy), int(mm), int(dd)),
            "change_pct": float(str(nav["navchngpct"]).replace("%", "")),
            "ytd_pct": float(str(nav["ytdtotretatnav"]).replace("%", "")),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 盈亏计算（纯函数，所有金额统一 USD）
# ---------------------------------------------------------------------------

def build_positions(holdings: list[dict], us_quotes: dict[str, dict],
                    fund: dict | None, fx_usdhkd: float) -> tuple[list[dict], dict]:
    """计算每持仓盈亏与组合汇总。

    us_quotes: parse_tencent_us 结果（ext_price 已合并）
    fund: {"nav": float, "prev_nav": float, "nav_date": date, "nav_change_pct": float} 或 None
    返回 (positions, summary)
    """
    fx = fx_usdhkd if fx_usdhkd and 7.5 <= fx_usdhkd <= 8.5 else FX_FALLBACK
    positions: list[dict] = []
    total_value = total_cost = base_value = 0.0

    for h in holdings:
        sym, qty, cost = h["symbol"], float(h["quantity"]), float(h["cost_price"])
        if h["market"] == "HKFUND":
            if not fund or not fund.get("nav"):
                # 行情/净值缺失(stale)：成本按 HKD/汇率折算 USD 口径，且落入循环末尾统一累加，避免 cost_basis 漏计失真
                positions.append(_position(h, 0.0, None, None, None, "fund", True,
                                           cost_usd=cost * qty / fx))
            else:
                nav, prev = fund["nav"], fund.get("prev_nav") or 0.0
                value = nav * qty / fx
                today = (nav - prev) * qty / fx if prev > 0 else 0.0
                pnl = (nav - cost) * qty / fx
                cost_usd = cost * qty / fx
                base_value += prev * qty / fx if prev > 0 else value
                positions.append(_position(h, nav, value, today, pnl, "fund", False,
                                           cost_usd=cost_usd, nav_date=str(fund.get("nav_date", "")),
                                           nav_change_pct=fund.get("nav_change_pct")))
        else:
            q = us_quotes.get(sym)
            if not q or not q.get("price"):
                # 行情缺失(stale)：USD 成本口径不变(默认 cost*qty)，落入循环末尾统一累加
                positions.append(_position(h, 0.0, None, None, None, "none", True))
            else:
                price_now = q["ext_price"] if q.get("ext_price") else q["price"]
                ext = "pre/post" if q.get("ext_price") else "regular"
                value = price_now * qty
                today = (price_now - q["pre_close"]) * qty if q["pre_close"] > 0 else 0.0
                pnl = (price_now - cost) * qty
                base_value += q["pre_close"] * qty if q["pre_close"] > 0 else value
                positions.append(_position(h, price_now, value, today, pnl, ext, False,
                                           pre_close=q["pre_close"], quote_time=q.get("ext_time") or q.get("quote_time", "")))
        p = positions[-1]
        total_value += p["value"]
        total_cost += p["cost_usd"]

    day_pnl = sum(p["today_pnl"] for p in positions if p["today_pnl"])
    total_pnl = total_value - total_cost
    # 注意：此处不做 round，保持与逐持仓字段浮点一致（下游展示层自行取整）
    summary = {
        "total_value": total_value,
        "cost_basis": total_cost,
        "day_pnl": day_pnl,
        "day_pnl_pct": day_pnl / base_value * 100 if base_value > 0 else 0.0,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl / total_cost * 100 if total_cost > 0 else 0.0,
        "base_value": base_value,
    }
    return positions, summary


def _position(h: dict, price, value, today, pnl, ext, stale,
              cost_usd=None, pre_close=None, quote_time="", nav_date="", nav_change_pct=None):
    """构造单持仓输出行（内部辅助）。金额保持原始浮点精度，不做 round。"""
    qty, cost = float(h["quantity"]), float(h["cost_price"])
    if cost_usd is None:
        cost_usd = cost * qty
    return {
        "id": h.get("id"),
        "symbol": h["symbol"], "name": h.get("name", ""), "market": h.get("market", "US"),
        "currency": h.get("currency", "USD"), "quantity": qty, "cost_price": cost,
        "price": price, "pre_close": pre_close,
        "change_pct": round((price - pre_close) / pre_close * 100, 4)
                      if (pre_close and pre_close > 0 and price) else None,
        "value": value if value is not None else 0.0,
        "today_pnl": today if today is not None else None,
        "position_pnl": pnl if pnl is not None else None,
        "position_pnl_pct": pnl / cost_usd * 100 if (cost_usd > 0 and pnl is not None) else None,
        "cost_usd": cost_usd,
        "ext": ext, "stale": stale,
        "quote_time": quote_time, "nav_date": nav_date, "nav_change_pct": nav_change_pct,
    }


# ---------------------------------------------------------------------------
# 网络函数（境内源直连；FT 优先走代理池）
# ---------------------------------------------------------------------------

async def _get_text(url: str, headers: dict | None = None, timeout: float = 10.0) -> str | None:
    """GET 并按 GBK 解码（腾讯/新浪），失败返回 None。"""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers=headers or {})
            if resp.status_code == 200:
                return resp.content.decode("gbk", errors="replace")
            logger.warning("GET %s → HTTP %d", url[:70], resp.status_code)
    except Exception as e:
        logger.warning("GET %s 失败: %s", url[:70], e)
    return None


async def fetch_us_quotes(symbols: list[str], session: str | None = None) -> dict[str, dict]:
    """抓取美股行情：腾讯批量主源 + 扩展时段新浪批量 + 东财兜底。

    返回 {symbol: quote}，quote 含 price/pre_close/ext_price/ext_time 等。
    """
    global _em_fail_streak, _last_em_used
    if not symbols:
        return {}
    if session is None:
        session, _, _ = us_market_session()

    raw = await _get_text(TENCENT_US_URL.format(symbols=",".join("us" + s for s in symbols)))
    quotes: dict[str, dict] = {}
    if raw:
        quotes = parse_tencent_us(raw, symbols)
        _em_fail_streak = 0
    else:
        _em_fail_streak += 1

    # 扩展时段合并新浪盘前盘后价
    if session in ("pre", "post") and quotes:
        raw2 = await _get_text(
            SINA_US_URL.format(symbols=",".join("gb_" + s.lower() for s in symbols)),
            headers=SINA_HEADERS,
        )
        if raw2:
            ext = parse_sina_us(raw2, symbols)
            for sym, q in quotes.items():
                e = ext.get(sym)
                if e and e.get("ext_price", 0) > 0:
                    q["ext_price"] = e["ext_price"]
                    q["ext_time"] = e["ext_time"]

    # 东财兜底：主源整体失败且连续>=3轮、距上次>=60s
    if not quotes and _em_fail_streak >= _EM_FAIL_THRESHOLD and time.monotonic() - _last_em_used >= _EM_MIN_INTERVAL_SEC:
        _last_em_used = time.monotonic()
        secids = ",".join(f"{EM_MARKET_MAP.get(s, EM_DEFAULT_MARKET)}.{s}" for s in symbols)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(EM_ULIST_URL, params={
                    "secids": secids, "fields": EM_FIELDS, "fltt": "2", "invt": "2"})
                data = resp.json()
                for item in (data.get("data") or {}).get("diff") or []:
                    sym = item.get("f12")
                    if sym in symbols:
                        quotes[sym] = {
                            "symbol": sym, "name": item.get("f14", ""),
                            "price": _f(item.get("f2")), "pre_close": _f(item.get("f18")),
                            "change_pct": _f(item.get("f3")), "high": 0.0, "low": 0.0,
                            "currency": "USD", "quote_time": "",
                            "ext_price": 0.0, "ext_time": "",
                        }
                logger.info("东财兜底: %d/%d", len(quotes), len(symbols))
        except Exception as e:
            logger.warning("东财兜底失败: %s", e)

    return quotes


async def fetch_fx_usdhkd() -> tuple[float, bool]:
    """获取 USD/HKD 汇率。返回 (汇率, 是否兜底)。"""
    raw = await _get_text(SINA_FX_URL, headers=SINA_HEADERS)
    v = parse_fx_usdhkd(raw or "")
    if v > 0:
        return v, False
    return FX_FALLBACK, True


async def fetch_fund_nav(proxy_urls: list[str] | None = None) -> dict | None:
    """FT 官网 GraphQL 获取基金净值。优先代理（前2个），最后直连重试。"""
    payload = {"query": FT_FUND_QUERY, "variables": FT_FUND_VARS}
    candidates = list(proxy_urls or [])[:2] + [None]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.post(FT_PDS_URL, json=payload, headers=FT_HEADERS)
                if resp.status_code == 200:
                    parsed = parse_ft_nav(resp.json())
                    if parsed:
                        return parsed
                    last_err = "nav 解析失败"
                else:
                    last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("FT 基金净值获取失败: %s", last_err)
    return None
