"""持仓数据抓取模块：美股+港股行情(腾讯主源/新浪扩展/东财兜底) + 基金净值(FT GraphQL) + 汇率。

数据源结论见 docs/us-hk-quote-api-research.md：
- 腾讯 us/hk 前缀混合批量：常规时段行情（美股无盘前盘后，港股无扩展时段）
- 新浪 gb_ 前缀批量：字段[21]-[24] 为美股盘前/盘后实时价
- 东财 ulist 批量：兜底（仅常规价，105=NASDAQ/106=NYSE/107=AMEX/116=港股，易封 IP，≤1次/分钟）
- FT 官网 GraphQL：基金官方日频净值（fundid=31402/shclcode=2B）
- 腾讯 whUSDHKD（总线攒批）：USD/HKD 汇率（失败兜底 7.80；新浪已退役）
- 新浪 fx_susdcnh：USD/CNH 离岸人民币汇率冷备（主价 twelvedata 30min 节流，缺失回落本批）
- Yahoo v8 chart 当日分时（5m 含盘前盘后）：US30Y/US10Y/VIX 主价源（server 沿用
  backfill 节奏刷新，当日 5m 分时顺带落库）；其余看板标的分时由主价快照
  「价变即写」落库累积，不请求单独的分时接口
"""

import asyncio
import json
import logging
import os
import re
import time
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import httpx

import config   # 触发 load_env_file：密钥类环境变量在本模块常量求值前注入
from tencent_bus import tencent_bus

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
SINA_PANEL_URL = "https://hq.sinajs.cn/list=DINIW,hf_OIL,hf_GC,fx_susdcnh"
# 24h数据源（境外现货交易所公共行情端点）：免 key 7×24 有价，美股闭市时段
# （夜盘/周末）代理价补全组合走势，中间价口径与底层夜盘价偏差实测 0.02%-0.1%
# （详见《美股夜盘数据源研报.md》）；同套端点亦提供 24h 盘口探测（weight 0）
H24_EXCHANGE_INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"
H24_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/24hr?symbol={symbol}"
H24_BOOK_TICKER_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker"
H24_PING_URL = "https://data-api.binance.vision/api/v3/ping"   # 代理境外可达性探测（weight 0）
# 注：端点域名为该数据源官方公开行情镜像域名（描述性文本一律使用「24h数据源」，
# 不指名具体交易所）；镜像域名境内直连可达，实测 200
# 同标的替代映射（自身无对应交易对、但可借替代标的涨跌幅推导夜盘价）：
# QQQM 与 QQQ 为同一基金的两份额类，净值比率恒定 → QQQM 夜盘价 =
# QQQM 收盘定格 × (QQQ 24h中间价 / QQQ 收盘定格)
H24_ALIAS = {"QQQM": "QQQ"}
# 24h数据源·股票行情端点（equity quote，另一套接口）：bookTicker 无对应
# 交易的美股标的（如 AMKR/PCG/F，F 为既有射频识别业务代号、严禁误配到其它
# 同名符号）夜盘活价补齐；
# 仅支持单 symbol 逐只请求（weight=1/只），网关域名境内直连可达（免代理），
# 双层鉴权：X-Auth-Token（网关）+ X-MBX-APIKEY（上游线协议 header 名，
# 接口契约原样保留，2026-10-10 命名统一误改 X-H24-APIKEY 致上游 -2014 回归），两值相同
# 网关地址与业务 key 全部经环境变量注入（QDII_EQUITY_QUOTE_URL /
# QDII_EQUITY_API_KEY）；任一缺失时股票行情补齐请求安全跳过，标的自然
# 回退新浪扩展价定格口径
EQUITY_QUOTE_URL = os.getenv("QDII_EQUITY_QUOTE_URL", "")
EQUITY_API_KEY = os.getenv("QDII_EQUITY_API_KEY", "")
# twelvedata 接口型美股/港股行情兜底（有 key 不 ban IP；免费 8 credits/min，
# 批量请求 credits=symbol 数，故单次批量上限 8 并轮换指针覆盖全部持仓）
TWELVEDATA_QUOTE_URL = "https://api.twelvedata.com/quote"
# key 经环境变量注入（QDII_TWELVEDATA_API_KEY）；未配置时兜底/锚点恢复/
# 外汇三条 twelvedata 请求路径全部短路跳过，走既有降级链
TWELVEDATA_API_KEY = os.getenv("QDII_TWELVEDATA_API_KEY", "")
TWELVEDATA_BATCH = 8   # 单次兜底批量上限（对齐免费 8 credits/min 限额）
# Yahoo v8 chart 当日分时（5m K 线含盘前盘后）：range=5d 拉取后解析层只取最后一个
# UTC 自然日——盘中即当日实时分时、闭市即上一交易时段全程；不用 range=1d 是因为
# 实测 Yahoo 期货/美元指数在每日美东凌晨存在 1d 空窗 bug（BZ=F 返回 200 但 0 点，
# 同刻 5d 有数）；旧 5d 日线 spark（YAHOO_CHART_URL）仅保留给 USDHKD fx_spark
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
YAHOO_DAY_CHART_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                       "?interval=5m&range=5d&includePrePost=true")
YAHOO_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"),
}
# Yahoo v8 chart 日线映射（仅剩 USDHKD：server 循环经 fetch_fx_spark 低频拉取 fx_spark）
YAHOO_PANEL_SYMBOLS = {"USDHKD": "HKD=X"}
# 看板标的 Yahoo 代码映射（美股 ETF/期货/美元指数/收益率/VIX/指数/离岸人民币）；
# 实际仅 US30Y/US10Y 与指数四项（VIX/DJI/NDX/SPX）由 server 沿用 backfill 节奏
# 刷新主价（长桥不可用时兜底；响应内当日 5m 分时顺带落库），其余映射仅作
# 标的全集 DAY_MINUTE_KEYS 的构成
YAHOO_DAY_SYMBOLS = {"QQQ": "QQQ", "SPY": "SPY", "SOXX": "SOXX",
                     "BZ": "BZ=F", "GC": "GC=F", "DXY": "DX-Y.NYB",
                     "US30Y": "^TYX", "US10Y": "^TNX", "VIX": "^VIX",
                     "DJI": "^DJI", "NDX": "^NDX", "SPX": "^GSPC",
                     "USDCNH": "CNH=X"}
# 看板主价快照存库/读 spark 的标的全集：分时全部由主价快照价变即写累积
# （US30Y/US10Y/VIX 由 Yahoo 主价刷新顺带落库），不请求单独的分时接口
DAY_MINUTE_KEYS = list(YAHOO_DAY_SYMBOLS) + ["HSI"]
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

# twelvedata 兜底节流：主源连续失败>=3轮才启用，且间隔>=60s；批量上限 8 只/次，
# 轮换指针逐次覆盖全部持仓（免费 8 credits/min，超限会 429）
_TD_FAIL_THRESHOLD = 3
_TD_MIN_INTERVAL_SEC = 60.0
_td_fail_streak = 0
_last_td_used = 0.0
_td_cursor = 0   # 兜底批量轮换指针（全部 symbol 列表中的起始下标）

# 24h数据源映射缓存：{美股代码: 交易符号}，exchangeInfo 低频刷新（24h TTL）
_H24_MAP_TTL_SEC = 86400.0
_h24_map_cache: dict[str, str] = {}
_h24_map_ts = 0.0


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


def trading_session_key(now_et: datetime) -> date:
    """一场口径交易日（session_key=开盘日）：一场=美东开盘→下一次开盘。

    - 工作日 >=09:30：当日已开盘，key=当日；
    - 工作日 <09:30：盘前/凌晨属于上一场，key=上一工作日；
    - 周末：回退到最近上一工作日（周六→周五、周日→周五，跨周末继续回退）。
    用途：tick 归场（次日凌晨采样归上一交易场，24h 曲线连续累计不重置）。
    """
    d = now_et.date()
    if now_et.weekday() < 5 and (now_et.hour, now_et.minute) >= (9, 30):
        return d
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def should_fetch_ext(session: str, now_et: datetime) -> bool:
    """判定当前是否应拉取新浪盘前/盘后扩展价（纯函数，供 fetch_quotes 调用）。

    - pre/post 时段：恒拉取；
    - closed 且工作日（周一~周五）的 16:00 后（当日盘后）与凌晨 <04:00（前一交易
      日盘后残留，简化口径按美东小时判断，不含节假日）：仍拉取——新浪 gb_ 夜间返
      回当日最后盘后价，保证夜盘时段前端保留盘后价不"倒退"；
    - closed 且周六凌晨 <04:00：周五盘后残留的延伸窗口，仍拉取；
    - 其余 closed（周六 04:00 起/周日、工作日 04:00-16:00 空窗）：不拉取。
    """
    if session in ("pre", "post"):
        return True
    if session == "closed":
        wd = now_et.weekday()
        hour = now_et.hour
        return (wd < 5 and (hour >= 16 or hour < 4)) or (wd == 5 and hour < 4)
    return False


# ---------------------------------------------------------------------------
# 解析纯函数
# ---------------------------------------------------------------------------

def _f(s: str | None, default: float = 0.0) -> float:
    """安全转 float。"""
    try:
        return float(s) if s not in (None, "") else default
    except (ValueError, TypeError):
        return default


def parse_tencent_quotes(raw: str, symbols: list[str],
                         prefixes: dict[str, str] | None = None) -> dict[str, dict]:
    """解析腾讯批量行情响应（GBK 已解码文本），支持美股 us / 港股 hk / 外盘期货 hf 前缀混合。

    prefixes: {symbol: "us"|"hk"|"hf"}，缺省全部按美股处理。
    us/hk 两市场为波浪线布局，前段字段一致：f[1]名称/f[3]现价/f[4]昨收/f[30]时间/f[33]高/f[34]低；
    币种分市场：美股 f[35]、港股 f[75]。
    hf 外盘期货为逗号布局：f[0]现价/f[4]高/f[5]低/f[6]时间/f[7]昨结/f[13]名称，
    涨跌幅必须按 f[7] 昨结自算（f[2] 是买价，不能用作昨结）。
    """
    prefixes = prefixes or {s: "us" for s in symbols}
    quotes: dict[str, dict] = {}
    for sym in symbols:
        prefix = prefixes.get(sym, "us")
        if prefix == "hf":
            # 外盘期货行（v_hf_{sym}）为逗号布局：守卫 14 大于最大访问索引 13（name=f[13]）
            m = re.search(rf'v_hf_{re.escape(sym)}="([^"]*)"', raw)
            if not m or not m.group(1):
                continue
            f = m.group(1).split(",")
            if len(f) < 14 or not f[0]:
                continue
            price, pre_close = _f(f[0]), _f(f[7])
            change_pct = (price - pre_close) / pre_close * 100 if pre_close > 0 else 0.0
            quotes[sym] = {
                "symbol": sym,
                "name": f[13],
                "price": price,
                "pre_close": pre_close,
                "change_pct": round(change_pct, 4),
                "high": _f(f[4]),
                "low": _f(f[5]),
                "currency": "USD",
                "quote_time": f[6],
                "ext_price": 0.0,
                "ext_time": "",
            }
            continue
        m = re.search(rf'v_{prefix}{re.escape(sym)}="([^"]*)"', raw)
        if not m or not m.group(1):
            continue
        f = m.group(1).split("~")
        if prefix == "hk":
            # 守卫阈值必须大于最大访问索引 75（currency=f[75]），否则截断行会抛 IndexError
            if len(f) < 76:
                continue
            currency = f[75] or "HKD"
        else:
            # 守卫 38：大于真实布局最大访问索引 35（currency=f[35]），
            # 并保留"37 字段截断行不入结果"的历史裕度（见 tests test_truncated_line）
            if len(f) < 38:
                continue
            currency = f[35] or "USD"
        price, pre_close = _f(f[3]), _f(f[4])
        # 两市场真实布局一致：时间 f[30]、高 f[33]、低 f[34]
        quote_time, high, low = f[30], _f(f[33]), _f(f[34])
        change_pct = (price - pre_close) / pre_close * 100 if pre_close > 0 else 0.0
        quotes[sym] = {
            "symbol": sym,
            "name": f[1],
            "price": price,
            "pre_close": pre_close,
            "change_pct": round(change_pct, 4),
            "high": high,
            "low": low,
            "currency": currency,
            "quote_time": quote_time,
            "ext_price": 0.0,
            "ext_time": "",
        }
    return quotes


def parse_tencent_us(raw: str, symbols: list[str]) -> dict[str, dict]:
    """解析腾讯美股批量响应（向后兼容包装：委托 parse_tencent_quotes，全 us 前缀）。"""
    return parse_tencent_quotes(raw, symbols, {s: "us" for s in symbols})


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


def parse_tencent_wh_fx(raw: str) -> float:
    """解析腾讯外汇行 v_whUSDHKD（波浪线布局 f[3] 现价），失败/越界返回 0.0。

    wh 行仅 ~21 字段且布局与 us 股票不同（f[4] 恒 0），不能复用
    parse_tencent_quotes 的 us/hk 分支；汇率场景只需现价。@author yys
    """
    m = re.search(r'v_whUSDHKD="([^"]*)"', raw or "")
    if not m:
        return 0.0
    f = m.group(1).split("~")
    v = _f(f[3]) if len(f) > 3 else 0.0
    return v if 7.5 <= v <= 8.5 else 0.0


def _parse_sina_hf(raw: str, code: str) -> dict:
    """解析新浪/腾讯 hf_ 外盘期货行（通用）：f[0]现价 f[7]昨结 f[13]名称，涨跌幅自算。

    昨结必须取 f[7]：f[2] 是买价，旧实现误用作昨结导致涨跌幅口径错误。
    字段不足/无匹配返回 {}。腾讯混排行 v_hf_{code} 与新浪 hq_str_hf_{code}
    共用同一正则（两者逗号布局一致，腾讯版仅尾部少成交量字段）。
    """
    m = re.search(rf'hf_{re.escape(code)}="([^"]*)"', raw or "")
    if not m:
        return {}
    f = m.group(1).split(",")
    # 守卫 14：大于最大访问索引 13（name=f[13]），截断行不入结果
    if len(f) < 14 or not f[0]:
        return {}
    price, pre = _f(f[0]), _f(f[7])
    return {"price": price, "pre_close": pre,
            "change_pct": (price - pre) / pre * 100 if pre > 0 else 0.0, "name": f[13]}


def parse_sina_oil(raw: str) -> dict:
    """解析新浪布伦特原油 hf_OIL：f[0]现价 f[7]昨结 f[13]名称（委托通用 hf 解析）。"""
    return _parse_sina_hf(raw, "OIL")


def parse_sina_diniw(raw: str) -> dict:
    """解析新浪美元指数 DINIW：f[1]现价 f[3]昨收 f[9]名称，涨跌幅自算；字段不足返回 {}。"""
    m = re.search(r'DINIW="([^"]*)"', raw or "")
    if not m:
        return {}
    f = m.group(1).split(",")
    # 守卫 10：大于最大访问索引 9（name=f[9]），截断行不入结果
    if len(f) < 10 or not f[1]:
        return {}
    price, pre = _f(f[1]), _f(f[3])
    return {"price": price, "pre_close": pre,
            "change_pct": (price - pre) / pre * 100 if pre > 0 else 0.0, "name": f[9]}


def parse_yahoo_chart(raw: str) -> dict:
    """解析 Yahoo v8 chart 响应（^TYX/^TNX/^VIX）：meta 现价/昨收 + adjclose 近5日 spark。

    spark 过滤 None 后取末尾 ≤60 点；非法 JSON/结构缺失返回 {}。
    """
    try:
        r = json.loads(raw)["chart"]["result"][0]
        meta = r["meta"]
        pre = float(meta.get("chartPreviousClose") or 0)
        price = float(meta["regularMarketPrice"])
        spark = [v for v in r["indicators"]["adjclose"][0]["adjclose"] if v]
        return {"price": price, "pre_close": pre,
                "change_pct": (price - pre) / pre * 100 if pre > 0 else 0.0,
                "spark": spark[-60:]}
    except Exception:
        return {}


def parse_yahoo_day_chart(raw: str) -> dict:
    """解析 Yahoo v8 chart 当日分时响应（range=5d interval=5m 含盘前盘后）。

    全量 5d 分时中只保留最后一个 UTC 自然日的点：盘中即当日实时分时、闭市即
    上一交易时段全程（Globex 期货以 UTC 日切分恰为完整交易时段），并规避 Yahoo
    期货/美元指数 range=1d 每日美东凌晨空窗 bug（实测 BZ=F 返回 200 但 0 点）。
    pre_close 不取 meta.chartPreviousClose（5d 窗口下语义=5 日前收盘，实测导致
    涨跌幅口径错误），改取最后 UTC 日之前的最后一个有效收盘点（=前一交易日收盘）。
    返回 {"price": 现价(meta), "pre_close": 昨收, "change_pct": 自算,
    "minutes": [(naive UTC datetime, price)]}——ts 统一 epoch→UTC naive（不做
    时区换算，「自然日」切分以库内最大 ts 为准）；None 收盘点跳过；非法 JSON/结构
    缺失返回 {}。
    """
    try:
        r = json.loads(raw)["chart"]["result"][0]
        meta = r["meta"]
        price = float(meta["regularMarketPrice"])
        ts_list = r.get("timestamp") or []
        closes = (r["indicators"].get("quote") or [{}])[0].get("close") or []
        all_pts: list[tuple[datetime, float]] = []
        for t, c in zip(ts_list, closes):
            if c:  # 未成交 5m 桶收盘为 None，跳过
                all_pts.append((datetime.fromtimestamp(t, tz=timezone.utc)
                                .replace(tzinfo=None), float(c)))
        minutes: list[tuple[datetime, float]] = all_pts
        prev_closes: list[float] = []
        if all_pts:
            last_day = all_pts[-1][0].date()
            minutes = [(t, p) for t, p in all_pts if t.date() == last_day]
            prev_closes = [p for t, p in all_pts if t.date() != last_day]
        pre = prev_closes[-1] if prev_closes else float(meta.get("chartPreviousClose") or 0)
        return {"price": price, "pre_close": pre,
                "change_pct": (price - pre) / pre * 100 if pre > 0 else 0.0,
                "minutes": minutes}
    except Exception:
        return {}


def parse_fx_usdcnh(raw: str) -> dict:
    """解析新浪 fx_susdcnh（美元/离岸人民币）：f[1]现价 f[3]昨收，涨跌幅自算。

    新浪 fx_* 行标准布局（实测昨收 f[3] 与涨跌额 f[11] 吻合：
    6.7068-6.7132=-0.0064）；价格合理区间 5.0~8.0，字段不足/越界返回 {}。
    """
    m = re.search(r'var hq_str_fx_susdcnh="([^"]*)"', raw or "")
    if not m:
        return {}
    f = m.group(1).split(",")
    if len(f) < 12 or not f[1]:
        return {}
    price, pre = _f(f[1]), _f(f[3])
    if not (5.0 <= price <= 8.0):
        return {}
    return {"price": price, "pre_close": pre,
            "change_pct": (price - pre) / pre * 100 if pre > 0 else 0.0, "name": f[9]}


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

def detect_isolated_spike(prev: float | None, cur: float | None,
                          nxt: float | None) -> bool:
    """tick 孤立毛刺检测：当前点大幅偏离前后邻居均值、且邻居彼此接近 → 坏值毛刺。

    判定：dev=|cur-(prev+nxt)/2| > 60（偏离量过绝对阈值）且 |prev-nxt| < dev*0.5
    （邻居互差远小于偏离量，排除开盘跳空等真实跳变），两者同时满足才判 True；
    任一边界为 None（序列首尾/缺值）不判，返回 False。
    成因：单持仓行情瞬时坏值（线上实例：p=87.38 邻居 281/280、p=-299.60 邻居 272/270）。
    边界：连续 ≥2 点同向坏值不可检——坏点相邻时其"邻居"仍含坏值，邻居互差不再
    远小于偏离量，判定条件天然不成立（无第二信息源交叉验证）；此类序列依赖
    写入口 decide_tick_action 的延迟确认兜底，本函数仅作为读出口的孤立点清洗。
    """
    if prev is None or cur is None or nxt is None:
        return False
    nb = (prev + nxt) / 2
    dev = abs(cur - nb)
    return dev > 60 and abs(prev - nxt) < dev * 0.5


def decide_tick_action(last_p: float | None, last_v: float | None,
                       new_p: float, has_pending: bool) -> str:
    """tick 写入口延迟确认决策（纯函数，供采集循环执行）。

    p/v 阈值分设（量纲说明）：被检测对象是 p（当日盈亏额，USD），其合理跳变
    上限与总资产规模挂钩，取 p 阈值 = max(60, 0.02×|last_v|)（总资产 2%，
    绝对下限 60）；旧版 5% 量纲过大，线上 last_v≈13000 时阈值 650，
    两根真实毛刺针（p 偏差 194/571）全部漏过入库。v（总资产）自身无独立
    跳变检测——现金与行情坏值不会使 v 脱离 p 单独突变，p 检测已覆盖。
    返回动作：
    - "write":   冷启动首条 / 偏差在阈值内 → 直接写入并更新 last；
    - "hold":    偏差超阈值且当前无暂存 → 暂存 pending，本轮不写库；
    - "discard": 已有暂存且本轮恢复到 last 附近（偏差 < 阈值一半）→ 上轮为毛刺，
                 丢弃 pending，本轮正常写入；
    - "confirm": 已有暂存且本轮仍异常（偏差 ≥ 阈值一半）→ 真实跳变（如开盘跳空），
                 补写 pending 保持曲线连续，并接受本轮写入。
    """
    if last_p is None:
        return "write"
    threshold = max(60.0, 0.02 * abs(last_v or 0.0))
    dev = abs(new_p - last_p)
    if has_pending:
        return "discard" if dev < threshold / 2 else "confirm"
    return "hold" if dev > threshold else "write"


def update_session_state(state: dict, session: str, et_dt: datetime,
                         quotes: dict) -> None:
    """一场口径状态机维护（纯函数，无 IO；仅就地修改 state 键值）。

    state 键（全部就地更新，缺失键自动补建）：
    - base_closes:  {symbol: 场基准价}——regular 时段用腾讯昨收(f[4])每轮刷新
      （数据源保证=一场基准=昨日收盘）；非 regular 沿用上一值；冷启动/重启落在
      非 regular 时段时退化用当轮 f[4]（2026-10-02 实证：凌晨 03:27 与盘前
      04:07 f[4] 均仍为上一场基准未切换，即退化值在该窗口语义恰好正确）；
    - close_prices: {symbol: 常规收盘定格价}——regular 时段跟随实时价（定格于
      16:00 最后一条）；post/closed(≥16:00)/凌晨(<04:00) 取腾讯现价(f[3]，实测
      收盘后至次日盘前全程保持 16:00 收盘价)；盘前(pre)仅缺失时用 f[3] 兜底
      （不覆盖已定格值，防盘前中后段 f[3] 可能切换为盘前成交价污染定格）；
    - base_logged:  bool，冷启动退化仅标注一次的开关；
    - base_fallback: list，本轮冷启动退化填充的 symbol（供调用方打日志；
      无退化时重置为 []——本函数无 IO，日志由调用方负责）。

    分支语义（session ∈ pre/regular/post/closed，et_dt 为美东时钟）：
    - regular：f[4] 即一场基准每轮刷新；收盘定格跟随实时价；
    - post 或 closed 且 et_hour>=16：f[3] 实测保持 16:00 收盘价 → 收盘定格
      （此时段港股若在盘中，f[3] 即实时价，定格跟随至 16:00 北京自然定格）；
    - closed 且 et_hour<4（凌晨）：f[4] 实证仍为上一场基准（未切换），f[3]
      保持收盘定格 → 用 f[3] 维护（旧版取 f[4] 会使 close==base，收盘定格段
      恒 0——「收盘定格 -$22.14」线上问题根因）；
    - pre（盘前）：f[4] 实证仍未切换、f[3] 保持定格 → 仅缺失时用 f[3] 填充，
      不覆盖已定格值（锚点恢复值同理受保护）；
    - 非 regular：基准缺失时退化填当轮 f[4]（仅缺的 symbol，已维护的不覆盖）。
    """
    base_closes: dict = state.setdefault("base_closes", {})
    close_prices: dict = state.setdefault("close_prices", {})
    state.setdefault("base_logged", False)
    et_hour = et_dt.hour
    if session == "regular":
        # regular：腾讯昨收(f[4])即一场基准，每轮刷新；收盘定格跟随实时价
        # （定格于 16:00 最后一条采样），实时累计=定格、夜盘增量=0
        for sym, q in quotes.items():
            if q.get("pre_close"):
                base_closes[sym] = q["pre_close"]
            if q.get("price"):
                close_prices[sym] = q["price"]
    elif session == "post" or (session == "closed" and et_hour >= 16):
        # post/晚间 closed：腾讯现价(f[3])实测保持 16:00 收盘价 → 收盘定格
        for sym, q in quotes.items():
            if q.get("price"):
                close_prices[sym] = q["price"]
    elif session == "closed" and et_hour < 4:
        # 凌晨 closed：f[4] 实证仍为上一场基准（未切换），f[3] 保持收盘定格；
        # 此时段港股（北京 12:00-16:00）若在盘中，f[3] 即实时价跟随至自然定格
        for sym, q in quotes.items():
            if q.get("price"):
                close_prices[sym] = q["price"]
    elif session == "pre":
        # 盘前：仅缺失时用 f[3] 兜底（实测保持收盘定格），不覆盖已定格值——
        # 防盘前中后段腾讯把 f[3] 切换为盘前成交价污染 16:00 定格
        for sym, q in quotes.items():
            if q.get("price") and sym not in close_prices:
                close_prices[sym] = q["price"]
    state["base_fallback"] = []
    if session != "regular":
        # 非 regular 沿用已维护基准；冷启动/重启缺失时退化用当轮 f[4]（实证
        # 凌晨/盘前 f[4] 仍为上一场基准未切换，退化值语义在该窗口恰好正确；
        # regular 时段腾讯会刷新为新一一场基准，09:30 自然对齐）
        missing = [s for s, q in quotes.items()
                   if s not in base_closes and q.get("pre_close")]
        for sym in missing:
            base_closes[sym] = quotes[sym]["pre_close"]
        if missing and not state["base_logged"]:
            state["base_fallback"] = missing
            state["base_logged"] = True


def build_positions(holdings: list[dict], us_quotes: dict[str, dict],
                    fund: dict | None, fx_usdhkd: float,
                    base_overrides: dict | None = None,
                    close_overrides: dict | None = None) -> tuple[list[dict], dict]:
    """计算每持仓盈亏与组合汇总（一场口径）。

    一场=美东开盘→下一次开盘（session_key=开盘日），三数值恒等式：
    day_pnl(实时累计) = close_pnl(收盘定格) + overnight_pnl(夜盘增量)。
    - 实时累计 today_pnl = (最新价-场基准)×qty：盘后/夜盘实时价计入；
      场基准 base_overrides[sym] 优先（_portfolio_loop 状态机维护的一场基准），
      缺失退化 q["pre_close"]（腾讯昨收，regular 时段两者等价）；
    - 收盘定格 close_price = close_overrides[sym]（常规收盘定格价），缺失退化
      实时价（无定格信息时夜盘增量视为 0）；
    - close_pnl = Σ(close_price-场基准)×qty/fx；overnight_pnl = day_pnl-close_pnl。
    每持仓行统一下发 overnight_change_pct=(price-close_price)/close_price×100
    （夜盘增量%，close_price 缺失为 None），前端徽标直接消费。

    us_quotes: parse_tencent_us 结果（ext_price 已合并）
    fund: {"nav": float, "prev_nav": float, "nav_date": date, "nav_change_pct": float} 或 None
    返回 (positions, summary)
    """
    fx = fx_usdhkd if fx_usdhkd and 7.5 <= fx_usdhkd <= 8.5 else FX_FALLBACK
    positions: list[dict] = []
    total_value = total_cost = base_value = 0.0
    close_pnl_total = 0.0

    for h in holdings:
        sym, qty, cost = h["symbol"], float(h["quantity"]), float(h["cost_price"])
        if h["market"] == "HKFUND":
            if not fund or not fund.get("nav"):
                # 行情/净值缺失(stale)：成本按 HKD/汇率折算 USD 口径，且落入循环末尾统一累加，避免 cost_basis 漏计失真
                positions.append(_position(h, 0.0, None, None, None, "fund", True,
                                           cost_usd=cost * qty / fx))
            else:
                # 基金官方日频净值：场基准=prev_nav、收盘定格=当日净值（日更无夜盘，
                # overnight 恒为 0），实时累计/定格两数值天然相等
                nav, prev = fund["nav"], fund.get("prev_nav") or 0.0
                value = nav * qty / fx
                today = (nav - prev) * qty / fx if prev > 0 else 0.0
                pnl = (nav - cost) * qty / fx
                cost_usd = cost * qty / fx
                base_value += prev * qty / fx if prev > 0 else value
                close_pnl_total += today
                positions.append(_position(h, nav, value, today, pnl, "fund", False,
                                           cost_usd=cost_usd, nav_date=str(fund.get("nav_date", "")),
                                           nav_change_pct=fund.get("nav_change_pct"),
                                           close_price=nav,
                                           live_change_pct=(nav - prev) / prev * 100 if prev > 0 else None,
                                           base_ref=prev if prev > 0 else None))
        elif h["market"] == "HK":
            # 港股上市 ETF：腾讯 hk 前缀行情，HKD 计价按汇率折算 USD；无扩展时段(ext=regular)
            q = us_quotes.get(sym)
            if not q or not q.get("price"):
                # 行情缺失(stale)：HKD 成本按汇率折算 USD 口径，落入循环末尾统一累加
                positions.append(_position(h, 0.0, None, None, None, "regular", True,
                                           cost_usd=cost * qty / fx))
            else:
                # 一场口径：场基准 overrides 优先、收盘定格 overrides 优先，HKD 折算 USD
                base = (base_overrides or {}).get(sym) or q["pre_close"]
                price_now = q["price"]
                value = price_now * qty / fx
                today = (price_now - base) * qty / fx if base > 0 else 0.0
                pnl = (price_now - cost) * qty / fx
                cost_usd = cost * qty / fx
                close_price = (close_overrides or {}).get(sym) or price_now
                close_pnl_i = (close_price - base) * qty / fx if base > 0 else 0.0
                close_pnl_total += close_pnl_i
                base_value += base * qty / fx if base > 0 else value
                positions.append(_position(h, price_now, value, today, pnl, "regular", False,
                                           cost_usd=cost_usd, pre_close=q["pre_close"],
                                           quote_time=q.get("quote_time", ""),
                                           close_price=close_price,
                                           live_change_pct=(price_now - base) / base * 100 if base > 0 else None,
                                           base_ref=base if base > 0 else None))
        else:
            q = us_quotes.get(sym)
            if not q or not q.get("price"):
                # 行情缺失(stale)：USD 成本口径不变(默认 cost*qty)，落入循环末尾统一累加
                positions.append(_position(h, 0.0, None, None, None, "none", True))
            else:
                # 一场口径：场基准 overrides 优先；最新价仍盘后扩展价优先（实时累计计入盘后夜盘）
                base = (base_overrides or {}).get(sym) or q["pre_close"]
                price_now = q["ext_price"] if q.get("ext_price") else q["price"]
                ext = "pre/post" if q.get("ext_price") else "regular"
                value = price_now * qty
                today = (price_now - base) * qty if base > 0 else 0.0
                pnl = (price_now - cost) * qty
                # 收盘定格：overrides 优先（post/closed 段状态机定格 16:00 收盘价），
                # 缺失退化实时价（夜盘增量视为 0，恒等式保持成立）
                close_price = (close_overrides or {}).get(sym) or price_now
                close_pnl_i = (close_price - base) * qty if base > 0 else 0.0
                close_pnl_total += close_pnl_i
                base_value += base * qty if base > 0 else value
                positions.append(_position(h, price_now, value, today, pnl, ext, False,
                                           pre_close=q["pre_close"],
                                           quote_time=q.get("ext_time") or q.get("quote_time", ""),
                                           close_price=close_price,
                                           live_change_pct=(price_now - base) / base * 100 if base > 0 else None,
                                           base_ref=base if base > 0 else None))
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
        # 一场口径三数值：实时累计/收盘定格/夜盘增量（恒等式 day_pnl=close+overnight）
        "close_pnl": close_pnl_total,
        "overnight_pnl": day_pnl - close_pnl_total,
    }
    return positions, summary


def _position(h: dict, price, value, today, pnl, ext, stale,
              cost_usd=None, pre_close=None, quote_time="", nav_date="",
              nav_change_pct=None, close_price=None, live_change_pct=None,
              base_ref=None):
    """构造单持仓输出行（内部辅助）。金额保持原始浮点精度，不做 round。"""
    qty, cost = float(h["quantity"]), float(h["cost_price"])
    if cost_usd is None:
        cost_usd = cost * qty
    # 夜盘增量%后端统一下发：(现价-收盘定格)/收盘定格×100；定格价缺失（stale 行）
    # 为 None，前端直接消费不再二次推导，与 overnight_pnl=day_pnl-close_pnl 同源
    overnight_change_pct = ((price - close_price) / close_price * 100
                            if (close_price and price) else None)
    # 收盘定格涨跌幅%（=今日盈亏的定格版分子）：(收盘定格-场基准)/场基准×100；
    # 非 regular 时段前端优先用它展示「今日涨跌」（收盘后不再混入盘后/夜盘段）
    day_close_pct = ((close_price - base_ref) / base_ref * 100
                     if (close_price and base_ref and base_ref > 0) else None)
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
        # 一场口径：常规收盘定格价、实时累计涨跌%（基准=base_overrides 优先）、
        # 收盘定格涨跌%（今日盈亏定格版分子）、夜盘增量%（基准=收盘定格价）
        "close_price": close_price,
        "live_change_pct": live_change_pct,
        "day_close_pct": day_close_pct,
        "overnight_change_pct": overnight_change_pct,
        "ext": ext, "stale": stale,
        "quote_time": quote_time, "nav_date": nav_date, "nav_change_pct": nav_change_pct,
    }


# ---------------------------------------------------------------------------
# 网络函数（境内源直连；FT/Yahoo 优先走代理池；新浪容错版直连失败走代理）
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


# 新浪域名对腾讯云服务器 IP 直连恒 403（2026-10-08 实测 hf_/gb_/fx_/DINIW
# 全端点），命中且有代理池时代理优先，省一次注定失败的直连；代理全败再
# 回退直连保底（保留无代理池/风控解封场景能力）
_PROXY_FIRST_HOSTS = ("hq.sinajs.cn",)


async def _get_text_resilient(url: str, headers: dict | None = None,
                              proxy_urls: list[str] | None = None,
                              timeout: float = 10.0) -> str | None:
    """境内新浪源容错版 GET：先直连，403/网络失败时代理候选依次重试（云服务器 IP 常被新浪拉黑）。

    新浪域名（_PROXY_FIRST_HOSTS）例外：服务器 IP 直连恒 403，有代理池时
    直接代理优先，代理全部失败后回退直连保底。
    """
    proxies = list(proxy_urls or [])[:2]
    proxy_first = bool(proxies) and any(h in url for h in _PROXY_FIRST_HOSTS)
    if not proxy_first:
        raw = await _get_text(url, headers=headers, timeout=timeout)
        if raw is not None:
            return raw
    for proxy in proxies:
        try:
            async with httpx.AsyncClient(timeout=timeout, proxy=proxy) as client:
                resp = await client.get(url, headers=headers or {})
                if resp.status_code == 200:
                    return resp.content.decode("gbk", errors="replace")
                logger.warning("代理 %s GET %s → HTTP %d", proxy, url[:60], resp.status_code)
        except Exception as e:
            logger.warning("代理 %s GET %s 失败: %s", proxy, url[:60], e)
    if proxy_first:
        raw = await _get_text(url, headers=headers, timeout=timeout)
        if raw is not None:
            return raw
    return None


async def fetch_quotes(us_symbols: list[str], hk_symbols: list[str],
                       session: str | None = None,
                       proxy_urls: list[str] | None = None) -> dict[str, dict]:
    """抓取美股+港股行情：腾讯混合批量主源 + 美股扩展时段新浪批量 + 东财兜底。

    us_symbols/hk_symbols 分别为美股/港股代码列表；
    proxy_urls 仅用于新浪扩展时段请求的直连失败重试（腾讯主源仍直连）；
    返回 {symbol: quote}，quote 含 price/pre_close/ext_price/ext_time 等。
    """
    global _td_fail_streak, _last_td_used, _td_cursor
    all_symbols = list(us_symbols) + list(hk_symbols)
    if not all_symbols:
        return {}
    if session is None:
        session, _, _ = us_market_session()

    prefixes = {s: "us" for s in us_symbols}
    prefixes.update({s: "hk" for s in hk_symbols})
    req = ",".join(prefixes[s] + s for s in all_symbols)
    # 攒批发送路由：与 A股监控/候选池共用一个批量节奏（少请求，同批全查）
    raw = await tencent_bus.get(req.split(","))
    quotes: dict[str, dict] = {}
    if raw:
        quotes = parse_tencent_quotes(raw, all_symbols, prefixes)
        _td_fail_streak = 0
    else:
        _td_fail_streak += 1

    # 扩展时段合并美股盘前盘后/夜盘扩展价（仅美股；港股无扩展时段概念）。
    # 主源长桥（批量含官方夜盘价）；长桥连续失败>=3轮/退避/token失效时自动
    # 回切新浪 gb_ 冷备（403 概率性，低频冷备可接受），恢复后切回长桥。
    # closed 的晚间/凌晨窗口也拉取：夜间返回当日最后盘后/夜盘价，避免夜盘
    # 价格"倒退"
    if should_fetch_ext(session, datetime.now(ET_TZ)) and quotes and us_symbols:
        ext: dict[str, dict] | None = None
        try:
            import longport_fetcher as lp
            if lp.is_available():
                ext = await lp.fetch_us_ext(us_symbols, session)
        except Exception as e:
            logger.warning("长桥扩展价获取异常: %s", e)
            ext = None
        if not ext:
            # 新浪冷备：长桥不可用/本轮失败（403 时代理池容错照旧）
            raw2 = await _get_text_resilient(
                SINA_US_URL.format(symbols=",".join("gb_" + s.lower() for s in us_symbols)),
                headers=SINA_HEADERS, proxy_urls=proxy_urls,
            )
            if raw2:
                ext = parse_sina_us(raw2, us_symbols)
        if ext:
            for sym, q in quotes.items():
                e = ext.get(sym)
                if e and e.get("ext_price", 0) > 0:
                    q["ext_price"] = e["ext_price"]
                    q["ext_time"] = e["ext_time"]

    # twelvedata 兜底：主源整体失败且连续>=3轮、距上次>=60s；批量上限 8 只
    # （免费 8 credits/min），轮换指针逐次覆盖全部持仓（美股/港股混合）
    if (not quotes and TWELVEDATA_API_KEY
            and _td_fail_streak >= _TD_FAIL_THRESHOLD
            and time.monotonic() - _last_td_used >= _TD_MIN_INTERVAL_SEC):
        _last_td_used = time.monotonic()
        hk_set = set(hk_symbols)
        batch = [all_symbols[i % len(all_symbols)]
                 for i in range(_td_cursor, _td_cursor + min(TWELVEDATA_BATCH, len(all_symbols)))]
        _td_cursor = (_td_cursor + len(batch)) % len(all_symbols)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(TWELVEDATA_QUOTE_URL, params={
                    "symbol": ",".join(batch), "apikey": TWELVEDATA_API_KEY})
                data = resp.json()
                # 批量返回 {symbol: {...}}；单 symbol 返回扁平对象（统一为字典形态）
                if isinstance(data, dict) and "symbol" in data and len(batch) == 1:
                    items = {str(data.get("symbol") or batch[0]): data}
                else:
                    items = data if isinstance(data, dict) else {}
                batch_set = set(batch)
                for sym, item in items.items():
                    sym = str(sym).split(":")[0]
                    if sym in batch_set and item and _f(item.get("close")) > 0:
                        quotes[sym] = {
                            "symbol": sym, "name": item.get("name", ""),
                            "price": _f(item.get("close")), "pre_close": _f(item.get("previous_close")),
                            "change_pct": _f(item.get("percent_change")),
                            "high": _f(item.get("high")), "low": _f(item.get("low")),
                            "currency": "HKD" if sym in hk_set else "USD",
                            "quote_time": item.get("datetime", ""),
                            "ext_price": 0.0, "ext_time": "",
                        }
                logger.info("twelvedata 兜底: %d/%d (本批 %s)", len(quotes), len(all_symbols), batch)
        except Exception as e:
            logger.warning("twelvedata 兜底失败: %s", e)

    return quotes


async def fetch_us_quotes(symbols: list[str], session: str | None = None) -> dict[str, dict]:
    """抓取美股行情（向后兼容包装：委托 fetch_quotes，无港股）。"""
    return await fetch_quotes(symbols, [], session)


def derive_alias_prices(overnight: dict[str, dict], close_map: dict[str, float],
                        us_symbols: list[str]) -> dict[str, dict]:
    """同标的替代推导（纯函数）：借替代标的 24h价涨跌幅还原无交易标的的夜盘价。

    映射表 H24_ALIAS（如 QQQM→QQQ，同一基金两份额类、净值比率恒定）：
    sym 夜盘价 = sym 收盘定格 × (alias 24h价 / alias 收盘定格)——把替代标的的
    百分比变动映射到自身基准得到最新净值。前提：sym 自身无 24h价、alias 有、
    两者收盘定格齐备（腾讯常规收盘）。返回补充进 overnight 的新增项。
    """
    added: dict[str, dict] = {}
    for sym, alias in H24_ALIAS.items():
        if sym not in us_symbols or sym in overnight or alias not in overnight:
            continue
        c_self = close_map.get(sym)
        c_alias = close_map.get(alias)
        a_price = (overnight.get(alias) or {}).get("price")
        if c_self and c_alias and a_price:
            added[sym] = {"price": c_self * a_price / c_alias}
    return added


def build_cny_summary(total_value_usd: float | None, usdcnh: float | None,
                      cny_invested: float | None,
                      expense_cny: float = 0.0) -> dict | None:
    """人民币累计口径看板（纯函数）：净投入口径的累计盈亏。

    个人收益直觉口径：当前总资产(USD 含现金) × USDCNH = 折人民币总值，
    与净投入本金（sys_config.cny_invested 累计投入 − expense_cny 个人消费
    折 CNY，与记账页 identity.invest_net_cny 同口径）之差即「赚了多少人民币」；
    汇率取市场看板 USDCNH 项（新浪 fx_susdcnh，复用既有数据流零新增请求）。
    任一输入缺失（未配置本金/看板未就绪）返回 None，前端降级显示 --。
    """
    if not (total_value_usd and usdcnh and cny_invested):
        return None
    invested_net = cny_invested - (expense_cny or 0.0)
    if invested_net <= 0:
        return None
    value = total_value_usd * usdcnh
    pnl = value - invested_net
    return {
        "rate": usdcnh,
        "invested": round(invested_net, 2),
        "invested_gross": round(cny_invested, 2),
        "expense": round(expense_cny or 0.0, 2),
        "value": round(value, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl / invested_net * 100, 2),
    }


async def fetch_td_session_anchors(us_symbols: list[str]) -> dict[str, dict]:
    """冷启动一场口径锚点恢复：twelvedata quote 批量拉当日 previous_close/close。

    场景：服务重启落在非 regular 时段，需补齐一场口径锚点（base=场基准、
    close=常规收盘定格）。base 取 twelvedata previous_close（=昨日收盘，与
    regular 时段腾讯 f[4] 场基准同口径，保证 day_pnl=close+overnight 恒等式
    跨重启一致）；缺失时退化用 open（存在跳空偏差）。close 取 close 字段
    （当日常规收盘价，盘后调用语义稳定）。

    限额纪律：免费 8 credits/min（credits=symbol 数），单批 8 只、间隔 60s
    （_TD_MIN_INTERVAL_SEC 复用），多只缺失分批渐进补齐（每分钟一批）。
    返回 {symbol: {"open": x, "prev": y, "close": z}}；失败返回 {}（调用方
    退化为腾讯兜底：f[4]=场基准、f[3]=定格，凌晨/盘前窗口语义恰好正确）。
    """
    global _last_td_used
    if not TWELVEDATA_API_KEY:
        return {}   # key 未配置：跳过请求，调用方退化为腾讯兜底锚点口径
    batch = [s for s in us_symbols][:TWELVEDATA_BATCH]
    if not batch or time.monotonic() - _last_td_used < _TD_MIN_INTERVAL_SEC:
        return {}
    _last_td_used = time.monotonic()
    out: dict[str, dict] = {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(TWELVEDATA_QUOTE_URL, params={
                "symbol": ",".join(batch), "apikey": TWELVEDATA_API_KEY})
            data = resp.json()
        if isinstance(data, dict) and "symbol" in data and len(batch) == 1:
            items = {str(data.get("symbol") or batch[0]): data}
        else:
            items = data if isinstance(data, dict) else {}
        for sym, item in items.items():
            sym = str(sym).split(":")[0]
            if sym in set(batch) and item:
                o, c = _f(item.get("open")), _f(item.get("close"))
                prev = _f(item.get("previous_close"))
                if o > 0 and c > 0:
                    out[sym] = {"open": o, "prev": prev if prev > 0 else o, "close": c}
        if out:
            logger.info("一场锚点恢复(twelvedata): %d/%d (本批 %s)",
                        len(out), len(us_symbols), batch)
    except Exception as e:
        logger.warning("一场锚点恢复失败: %s", e)
    return out


async def resolve_h24_map(us_symbols: list[str],
                          proxy_urls: list[str] | None = None) -> dict[str, str]:
    """解析美股代码 → 24h数据源交易符号映射（如 AMD→AMDBUSDT）。

    exchangeInfo 全量拉取（weight 10）后过滤 status=TRADING 的可交易符号，
    规则：代码+"B"+计价后缀 精确匹配；结果缓存 24h（映射关系极少变化）。
    请求直连优先、失败经境外代理（proxy_urls 前 2 个）兜底；
    失败返回上次缓存或空 dict（不写缓存，下轮重试）；无对应交易的
    持仓（如 QQQM/PCG/AMKR/F）自然缺省，夜盘维持定格口径。
    """
    global _h24_map_cache, _h24_map_ts
    now_mono = time.monotonic()
    if _h24_map_cache and now_mono - _h24_map_ts < _H24_MAP_TTL_SEC:
        return {s: p for s, p in _h24_map_cache.items() if s in set(us_symbols)}
    candidates = list(proxy_urls or [])[:2] + [None]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.get(H24_EXCHANGE_INFO_URL)
                data = resp.json()
            pairs = {s["symbol"] for s in data.get("symbols", [])
                     if s.get("status") == "TRADING" and s.get("symbol", "").endswith("USDT")}
            mapping = {s: s + "BUSDT" for s in us_symbols if s + "BUSDT" in pairs}
            if mapping:
                _h24_map_cache, _h24_map_ts = mapping, now_mono
            return mapping
        except Exception as e:
            last_err = str(e)
    logger.warning("24h数据源 exchangeInfo 获取失败(用上次缓存): %s", last_err)
    return {s: p for s, p in _h24_map_cache.items() if s in set(us_symbols)}


async def fetch_h24_prices(bmap: dict[str, str],
                           proxy_urls: list[str] | None = None) -> dict[str, dict]:
    """批量拉 24h数据源 bookTicker，取买卖一中间价 (bid+ask)/2（研报 4.3 稳健口径）。

    攒批原则（用户：次数要少，尽量同批全查）：symbols 参数一次携带全部交易
    符号（≤20 只 weight 仅 1，URL 编码 JSON 数组），替代逐只请求（原 15 只=30
    weight/轮）；请求直连优先（实测境内可达），失败按 proxy_urls 顺序逐个
    代理重试；全部失败返回 {}（下轮再试）。
    返回 {美股代码: {"price": 中间价}}。
    """
    if not bmap:
        return {}
    out: dict[str, dict] = {}
    candidates = [None] + list(proxy_urls or [])[:2]
    pair2sym = {p: s for s, p in bmap.items()}
    symbols_param = quote(json.dumps(sorted(bmap.values()), separators=(",", ":")))
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=10, proxy=proxy) as client:
                resp = await client.get(
                    H24_BOOK_TICKER_URL + "?symbols=" + symbols_param)
                rows = resp.json()
            if not isinstance(rows, list):
                continue   # 换下一个候选重试
            for row in rows:
                sym = pair2sym.get(str(row.get("symbol", "")))
                mid = (Decimal(str(row.get("bidPrice", "0")))
                       + Decimal(str(row.get("askPrice", "0")))) / 2
                if sym and mid > 0:
                    out[sym] = {"price": float(mid)}
            return out
        except Exception:
            continue   # 换下一个代理重试；全部失败静默跳过（下轮再试）
    return out


async def fetch_overnight_quotes(us_symbols: list[str],
                                 proxy_urls: list[str] | None = None) -> dict[str, dict]:
    """夜盘/周末美股代理价抓取（24h数据源，7×24 有价）。

    美东闭市时段（夜盘 20:00-04:00 + 周末）以 24h数据源中间价近似
    夜盘价（实测偏差 0.02%-0.1%，用户允许误差），_portfolio_loop 合并后
    组合总值 24h 曲线连续；无对应交易/拉取失败的标的自然缺省，维持
    19:59 盘后价定格口径。proxy_urls 为境外代理列表（直连失败时兜底）。
    """
    bmap = await resolve_h24_map(us_symbols, proxy_urls)
    if not bmap:
        return {}
    return await fetch_h24_prices(bmap, proxy_urls)


async def fetch_equity_overnight_quotes(symbols: list[str]) -> dict[str, dict]:
    """股票行情端点补齐：bookTicker 无交易美股标的的夜盘活价（逐只并发）。

    24h数据源 bookTicker 覆盖不到、亦无同标的替代的标的（如 AMKR/PCG/F）
    经 equity quote 端点逐只补齐（接口仅支持单 symbol，weight=1/只，gather
    并发一轮一次往返），取买卖一中间价 (bid+ask)/2 与 bookTicker 同口径；
    网关域名境内直连可达（免代理）。失败/零价/symbol 不符的标的自然缺省，
    维持盘后价定格口径。返回 {美股代码: {"price": 中间价}}。
    """
    if not symbols:
        return {}
    if not EQUITY_QUOTE_URL or not EQUITY_API_KEY:
        return {}   # 网关地址/key 未配置：跳过补齐，标的维持盘后价定格口径
    headers = {"X-Auth-Token": EQUITY_API_KEY, "X-MBX-APIKEY": EQUITY_API_KEY}

    async def _one(client: httpx.AsyncClient, sym: str) -> tuple[str, dict | None]:
        try:
            resp = await client.get(EQUITY_QUOTE_URL, params={"symbol": sym})
            data = resp.json()
            mid = (Decimal(str(data.get("bidPrice", "0")))
                   + Decimal(str(data.get("askPrice", "0")))) / 2
            if data.get("symbol") == sym and mid > 0:
                return sym, {"price": float(mid)}
        except Exception:
            pass
        return sym, None

    async with httpx.AsyncClient(timeout=10, headers=headers) as client:
        pairs = await asyncio.gather(*(_one(client, s) for s in symbols))
    return {s: q for s, q in pairs if q}


async def fetch_td_fx(pair: str) -> float:
    """twelvedata 外汇行情（USD/CNH、USD/HKD 等），失败/越界返回 0.0。

    免费 8 credits/min 共享限额：USD/CNH 由 _market_loop 30min 节流调用
    （48 credits/天），USD/HKD 仅腾讯主源失败时冷备调用（可忽略）；无独立
    节流，频次由调用方控制。
    """
    if not TWELVEDATA_API_KEY:
        return 0.0   # key 未配置：跳过请求（USD/HKD 走腾讯主源/静态兜底）
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(TWELVEDATA_QUOTE_URL, params={
                "symbol": pair, "apikey": TWELVEDATA_API_KEY})
            data = resp.json()
            if isinstance(data, dict):
                v = _f(str(data.get("close") or data.get("price") or 0))
                if v > 0:
                    return v
            logger.warning("twelvedata %s 无有效价: %s", pair,
                           str(data)[:120] if isinstance(data, dict) else type(data))
    except Exception as e:
        logger.warning("twelvedata %s 获取失败: %s", pair, e)
    return 0.0


async def fetch_fx_usdhkd(proxy_urls: list[str] | None = None) -> tuple[float, bool]:
    """获取 USD/HKD 汇率。返回 (汇率, 是否兜底)。

    主源腾讯 whUSDHKD 走 tencent_bus 攒批总线（与股票/候选池同请求节奏，
    零额外请求；新浪易风控已退役），失败回切 twelvedata 冷备
    （TD 免费额度让位锚点恢复与 USDCNH 主价），再失败置 7.80 静态兜底。@author yys
    """
    raw = await tencent_bus.get(["whUSDHKD"])
    v = parse_tencent_wh_fx(raw or "")
    if v > 0:
        return v, False
    v = await fetch_td_fx("USD/HKD")
    if 7.5 <= v <= 8.5:
        return v, True
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


# ---------------------------------------------------------------------------
# 市场参考看板（QQQ/SPY/SOXX/HSI/BZ/GC/DXY/US30Y/US10Y/VIX/DJI/NDX/SPX/USDCNH + 当日分时 spark）
# ---------------------------------------------------------------------------

MARKET_PANEL = [  # (key, 腾讯代码(空=冷备/回落价 或 Yahoo 主价), 展示名, 币种, 分时口径标记)
    # 第 5 位 spark_src 仅作历史主源标记：现行分时统一读 market_ticks 组装——
    # 主价快照价变即写累积（US30Y/US10Y/VIX 由 Yahoo 主价刷新顺带落库 5m 分时），
    # 不再单独请求分时接口
    ("DJI", "", "道琼斯指数", "", "yahoo_day"),           # 主价长桥 .DJI.US 实时（Yahoo ^DJI 兜底）
    ("NDX", "", "纳指100指数", "", "yahoo_day"),          # 主价长桥 .NDX.US 实时（Yahoo ^NDX 兜底）
    ("SPX", "", "标普500指数", "", "yahoo_day"),          # 主价长桥 .SPX.US 实时（Yahoo ^GSPC 兜底）
    ("QQQ", "usQQQ", "QQQ", "USD", "yahoo_day"),          # ETF 主价腾讯混排（真指数见上方三项）
    ("SPY", "usSPY", "SPY", "USD", "yahoo_day"),          # ETF 主价腾讯混排
    ("SOXX", "usSOXX", "费城半导体", "USD", "yahoo_day"),  # 主价腾讯混排
    ("HSI", "hkHSI", "恒生指数", "HKD", "tencent"),   # 主价腾讯混排
    ("BZ", "hf_OIL", "布伦特原油", "USD", "yahoo_day"),   # 主价腾讯 hf_ 混排(新浪兜底)
    ("GC", "hf_GC", "COMEX黄金", "USD", "yahoo_day"),     # 主价腾讯 hf_ 混排(新浪兜底)
    ("DXY", "", "美元指数", "", "yahoo_day"),             # 主价 Yahoo backfill(4h,新浪批冷备)
    ("US30Y", "", "美债30Y", "%", "yahoo_day"),           # 主价 Yahoo ^TYX（刷新顺带 5m 分时落库），单位百分点
    ("US10Y", "", "美债10Y", "%", "yahoo_day"),           # 主价 Yahoo ^TNX（同上）
    ("VIX", "", "恐慌指数", "", "yahoo_day"),             # 主价长桥 .VIX.US 实时（Yahoo ^VIX 兜底）
    ("USDCNH", "", "美元人民币", "", "yahoo_day"),        # 主价 twelvedata(30min,新浪批冷备)
]


def _split_tencent_code(code: str) -> tuple[str, str]:
    """腾讯看板代码拆为 (prefix, symbol)：外盘期货 hf_ 前缀含下划线共 3 位，us/hk 取前 2 位。"""
    if code.startswith("hf_"):
        return "hf", code[3:]
    return code[:2], code[2:]


async def _fetch_yahoo_chart(symbol: str, proxy_urls: list[str] | None = None) -> dict:
    """抓取 Yahoo v8 chart：代理池优先（前2个，复用 fetch_fund_nav 候选模式），全失败重试一轮直连。

    全部失败返回 {}（该项 price 置 None，不致命）。响应为 UTF-8 JSON，用 resp.text 自动解码。
    """
    url = YAHOO_CHART_URL.format(sym=quote(symbol, safe=""))
    candidates = list(proxy_urls or [])[:2] + [None]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.get(url, headers=YAHOO_HEADERS)
                if resp.status_code == 200:
                    parsed = parse_yahoo_chart(resp.text)
                    if parsed:
                        return parsed
                    last_err = "chart 解析失败"
                else:
                    last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("Yahoo chart %s 获取失败: %s", symbol, last_err)
    return {}


async def fetch_yahoo_day_minutes(symbol: str, proxy_urls: list[str] | None = None) -> dict:
    """抓取 Yahoo v8 chart 当日分时（range=5d interval=5m 含盘前盘后，解析取最后
    UTC 自然日），代理池优先。

    代理候选取前 2 个、全失败重试一轮直连（复用 _fetch_yahoo_chart 候选模式）；
    闭市时段解析结果天然为上一交易时段全程；失败返回 {}（调用方当轮跳过，
    下轮 TTL 到期再试）。
    """
    url = YAHOO_DAY_CHART_URL.format(sym=quote(symbol, safe=""))
    candidates = list(proxy_urls or [])[:2] + [None]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.get(url, headers=YAHOO_HEADERS)
                if resp.status_code == 200:
                    parsed = parse_yahoo_day_chart(resp.text)
                    if parsed:
                        return parsed
                    last_err = "当日分时解析失败"
                else:
                    last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("Yahoo 当日分时 %s 获取失败: %s", symbol, last_err)
    return {}


async def fetch_sina_panel(proxy_urls: list[str] | None = None) -> dict:
    """抓新浪看板批（BZ/GC 兜底 + USDCNH 冷备 + DXY 回落冷备，一次合并请求）。

    数据源架构升级（2026-10-05）：本批由 server _market_loop 以 15min 节流
    低频调用（纯兜底批：BZ/GC 兜底 + USDCNH 冷备 + DXY 回落冷备；主价职责
    已全部分离——DXY→Yahoo backfill、USDCNH→twelvedata、VIX→长桥），
    产物缓存透传 fetch_market_panel。返回 {dxy: dict, cnh: float, hf: {key: dict}}。
    """
    raw = await _get_text_resilient(SINA_PANEL_URL, headers=SINA_HEADERS,
                                    proxy_urls=proxy_urls)
    if not raw:
        return {}
    cnh = parse_fx_usdcnh(raw)
    return {"dxy": parse_sina_diniw(raw), "cnh": cnh,
            "hf": {"BZ": _parse_sina_hf(raw, "OIL"), "GC": _parse_sina_hf(raw, "GC")}}


async def fetch_market_panel(proxy_urls: list[str] | None = None,
                             yahoo_day: dict[str, dict] | None = None,
                             spark_map: dict[str, list] | None = None,
                             fetch_missing: bool = True,
                             sina_panel: dict | None = None,
                             td_usdcnh: float | None = None) -> list[dict]:
    """抓市场看板 14 项主价并组装 spark：腾讯混排批量 + 新浪低频批(BZ/GC 兜底/
    USDCNH 与 DXY 冷备，server 15min 节流传入) + twelvedata USDCNH 主价 +
    Yahoo(US30Y/US10Y/DXY 主价，代理池；VIX/DJI/NDX/SPX 四项指数由长桥实时价
    经 yahoo_day 注入，Yahoo 仅兜底) + spark 经 spark_map 注入。

    数据分工：
    - 主价：QQQ/SPY/SOXX/HSI/BZ/GC 腾讯混排（BZ/GC 新浪 hf 低频兜底）；DXY
      Yahoo DX-Y.NYB backfill（4h，失败回落新浪批冷备）；USDCNH twelvedata
      主价（30min）缺失回落新浪批冷备价；US30Y/US10Y 取 yahoo_day（server
      主价刷新轮传入）；VIX/DJI/NDX/SPX 取 yahoo_day（server 长桥
      .VIX/.DJI/.NDX/.SPX 实时价填充，长桥不可用时 Yahoo ^VIX/^DJI/^NDX/^GSPC 兜底）；
    - spark：全部由 server 读库（market_ticks 主价快照累积）后经 spark_map 传入
      [ts,price] 对（前端悬浮显示时间+价格），本函数不请求分时接口；缺项置 []。
    fetch_missing=False 时跳过内部 Yahoo 补拉（server 调用方传入：节流单点化到
    server 侧 due 判定，避免缓存为空期间每轮请求的放大；默认 True 保独立可用）。
    单项失败不致命：price/change_pct 置 None（前端隐藏涨跌）、spark 置空数组。
    返回 [{key,name,price,change_pct,currency,spark:[[ts,price],...]}]，name 用 MARKET_PANEL 中文展示名。
    """
    # 1. 腾讯混排一次：6 代码（usQQQ,usSPY,usSOXX,hkHSI,hf_OIL,hf_GC），us/hk/hf 三前缀并存
    tencent_codes = [code for _, code, _, _, _ in MARKET_PANEL if code]
    code_sym = {c: _split_tencent_code(c)[1] for c in tencent_codes}   # 腾讯代码 → 输出 symbol
    sym_prefix = {s: p for p, s in (_split_tencent_code(c) for c in tencent_codes)}
    quotes: dict[str, dict] = {}
    # 攒批发送路由：看板主价与监控/候选池/持仓共用一个批量节奏
    raw = await tencent_bus.get(tencent_codes)
    if raw:
        quotes = parse_tencent_quotes(raw, list(code_sym.values()), sym_prefix)

    # 2. 新浪低频批（server 15min 节流传入）：纯兜底批——BZ/GC 兜底腾讯失败项
    #    + USDCNH 冷备价（twelvedata 缺失时）+ DXY 回落冷备价（Yahoo 缺失时）；
    #    未传入时现场拉一次保独立可用
    sina = sina_panel if sina_panel is not None else await fetch_sina_panel(proxy_urls)
    sina_dxy: dict = sina.get("dxy") or {}
    sina_cnh: dict = sina.get("cnh") or {}
    sina_hf: dict[str, dict] = sina.get("hf") or {}

    # 3. Yahoo 主价兜底：US30Y/US10Y/VIX 主价缓存缺失时并发补拉（一次请求
    #    主价+分时两用）；正常轮 server 刷新轮已传 yahoo_day，此处零请求。
    #    fetch_missing=False 直接跳过（server 侧节流单点化，缓存为空也不放大请求）
    yahoo_day = dict(yahoo_day or {})
    need_keys = [k for k in ("US30Y", "US10Y", "VIX", "DJI", "NDX", "SPX", "DXY")
                 if not (yahoo_day.get(k) or {}).get("price")] if fetch_missing else []

    async def _yahoo(key: str) -> tuple[str, dict]:
        return key, await fetch_yahoo_day_minutes(YAHOO_DAY_SYMBOLS[key], proxy_urls)

    if need_keys:
        results = await asyncio.gather(*(_yahoo(k) for k in need_keys),
                                       return_exceptions=True)
        for r in results:
            if not isinstance(r, Exception):
                yahoo_day[r[0]] = r[1]

    # 4. 组装输出：无行情的项 price/change_pct 置 None（前端隐藏涨跌），不致命；
    #    spark 统一取 spark_map（server 读库当日分时，缺项为 []）
    panel: list[dict] = []
    for key, code, name, cur, spark_src in MARKET_PANEL:
        price = change = None
        if key == "USDCNH":
            # 主价 twelvedata（30min），缺失回落新浪批冷备价（15min 鲜度）
            q = {"price": td_usdcnh} if td_usdcnh else sina_cnh
        elif key in ("BZ", "GC"):
            # 腾讯 hf 行优先，失败用新浪 hf 兜底
            q = quotes.get(code_sym.get(code, "")) or {}
            if not q.get("price"):
                q = sina_hf.get(key) or {}
        elif key in ("US30Y", "US10Y", "VIX", "DJI", "NDX", "SPX", "DXY"):
            # 主价+分时同源 Yahoo 当日分时缓存；VIX/DJI/NDX/SPX 常态由长桥
            # 实时价填充 yahoo_day（server 注入），Yahoo 仅长桥不可用时兜底；
            # DXY 唯一免费源 Yahoo（server 4h backfill 节流），失败窗口回落
            # 新浪批冷备价；收益率类 price 保留 3 位小数（前端再 fmt）
            q = yahoo_day.get(key) or {}
            if not q.get("price") and key == "DXY":
                q = sina_dxy
            if q.get("price") and key in ("US30Y", "US10Y"):
                q = {**q, "price": round(q["price"], 3)}
        else:
            q = quotes.get(code_sym.get(code, "")) or {}
        if q.get("price"):
            price, change = q["price"], q.get("change_pct")
        panel.append({"key": key, "name": name, "price": price, "change_pct": change,
                      "currency": cur,
                      "spark": list((spark_map or {}).get(key) or [])})
    return panel


async def fetch_fx_spark(proxy_urls: list[str] | None = None) -> list[float]:
    """抓 USD/HKD 近 5 日日线走势（Yahoo HKD=X，走代理池）：仅取 spark 字段。

    供 server _fund_nav_loop 随汇率刷新低频拉取；chart 失败返回空数组（不致命）。
    """
    q = await _fetch_yahoo_chart(YAHOO_PANEL_SYMBOLS["USDHKD"], proxy_urls)
    return (q.get("spark") or [])[-60:]
