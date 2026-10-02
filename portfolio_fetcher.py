"""持仓数据抓取模块：美股+港股行情(腾讯主源/新浪扩展/东财兜底) + 基金净值(FT GraphQL) + 汇率。

数据源结论见 docs/us-hk-quote-api-research.md：
- 腾讯 us/hk 前缀混合批量：常规时段行情（美股无盘前盘后，港股无扩展时段）
- 新浪 gb_ 前缀批量：字段[21]-[24] 为美股盘前/盘后实时价
- 东财 ulist 批量：兜底（仅常规价，105=NASDAQ/106=NYSE/107=AMEX/116=港股，易封 IP，≤1次/分钟）
- FT 官网 GraphQL：基金官方日频净值（fundid=31402/shclcode=2B）
- 新浪 fx_susdhkd：USD/HKD 汇率（失败兜底 7.80）
"""

import asyncio
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

from zoneinfo import ZoneInfo

# 美东时区（DST 自动正确；缺失 tzdata 时 import 阶段直接报错，保持 fail-fast）
ET_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# 接口常量
# ---------------------------------------------------------------------------

TENCENT_US_URL = "http://qt.gtimg.cn/q={symbols}"
TENCENT_MINUTE_URL = "https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={code}"
SINA_US_URL = "https://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}
SINA_FX_URL = "https://hq.sinajs.cn/list=fx_susdhkd"
SINA_PANEL_URL = "https://hq.sinajs.cn/list=DINIW,hf_OIL,hf_GC"
EM_ULIST_URL = "http://push2.eastmoney.com/api/qt/ulist.np/get"
EM_FIELDS = "f2,f3,f12,f13,f14,f18,f124"
# Yahoo v8 chart（美债收益率/恐慌指数）：v7 quote 已封（Unauthorized），v8 需 UA 伪装 + 代理
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
YAHOO_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"),
}
# Yahoo v8 chart 映射：US30Y/US10Y/VIX 主价+spark 均取 Yahoo；
# BZ/GC/DXY 仅取 spark（主价以腾讯/新浪为准）；USDHKD 供 server fx_spark 低频拉取
YAHOO_PANEL_SYMBOLS = {"US30Y": "^TYX", "US10Y": "^TNX", "VIX": "^VIX",
                       "BZ": "BZ=F", "GC": "GC=F", "DXY": "DX-Y.NYB",
                       "USDHKD": "HKD=X"}
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

# 东财兜底市场号（105=NASDAQ 106=NYSE 107=AMEX 116=港股），未知美股符号默认 105
EM_MARKET_MAP = {
    "MU": "105", "SKHY": "105", "QQQ": "105", "QQQM": "105", "AMD": "105",
    "AMKR": "105", "SMH": "105", "NVDA": "105", "LITE": "105",
    "BABA": "106", "PCG": "106", "F": "106",
    "SPYM": "107", "DRAM": "107", "DISK": "107",
}
EM_DEFAULT_MARKET = "105"
EM_HK_MARKET = "116"  # 东财港股市场号（secid 格式 "116.03152"）

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


def parse_tencent_minute(raw: str) -> list[tuple[str, float]]:
    """解析腾讯分时接口为 [(HHMM, 价格)]；JSON 非法/结构缺失返回 []。"""
    try:
        d = json.loads(raw)["data"]
        code = next(iter(d))
        rows = d[code]["data"]["data"]
        out: list[tuple[str, float]] = []
        for r in rows:
            p = str(r).split()
            if len(p) >= 2:
                out.append((p[0], float(p[1])))
        return out
    except Exception:
        return []


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
      （数据源保证=一场基准）；非 regular 沿用上一值；冷启动/重启落在非 regular
      时段时退化用当轮 f[4]（次日凌晨该值语义可能已切换，存在隔夜偏差，
      09:30 开盘自愈）；
    - close_prices: {symbol: 常规收盘定格价}——regular 时段跟随实时价（定格于
      16:00 最后一条）；post/closed(≥16:00) 取腾讯现价(f[3]，实测盘后保持收盘价)；
      次日凌晨(<04:00)/盘前取 f[4]（实测已切换为昨日常规收盘）；
    - base_logged:  bool，冷启动退化仅标注一次的开关；
    - base_fallback: list，本轮冷启动退化填充的 symbol（供调用方打日志；
      无退化时重置为 []——本函数无 IO，日志由调用方负责）。

    分支语义（session ∈ pre/regular/post/closed，et_dt 为美东时钟）：
    - regular：f[4] 即一场基准每轮刷新；收盘定格跟随实时价；
    - post 或 closed 且 et_hour>=16：f[3] 实测保持 16:00 收盘价 → 收盘定格；
    - pre 或 closed 且 et_hour<4：f[4] 实测已切换为昨日常规收盘 → 定格兜底；
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
    elif session == "pre" or (session == "closed" and et_hour < 4):
        # 次日凌晨/盘前：腾讯昨收(f[4])实测已切换为昨日常规收盘 → 定格兜底
        for sym, q in quotes.items():
            if q.get("pre_close"):
                close_prices[sym] = q["pre_close"]
    state["base_fallback"] = []
    if session != "regular":
        # 非 regular 沿用已维护基准；冷启动/重启缺失时退化用当轮 f[4]
        # （次日凌晨该值可能已是次日语义，存在隔夜偏差，09:30 开盘自愈）
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
                                           live_change_pct=(nav - prev) / prev * 100 if prev > 0 else None))
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
                                           live_change_pct=(price_now - base) / base * 100 if base > 0 else None))
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
                                           live_change_pct=(price_now - base) / base * 100 if base > 0 else None))
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
              nav_change_pct=None, close_price=None, live_change_pct=None):
    """构造单持仓输出行（内部辅助）。金额保持原始浮点精度，不做 round。"""
    qty, cost = float(h["quantity"]), float(h["cost_price"])
    if cost_usd is None:
        cost_usd = cost * qty
    # 夜盘增量%后端统一下发：(现价-收盘定格)/收盘定格×100；定格价缺失（stale 行）
    # 为 None，前端直接消费不再二次推导，与 overnight_pnl=day_pnl-close_pnl 同源
    overnight_change_pct = ((price - close_price) / close_price * 100
                            if (close_price and price) else None)
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
        # 夜盘增量%（基准=收盘定格价，后端统一下发）
        "close_price": close_price,
        "live_change_pct": live_change_pct,
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


async def _get_text_resilient(url: str, headers: dict | None = None,
                              proxy_urls: list[str] | None = None,
                              timeout: float = 10.0) -> str | None:
    """境内新浪源容错版 GET：先直连，403/网络失败时代理候选依次重试（云服务器 IP 常被新浪拉黑）。"""
    raw = await _get_text(url, headers=headers, timeout=timeout)
    if raw is not None:
        return raw
    for proxy in list(proxy_urls or [])[:2]:
        try:
            async with httpx.AsyncClient(timeout=timeout, proxy=proxy) as client:
                resp = await client.get(url, headers=headers or {})
                if resp.status_code == 200:
                    return resp.content.decode("gbk", errors="replace")
                logger.warning("代理 %s GET %s → HTTP %d", proxy, url[:60], resp.status_code)
        except Exception as e:
            logger.warning("代理 %s GET %s 失败: %s", proxy, url[:60], e)
    return None


async def fetch_quotes(us_symbols: list[str], hk_symbols: list[str],
                       session: str | None = None,
                       proxy_urls: list[str] | None = None) -> dict[str, dict]:
    """抓取美股+港股行情：腾讯混合批量主源 + 美股扩展时段新浪批量 + 东财兜底。

    us_symbols/hk_symbols 分别为美股/港股代码列表；
    proxy_urls 仅用于新浪扩展时段请求的直连失败重试（腾讯主源仍直连）；
    返回 {symbol: quote}，quote 含 price/pre_close/ext_price/ext_time 等。
    """
    global _em_fail_streak, _last_em_used
    all_symbols = list(us_symbols) + list(hk_symbols)
    if not all_symbols:
        return {}
    if session is None:
        session, _, _ = us_market_session()

    prefixes = {s: "us" for s in us_symbols}
    prefixes.update({s: "hk" for s in hk_symbols})
    req = ",".join(prefixes[s] + s for s in all_symbols)
    raw = await _get_text(TENCENT_US_URL.format(symbols=req))
    quotes: dict[str, dict] = {}
    if raw:
        quotes = parse_tencent_quotes(raw, all_symbols, prefixes)
        _em_fail_streak = 0
    else:
        _em_fail_streak += 1

    # 扩展时段合并新浪盘前盘后价（仅美股；港股无扩展时段概念）。
    # closed 的晚间/凌晨窗口也拉取：新浪夜间返回当日最后盘后价，避免夜盘价格"倒退"
    if should_fetch_ext(session, datetime.now(ET_TZ)) and quotes and us_symbols:
        raw2 = await _get_text_resilient(
            SINA_US_URL.format(symbols=",".join("gb_" + s.lower() for s in us_symbols)),
            headers=SINA_HEADERS, proxy_urls=proxy_urls,
        )
        if raw2:
            ext = parse_sina_us(raw2, us_symbols)
            for sym, q in quotes.items():
                e = ext.get(sym)
                if e and e.get("ext_price", 0) > 0:
                    q["ext_price"] = e["ext_price"]
                    q["ext_time"] = e["ext_time"]

    # 东财兜底：主源整体失败且连续>=3轮、距上次>=60s（美股/港股混合请求）
    if not quotes and _em_fail_streak >= _EM_FAIL_THRESHOLD and time.monotonic() - _last_em_used >= _EM_MIN_INTERVAL_SEC:
        _last_em_used = time.monotonic()
        hk_set = set(hk_symbols)
        secids = ",".join(
            f"{EM_HK_MARKET}.{s}" if s in hk_set else f"{EM_MARKET_MAP.get(s, EM_DEFAULT_MARKET)}.{s}"
            for s in all_symbols
        )
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(EM_ULIST_URL, params={
                    "secids": secids, "fields": EM_FIELDS, "fltt": "2", "invt": "2"})
                data = resp.json()
                for item in (data.get("data") or {}).get("diff") or []:
                    sym = item.get("f12")
                    if sym in prefixes:
                        quotes[sym] = {
                            "symbol": sym, "name": item.get("f14", ""),
                            "price": _f(item.get("f2")), "pre_close": _f(item.get("f18")),
                            "change_pct": _f(item.get("f3")), "high": 0.0, "low": 0.0,
                            "currency": "HKD" if sym in hk_set else "USD", "quote_time": "",
                            "ext_price": 0.0, "ext_time": "",
                        }
                logger.info("东财兜底: %d/%d", len(quotes), len(all_symbols))
        except Exception as e:
            logger.warning("东财兜底失败: %s", e)

    return quotes


async def fetch_us_quotes(symbols: list[str], session: str | None = None) -> dict[str, dict]:
    """抓取美股行情（向后兼容包装：委托 fetch_quotes，无港股）。"""
    return await fetch_quotes(symbols, [], session)


async def fetch_overnight_quotes(us_symbols: list[str],
                                 proxy_urls: list[str] | None = None) -> dict[str, dict]:
    """夜盘 ATS 实时行情抓取（美东 20:00-04:00 时段专用源插槽）。

    当前恒返回 {}：美股夜盘撮合（ATS 盘后夜间时段）行情无稳定免费源（腾讯/新浪/
    东财/Yahoo v8 均实测不提供），20:00-04:00 期间以 19:59 盘后价定格展示；未来
    接入夜盘源（如 IBKR/Polygon 等）时在此返回 {symbol: {"price": 最新价}}，
    _portfolio_loop 检测到非空结果会合并为实时价，结果为空自然降级定格口径。
    proxy_urls 预留：未来夜盘源若需代理池透传（取法仿 _get_text_resilient）。
    """
    return {}


async def fetch_fx_usdhkd(proxy_urls: list[str] | None = None) -> tuple[float, bool]:
    """获取 USD/HKD 汇率。返回 (汇率, 是否兜底)。

    直连 403/失败时按 proxy_urls 前 2 个代理重试（云服务器 IP 常被新浪拉黑）。
    """
    raw = await _get_text_resilient(SINA_FX_URL, headers=SINA_HEADERS, proxy_urls=proxy_urls)
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


# ---------------------------------------------------------------------------
# 市场参考看板（QQQ/SPY/SOXX/HSI/BZ/GC/DXY/US30Y/US10Y/VIX + sparkline）
# ---------------------------------------------------------------------------

MARKET_PANEL = [  # (key, 腾讯代码(空=新浪DINIW或Yahoo见YAHOO_PANEL_SYMBOLS), 展示名, 币种, spark 数据源)
    # 第 5 位 spark_source："tencent"=腾讯分时接口（仅股指，code 必须是 us/hk 股指代码）、
    # "yahoo"=Yahoo v8 chart 日线、" "=无 spark。显式双源分流，hf_ 期货代码绝不进腾讯分时拉取
    ("QQQ", "usQQQ", "纳指100", "USD", "tencent"),
    ("SPY", "usSPY", "标普500", "USD", "tencent"),
    ("SOXX", "usSOXX", "费城半导体", "USD", "tencent"),
    ("HSI", "hkHSI", "恒生指数", "HKD", "tencent"),
    ("BZ", "hf_OIL", "布伦特原油", "USD", "yahoo"),   # 主价腾讯 hf_ 混排(新浪兜底)；spark 走 Yahoo BZ=F
    ("GC", "hf_GC", "COMEX黄金", "USD", "yahoo"),     # 主价腾讯 hf_ 混排(新浪兜底)；spark 走 Yahoo GC=F
    ("DXY", "", "美元指数", "", "yahoo"),             # 主价新浪 DINIW；spark 走 Yahoo DX-Y.NYB
    ("US30Y", "", "美债30Y", "%", "yahoo"),           # Yahoo ^TYX，单位百分点
    ("US10Y", "", "美债10Y", "%", "yahoo"),           # Yahoo ^TNX，单位百分点
    ("VIX", "", "恐慌指数", "", "yahoo"),             # Yahoo ^VIX
]


def _downsample_minute(points: list[tuple[str, float]], limit: int = 60) -> list[float]:
    """分时点均匀降采样到 <=limit 个，保留首尾，输出纯价格序列供 sparkline 使用。"""
    if not points:
        return []
    if len(points) <= limit:
        return [p[1] for p in points]
    step = (len(points) - 1) / (limit - 1)
    idxs = [round(i * step) for i in range(limit)]
    return [points[i][1] for i in idxs]


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


async def fetch_market_panel(proxy_urls: list[str] | None = None) -> list[dict]:
    """抓市场看板 10 项：腾讯混排批量 + 新浪(DINIW主用/hf兜底) + Yahoo chart(代理池) + sparkline。

    数据分工（MARKET_PANEL 第 5 位 spark_source 显式声明来源）：
    - QQQ/SPY/SOXX/HSI：腾讯 us/hk 波浪线行情 + minute 分时 spark（tencent，降采样≤60点）
    - BZ/GC：主价腾讯 hf_ 逗号行（与股指同一混排请求），新浪 hf_OIL/hf_GC 兜底；
      走势 spark 走 Yahoo BZ=F/GC=F 日线（yahoo，仅取 spark，主价不从 Yahoo 取）
    - DXY：主价新浪 DINIW；走势 spark 走 Yahoo DX-Y.NYB 日线（yahoo，仅取 spark）
    - US30Y/US10Y/VIX：Yahoo v8 chart（代理池优先，全失败直连重试），主价+spark 同源
    单项失败不致命：price/change_pct 置 None（前端隐藏涨跌）、spark 置空数组。
    返回 [{key,name,price,change_pct,currency,spark:[float,...]}]，name 用 MARKET_PANEL 中文展示名。
    """
    # 1. 腾讯混排一次：6 代码（usQQQ,usSPY,usSOXX,hkHSI,hf_OIL,hf_GC），us/hk/hf 三前缀并存
    tencent_codes = [code for _, code, _, _, _ in MARKET_PANEL if code]
    code_sym = {c: _split_tencent_code(c)[1] for c in tencent_codes}   # 腾讯代码 → 输出 symbol
    sym_prefix = {s: p for p, s in (_split_tencent_code(c) for c in tencent_codes)}
    quotes: dict[str, dict] = {}
    raw = await _get_text(TENCENT_US_URL.format(symbols=",".join(tencent_codes)))
    if raw:
        quotes = parse_tencent_quotes(raw, list(code_sym.values()), sym_prefix)

    # 2. 新浪一次：DINIW 主用（美元指数）+ hf_OIL/hf_GC 兜底腾讯失败项
    sina_dxy: dict = {}
    sina_hf: dict[str, dict] = {}
    raw_sina = await _get_text_resilient(SINA_PANEL_URL, headers=SINA_HEADERS,
                                         proxy_urls=proxy_urls)
    if raw_sina:
        sina_dxy = parse_sina_diniw(raw_sina)
        sina_hf["BZ"] = _parse_sina_hf(raw_sina, "OIL")
        sina_hf["GC"] = _parse_sina_hf(raw_sina, "GC")

    # 3. Yahoo v8 chart 并发 6 个（^TYX/^TNX/^VIX 主价+spark；BZ=F/GC=F/DX-Y.NYB 仅 spark）：
    #    代理池优先、直连兜底；USDHKD 不在面板并发内（server 循环经 fetch_fx_spark 低频拉取）
    panel_yahoo_keys = [k for k in YAHOO_PANEL_SYMBOLS if k != "USDHKD"]

    async def _yahoo(key: str) -> tuple[str, dict]:
        return key, await _fetch_yahoo_chart(YAHOO_PANEL_SYMBOLS[key], proxy_urls)

    yahoo_map: dict[str, dict] = {}
    results = await asyncio.gather(*(_yahoo(k) for k in panel_yahoo_keys),
                                   return_exceptions=True)
    for r in results:
        if not isinstance(r, Exception):
            yahoo_map[r[0]] = r[1]

    # 4. sparkline：仅 spark_source="tencent" 的股指项并发拉腾讯分时接口并降采样（失败→空数组）；
    #    BZ/GC 等 yahoo 源项的 spark 已在第 3 步 Yahoo chart 批量里，不进腾讯分时请求（消除死请求）
    spark_map: dict[str, list[float]] = {}
    spark_codes = [code for _, code, _, _, src in MARKET_PANEL if src == "tencent" and code]

    async def _spark(code: str) -> tuple[str, list[float]]:
        # _get_text 失败返回 None，parse_tencent_minute 对 None 返回 []，故不会抛出
        return code, _downsample_minute(parse_tencent_minute(await _get_text(
            TENCENT_MINUTE_URL.format(code=code)) or ""))

    results = await asyncio.gather(*(_spark(c) for c in spark_codes), return_exceptions=True)
    for r in results:
        if not isinstance(r, Exception):
            spark_map[r[0]] = r[1]

    # 5. 组装输出：无行情的项 price/change_pct 置 None（前端隐藏涨跌），不致命
    panel: list[dict] = []
    for key, code, name, cur, spark_src in MARKET_PANEL:
        price = change = None
        spark_data: list[float] = []
        # Yahoo chart 结果：US30Y/US10Y/VIX 主价+spark 同源；BZ/GC/DXY 仅取 spark
        yq = yahoo_map.get(key) or {}
        if key == "DXY":
            # 主价新浪 DINIW；走势 spark 用 Yahoo 日线（主价不从 Yahoo 取）
            q = sina_dxy
            if spark_src == "yahoo":
                spark_data = (yq.get("spark") or [])[-60:]
        elif key in ("BZ", "GC"):
            # 腾讯 hf 行优先，失败用新浪 hf 兜底；走势 spark 用 Yahoo 日线
            q = quotes.get(code_sym.get(code, "")) or {}
            if not q.get("price"):
                q = sina_hf.get(key) or {}
            if spark_src == "yahoo":
                spark_data = (yq.get("spark") or [])[-60:]
        elif key in ("US30Y", "US10Y", "VIX"):
            q = yq
            # 收益率类（美债30Y/10Y）price 保留 3 位小数（前端再 fmt）
            if q.get("price") and key in ("US30Y", "US10Y"):
                q = {**q, "price": round(q["price"], 3)}
            if spark_src == "yahoo":
                spark_data = (q.get("spark") or [])[-60:]
        else:
            q = quotes.get(code_sym.get(code, "")) or {}
            if spark_src == "tencent":
                spark_data = spark_map.get(code, [])
        if q.get("price"):
            price, change = q["price"], q.get("change_pct")
        panel.append({"key": key, "name": name, "price": price, "change_pct": change,
                      "currency": cur, "spark": spark_data})
    return panel


async def fetch_fx_spark(proxy_urls: list[str] | None = None) -> list[float]:
    """抓 USD/HKD 近 5 日日线走势（Yahoo HKD=X，走代理池）：仅取 spark 字段。

    供 server _fund_nav_loop 随汇率刷新低频拉取；chart 失败返回空数组（不致命）。
    """
    q = await _fetch_yahoo_chart(YAHOO_PANEL_SYMBOLS["USDHKD"], proxy_urls)
    return (q.get("spark") or [])[-60:]
