"""日K数据底座：长桥前复权日K增量落库 + 技术指标缓存。

表 daily_bars 增量累积（UNIQUE 幂等）；指标每日刷新后预算缓存，
提醒引擎求值只做比对。@author yys
"""
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

import database as db

logger = logging.getLogger("qdii.daily_bars")

ATR_PERIOD = 22
MA_FAST = 50
MA_SLOW = 200
RSI_PERIOD = 14
HHV_PERIOD = 22
DEFAULT_ATR_K = 3.0
WEEK52_WINDOW = 252      # 52 周交易日（deep_dip 回撤基准窗口）
VIX_PCTL_WINDOW = 504    # 2 年交易日（VIX 低分位止盈过滤窗口）

# 指标缓存：{symbol: {atr22, ma50, ma200, rsi14, hhv22, stop_level, enough_history, asof}}
INDICATORS: dict[str, dict] = {}

# VIX 2 年分位缓存（refresh_symbol("VIX") 成功后重算；报告 V2 止盈过滤条件）
VIX_PCTL: float | None = None
VIX_ASOF: str | None = None


def sma(values: list[float], n: int) -> list:
    """简单均线，前 n-1 个为 None。"""
    out = [None] * len(values)
    acc = 0.0
    for i, v in enumerate(values):
        acc += v
        if i >= n:
            acc -= values[i - n]
        if i >= n - 1:
            out[i] = acc / n
    return out


def rsi_wilder(closes: list[float], n: int = RSI_PERIOD) -> list:
    """Wilder 平滑 RSI，前 n 个为 None。"""
    out = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / n, losses / n
    out[n] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    return out


def atr_wilder(bars: list[dict], n: int = ATR_PERIOD) -> list:
    """Wilder 平滑 ATR（用 high/low/close），前 n 个为 None。

    内部对 high/low/close 做 float() 归一：兼容长桥 SDK 与 pymysql
    DECIMAL 列返回的 Decimal（Decimal 与 float 混算会 TypeError）。
    """
    if len(bars) <= n:
        return [None] * len(bars)
    trs = [float(bars[0]["high"]) - float(bars[0]["low"])]
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[1:n + 1]) / n
    out = [None] * n + [atr]
    for i in range(n + 1, len(bars)):
        atr = (atr * (n - 1) + trs[i]) / n
        out.append(atr)
    return out


def hhv(values: list[float], n: int = HHV_PERIOD) -> list:
    """滚动窗口最高值，前 n-1 个为 None。"""
    out = [None] * len(values)
    for i in range(n - 1, len(values)):
        out[i] = max(values[i - n + 1: i + 1])
    return out


def ema(values: list[float | None], n: int) -> list:
    """指数移动平均（首值以首个有效值 seed，标准国内行情软件口径）。"""
    out: list = [None] * len(values)
    k = 2.0 / (n + 1)
    prev = None
    for i, v in enumerate(values):
        if v is None:
            continue
        prev = float(v) if prev is None else (float(v) - prev) * k + prev
        out[i] = prev
    return out


def macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """MACD(12,26,9)：DIF=EMA快−EMA慢、DEA=DIF 的 EMA、柱=2×(DIF−DEA)（国内惯例）。

    EMA 首值 seed 使全序列非 None（暖机段为近似值，随样本收敛）。
    契约：closes 须全非 None（None 进 float() 直接 TypeError）。
    """
    cs = [float(c) for c in closes]
    ef, es = ema(cs, fast), ema(cs, slow)
    dif = [a - b if a is not None and b is not None else None
           for a, b in zip(ef, es)]
    dea = ema([v if v is not None else 0.0 for v in dif], signal)
    hist = [2.0 * (d - e) if d is not None and e is not None else None
            for d, e in zip(dif, dea)]
    return {"dif": dif, "dea": dea, "hist": hist}


def kdj(highs: list, lows: list, closes: list,
        n: int = 9, m1: int = 3, m2: int = 3) -> dict:
    """KDJ(9,3,3)：RSV=(C−Ln)/(Hn−Ln)×100；K/D 按 m1/m2 平滑（初值 50）；J=3K−2D。

    前 n−1 根样本不足置 None；Hn=Ln（一字）时 RSV=50 防除零。
    """
    hs = [float(x) for x in highs]
    ls = [float(x) for x in lows]
    cs = [float(x) for x in closes]
    k_prev = d_prev = 50.0
    out = {"k": [], "d": [], "j": []}
    for i in range(len(cs)):
        if i < n - 1:
            out["k"].append(None); out["d"].append(None); out["j"].append(None)
            continue
        hh = max(hs[i - n + 1:i + 1])
        ll = min(ls[i - n + 1:i + 1])
        rsv = 50.0 if hh == ll else (cs[i] - ll) / (hh - ll) * 100.0
        k = (m1 - 1) / m1 * k_prev + 1.0 / m1 * rsv
        d = (m2 - 1) / m2 * d_prev + 1.0 / m2 * k
        out["k"].append(k); out["d"].append(d); out["j"].append(3.0 * k - 2.0 * d)
        k_prev, d_prev = k, d
    return out


def aggregate_bars(bars: list[dict], tf: str) -> list[dict]:
    """日K聚合周K/月K：ISO 周（周一起始）/自然月分组。

    开=组首开、收=组末收、高=区间最高、低=区间最低、量=区间和、date=组首交易日。
    day 原样返回（调用方无需分支）。date 兼容 str/date/datetime；
    输入须按时间升序（依赖顺序分组，乱序会错切组）。
    """
    if tf not in ("week", "month"):
        return bars

    def _d(b):
        v = b["date"]
        # datetime 是 date 子类必须先判，且 str() 带 " 00:00:00" 后缀须截断
        if isinstance(v, datetime):
            return date.fromisoformat(str(v)[:10])
        return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])

    def _key(d):
        return (d.isocalendar()[0], d.isocalendar()[1]) if tf == "week" \
            else (d.year, d.month)

    out: list[dict] = []
    cur_key = None
    agg: dict | None = None
    for b in bars:
        d = _d(b)
        key = _key(d)
        if key != cur_key:
            if agg:
                out.append(agg)
            cur_key = key
            agg = {"date": str(d), "open": float(b["open"]),
                   "high": float(b["high"]), "low": float(b["low"]),
                   "close": float(b["close"]), "volume": int(b["volume"] or 0)}
        else:
            agg["high"] = max(agg["high"], float(b["high"]))
            agg["low"] = min(agg["low"], float(b["low"]))
            agg["close"] = float(b["close"])
            agg["volume"] += int(b["volume"] or 0)
    if agg:
        out.append(agg)
    return out


def ticks_to_bars(rows: list[dict], bucket_sec: int = 300) -> list[dict]:
    """market_ticks 聚合分时K（库内分时K专用，UTC naive 口径）。

    开=桶首价、收=桶末价、高/低=桶内极值、volume=tick 数（活跃度近似，
    ticks 无量能字段）；桶边界按 UTC epoch 分桶，date 标签转北京时间
    显示（+8h，与看板 spark 悬浮「显示北京时间」先例一致）。
    输入须按时间升序（依赖顺序分桶，乱序会错切桶）。
    """
    if not rows:
        return []
    bars: list[dict] = []
    cur_key = None
    for r in rows:
        ts = r["ts"]
        p = float(r["price"])
        # UTC naive → 必须显式挂 UTC 再取 epoch（否则按服务器本地时区错位）
        epoch = ts.replace(tzinfo=timezone.utc).timestamp() \
            if ts.tzinfo is None else ts.timestamp()
        key = int(epoch // bucket_sec)
        if key != cur_key:
            cur_key = key
            # 桶边界按 UTC epoch 分桶，仅显示标签转北京时间
            label = (ts + timedelta(hours=8)).strftime("%m-%d %H:%M")
            bars.append({"date": label,
                         "open": p, "high": p, "low": p, "close": p, "volume": 1})
        else:
            b = bars[-1]
            b["high"] = max(b["high"], p)
            b["low"] = min(b["low"], p)
            b["close"] = p
            b["volume"] += 1
    return bars


def compute_indicators(bars: list[dict], k: float = DEFAULT_ATR_K) -> dict:
    """最新指标快照（bars 升序）。历史不足时对应项为 None。

    入参 bars 的价格字段兼容 Decimal（长桥 SDK / pymysql DECIMAL），
    内部统一归一为 float 后计算，避免混型 TypeError。
    """
    if not bars:
        raise ValueError("compute_indicators 需要至少一根日K")
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    ma50 = sma(closes, MA_FAST)
    ma200 = sma(closes, MA_SLOW)
    rsi = rsi_wilder(closes)
    # 归一化 bars 再喂 atr_wilder（date 保留原值，指标只用 high/low/close）
    norm_bars = [{"high": float(b["high"]), "low": float(b["low"]),
                  "close": float(b["close"]), "date": b.get("date")} for b in bars]
    atr = atr_wilder(norm_bars)
    hh = hhv(highs)
    week52 = max(closes[-WEEK52_WINDOW:]) \
        if len(closes) >= WEEK52_WINDOW else None
    i = len(bars) - 1
    enough = len(bars) >= MA_SLOW
    atr_v = atr[i]
    hh_v = hh[i]
    return {
        "asof": bars[i].get("date"),
        "enough_history": enough,
        "ma50": ma50[i], "ma200": ma200[i], "rsi14": rsi[i],
        "atr22": atr_v, "hhv22": hh_v,
        "week52_high": week52,
        "stop_level": (hh_v - k * atr_v) if (hh_v is not None and atr_v is not None) else None,
        "last_close": closes[i],
    }


def get_indicators(symbol: str) -> dict | None:
    """读缓存（刷新后有效；未加载返回 None，调用方按规则禁用处理）。"""
    return INDICATORS.get(symbol)


def _vix_percentile(closes: list[float]) -> float:
    """当日收盘在近 VIX_PCTL_WINDOW 个交易日（含当日，2 年）中的百分位（0~100）。

    @author yys"""
    win = closes[-VIX_PCTL_WINDOW:]
    cur = win[-1]
    return sum(1 for v in win if v <= cur) / len(win) * 100.0


def get_vix_percentile(qqq_asof: str | None = None) -> float | None:
    """VIX 2 年分位；缓存为空或 VIX 落后 QQQ 基准日（拉取失败）返回 None。

    None 语义 = 止盈规则不触发（回测报告同款 NaN 处理）。
    @author yys"""
    if VIX_PCTL is None or not VIX_ASOF:
        return None
    if qqq_asof and str(qqq_asof)[:10] > VIX_ASOF:
        return None
    return VIX_PCTL


# ---------------------------------------------------------------------------
# 长桥日K拉取 → 增量合并 → 除权检测 → 落库 → 指标重算（每日 07:00 刷新）
# ---------------------------------------------------------------------------


def lp_symbol(symbol: str) -> str:
    """长桥符号必须带 .US 后缀（缺后缀静默空陷阱；BRK.B 这类带点
    符号不能以 "." 判定，必须 endswith(".US")）。"""
    return symbol if symbol.endswith(".US") else f"{symbol}.US"


def merge_bars(existing: dict[str, float], fetched: list[dict]) -> tuple[list, bool]:
    """增量合并：返回 (新增行列表, 是否需要整段重拉)。

    existing: {date_str: close}；fetched: [{date, close,...}] 升序（open/
    high/low/volume 可缺省，纯逻辑用例只带 date/close）。重叠区间 close
    偏差 >0.5% 判定除权/拆股 → 整段重拉。"""
    reload_needed = False
    for b in fetched:
        d = str(b["date"])[:10]
        old = existing.get(d)
        if old is not None and old > 0 and abs(float(b["close"]) - old) / old > 0.005:
            reload_needed = True
    max_d = max(existing) if existing else None
    new_rows = [(b["date"], float(b.get("open") or 0), float(b.get("high") or 0),
                 float(b.get("low") or 0), float(b["close"]),
                 int(b.get("volume") or 0))
                for b in fetched if max_d is None or str(b["date"])[:10] > max_d]
    return new_rows, reload_needed


async def refresh_symbol(symbol: str) -> dict:
    """单只增量刷新：拉取→除权检测→落库→重算指标缓存。@author yys"""
    global VIX_PCTL, VIX_ASOF
    import longport_fetcher as lp
    if not lp.is_available():
        return {"symbol": symbol, "skipped": "longport_unavailable"}
    bars_raw = await lp.fetch_daily_bars_raw(lp_symbol(symbol), 1000)
    if not bars_raw:
        return {"symbol": symbol, "skipped": "empty_or_failed"}
    fetched = [{"date": b.timestamp.strftime("%Y-%m-%d"),
                "open": b.open, "high": b.high, "low": b.low,
                "close": b.close, "volume": b.volume} for b in bars_raw]
    fetched.sort(key=lambda b: b["date"])
    existing_rows = await db.get_daily_bars(symbol)
    existing = {str(r["date"])[:10]: float(r["close"]) for r in existing_rows}
    new_rows, reload_needed = merge_bars(existing, fetched)
    if reload_needed:
        rows = [(b["date"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                for b in fetched]
        await db.replace_daily_bars(symbol, rows)
        logger.info("日K除权事件整段重拉: %s (%d 根)", symbol, len(rows))
    elif new_rows:
        await db.save_daily_bars(symbol, new_rows)
    bars = await db.get_daily_bars(symbol)
    INDICATORS[symbol] = compute_indicators(bars)
    if symbol == "VIX":
        vix_closes = [float(b["close"]) for b in bars]
        VIX_PCTL = _vix_percentile(vix_closes)
        VIX_ASOF = str(bars[-1]["date"])[:10]
    return {"symbol": symbol, "added": len(new_rows), "reload": reload_needed,
            "total": len(bars)}


async def refresh_all(us_symbols: list[str], gap_seconds: float = 3.0) -> dict:
    """全部持仓逐只刷新（串行、间隔 ≥3s，限流契约）；GAPS 记录不可用符号。@author yys"""
    result = {}
    gaps = []
    for i, sym in enumerate(us_symbols):
        if i:
            await asyncio.sleep(gap_seconds)
        try:
            r = await refresh_symbol(sym)
            result[sym] = r
            if r.get("skipped"):
                gaps.append(sym)
        except Exception as e:
            logger.warning("日K刷新失败 %s: %s", sym, e)
            gaps.append(sym)
    try:
        old = await db.get_config("DAILY_BARS_GAPS")
        gap_set = {s for s in (old or "").split(",") if s} if old else set()
        if gap_set != set(gaps):
            await db.set_config("DAILY_BARS_GAPS", ",".join(gaps), "长桥无日K符号清单")
    except Exception as e:
        logger.warning("GAPS 写回失败: %s", e)
    return result


async def daily_refresh_loop(holdings_loader=None):
    """后台任务：启动即刷一次，此后每日北京时间 07:00 刷新。@author yys

    holdings_loader: async () -> list[dict]（默认 db.get_holdings）。"""
    loader = holdings_loader or (lambda: db.get_holdings(enabled_only=True))
    while True:
        try:
            holdings = await loader()
            symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if "VIX" not in symbols:      # VIX 分位数据源（V2 止盈过滤），固定追加不依赖持仓
                symbols.append("VIX")
            if symbols:
                logger.info("日K刷新开始: %d 只", len(symbols))
                await refresh_all(symbols)
                logger.info("日K刷新完成")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("日K刷新循环异常: %s", e)
        # 睡到下一个北京时间 07:00（naive 本地时区即服务器北京时间）
        now = datetime.now()
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
