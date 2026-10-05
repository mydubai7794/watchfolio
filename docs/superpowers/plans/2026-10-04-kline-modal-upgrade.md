# K线弹层增强实施计划（悬浮/量能/MACD/KDJ/平移缩放/日月周K/库内分时K）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** K线弹层升级：悬浮十字线+信息浮层（OHLC/涨跌幅/量+RSI/MACD/KDJ）、拖动平移、滚轮/双指缩放、日/周/月K切换、库内分时K（market_ticks 聚合，仅有数据时显示）。

**Architecture:** 方案A——服务端周期聚合与指标计算（daily_bars.py 纯函数 + kline 端点 tf 参数 + 60s 缓存），前端纯渲染开窗（一次拉满 1000 根，pan/zoom 零请求）。**零新增长桥历史K请求**（额度约束见 spec §2）。

**Tech Stack:** FastAPI + aiomysql（远程库）+ 单文件前端（原生 JS + canvas，无构建）+ pytest（零连库零外网）。

**纪律（全 task 通用）：** 禁止 git add/commit（工作区留痕）；中文注释 + `@author yys`；禁 TODO/伪代码/违禁词（描述性文本只称 长桥/24h数据源 等）；测试零连库零外网零 lifespan；aiomysql Connection 无 execute（必须 `async with conn.cursor()`）；market_ticks 为 **UTC naive**（`.timestamp()` 前必须 `replace(tzinfo=timezone.utc)`，否则按服务器本地时区错位）；日线 close 为 DECIMAL（聚合/指标全链 float() 归一）。

**Spec:** `docs/superpowers/specs/2026-10-04-kline-modal-upgrade-design.md`

---

### Task 1: daily_bars.py 纯函数 macd/kdj/aggregate_bars/ticks_to_bars（TDD）

**Files:**
- Modify: `daily_bars.py`（`hhv` 函数后、`compute_indicators` 前插入）
- Test: `tests/test_daily_bars.py`（追加）

- [ ] **Step 1: 写失败测试（tests/test_daily_bars.py 追加）**

```python
# ---- macd/kdj/aggregate_bars/ticks_to_bars（K线弹层增强）----

def test_ema_seeded_exact():
    """EMA 首值 seed=首个值；n=2 时手工精确值。"""
    out = dbar.ema([10.0, 10.0, 10.0, 20.0], 2)
    assert out[0] == 10.0 and out[1] == 10.0 and out[2] == 10.0
    assert abs(out[3] - 50.0 / 3.0) < 1e-9


def test_macd_structure_and_sign():
    """MACD：长度对齐、hist=2×(DIF−DEA)（国内惯例）、单调涨 DIF>0、横盘归零。"""
    rises = [float(i) for i in range(1, 41)]        # 40 根单调涨
    flat = [10.0] * 40
    for closes in (rises, flat):
        m = dbar.macd(closes)
        assert len(m["dif"]) == len(m["dea"]) == len(m["hist"]) == len(closes)
        for i in range(len(closes)):
            if m["dif"][i] is not None and m["dea"][i] is not None:
                assert abs(m["hist"][i] - 2.0 * (m["dif"][i] - m["dea"][i])) < 1e-9
    assert dbar.macd(rises)["dif"][-1] > 0           # 单调涨 → DIF 为正
    assert abs(dbar.macd(flat)["dif"][-1]) < 1e-6    # 横盘 → DIF≈0


def test_kdj_exact_n2():
    """KDJ n=2 手工精确值：RSV=66.667→K=55.556/D=51.852/J=62.963；首根样本不足 None。"""
    out = dbar.kdj([10.0, 12.0], [9.0, 10.0], [10.0, 11.0], n=2)
    assert out["k"][0] is None and out["d"][0] is None and out["j"][0] is None
    assert abs(out["k"][1] - 55.556) < 1e-3
    assert abs(out["d"][1] - 51.852) < 1e-3
    assert abs(out["j"][1] - 62.963) < 1e-3


def test_kdj_flat_hh_equals_ll():
    """高=低（一字）时 RSV=50 防除零。"""
    out = dbar.kdj([10.0] * 5, [10.0] * 5, [10.0] * 5, n=3)
    assert abs(out["k"][-1] - 50.0) < 1e-9


def test_aggregate_bars_week_and_month():
    """2026-09-28(周一)~10-02 五根日K：周K 聚成 1 根（ISO 周 40），月K 聚成 2 根。"""
    bars = [
        {"date": f"2026-09-{d}", "open": 10.0 + d, "high": 12.0 + d,
         "low": 9.0 + d, "close": 11.0 + d, "volume": 100 * d}
        for d in (28, 29, 30)
    ] + [
        {"date": f"2026-10-{d}", "open": 10.0 + d, "high": 12.0 + d,
         "low": 9.0 + d, "close": 11.0 + d, "volume": 100 * d}
        for d in ("01", "02")
    ]
    wk = dbar.aggregate_bars(bars, "week")
    assert len(wk) == 1
    assert wk[0]["date"] == "2026-09-28"
    assert wk[0]["open"] == 38.0 and wk[0]["close"] == 42.0    # 首开/末收
    assert wk[0]["high"] == 44.0 and wk[0]["low"] == 37.0      # 10-04 未入组
    assert wk[0]["volume"] == 100 * (28 + 29 + 30 + 1 + 2)
    mo = dbar.aggregate_bars(bars, "month")
    assert len(mo) == 2 and mo[0]["date"] == "2026-09-28" and mo[1]["date"] == "2026-10-01"
    assert dbar.aggregate_bars(bars, "day") == bars            # day 原样返回


def test_ticks_to_bars_bucket_300s():
    """300s 分桶：开=桶首价、收=桶末价、高/低=桶内极值、量=tick 数；跨桶正确切分。"""
    from datetime import datetime, timezone
    def t(sec):
        return datetime(2026, 10, 2, 13, 30, 0, tzinfo=timezone.utc) \
            .replace(tzinfo=None) + timedelta(seconds=sec)
    rows = [
        {"ts": t(0), "price": 100.0}, {"ts": t(120), "price": 101.0},
        {"ts": t(299), "price": 99.0},                       # 桶 A：3 tick
        {"ts": t(300), "price": 98.0}, {"ts": t(480), "price": 97.0},  # 桶 B
    ]
    bars = dbar.ticks_to_bars(rows, bucket_sec=300)
    assert len(bars) == 2
    assert bars[0]["open"] == 100.0 and bars[0]["close"] == 99.0
    assert bars[0]["high"] == 101.0 and bars[0]["low"] == 99.0
    assert bars[0]["volume"] == 3
    assert bars[1]["open"] == 98.0 and bars[1]["close"] == 97.0
    assert bars[0]["date"] == "10-02 13:30"
    assert dbar.ticks_to_bars([]) == []
```

文件顶部测试 import 区需含 `from datetime import timedelta`（若未有则补）。

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: FAIL/ERROR `AttributeError: module 'daily_bars' has no attribute 'ema'`

- [ ] **Step 3: 实现（daily_bars.py 插入，`from datetime import date, datetime, timezone` 若缺则顶部补）**

```python
def ema(values: list[float], n: int) -> list:
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
    day 原样返回（调用方无需分支）。date 兼容 str/date。
    """
    if tf not in ("week", "month"):
        return bars

    def _d(b):
        v = b["date"]
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
    ticks 无量能字段）；date 为「MM-DD HH:MM」字符串（桶起始时刻，UTC）。
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
            bars.append({"date": ts.strftime("%m-%d %H:%M"),
                         "open": p, "high": p, "low": p, "close": p, "volume": 1})
        else:
            b = bars[-1]
            b["high"] = max(b["high"], p)
            b["low"] = min(b["low"], p)
            b["close"] = p
            b["volume"] += 1
    return bars
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 全部 passed（原 14 + 新 7）

- [ ] **Step 5: 回归 + pyflakes（不提交，工作区留痕）**

Run: `python3 -m pytest tests/test_backtest.py tests/test_alert_engine.py -q && python3 -m pyflakes daily_bars.py`
Expected: passed；零新增警告

---

### Task 2: database.py get_market_ticks_range + server.py kline 端点 tf 扩展（TDD）

**Files:**
- Modify: `database.py`（`get_market_day_spark` 后追加一个函数）
- Modify: `server.py:2168-2198`（kline 端点重写 + 模块级缓存）
- Test: `tests/test_alert_api.py`（追加）

- [ ] **Step 1: 写失败测试（tests/test_alert_api.py 追加，沿用文件既有 fake 模式与 monkeypatch server.db）**

```python
# ---- kline tf 扩展（周/月聚合 + 分时聚合 + 缓存）----

def _fake_daily_bars_five():
    """2026-09-28(周一)~10-02 五根日K（跨 ISO 周 40 与两个自然月）。"""
    days = ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]
    return [{"date": d, "open": 10.0 + i, "high": 12.0 + i, "low": 9.0 + i,
             "close": 11.0 + i, "volume": 100} for i, d in enumerate(days)]


def test_kline_tf_week_month(monkeypatch):
    """week 聚 1 根 / month 聚 2 根；series 与 bars 等长；tf=day 原样。"""
    monkeypatch.setattr(server.db, "get_daily_bars",
                        lambda sym, limit=None: _ag(_fake_daily_bars_five(), limit))
    monkeypatch.setattr(server.db, "get_alert_rule",
                        lambda sym: _ar())
    monkeypatch.setattr(server.db, "get_holdings", lambda e=True: [])
    server._KLINE_CACHE.clear()
    client = TestClient(server.app)
    for tf, n in (("week", 1), ("month", 2), ("day", 5)):
        r = client.get(f"/api/kline/QQQ?tf={tf}&range=1000",
                       cookies=_ck())
        assert r.status_code == 200
        d = r.json()
        assert len(d["bars"]) == n
        assert len(d["rsi_series"]) == n
        assert set(d["macd_series"]) == {"dif", "dea", "hist"}
        assert set(d["kdj_series"]) == {"k", "d", "j"}
        assert d["volume_mode"] == "volume"


def test_kline_tf_5m_ticks(monkeypatch):
    """5m：tick 聚合 OHLC/计数正确；无 ticks 返回空 bars（前端隐藏分时入口）。"""
    from datetime import datetime, timedelta
    t0 = datetime(2026, 10, 2, 13, 30, 0)
    rows = [{"ts": t0 + timedelta(seconds=s), "price": p}
            for s, p in ((0, 100.0), (120, 101.0), (299, 99.0),
                         (300, 98.0))]
    monkeypatch.setattr(server.db, "get_market_ticks_range", lambda sym, limit=5000: rows)
    monkeypatch.setattr(server.db, "get_alert_rule", lambda sym: _ar())
    monkeypatch.setattr(server.db, "get_holdings", lambda e=True: [])
    server._KLINE_CACHE.clear()
    client = TestClient(server.app)
    d = client.get("/api/kline/QQQ?tf=5m&range=1000", cookies=_ck()).json()
    assert len(d["bars"]) == 2
    assert d["bars"][0]["open"] == 100.0 and d["bars"][0]["close"] == 99.0
    assert d["bars"][0]["volume"] == 3
    assert d["volume_mode"] == "ticks"
    # 空 ticks → 空 bars
    monkeypatch.setattr(server.db, "get_market_ticks_range", lambda sym, limit=5000: [])
    server._KLINE_CACHE.clear()
    d = client.get("/api/kline/QQQ?tf=5m&range=1000", cookies=_ck()).json()
    assert d["ok"] is True and d["bars"] == []


def test_kline_tf_invalid_and_cache(monkeypatch):
    """非法 tf → 400；60s 缓存命中不重查库。"""
    calls = {"n": 0}

    async def fake_bars(sym, limit=None):
        calls["n"] += 1
        return _ag(_fake_daily_bars_five(), limit)

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", lambda sym: _ar())
    monkeypatch.setattr(server.db, "get_holdings", lambda e=True: [])
    server._KLINE_CACHE.clear()
    client = TestClient(server.app)
    assert client.get("/api/kline/QQQ?tf=hour", cookies=_ck()).status_code == 400
    client.get("/api/kline/QQQ?tf=week&range=1000", cookies=_ck())
    client.get("/api/kline/QQQ?tf=week&range=1000", cookies=_ck())
    assert calls["n"] == 1          # 第二次走缓存


def test_kline_range_truncate(monkeypatch):
    """range 截断：bars 与 series 同步截尾对齐。"""
    monkeypatch.setattr(server.db, "get_daily_bars",
                        lambda sym, limit=None: _ag(_fake_daily_bars_five(), limit))
    monkeypatch.setattr(server.db, "get_alert_rule", lambda sym: _ar())
    monkeypatch.setattr(server.db, "get_holdings", lambda e=True: [])
    server._KLINE_CACHE.clear()
    client = TestClient(server.app)
    d = client.get("/api/kline/QQQ?tf=month&range=1", cookies=_ck()).json()
    assert len(d["bars"]) == 1 and d["bars"][0]["date"] == "2026-10-01"
    assert len(d["rsi_series"]) == 1
```

同文件若无可直接复用的辅助，追加（已存在则跳过）：

```python
def _ag(bars, limit):
    """get_daily_bars 的 limit 语义模拟（None=全量）。"""
    return bars if limit is None else bars[-limit:]


async def _ar():
    return None


def _ck():
    return {"session_token": "t"}
```

注意：TestClient 不用 with（零 lifespan）；若既有用例已有等价 `_ck`/鉴权打桩则复用改名，避免重复定义。

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: FAIL（`_KLINE_CACHE` 不存在 / tf 参数行为缺失）

- [ ] **Step 3: 实现 database.py（`get_market_day_spark` 函数后追加）**

```python
async def get_market_ticks_range(symbol: str, limit: int = 5000) -> list[dict]:
    """按时间升序取该标的最近 limit 条分时 tick（分时K聚合用）。"""
    rows = await execute_dict(
        "SELECT ts, price FROM (SELECT ts, price FROM market_ticks "
        "WHERE symbol = %s ORDER BY ts DESC LIMIT %s) t ORDER BY ts ASC",
        (symbol, int(limit)),
    )
    return [{"ts": r["ts"], "price": float(r["price"])} for r in rows]
```

- [ ] **Step 4: 实现 server.py（替换 kline 端点；kline 函数上方模块级加缓存）**

```python
_KLINE_CACHE: dict[str, tuple[float, dict]] = {}    # (symbol, tf) -> (monotonic, 全量 payload)


def _round_series(vals: list, nd: int) -> list:
    """指标序列统一取整（None 占位保留）。"""
    return [round(v, nd) if v is not None else None for v in vals]


def _kl_series(bars: list[dict]) -> dict:
    """对 bars 计算 RSI/MACD/KDJ 三指标序列（与 bars 等长）。"""
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    macd = daily_bars.macd(closes)
    kdj = daily_bars.kdj(highs, lows, closes)
    return {"rsi_series": _round_series(daily_bars.rsi_wilder(closes), 2),
            "macd_series": {k: _round_series(v, 3) for k, v in macd.items()},
            "kdj_series": {k: _round_series(v, 2) for k, v in kdj.items()}}


@app.get("/api/kline/{symbol}")
async def kline(symbol: str, request: Request, range: int = 120, tf: str = "day"):
    """K线 + 指标 + 标注：tf=day/week/month（daily_bars 聚合）/5m（market_ticks
    聚合，库内分时）。零新增长桥历史K请求；(symbol,tf) 60s 内存缓存，
    缓存存全量、range 仅截尾响应。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    if tf not in ("day", "week", "month", "5m"):
        return JSONResponse(status_code=400,
                            content={"ok": False, "message": f"invalid tf: {tf}"})
    cache_key = f"{symbol}:{tf}"
    hit = _KLINE_CACHE.get(cache_key)
    if not (hit and time.monotonic() - hit[0] < 60.0):
        rule = await db.get_alert_rule(symbol)
        params = alert_engine._merged_params(rule)
        holdings = await db.get_holdings(enabled_only=True)
        cost = next((float(h["cost_price"]) for h in holdings
                     if h["symbol"] == symbol), None)
        levels = params["price_plan"].get("levels", [])
        ind = daily_bars.get_indicators(symbol) if tf == "day" else None
        marks = {
            "cost": cost,
            "levels": [{"pct": p, "price": round(cost * (1 + p / 100.0), 2)}
                       for p in levels] if cost else [],
            "stop_level": ind.get("stop_level") if ind else None,
            "hhv22": ind.get("hhv22") if ind else None,
        }
        if tf == "5m":
            rows = await db.get_market_ticks_range(symbol)
            bars = daily_bars.ticks_to_bars(rows, bucket_sec=300)
            volume_mode = "ticks"
        else:
            limit = None if tf != "day" else (None if range >= 1000 else range)
            bars = await db.get_daily_bars(symbol, limit=limit)
            if tf != "day":
                bars = daily_bars.aggregate_bars(bars, tf)
            volume_mode = "volume"
        payload = {"ok": True, "symbol": symbol, "tf": tf, "bars": bars,
                   "volume_mode": volume_mode, "marks": marks,
                   "indicators": {k: ind.get(k) for k in
                                  ("ma50", "ma200", "rsi14", "stop_level",
                                   "enough_history", "asof")} if ind else {}}
        payload.update(_kl_series(bars))
        _KLINE_CACHE[cache_key] = (time.monotonic(), payload)
        hit = _KLINE_CACHE[cache_key]
    p = hit[1]
    n = len(p["bars"])
    keep = n if range >= n else range        # 截尾窗口（series 与 bars 同步截）
    if keep < n:
        cut = n - keep
        out = dict(p)
        out["bars"] = p["bars"][cut:]
        out["rsi_series"] = p["rsi_series"][cut:]
        out["macd_series"] = {k: v[cut:] for k, v in p["macd_series"].items()}
        out["kdj_series"] = {k: v[cut:] for k, v in p["kdj_series"].items()}
        return out
    return p
```

注：`time` 模块 server.py 已 import；day 分支保持 `limit = None if range >= 1000 else range` 语义与现网一致（缓存命中期内 range 只影响截尾）。

- [ ] **Step 5: 跑测试确认通过 + 回归**

Run: `python3 -m pytest tests/test_alert_api.py tests/test_daily_bars.py tests/test_backtest.py -q && python3 -m pyflakes server.py database.py`
Expected: 全部 passed；零新增警告

---

### Task 3: 前端 K线弹层重构（tabs/平移缩放/悬浮/量能/指标切换）

**Files:**
- Modify: `static/portfolio.html`（HTML 弹层结构 + CSS + kline JS 区整体替换）

- [ ] **Step 1: 替换弹层 HTML（`#klineMask` 现有结构整体替换）**

```html
<div class="modal-mask" id="klineMask">
  <div class="modal kline-modal">
    <div class="kline-head">
      <span id="klTitle">日K</span>
      <div class="kline-tabs" id="klTabs">
        <button data-tf="day" class="on">日K</button>
        <button data-tf="week">周K</button>
        <button data-tf="month">月K</button>
        <button data-tf="5m" id="klTab5m" hidden>分时</button>
      </div>
      <button class="kline-close" onclick="closeKline()">×</button>
    </div>
    <div class="kline-tip" id="klTip" hidden></div>
    <canvas id="klMain"></canvas>
    <canvas id="klVol"></canvas>
    <div class="kline-intabs" id="klIndTabs">
      <button data-ind="rsi" class="on">RSI</button>
      <button data-ind="macd">MACD</button>
      <button data-ind="kdj">KDJ</button>
    </div>
    <canvas id="klRsi"></canvas>
    <div class="kline-info" id="klInfo"></div>
  </div>
</div>
```

- [ ] **Step 2: 追加 CSS（kline 弹层样式区，沿用现有色板变量 UP_COLOR 等对应色值体系）**

```css
.kline-modal { width: min(860px, 94vw); }
.kline-head { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
.kline-head #klTitle { font-weight: 600; font-size: 15px; }
.kline-tabs, .kline-intabs { display: flex; gap: 4px; }
.kline-tabs button, .kline-intabs button {
  border: 1px solid #DCDFE6; background: #fff; color: #606266; border-radius: 4px;
  padding: 2px 10px; font-size: 12px; cursor: pointer; }
.kline-tabs button.on, .kline-intabs button.on {
  background: #2A6CF0; border-color: #2A6CF0; color: #fff; }
.kline-close { margin-left: auto; border: none; background: none; font-size: 20px;
  color: #909399; cursor: pointer; }
#klMain { width: 100%; height: 300px; cursor: crosshair; touch-action: none; }
#klVol { width: 100%; height: 64px; }
#klRsi { width: 100%; height: 96px; }
.kline-intabs { margin: 4px 0 2px; }
.kline-tip { position: fixed; z-index: 120; background: rgba(38, 42, 52, .94);
  color: #fff; border-radius: 6px; padding: 8px 10px; font-size: 12px;
  line-height: 1.7; pointer-events: none; max-width: 240px; }
.kline-tip b { color: #F3D19E; }
@media (max-width: 640px) {
  #klMain { height: 220px; } #klVol { height: 48px; } #klRsi { height: 80px; }
}
```

- [ ] **Step 3: 替换 kline JS（`openKline`/`drawKline` 区整体替换；`closeKline` 保留现有）**

```javascript
// ---- K线弹层（周期 tabs + 拖动/缩放 + 悬浮详情 + 量能 + 指标切换）----
// 数据一次拉满（range=1000），平移缩放纯客户端开窗零请求；
// 分时 tab 惰性加载，market_ticks 无数据则隐藏。@author yys

const KL = {
  sym: '', tf: 'day', data: {},        // tf -> {bars, rsi_series, macd_series, kdj_series, marks, volume_mode, _at}
  off: 0, count: 120,                  // 可见窗口：全量尾部锚定，off=从可见起点到全量尾部的偏移
  ind: 'rsi', drag: null, pinch: null, tipIdx: -1,
};

async function klLoad(tf) {
  if (KL.data[tf] && Date.now() - KL.data[tf]._at < 60000) return KL.data[tf];
  const r = await fetch('/api/kline/' + encodeURIComponent(KL.sym) +
    '?tf=' + tf + '&range=1000');
  if (!r.ok) throw new Error('http ' + r.status);
  const d = await r.json();
  d._at = Date.now();
  KL.data[tf] = d;
  return d;
}

async function openKline(sym) {
  KL.sym = sym; KL.tf = 'day'; KL.data = {}; KL.off = 0; KL.count = 120;
  KL.ind = 'rsi'; KL.tipIdx = -1;
  document.getElementById('klTab5m').hidden = false;   // 试探后再决定隐藏
  document.querySelectorAll('#klTabs button').forEach(b =>
    b.classList.toggle('on', b.dataset.tf === 'day'));
  document.querySelectorAll('#klIndTabs button').forEach(b =>
    b.classList.toggle('on', b.dataset.ind === 'rsi'));
  document.getElementById('klTitle').textContent = sym + ' K线';
  document.getElementById('klInfo').textContent = '加载中…';
  klClearCanvas();
  document.getElementById('klineMask').classList.add('show');
  try {
    await klApply('day');
    // 分时可用性试探（不强制打开）：空数据隐藏入口
    try {
      const d5 = await klLoad('5m');
      if (!(d5.bars || []).length) document.getElementById('klTab5m').hidden = true;
    } catch (e) { document.getElementById('klTab5m').hidden = true; }
  } catch (e) {
    closeKline(); alert(sym + ' K线加载失败，请稍后重试');
  }
}

function klClearCanvas() {
  ['klMain', 'klVol', 'klRsi'].forEach(id => {
    const cv = document.getElementById(id);
    cv.getContext('2d').clearRect(0, 0, cv.width, cv.height);
  });
  const tip = document.getElementById('klTip');
  if (tip) tip.hidden = true;
}

function closeKline() {
  const m = document.getElementById('klineMask');
  if (m) m.classList.remove('show');
  KL.drag = null; KL.pinch = null; KL.tipIdx = -1;
  const tip = document.getElementById('klTip');
  if (tip) tip.hidden = true;
}

async function klApply(tf) {
  const d = await klLoad(tf);
  if (!(d.bars || []).length) throw new Error('empty');
  KL.tf = tf;
  KL.off = 0;
  KL.count = Math.min(120, d.bars.length);
  document.querySelectorAll('#klTabs button').forEach(b =>
    b.classList.toggle('on', b.dataset.tf === tf));
  document.getElementById('klTitle').textContent =
    KL.sym + ' ' + (tf === '5m' ? '分时(5m)' : tf === 'day' ? '日K' :
                    tf === 'week' ? '周K' : '月K');
  klRender();
}

function klSlice() {
  const d = KL.data[KL.tf];
  const n = d.bars.length;
  const end = n - KL.off;                 // 可见窗口右端（不含）
  const start = Math.max(0, end - KL.count);
  return { d, n, start, end: Math.min(end, n) };
}

function klRender() {
  const { d, n, start, end } = klSlice();
  const bars = d.bars.slice(start, end);
  if (!bars.length) return;
  const rsi = d.rsi_series.slice(start, end);
  const macd = d.macd_series || { dif: [], dea: [], hist: [] };
  const kdj = d.kdj_series || { k: [], d: [], j: [] };
  klDrawMain(bars, d.marks || {}, start, n);
  klDrawVol(bars, d.volume_mode === 'ticks');
  klDrawInd(bars,
    { rsi, dif: macd.dif.slice(start, end), dea: macd.dea.slice(start, end),
      hist: macd.hist.slice(start, end),
      k: kdj.k.slice(start, end), d2: kdj.d.slice(start, end), j: kdj.j.slice(start, end) });
}

function klGeom(W, count) {
  const pad = { l: 6, r: 64, t: 10, b: 20 };
  const cw = (W - pad.l - pad.r) / count;
  return { pad, cw, x: i => pad.l + (i + 0.5) * cw };
}

function klDrawMain(bars, marks, start, n) {
  const cv = document.getElementById('klMain');
  const ctx = setupCanvas(cv, 300);
  const W = cv._cssW, H = cv._cssH;
  ctx.clearRect(0, 0, W, H);
  const { pad, cw, x } = klGeom(W, bars.length);
  let lo = Math.min(...bars.map(b => +b.low));
  let hi = Math.max(...bars.map(b => +b.high));
  const lines = [];
  if (marks.cost != null) lines.push(['成本', +marks.cost, '#E6A23C']);
  (marks.levels || []).forEach(v => lines.push(
    [(v.pct > 0 ? '+' : '') + v.pct + '%', +v.price, v.pct > 0 ? DOWN_COLOR : TXT2]));
  if (marks.stop_level != null) lines.push(['止损', +marks.stop_level, UP_COLOR]);
  if (marks.hhv22 != null) lines.push(['22日高', +marks.hhv22, '#2A6CF0']);
  lines.forEach(([, p]) => { lo = Math.min(lo, p); hi = Math.max(hi, p); });
  const span = (hi - lo) || 1;
  lo -= span * 0.04; hi += span * 0.04;
  const y = p => pad.t + (hi - p) / (hi - lo) * (H - pad.t - pad.b);
  ctx.font = F11; ctx.textAlign = 'left'; ctx.textBaseline = 'middle';
  for (let g = 0; g <= 4; g++) {
    const p = lo + (hi - lo) * g / 4;
    ctx.strokeStyle = '#F0F0F2';
    ctx.beginPath(); ctx.moveTo(pad.l, y(p)); ctx.lineTo(W - pad.r, y(p)); ctx.stroke();
    ctx.fillStyle = TXT3;
    ctx.fillText(p.toFixed(2), W - pad.r + 4, y(p));
  }
  bars.forEach((b, i) => {
    const cx = x(i);
    const up = +b.close >= +b.open;
    ctx.strokeStyle = ctx.fillStyle = up ? UP_COLOR : DOWN_COLOR;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(cx, y(+b.high)); ctx.lineTo(cx, y(+b.low)); ctx.stroke();
    const top = y(Math.max(+b.open, +b.close));
    const bot = y(Math.min(+b.open, +b.close));
    const bw = Math.max(cw * 0.7, 1);
    ctx.fillRect(cx - bw / 2, top, bw, Math.max(bot - top, 1));
  });
  ctx.textAlign = 'center';
  for (let g = 0; g < 5; g++) {
    const i = Math.round((bars.length - 1) * g / 4);
    ctx.fillStyle = TXT3;
    ctx.fillText(String(bars[i].date).slice(KL.tf === '5m' ? 5 : 5),
                 x(i), H - 8);            // 日/周/月: MM-DD；分时: 同段（HH:MM 同长度）
  }
  ctx.textAlign = 'left';
  lines.forEach(([name, p, color]) => {
    ctx.strokeStyle = color; ctx.setLineDash([4, 3]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.l, y(p)); ctx.lineTo(W - pad.r, y(p)); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = color;
    ctx.fillText(name + ' ' + p.toFixed(2), W - pad.r + 4, y(p) - 8);
  });
  // 悬浮十字线
  if (KL.tipIdx >= 0 && KL.tipIdx < bars.length) {
    const cx = x(KL.tipIdx);
    ctx.strokeStyle = '#C0C4CC'; ctx.setLineDash([3, 3]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(cx, pad.t); ctx.lineTo(cx, H - pad.b); ctx.stroke();
    ctx.setLineDash([]);
  }
}

function klDrawVol(bars, tickMode) {
  const cv = document.getElementById('klVol');
  const ctx = setupCanvas(cv, 64);
  const W = cv._cssW, H = cv._cssH;
  ctx.clearRect(0, 0, W, H);
  const { pad, cw, x } = klGeom(W, bars.length);
  const mx = Math.max(...bars.map(b => +b.volume), 1);
  bars.forEach((b, i) => {
    const h = (+b.volume / mx) * (H - 14);
    const up = +b.close >= +b.open;
    ctx.fillStyle = up ? 'rgba(224,68,68,.55)' : 'rgba(34,168,116,.55)';
    ctx.fillRect(x(i) - Math.max(cw * 0.7, 1) / 2, H - 12 - h,
                 Math.max(cw * 0.7, 1), Math.max(h, 1));
  });
  ctx.font = F11; ctx.fillStyle = TXT3; ctx.textBaseline = 'top';
  ctx.textAlign = 'left';
  ctx.fillText(tickMode ? 'tick数 ' + mx : '量 ' + mx, pad.l, 2);
}

function klDrawInd(bars, s) {
  const cv = document.getElementById('klRsi');
  const ctx = setupCanvas(cv, 96);
  const W = cv._cssW, H = cv._cssH;
  ctx.clearRect(0, 0, W, H);
  const { pad, cw, x } = klGeom(W, bars.length);
  const rx = i => x(i);
  const line = (vals, color, y, width) => {
    ctx.strokeStyle = color; ctx.lineWidth = width || 1.5; ctx.lineJoin = 'round';
    ctx.beginPath();
    let started = false;
    vals.forEach((v, i) => {
      if (v == null) { started = false; return; }
      const px = rx(i), py = y(+v);
      if (!started) { ctx.moveTo(px, py); started = true; } else ctx.lineTo(px, py);
    });
    ctx.stroke();
  };
  ctx.font = F11; ctx.textBaseline = 'middle';
  if (KL.ind === 'rsi') {
    const ry = v => 6 + (100 - v) / 100 * (H - 12);
    [30, 70].forEach(v => {
      ctx.strokeStyle = '#DCDFE6'; ctx.setLineDash([3, 3]); ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad.l, ry(v)); ctx.lineTo(W - pad.r, ry(v)); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = TXT3; ctx.textAlign = 'left';
      ctx.fillText(String(v), W - pad.r + 4, ry(v));
    });
    line(s.rsi, '#2A6CF0', ry);
  } else if (KL.ind === 'macd') {
    const vals = [...s.dif, ...s.dea, ...s.hist].filter(v => v != null);
    const lo = Math.min(...vals, -1e-9), hi = Math.max(...vals, 1e-9);
    const ry = v => 6 + (hi - v) / (hi - lo) * (H - 12);
    ctx.strokeStyle = '#DCDFE6'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.l, ry(0)); ctx.lineTo(W - pad.r, ry(0)); ctx.stroke();
    s.hist.forEach((v, i) => {
      if (v == null) return;
      ctx.fillStyle = v >= 0 ? 'rgba(224,68,68,.6)' : 'rgba(34,168,116,.6)';
      const y0 = ry(0), y1 = ry(+v);
      ctx.fillRect(x(i) - Math.max(cw * 0.5, 1) / 2, Math.min(y0, y1),
                   Math.max(cw * 0.5, 1), Math.max(Math.abs(y1 - y0), 1));
    });
    line(s.dif, '#E6A23C', ry, 1.3);
    line(s.dea, '#2A6CF0', ry, 1.3);
    ctx.fillStyle = TXT3; ctx.textAlign = 'left';
    ctx.fillText('0', W - pad.r + 4, ry(0));
  } else {                                   // kdj
    const vals = [...s.k, ...s.d2, ...s.j].filter(v => v != null);
    const lo = Math.min(...vals, 0), hi = Math.max(...vals, 100);
    const ry = v => 6 + (hi - v) / (hi - lo) * (H - 12);
    [20, 80].forEach(v => {
      ctx.strokeStyle = '#DCDFE6'; ctx.setLineDash([3, 3]); ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad.l, ry(v)); ctx.lineTo(W - pad.r, ry(v)); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = TXT3; ctx.textAlign = 'left';
      ctx.fillText(String(v), W - pad.r + 4, ry(v));
    });
    line(s.k, '#E6A23C', ry, 1.3);
    line(s.d2, '#2A6CF0', ry, 1.3);
    line(s.j, '#909399', ry, 1);
  }
}

// ---- 交互：拖动平移 / 滚轮与双指缩放 / 双击复位 / 悬浮详情 ----

function klZoom(factor, anchorIdx) {
  const d = KL.data[KL.tf];
  const minC = 20, maxC = d.bars.length;
  const newC = Math.round(KL.count * factor);
  if (newC < minC || newC > maxC) return;
  const { start } = klSlice();
  const ratio = (anchorIdx - start) / KL.count;      // 锚点在窗口内的比例
  const anchorGlobal = start + ratio * KL.count;
  KL.count = Math.min(maxC, Math.max(minC, newC));
  const wantStart = Math.round(anchorGlobal - ratio * KL.count);
  KL.off = Math.max(0, Math.min(d.bars.length - KL.count,
                                d.bars.length - (wantStart + KL.count)));
  klRender();
}

function klBindEvents() {
  const cv = document.getElementById('klMain');
  cv.addEventListener('mousedown', e => {
    KL.drag = { x: e.clientX, off: KL.off }; KL.tipIdx = -1;
  });
  window.addEventListener('mousemove', e => {
    if (KL.drag) {
      const d = KL.data[KL.tf];
      const cwPx = klCwPx();
      const shift = Math.round((e.clientX - KL.drag.x) / cwPx);
      KL.off = Math.max(0, Math.min(d.bars.length - KL.count,
                                    KL.drag.off + shift));
      klRender();
      return;
    }
  });
  window.addEventListener('mouseup', () => { KL.drag = null; });
  cv.addEventListener('mousemove', e => {
    if (KL.drag) return;
    const idx = klIdxAt(e);
    if (idx == null) { KL.tipIdx = -1; klHideTip(); return; }
    KL.tipIdx = idx;
    klShowTip(e, idx);
    klRender();
  });
  cv.addEventListener('mouseleave', () => { KL.tipIdx = -1; klHideTip(); klRender(); });
  cv.addEventListener('wheel', e => {
    e.preventDefault();
    const idx = klIdxAt(e);
    klZoom(e.deltaY > 0 ? 1.2 : 1 / 1.2, idx == null ? KL.count - 1 : idx);
  }, { passive: false });
  cv.addEventListener('dblclick', () => {
    KL.off = 0; KL.count = Math.min(120, KL.data[KL.tf].bars.length); klRender();
  });
  // 触摸：单指拖动 / 双指缩放 / 点按悬浮
  cv.addEventListener('touchstart', e => {
    if (e.touches.length === 1) {
      KL.drag = { x: e.touches[0].clientX, off: KL.off, moved: false };
      KL.pinch = null;
    } else if (e.touches.length === 2) {
      KL.pinch = { dist: klTouchDist(e), count: KL.count };
      KL.drag = null;
    }
  }, { passive: true });
  cv.addEventListener('touchmove', e => {
    e.preventDefault();
    if (KL.pinch && e.touches.length === 2) {
      const f = KL.pinch.dist / Math.max(klTouchDist(e), 1);
      klZoom(f, KL.count - 1);
    } else if (KL.drag && e.touches.length === 1) {
      KL.drag.moved = true;
      const d = KL.data[KL.tf];
      const shift = Math.round((e.touches[0].clientX - KL.drag.x) / klCwPx());
      KL.off = Math.max(0, Math.min(d.bars.length - KL.count,
                                    KL.drag.off + shift));
      klRender();
    }
  }, { passive: false });
  cv.addEventListener('touchend', e => {
    if (KL.drag && !KL.drag.moved && e.changedTouches.length) {
      const rect = cv.getBoundingClientRect();
      const fake = { clientX: e.changedTouches[0].clientX,
                     clientY: e.changedTouches[0].clientY };
      const idx = klIdxAt(fake);
      if (idx != null) { KL.tipIdx = idx; klShowTip(fake, idx); klRender(); }
    }
    KL.drag = null; KL.pinch = null;
  });
  // tabs 事件（委托）
  document.getElementById('klTabs').addEventListener('click', async e => {
    const btn = e.target.closest('button[data-tf]');
    if (!btn || btn.classList.contains('on')) return;
    const tf = btn.dataset.tf;
    try {
      await klApply(tf);
    } catch (err) {
      if (tf === '5m') {
        btn.hidden = true;
        alert(KL.sym + ' 暂无分时数据');
      } else { alert(KL.sym + ' 加载失败，请稍后重试'); }
    }
  });
  document.getElementById('klIndTabs').addEventListener('click', e => {
    const btn = e.target.closest('button[data-ind]');
    if (!btn) return;
    KL.ind = btn.dataset.ind;
    document.querySelectorAll('#klIndTabs button').forEach(b =>
      b.classList.toggle('on', b === btn));
    klRender();
  });
  window.addEventListener('resize', () => {
    if (document.getElementById('klineMask').classList.contains('show')) klRender();
  });
}

function klTouchDist(e) {
  const dx = e.touches[0].clientX - e.touches[1].clientX;
  const dy = e.touches[0].clientY - e.touches[1].clientY;
  return Math.sqrt(dx * dx + dy * dy);
}

function klCwPx() {
  const cv = document.getElementById('klMain');
  const { pad } = klGeom(cv._cssW || cv.getBoundingClientRect().width, KL.count);
  const w = cv._cssW || cv.getBoundingClientRect().width;
  return Math.max((w - pad.l - pad.r) / KL.count, 1);
}

function klIdxAt(e) {
  const cv = document.getElementById('klMain');
  const rect = cv.getBoundingClientRect();
  const { pad } = klGeom(rect.width, KL.count);
  const px = e.clientX - rect.left - pad.l;
  const cw = (rect.width - pad.l - pad.r) / KL.count;
  if (px < 0 || px > rect.width - pad.l - pad.r) return null;
  const idx = Math.floor(px / cw);
  return Math.max(0, Math.min(KL.count - 1, idx));
}

function klShowTip(e, idx) {
  const { d, start } = klSlice();
  const b = d.bars[start + idx];
  const prev = d.bars[start + idx - 1];
  const pct = prev ? ((+b.close - +prev.close) / +prev.close * 100) : null;
  const r = (d.rsi_series || [])[start + idx];
  const m = (d.macd_series || {}), k = (d.kdj_series || {});
  const fmt2 = v => v == null ? '--' : (+v).toFixed(2);
  const tip = document.getElementById('klTip');
  tip.innerHTML =
    '<b>' + esc(String(b.date)) + '</b><br>' +
    '开 ' + fmt2(b.open) + '　高 ' + fmt2(b.high) + '<br>' +
    '低 ' + fmt2(b.low) + '　收 ' + fmt2(b.close) +
    (pct == null ? '' : '（' + (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%）') + '<br>' +
    (d.volume_mode === 'ticks' ? 'tick数 ' : '量 ') + (+b.volume).toLocaleString() + '<br>' +
    'RSI ' + fmt2(r) + '　MACD ' + fmt2((m.dif || [])[start + idx]) +
    '/' + fmt2((m.dea || [])[start + idx]) + '<br>' +
    'KDJ ' + fmt2((k.k || [])[start + idx]) + '/' + fmt2((k.d || [])[start + idx]) +
    '/' + fmt2((k.j || [])[start + idx]);
  tip.hidden = false;
  const tw = tip.offsetWidth, th = tip.offsetHeight;
  let left = e.clientX + 16, top = e.clientY + 12;
  if (left + tw > window.innerWidth - 8) left = e.clientX - tw - 16;
  if (top + th > window.innerHeight - 8) top = e.clientY - th - 12;
  tip.style.left = left + 'px';
  tip.style.top = top + 'px';
}

function klHideTip() {
  const tip = document.getElementById('klTip');
  if (tip) tip.hidden = true;
}

// 弹层初始化：事件只绑一次
if (!window._klBound) { klBindEvents(); window._klBound = true; }
```

同时：信息行 `#klInfo` 内容改为日级指标摘要（MA50/MA200/RSI14/止损位，来自 day 响应 `indicators`，`Object.entries` 拼接，空则「--」）；旧 `openKline` 内对 `klTitle/klInfo` 的引用与新结构对齐；`closeKline` 若旧实现仍被内联 onclick 引用则保持函数名不变（上文已提供新实现）。

- [ ] **Step 4: 语法与回归验证**

Run: 提取全部 `<script>` 到 /tmp/opencode/p_kline.js 后 `node --check /tmp/opencode/p_kline.js`
Expected: 无输出（语法通过）

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: passed（前端改动不碰后端）

- [ ] **Step 5: 自查清单**

- 旧 `drawKline`/`openKline` 全量替换无残留引用（grep `drawKline(`、`rsiCv`）
- `esc(String(...))` 防数字 0 空串（既有坑）
- 分时 tab 三态：无数据隐藏 / 有数据可切 / 加载失败提示
- 双击复位 / 拖动到边界 clamp / 缩放最小 20 根

---

### Task 4: 部署与线上验收

- [ ] **Step 1: 全量基线**

Run: `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_daily_bars.py tests/test_alert_engine.py tests/test_mailer.py tests/test_alert_api.py tests/test_backtest.py tests/test_quotes_hub.py tests/test_longport_push.py -q`
Expected: 全绿（基线 337 + Task1/2 新增）

- [ ] **Step 2: 部署重启（AGENTS.md 标准 deploy）**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' ./ root@***REMOVED***:/opt/qdii/ && ssh root@***REMOVED*** "systemctl restart qdii"
```

- [ ] **Step 3: API 验收（带 session）**

```bash
ssh root@***REMOVED*** "TOKEN=\$(mysql -h127.0.0.1 -P3914 -udubai -p'***REMOVED***' qdii -N -e \"SELECT token FROM sessions WHERE expires > NOW() ORDER BY created_at DESC LIMIT 1\"); for TF in day week month 5m; do echo ==\$TF==; curl -s -H \"Cookie: session_token=\$TOKEN\" \"http://127.0.0.1:8888/api/kline/QQQ?tf=\$TF&range=5\" | head -c 260; echo; done"
```

Expected: day≈5 根全字段；week/month 为聚合根；5m QQQ 有 bars（volume_mode=ticks）；`journalctl -u qdii --since '2 min ago'` 无 Traceback。

- [ ] **Step 4: 浏览器人工验收（用户）**

线上打开持仓页 → 点 symbol：日K 默认 120 根；拖动/滚轮/双击；切周K/月K；QQQ 切分时；悬浮看 OHLC+涨跌幅+量+RSI/MACD/KDJ；指标 tab 切换；其余 12 只无分时 tab。

- [ ] **Step 5: 额度复核**

`journalctl -u qdii --since '30 min ago' | grep -c 日K刷新`——K线弹层操作不产生任何「日K刷新」外的长桥请求（spec §2 零新增额度约束成立）。
