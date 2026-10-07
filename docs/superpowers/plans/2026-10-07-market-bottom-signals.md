# 市场底部信号（个股技术面预警 + 宏观估值看板）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为现有预警引擎新增 200 日均线区域/斐波那契回撤两类规则并在 K线画均线与斐波那契线；新增宏观四指标（巴菲特/CAPE/2s10s利差/恐慌贪婪）日频落库与纯看板展示。

**Architecture:** 一期全部复用 daily_bars 库内日K推导（零新增请求）；规则接入 alert_engine 既有冷却状态机；K线 overlays 随 /api/kline 下发。二期新建 macro_fetcher.py 日更循环（直连优先→代理容错→失败当日放弃），落 macro_indicators 表，GET /api/macro 供前端四张迷你卡。

**Tech Stack:** Python 3.12 / FastAPI / httpx / aiomysql / pytest；前端单文件 portfolio.html（Canvas 手绘，无构建）。

**Spec:** `docs/superpowers/specs/2026-10-07-market-bottom-signals-design.md`（含口径契约，改动前必读）

---

## 全局纪律（每个任务都必须遵守）

1. **禁止 `git add -A` / `git add .`**：仓库有无关未跟踪文件（.playwright-cli/、monthStatement/ 等），提交一律 `git add <明确文件列表>`。
2. 新增 Python 代码 `@author yys`；禁止 `// TODO`、伪代码、占位符。
3. 全中文注释与提交信息（代码关键字除外）。
4. **命名纪律**：外部数据源在本系统内只称 FRED/multpl/CNN（系统内数据源名称），严禁出现任何违禁词。
5. 测试命令（基线全绿才算完成）：

```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py \
  tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py \
  tests/test_alert_api.py tests/test_daily_bars.py tests/test_longport_push.py -q
```

预期末行：`NNN passed`（当前基线 372 passed，新增用例只增不减）。

---

### Task 1: daily_bars — 新增 ma200_bias 与 fib 回撤指标

**Files:**
- Modify: `daily_bars.py`（compute_indicators，约 224-256 行；常量区约 14-21 行）
- Test: `tests/test_daily_bars.py`（文件尾部追加）

- [ ] **Step 1: 写失败测试**（追加到 tests/test_daily_bars.py 末尾）

```python
def test_compute_indicators_fib_retracement():
    # 先涨后跌：低点段 close=10 → 高点段 close=20 → 末日回撤到 15；
    # high=close+1 / low=close-1（_bars 默认）→ H=21, L=9, depth=(21-15)/12=0.5
    closes = [10.0] * 105 + [20.0] * 105
    closes[-1] = 15.0
    snap = dbar.compute_indicators(_bars(closes))
    fib = snap["fib"]
    assert fib is not None
    assert fib["high"] == pytest.approx(21.0)
    assert fib["low"] == pytest.approx(9.0)
    assert fib["depth"] == pytest.approx(0.5)
    assert fib["levels"]["500"] == pytest.approx(15.0)
    assert fib["levels"]["382"] == pytest.approx(21.0 - 0.382 * 12.0)


def test_compute_indicators_fib_none_when_downtrend():
    # 先高后低（下跌延续段，L 晚于 H）：fib 必须为 None
    closes = [20.0] * 105 + [10.0] * 105
    snap = dbar.compute_indicators(_bars(closes))
    assert snap["fib"] is None


def test_compute_indicators_ma200_bias():
    bars = _bars([10.0 + 0.1 * i for i in range(210)])
    snap = dbar.compute_indicators(bars)
    expected = (snap["last_close"] - snap["ma200"]) / snap["ma200"] * 100.0
    assert snap["ma200_bias"] == pytest.approx(expected)


def test_compute_indicators_ma200_bias_none_when_short():
    snap = dbar.compute_indicators(_bars([1.0] * 60))
    assert snap["ma200"] is None and snap["ma200_bias"] is None
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 4 failed（KeyError/AttributeError：`snap["fib"]`/`ma200_bias` 不存在）

- [ ] **Step 3: 实现**（daily_bars.py）

3a. 常量区（`WEEK52_WINDOW = 252` 下方）加：

```python
FIB_WINDOW = 252             # 斐波那契回撤波段窗口（与 WEEK52_WINDOW 同源口径、独立常量）
```

3b. `compute_indicators` 内，`week52 = ...` 之后、`i = len(bars) - 1` 之前插入：

```python
    # 斐波那契回撤：FIB_WINDOW 窗口内先低后高的上涨波段才定义回撤；
    # L 晚于 H（下跌延续段）或 H==L 时 fib=None，下游规则以 None 静默禁用
    fib = None
    win_h = highs[-FIB_WINDOW:]
    win_l = lows[-FIB_WINDOW:]
    hi_idx = win_h.index(max(win_h))
    lo_idx = win_l.index(min(win_l))
    H, L = win_h[hi_idx], win_l[lo_idx]
    if H > L and lo_idx < hi_idx:
        fib = {"high": H, "low": L,
               "depth": (H - closes[-1]) / (H - L),
               "levels": {"382": H - 0.382 * (H - L),
                          "500": H - 0.5 * (H - L),
                          "618": H - 0.618 * (H - L)}}
```

3c. 返回 dict 中 `"last_close": closes[i],` 之后追加两行：

```python
        "ma200_bias": (closes[i] - ma200[i]) / ma200[i] * 100.0 if ma200[i] else None,
        "fib": fib,
```

注意：`fib` 里用 `closes[-1]` 与 `closes[i]` 等价（i 即最后一根），保持局部一致即可。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add daily_bars.py tests/test_daily_bars.py
git commit -m "feat(alert): 日K指标底座新增200日线乖离与斐波那契回撤(ma200_bias/fib)"
```

---

### Task 2: alert_engine — ma200_zone 与 fib_retrace 两类新规则

**Files:**
- Modify: `alert_engine.py`（DEFAULT_PARAMS 约 21-28 行；新规则函数放 eval_deep_dip 之后；evaluate 约 259-285 行）
- Test: `tests/test_alert_engine.py`（文件尾部追加）

- [ ] **Step 1: 写失败测试**（追加到 tests/test_alert_engine.py 末尾；模块已 `import alert_engine as ae`）

```python
# ---------------------------------------------------------------------------
# V3 新规则：ma200_zone（年线区域）/ fib_retrace（斐波那契回撤）
# ---------------------------------------------------------------------------

_IND_OK = {"enough_history": True, "ma200": 100.0,
           "fib": {"high": 120.0, "low": 80.0, "depth": 0.55,
                   "levels": {"382": 104.8, "500": 100.0, "618": 95.2}}}


def test_default_params_include_v3_rules():
    assert "ma200_zone" in ae.DEFAULT_PARAMS
    assert "fib_retrace" in ae.DEFAULT_PARAMS


def test_ma200_zone_deep_bias_fires():
    state = {}
    evs = ae.eval_ma200_zone("QQQ", 85.0, ae._merged_params(None), state, _IND_OK)
    assert len(evs) == 1
    assert evs[0]["rule"] == "ma200_zone" and evs[0]["level"] == "warning"
    assert evs[0]["body"]["bias_pct"] == -15.0


def test_ma200_zone_touch_fires():
    state = {}
    evs = ae.eval_ma200_zone("QQQ", 101.0, ae._merged_params(None), state, _IND_OK)
    assert len(evs) == 1 and evs[0]["level"] == "info"
    assert "贴近200日线" in evs[0]["title"]


def test_ma200_zone_cooldown_and_reset():
    params = ae._merged_params(None)
    state = {}
    assert len(ae.eval_ma200_zone("QQQ", 85.0, params, state, _IND_OK)) == 1
    assert ae.eval_ma200_zone("QQQ", 84.0, params, state, _IND_OK) == []   # 冷却内
    ae.eval_ma200_zone("QQQ", 99.0, params, state, _IND_OK)                # bias=-1 离开深乖离带
    assert "bias" not in state["ma200_zone"]                               # 冷却重置
    assert len(ae.eval_ma200_zone("QQQ", 85.0, params, state, _IND_OK)) == 1


def test_ma200_zone_gates():
    p = ae._merged_params(None)
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {},
                              {"enough_history": False, "ma200": 100.0}) == []
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {}, {"enough_history": True}) == []
    assert ae.eval_ma200_zone("QQQ", 85.0, p, {}, None) == []


def test_ma200_zone_param_override():
    params = {k: dict(v) for k, v in ae.DEFAULT_PARAMS.items()}
    params["ma200_zone"]["bias_low"] = -20.0
    assert ae.eval_ma200_zone("QQQ", 85.0, params, {}, _IND_OK) == []      # -15 未达 -20


def test_fib_retrace_fires():
    evs = ae.eval_fib_retrace("QQQ", 98.0, ae._merged_params(None), {}, _IND_OK)
    assert len(evs) == 1 and evs[0]["rule"] == "fib_retrace"
    assert evs[0]["body"]["fib_500"] == 100.0
    assert evs[0]["body"]["depth"] == 0.55


def test_fib_retrace_cooldown_and_reset():
    p = ae._merged_params(None)
    state = {}
    assert len(ae.eval_fib_retrace("QQQ", 98.0, p, state, _IND_OK)) == 1
    assert ae.eval_fib_retrace("QQQ", 97.0, p, state, _IND_OK) == []       # 冷却内
    ind2 = dict(_IND_OK, fib=dict(_IND_OK["fib"], depth=0.42))             # 离开带 <0.45
    ae.eval_fib_retrace("QQQ", 97.0, p, state, ind2)
    assert "main" not in state["fib_retrace"]
    assert len(ae.eval_fib_retrace("QQQ", 98.0, p, state, _IND_OK)) == 1


def test_fib_retrace_gates():
    p = ae._merged_params(None)
    assert ae.eval_fib_retrace("QQQ", 98.0, p, {}, dict(_IND_OK, fib=None)) == []
    assert ae.eval_fib_retrace("QQQ", 98.0, p, {},
                              {"enough_history": False, "fib": _IND_OK["fib"]}) == []
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_engine.py -q`
Expected: 新增用例 fail（`eval_ma200_zone` 不存在）；存量用例仍 pass

- [ ] **Step 3: 实现**（alert_engine.py）

3a. `DEFAULT_PARAMS` 追加两键：

```python
    "ma200_zone": {"enabled": True, "bias_low": -10.0, "band_pct": 2.0},
    "fib_retrace": {"enabled": True, "depth": 0.5},
```

3b. `eval_deep_dip` 之后新增两个函数：

```python
def eval_ma200_zone(symbol: str, price: float, params: dict, state: dict,
                    ind: dict | None) -> list[dict]:
    """年线区域提醒：深度乖离（bias 键）与贴近年线（touch 键）两个独立子信号。

    与 extreme_guard 的 MA50 乖离口径独立（本规则锚 200 日年线）；
    短历史（enough_history=False）静默禁用，同 atr_stop 惯例。@author yys"""
    cfg = params.get("ma200_zone", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    ma200 = ind.get("ma200")
    if not ma200:
        return []
    bias = (price - ma200) / ma200 * 100.0
    bias_low = float(cfg.get("bias_low", -10.0))
    band = float(cfg.get("band_pct", 2.0))
    events = []
    now = time.time()
    st = state.setdefault("ma200_zone", {})
    if bias > bias_low + 0.5:
        st.pop("bias", None)      # 离开深乖离带 → 冷却重置（0.5 个百分点边际）
    elif bias <= bias_low and _cooldown_ok(st, "bias", now):
        _set_cooldown(st, "bias", now, COOLDOWN_SECONDS)
        events.append(_event(
            symbol, "ma200_zone",
            f"{symbol} 现价较200日线乖离 {bias:+.1f}%，历史级低估区",
            "warning",
            {"bias_pct": round(bias, 2), "ma200": round(ma200, 2), "price": price}))
    if abs(bias) > band + 0.5:
        st.pop("touch", None)     # 离开年线带 → 冷却重置
    elif abs(bias) <= band and _cooldown_ok(st, "touch", now):
        _set_cooldown(st, "touch", now, COOLDOWN_SECONDS)
        events.append(_event(
            symbol, "ma200_zone",
            f"{symbol} 贴近200日线（乖离 {bias:+.1f}%），长线支撑测试",
            "info",
            {"bias_pct": round(bias, 2), "ma200": round(ma200, 2), "price": price}))
    return events


def eval_fib_retrace(symbol: str, price: float, params: dict, state: dict,
                     ind: dict | None) -> list[dict]:
    """斐波那契回撤提醒：252 日上涨波段回撤深度 ≥ 阈值 → 底部区域提示。

    波段口径见 daily_bars.compute_indicators（先低后高才定义回撤，
    下跌延续段 fib=None 静默禁用）。@author yys"""
    cfg = params.get("fib_retrace", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    fib = ind.get("fib")
    if not fib or fib.get("depth") is None:
        return []
    depth = float(fib["depth"])
    want = float(cfg.get("depth", 0.5))
    now = time.time()
    st = state.setdefault("fib_retrace", {})
    if depth < want - 0.05:
        st.pop("main", None)      # 回撤收窄离开触发带 → 冷却重置
        return []
    if depth >= want and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        lv = fib.get("levels") or {}
        return [_event(
            symbol, "fib_retrace",
            f"{symbol} 本轮涨幅已回撤 {depth * 100:.0f}%，斐波那契底部区域关注",
            "info",
            {"depth": round(depth, 3), "swing_high": fib.get("high"),
             "swing_low": fib.get("low"), "fib_382": lv.get("382"),
             "fib_500": lv.get("500"), "fib_618": lv.get("618"), "price": price})]
    return []
```

3c. `evaluate()` 内 `events += eval_deep_dip(...)` 之后追加：

```python
        events += eval_ma200_zone(symbol, price, params, state, ind)
        events += eval_fib_retrace(symbol, price, params, state, ind)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_engine.py tests/test_alert_api.py -q`
Expected: all passed（alert_api 的 kline 用例依赖 `_RULE_KEYS` 自动派生，同步验证无破坏）

- [ ] **Step 5: 提交**

```bash
git add alert_engine.py tests/test_alert_engine.py
git commit -m "feat(alert): 新增年线区域(ma200_zone)与斐波那契回撤(fib_retrace)预警规则"
```

---

### Task 3: server.py — 新参数键校验规格

**Files:**
- Modify: `server.py`（`_NUMERIC_PARAM_SPECS` 约 2086-2093 行）
- Test: `tests/test_alert_api.py`（尾部追加）

- [ ] **Step 1: 写失败测试**

```python
def test_rule_params_v3_new_keys_validation():
    # 新键数值规格：band_pct 开区间 >0 上界 20；depth 含 0.3 下界上界 0.9
    assert server._validate_rule_params(
        {"ma200_zone": {"bias_low": -12.0, "band_pct": 3.0},
         "fib_retrace": {"depth": 0.618}}) is None
    assert "band_pct" in server._validate_rule_params({"ma200_zone": {"band_pct": 0}})
    assert "band_pct" in server._validate_rule_params({"ma200_zone": {"band_pct": 25}})
    assert "depth" in server._validate_rule_params({"fib_retrace": {"depth": 0.2}})
    assert "depth" in server._validate_rule_params({"fib_retrace": {"depth": 0.95}})
    # 新规则键已进白名单（_RULE_KEYS 从 DEFAULT_PARAMS 自动派生）
    assert server._validate_rule_params({"ma200_zone": {}}) is None
    assert server._validate_rule_params({"fib_retrace": {}}) is None
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py::test_rule_params_v3_new_keys_validation -q`
Expected: FAIL（`band_pct`/`depth` 越界未拦截——注意 Task 2 完成后新键已在白名单，但无规格时越界值不报错）

- [ ] **Step 3: 实现**（`_NUMERIC_PARAM_SPECS` 字典追加）

```python
    "band_pct": (0.0, 20.0, True),          # 年线贴近带宽 >0 且 ≤20（pct）
    "depth": (0.3, 0.9, False),             # 斐波那契回撤阈值 0.3~0.9
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add server.py tests/test_alert_api.py
git commit -m "feat(alert): 策略包校验新增band_pct/depth数值规格"
```

---

### Task 4: server.py — /api/kline day 周期 overlays（均线序列+fib）

**Files:**
- Modify: `server.py`（kline 端点约 2217-2275 行）
- Test: `tests/test_alert_api.py`（尾部追加）

- [ ] **Step 1: 写失败测试**

```python
def test_kline_overlays_day_and_truncation(monkeypatch):
    import daily_bars as dbar
    client = _client(monkeypatch)
    bars = [{"date": date(2026, 9, d), "open": 10, "high": 11, "low": 9,
             "close": 10.5, "volume": 5} for d in range(1, 11)]

    async def fake_bars(sym, limit=None):
        return bars

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule",
                        lambda s: asyncio.sleep(0, result=None))
    monkeypatch.setattr(server.db, "get_holdings",
                        lambda enabled_only=True: asyncio.sleep(0, result=[]))
    monkeypatch.setitem(dbar.INDICATORS, "QQQ",
                        {"ma50": 10.4, "ma200": 10.0, "rsi14": 50.0, "stop_level": None,
                         "hhv22": 11.0, "atr22": 0.2, "enough_history": True,
                         "asof": "2026-09-10",
                         "fib": {"high": 11.0, "low": 9.0, "depth": 0.4,
                                 "levels": {"382": 10.24, "500": 10.0, "618": 9.76}}})
    server._KLINE_CACHE.clear()          # 防前序用例同 key 缓存污染
    d = client.get("/api/kline/QQQ?tf=day&range=1000").json()
    assert len(d["overlays"]["ma50_series"]) == 10
    assert d["overlays"]["ma50_series"][0] is None          # 10 根不足 50，None 占位
    assert d["overlays"]["fib"]["levels"]["500"] == 10.0
    assert d["overlays"]["fib"]["depth"] == 0.4
    d4 = client.get("/api/kline/QQQ?tf=day&range=4").json()  # 走缓存截尾分支
    assert len(d4["overlays"]["ma50_series"]) == 4
    assert d4["overlays"]["fib"]["levels"]["500"] == 10.0    # fib 横线不随截尾切
    server._KLINE_CACHE.clear()
    dw = client.get("/api/kline/QQQ?tf=week&range=1000").json()
    assert dw["overlays"] == {}                               # 非 day 周期为空 dict
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py::test_kline_overlays_day_and_truncation -q`
Expected: FAIL（响应无 `overlays` 键）

- [ ] **Step 3: 实现**（kline 端点两处改动）

3a. `else:` 分支（日/周/月取数）之后、组装 `payload = {...}` 之前插入：

```python
        overlays: dict = {}
        if tf == "day":
            closes = [float(b["close"]) for b in bars]
            overlays = {
                "ma50_series": _round_series(
                    daily_bars.sma(closes, daily_bars.MA_FAST), 2),
                "ma200_series": _round_series(
                    daily_bars.sma(closes, daily_bars.MA_SLOW), 2),
                "fib": (ind or {}).get("fib") or {},
            }
```

3b. `payload = {...}` 的 dict 字面量追加 `"overlays": overlays,` 一项。

3c. 截尾分支（`if keep < n:` 内、`return out` 前）追加：

```python
        ov = p.get("overlays") or {}
        out["overlays"] = {**ov,
                           "ma50_series": (ov.get("ma50_series") or [])[cut:],
                           "ma200_series": (ov.get("ma200_series") or [])[cut:]}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add server.py tests/test_alert_api.py
git commit -m "feat(kline): 日K响应新增overlays(MA50/MA200序列+斐波那契档位)"
```

---

### Task 5: database.py — macro_indicators 表与 CRUD

**Files:**
- Modify: `database.py`（init_tables 约 459-462 行调用处；新函数放 get_alert_rules 之前的提醒模块 CRUD 区域之后）
- Test: `tests/test_alert_api.py`（尾部追加）

- [ ] **Step 1: 写失败测试**

```python
def test_macro_indicator_crud(monkeypatch):
    import database as db
    captured = []

    async def fake_execute(sql, args=None, _pool=None):
        captured.append((sql, args))

    async def fake_exec_dict(sql, args=None, _pool=None):
        if "MAX(asof)" in sql:
            return [{"name": "cape", "asof": date(2026, 10, 6),
                     "value": 28.1, "extra": None}]
        return [{"asof": date(2026, 10, 6), "value": 28.1}]

    monkeypatch.setattr(db, "execute", fake_execute)
    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    asyncio.run(db.upsert_macro_indicator("cape", "2026-10-06", 28.1, {"rating": "x"}))
    sql, args = captured[0]
    assert "ON DUPLICATE KEY" in sql
    assert args[0] == "cape" and args[1] == "2026-10-06"
    latest = asyncio.run(db.get_macro_latest())
    assert latest[0]["name"] == "cape" and latest[0]["value"] == 28.1
    hist = asyncio.run(db.get_macro_history("cape", days=365))
    assert hist[0]["value"] == 28.1
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py::test_macro_indicator_crud -q`
Expected: FAIL（`upsert_macro_indicator` 不存在）

- [ ] **Step 3: 实现**（database.py 两处）

3a. 新建表函数（放在 init_alert_tables 之后）：

```python
async def init_macro_tables():
    """宏观估值指标表（幂等）。@author yys"""
    pool = await get_pool()
    await execute(
        """
        CREATE TABLE IF NOT EXISTS macro_indicators (
            id          BIGINT AUTO_INCREMENT PRIMARY KEY,
            name        VARCHAR(32) NOT NULL COMMENT 'buffett/cape/yield_2s10s/fear_greed',
            asof        DATE NOT NULL,
            value       DOUBLE NOT NULL,
            extra       TEXT COMMENT 'JSON 扩展（rating/source/分子分母等）',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_name_asof (name, asof)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='宏观估值指标日频快照'
        """,
        _pool=pool,
    )
```

3b. `init_tables()` 内 `await init_alert_tables()` 之后追加：

```python
    # ---- 宏观估值指标（巴菲特/CAPE/利差/恐慌贪婪，日频幂等）----
    await init_macro_tables()
```

3c. CRUD 三个函数（放提醒模块 CRUD 区域末尾，`_json_field` 附近）：

```python
async def upsert_macro_indicator(name: str, asof: str, value: float,
                                 extra: dict | None = None) -> int:
    """写入单条宏观指标（name+asof 幂等）。@author yys"""
    return await execute(
        "INSERT INTO macro_indicators (name, asof, value, extra) VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE value = VALUES(value), extra = VALUES(extra)",
        (name, asof, value,
         json.dumps(extra, ensure_ascii=False) if extra else None))


async def get_macro_latest() -> list[dict]:
    """四项宏观指标各自最新一行。@author yys"""
    return await execute_dict(
        "SELECT m.name, m.asof, m.value, m.extra FROM macro_indicators m "
        "JOIN (SELECT name, MAX(asof) mx FROM macro_indicators GROUP BY name) t "
        "ON m.name = t.name AND m.asof = t.mx")


async def get_macro_history(name: str, days: int = 365) -> list[dict]:
    """单指标历史（升序，供看板 spark）。@author yys"""
    return await execute_dict(
        "SELECT asof, value FROM macro_indicators WHERE name = %s "
        "AND asof >= DATE_SUB(CURDATE(), INTERVAL %s DAY) ORDER BY asof",
        (name, days))
```

注意：database.py 顶部已 `import json`（上一提交引入），无则补。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add database.py tests/test_alert_api.py
git commit -m "feat(macro): 宏观指标表macro_indicators建表与幂等CRUD"
```

---

### Task 6: macro_fetcher.py — 四指标抓取与解析（新文件）

**Files:**
- Create: `macro_fetcher.py`
- Test: `tests/test_macro_fetcher.py`（新建）

- [ ] **Step 1: 写失败测试**（新建 tests/test_macro_fetcher.py）

```python
"""macro_fetcher 解析纯逻辑测试（fixture 字符串，零网络）。@author yys"""
import macro_fetcher as mf


def test_parse_fred_csv_latest_skipping_dots():
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,1.5\n") == \
        ("2026-10-06", 1.5)
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,\n") is None
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n") is None


def test_parse_fred_csv_header_only():
    assert mf.parse_fred_csv("DATE,WILL5000PRFC\n") is None


def test_parse_multpl_cape():
    t = '[{"date":"2026-10-06","value":"28.14"},{"date":"2026-09-29","value":"27.9"}]'
    assert mf.parse_multpl_cape(t) == ("2026-10-06", 28.14)
    assert mf.parse_multpl_cape("[]") is None
    assert mf.parse_multpl_cape("<html>oops") is None


def test_parse_cnn_fg():
    t = '{"fear_greed":{"score":38.5,"rating":"Fear","previous":40}}'
    assert mf.parse_cnn_fg(t) == (38.5, "Fear")
    assert mf.parse_cnn_fg("{}") is None
    assert mf.parse_cnn_fg("not json") is None


def test_fetchers_registry():
    assert {f.__name__ for f in mf.FETCHERS} == {
        "fetch_buffett", "fetch_cape", "fetch_yield_spread", "fetch_fear_greed"}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_macro_fetcher.py -q`
Expected: FAIL（ModuleNotFoundError: macro_fetcher）

- [ ] **Step 3: 实现**（新建 macro_fetcher.py，全文如下）

```python
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
```

注意：`db.get_alive_proxies()` 返回 `SELECT * FROM proxy_pool` 行，代理地址列名为 `url`（database.py:567-571），若列名不同以实际为准（grep proxy_pool 建表语句确认）。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_macro_fetcher.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add macro_fetcher.py tests/test_macro_fetcher.py
git commit -m "feat(macro): 四宏观指标日频抓取解析模块(直连优先/代理容错/幂等落库)"
```

---

### Task 7: server.py — macro 循环注册 + GET /api/macro

**Files:**
- Modify: `server.py`（import 区；startup 任务注册约 183-191 行；shutdown cancel 列表；API 端点放 /api/indicators 附近）
- Test: `tests/test_alert_api.py`（尾部追加）

- [ ] **Step 1: 写失败测试**

```python
def test_macro_endpoint(monkeypatch):
    client = _client(monkeypatch)

    async def fake_latest():
        return [{"name": "cape", "asof": date(2026, 10, 6), "value": 28.1,
                 "extra": None},
                {"name": "fear_greed", "asof": date(2026, 10, 7), "value": 22.0,
                 "extra": json.dumps({"source": "cnn"})}]

    async def fake_hist(name, days=365):
        return [{"asof": date(2026, 10, 6), "value": 28.1},
                {"asof": date(2026, 10, 7), "value": 28.3}]

    monkeypatch.setattr(server.db, "get_macro_latest", fake_latest)
    monkeypatch.setattr(server.db, "get_macro_history", fake_hist)
    d = client.get("/api/macro").json()
    assert [i["name"] for i in d["indicators"]] == ["cape", "fear_greed"]
    assert d["status"]["cape"] == "expensive"
    assert d["status"]["fear_greed"] == "cheap"
    assert len(d["indicators"][0]["history"]) == 2
    assert d["indicators"][1]["extra"] == {"source": "cnn"}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py::test_macro_endpoint -q`
Expected: FAIL（404 Not Found）

- [ ] **Step 3: 实现**（server.py 四处）

3a. 顶部 import 区（`import daily_bars` 附近）加：

```python
import macro_fetcher
```

3b. startup 注册（`ledger_fx_task = ...` 之后）：

```python
    # 宏观估值指标日更（22:40，纯看板展示不进 alerts）
    global macro_task
    macro_task = asyncio.create_task(macro_fetcher.macro_daily_loop())
```

并在模块顶部全局变量声明区（`ledger_fx_task` 等同级处）补 `macro_task = None`；shutdown 的 cancel 列表（`if ledger_fx_task: ledger_fx_task.cancel()` 之后）补：

```python
    if macro_task:
        macro_task.cancel()
```

3c. API 端点（放 `/api/indicators/{symbol}` 之后）：

```python
_MACRO_STATUS_RULES = {
    "buffett": lambda v: "cheap" if v < 80 else ("neutral" if v <= 120 else "expensive"),
    "cape": lambda v: "cheap" if v < 15 else ("neutral" if v <= 25 else "expensive"),
    "yield_2s10s": lambda v: "risk" if v < 0 else ("neutral" if v <= 1 else "cheap"),
    "fear_greed": lambda v: "cheap" if v < 25 else ("neutral" if v <= 55 else "expensive"),
}


@app.get("/api/macro")
async def macro_view(request: Request):
    """宏观估值四指标：最新值 + 近一年历史 + 状态判定（纯看板展示，
    不进 alerts 铃声流）。@author yys"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    latest = {r["name"]: r for r in await db.get_macro_latest()}
    indicators = []
    status = {}
    for name in ("buffett", "cape", "yield_2s10s", "fear_greed"):
        row = latest.get(name)
        if not row:
            continue
        try:
            extra = json.loads(row.get("extra")) if row.get("extra") else {}
        except Exception:
            extra = {}
        hist = await db.get_macro_history(name, days=365)
        indicators.append({
            "name": name, "value": float(row["value"]),
            "asof": str(row["asof"]), "extra": extra,
            "history": [[str(h["asof"]), float(h["value"])] for h in hist],
        })
        rule = _MACRO_STATUS_RULES.get(name)
        status[name] = rule(float(row["value"])) if rule else "neutral"
    return {"ok": True, "indicators": indicators, "status": status}
```

注意：确认 server.py 顶部已 `import json`（SSE 广播在用，应当已有；无则补）。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: all passed

- [ ] **Step 5: 提交**

```bash
git add server.py tests/test_alert_api.py
git commit -m "feat(macro): macro日更循环注册与/api/macro端点(最新值+历史+状态)"
```

---

### Task 8: 前端 — K线均线/斐波那契画线 + 市场估值区块

**Files:**
- Modify: `static/portfolio.html`（klRender 约 2077-2092 行；klDrawMain 约 2101-2164 行；klInfoLine 约 2050-2064 行；市场看板卡片约 354-390 行；CSS 区约 288-294 行；JS 看板区约 609-651 行）

无 JS 测试设施，本任务靠代码审查 + 最终 API 手动验证。改动保持单文件零构建。

- [ ] **Step 1: klRender 传 overlays**

`klRender` 内 `klDrawMain(bars, d.marks || {});` 改为：

```javascript
  klDrawMain(bars, d.marks || {}, (KL.tf === 'day' && d.overlays) || null);
```

- [ ] **Step 2: klDrawMain 画均线与斐波那契**

2a. 函数签名改 `function klDrawMain(bars, marks, ov) {`。

2b. `lines` 数组组装段（`if (marks.hhv22 != null)...` 之后、`lines.forEach(([, p]) => {...})` 之前）插入 fib 三档横线：

```javascript
  const fib = ov && ov.fib;
  if (fib && fib.levels) {
    [['Fib38.2', fib.levels['382']], ['Fib50', fib.levels['500']],
     ['Fib61.8', fib.levels['618']]]
      .forEach(([nm, p]) => { if (p != null) lines.push([nm, +p, '#8E9ABF']); });
  }
```

2c. 蜡烛 `bars.forEach(...)` 绘制循环之后插入均线叠加与图例：

```javascript
  // 均线叠加（仅日K有 overlays）：MA50 紫 / MA200 蓝，None 断段
  if (ov) {
    const maLine = (vals, color) => {
      if (!vals || !vals.length) return;
      ctx.strokeStyle = color; ctx.lineWidth = 1.2; ctx.lineJoin = 'round';
      ctx.beginPath();
      let started = false;
      vals.forEach((v, i) => {
        if (v == null) { started = false; return; }
        const px = x(i), py = y(+v);
        if (!started) { ctx.moveTo(px, py); started = true; } else ctx.lineTo(px, py);
      });
      ctx.stroke();
    };
    maLine(ov.ma50_series, '#9B59B6');
    maLine(ov.ma200_series, '#2A6CF0');
    ctx.font = F11; ctx.textBaseline = 'middle'; ctx.textAlign = 'left';
    ctx.fillStyle = '#9B59B6'; ctx.fillText('MA50', pad.l + 4, pad.t + 4);
    ctx.fillStyle = '#2A6CF0'; ctx.fillText('MA200', pad.l + 44, pad.t + 4);
  }
```

- [ ] **Step 3: klInfoLine 加 Fib 回撤**

`if (ind.asof) parts.push(...)` 之前插入：

```javascript
  const fib = d && d.overlays && d.overlays.fib;
  if (fib && fib.depth != null) parts.push('Fib回撤 ' + Math.round(fib.depth * 100) + '%');
```

- [ ] **Step 4: 市场估值区块**

4a. HTML：`<!-- 市场看板 -->` 卡片结束与 `<!-- 分析看板 -->` 之间插入：

```html
  <!-- 市场估值：宏观四指标（巴菲特/CAPE/利差/恐慌贪婪，日更纯展示） -->
  <div class="card">
    <div class="card-title">市场估值</div>
    <div class="macro-grid" id="macroGrid"></div>
  </div>
```

4b. CSS（`.kline-tip b` 规则之后追加）：

```css
/* ---- 市场估值：2×2 迷你卡（宏观指标日更） ---- */
.macro-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
.macro-cell { border: 1px solid #EBEEF5; border-radius: 8px; padding: 8px 10px; }
.macro-head { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--txt2); }
.macro-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; flex: none; }
.macro-val { font-size: 20px; font-weight: 600; margin: 4px 0 2px; }
.macro-asof { font-size: 11px; color: var(--txt3); }
.macro-spark { width: 100%; height: 30px; display: block; }
@media (max-width: 768px) { .macro-grid { grid-template-columns: 1fr; } }
```

4c. JS（`renderMarket` 函数之后追加）：

```javascript
// ===== 市场估值（宏观四指标，日更纯展示，不进 alerts）=====
const MACRO_META = {
  buffett: ['巴菲特指标', '总市值/GDP %', v => Math.round(v).toString()],
  cape: ['席勒CAPE', '10年通胀调整PE', v => v.toFixed(1)],
  yield_2s10s: ['美债2s10s利差', '倒挂=风险 pct', v => v.toFixed(2)],
  fear_greed: ['恐慌贪婪指数', '低于25极度恐慌(机会)', v => Math.round(v).toString()],
};
const MACRO_COLORS = { cheap: '#00A870', neutral: '#909399',
                       expensive: '#E5484D', risk: '#E5484D' };

/** 拉取 /api/macro 并渲染（失败静默保留旧卡片）。@author yys */
async function loadMacro() {
  let d = null;
  try { d = await (await fetch('/api/macro')).json(); } catch (e) { return; }
  renderMacro(d);
}

function renderMacro(d) {
  const box = document.getElementById('macroGrid');
  if (!box) return;
  box.innerHTML = '';
  ((d && d.indicators) || []).forEach(m => {
    const meta = MACRO_META[m.name];
    if (!meta) return;
    const st = (d.status || {})[m.name] || 'neutral';
    const cell = document.createElement('div');
    cell.className = 'macro-cell';
    cell.innerHTML = `<div class="macro-head">
        <span class="macro-dot" style="background:${MACRO_COLORS[st] || MACRO_COLORS.neutral}"></span>
        <span>${esc(meta[0])}</span><span style="margin-left:auto">${esc(meta[1])}</span></div>
      <div class="macro-val">${meta[2](+m.value)}</div>
      <div class="macro-asof">截至 ${esc(m.asof || '--')}</div>
      <canvas class="macro-spark"
        ${(m.history && m.history.length >= 5) ? '' : 'style="display:none"'}></canvas>`;
    const tip = document.createElement('div');
    tip.className = 'mkt-tip';
    cell.appendChild(tip);
    box.appendChild(cell);
    if (m.history && m.history.length >= 5)
      drawSpark(cell.querySelector('canvas'), m.history, true, tip, 2);
  });
  if (!box.children.length) box.innerHTML = '<div class="macro-asof">暂无数据</div>';
}
```

（`esc`/`drawSpark`/`mkt-tip` 均为现有实现，直接复用。）

4d. 调用接线：定位包含 `renderMarket(d);`（约 907 行，overview 加载函数）的位置，同一函数内紧随其后追加一行 `loadMacro();`。

- [ ] **Step 5: 语法自检**

Run: `node --check <(sed -n '/<script>/,/<\/script>/p' static/portfolio.html | sed '1d;$d') 2>/dev/null || grep -c "function loadMacro" static/portfolio.html`
Expected: 输出 `1`（loadMacro 已定义；若环境有 node 可做更强 JS 语法检查）

- [ ] **Step 6: 提交**

```bash
git add static/portfolio.html
git commit -m "feat(ui): K线叠加MA50/MA200与斐波那契档位线+市场估值四指标卡片"
```

---

### Task 9: 最终验证

- [ ] **Step 1: 全量基线测试**

Run 全局纪律中的测试命令。
Expected: `NNN passed`（≥ 372 + 新增 ~18），无 failed/error。

- [ ] **Step 2: 服务可启动性冒烟（不起服务，仅语法/导入检查）**

```bash
python3 -c "import ast,sys; [ast.parse(open(f).read(), f) for f in ('daily_bars.py','alert_engine.py','macro_fetcher.py','server.py','database.py')]; print('syntax ok')"
python3 -c "import macro_fetcher; print('import ok')"
```

Expected: `syntax ok` / `import ok`（**严禁本地起服务**，双实例会放大外部请求触发风控）

- [ ] **Step 3: 汇总**

输出改动文件清单 + 测试计数 + 提交列表（`git log --oneline -9`）。

---

## Self-Review 记录

- Spec 覆盖：3.1→Task1、3.2→Task2、3.3→Task3、3.4/3.5→Task4/8、4.1→Task6、4.2→Task5、4.3→Task7、4.4→Task8、5→各任务测试+Task9。✓
- 占位符扫描：无 TBD/TODO/「适当处理」。✓
- 类型一致性：`fib["levels"]` 键 "382"/"500"/"618" 在 Task1/2/4/8 一致；`upsert_macro_indicator(name, asof, value, extra)` 签名 Task5/6 一致；`/api/macro` 响应字段 Task7/8 一致。✓
