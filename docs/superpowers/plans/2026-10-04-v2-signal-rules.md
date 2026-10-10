# V2 双规则接入提醒引擎实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把回测报告 V2 双规则（QQQ 浮盈≥20% 且 VIX 2年低分位止盈 / 距 52 周高 -15% 深跌回投）接入提醒引擎，仅 QQQ 生效。

**Architecture:** VIX 日K CSV 一次性导入 daily_bars(symbol='VIX') + 长桥日增量（daily_refresh_loop 固定追加 VIX）；compute_indicators 扩展 week52_high；daily_bars 模块级 VIX 分位缓存；alert_engine 新增两条 eval 规则（沿用冷却/重置带模式）；前端提醒中心加两张规则卡。

**Tech Stack:** Python/FastAPI/aiomysql 既有栈；无新依赖。

**纪律（全 task 通用）:** 禁止 git add/commit；中文注释 + `@author yys`；禁 TODO/违禁词；测试零连库/零外网/零 lifespan（TestClient 不用 with）；计划代码笔误以语义正确为准修并记录偏差。

**Spec:** `docs/superpowers/specs/2026-10-04-v2-signal-rules-design.md`

---

### Task 1: daily_bars 扩展（week52_high + VIX 分位 + 刷新追加 + 种子脚本）

**Files:**
- Modify: `daily_bars.py`（compute_indicators L240-247 return、refresh_symbol L286-311、daily_refresh_loop L339-361、模块级变量区）
- Create: `vix_seed.py`
- Test: `tests/test_daily_bars.py`

- [ ] **Step 1: 写失败测试**（追加到 tests/test_daily_bars.py，注意该文件已 import daily_bars as dbar、无 datetime 依赖需求）

```python
# ---------------- V2 规则支撑：week52_high / VIX 分位 / CSV 种子 ----------------

def _mk_bars(n, base=100.0):
    """构造 n 根单边递增日K（close=i+base）。"""
    return [{"date": f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}",
             "open": base + i, "high": base + i, "low": base + i,
             "close": base + i, "volume": 0} for i in range(n)]


def test_compute_indicators_week52_high():
    ind = dbar.compute_indicators(_mk_bars(300))
    # 300 根递增（100..399）：近 252 根最高 = 399
    assert ind["week52_high"] == 399.0
    ind2 = dbar.compute_indicators(_mk_bars(100))
    # 历史不足 252 根 → None（deep_dip 不触发）
    assert ind2["week52_high"] is None


def test_vix_percentile():
    # 末值 4：窗口 [5,1,2,3,4] 中 <=4 的有 4 个 → 80.0
    assert dbar._vix_percentile([5.0, 1.0, 2.0, 3.0, 4.0]) == 80.0
    # 末值为最小 → 1/N
    assert dbar._vix_percentile([9.0, 8.0, 1.0]) == pytest.approx(100.0 / 3)
    # 窗口截断：>504 根取尾
    closes = [10.0] * 503 + [1.0] + [100.0]   # 505 根，末值 100（全局最高）
    assert dbar._vix_percentile(closes) == 100.0   # 尾窗 504 根里 100 仍是最大


def test_get_vix_percentile_stale_guard(monkeypatch):
    monkeypatch.setattr(dbar, "VIX_PCTL", 15.0)
    monkeypatch.setattr(dbar, "VIX_ASOF", "2026-10-01")
    # VIX 落后 QQQ 基准日 → None（拉取失败退化）
    assert dbar.get_vix_percentile("2026-10-02") is None
    # 同步/领先 → 返回分位
    assert dbar.get_vix_percentile("2026-10-01") == 15.0
    assert dbar.get_vix_percentile("2026-09-30") == 15.0
    assert dbar.get_vix_percentile(None) == 15.0
    # 缓存空 → None
    monkeypatch.setattr(dbar, "VIX_PCTL", None)
    assert dbar.get_vix_percentile(None) is None


def test_vix_seed_parse_csv(tmp_path):
    import vix_seed
    p = tmp_path / "vix.csv"
    p.write_text("date,close\n2026-10-02,15.31\n2016-10-03,13.57\n", encoding="utf-8")
    rows = vix_seed.parse_csv(str(p))
    # 排序升序 + open/high/low=close 填充 + volume=0
    assert rows == [("2016-10-03", 13.57, 13.57, 13.57, 13.57, 0),
                    ("2026-10-02", 15.31, 15.31, 15.31, 15.31, 0)]
```

注意：`pytest` 需在文件顶部 import（若已有则不重复）。`_mk_bars` 的 date 只需可排序字符串，允许超界月份值（如 "2026-11-01"），无 DB 交互。

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 新增 4 个测试 FAIL（AttributeError: no attribute `_vix_percentile` / `week52_high` / `VIX_PCTL` / No module named vix_seed），既有 20+1 个 PASS。

- [ ] **Step 3: 实现 daily_bars.py 扩展**

3a. 模块级变量（`INDICATORS` 定义之后、`compute_indicators` 之前合适位置）：

```python
# VIX 2 年分位缓存（refresh_symbol("VIX") 成功后重算；报告 V2 止盈过滤条件）
VIX_PCTL: float | None = None
VIX_ASOF: str | None = None
```

3b. `compute_indicators` 内（`hh = hhv(highs)` 之后加一行，return dict 加一键）：

```python
    week52 = max(closes[-252:]) if len(closes) >= 252 else None
```
return dict 中 `"hhv22": hh_v,` 之后加：
```python
        "week52_high": week52,
```

3c. 纯函数（放在 `get_indicators` 之后）：

```python
def _vix_percentile(closes: list[float]) -> float:
    """当日收盘在近 504 个交易日（含当日，2 年）中的百分位（0~100）。

    @author yys"""
    win = closes[-504:]
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
```

3d. `refresh_symbol`（L308-309 `bars = await db.get_daily_bars(symbol)` 与 `INDICATORS[symbol] = ...` 之间或之后）追加：

```python
    if symbol == "VIX":
        global VIX_PCTL, VIX_ASOF
        vix_closes = [float(b["close"]) for b in bars]
        VIX_PCTL = _vix_percentile(vix_closes)
        VIX_ASOF = str(bars[-1]["date"])[:10]
```

3e. `daily_refresh_loop` L347 改为：

```python
            symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if "VIX" not in symbols:      # VIX 分位数据源（V2 止盈过滤），固定追加不依赖持仓
                symbols.append("VIX")
```

- [ ] **Step 4: 创建 vix_seed.py**

```python
"""VIX 日K种子导入：CSV(date,close) → daily_bars(symbol='VIX')，幂等可重跑。

数据来源：QQQ×VIX 回测项目的 Yahoo 10 年日K导出（2016-10 起 2516 根），
历史底座一次导入；此后日增量由长桥日K刷新循环承担（lp_symbol→VIX.US）。
用法（本地/服务器均可，database.py 即线上远程库）：
    python3 vix_seed.py [csv路径]
@author yys
"""
import asyncio
import csv
import os
import sys

import database as db

SYMBOL = "VIX"
CANDIDATES = ["/root/kline_csv/VIX_US_day_10y.csv",
              "VIX_US_day_10y.csv",
              "/tmp/opencode/backtest/kline_csv/VIX_US_day_10y.csv"]


def parse_csv(path: str) -> list[tuple]:
    """解析 date,close 两列 → daily_bars 行元组（open/high/low=close，volume=0）。

    分位只看 close；OHLC 填充 close 是 daily_bars 表 NOT NULL 约束的最小满足。"""
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d, c = r["date"].strip()[:10], float(r["close"])
            rows.append((d, c, c, c, c, 0))
    rows.sort(key=lambda x: x[0])
    return rows


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else next(
        (p for p in CANDIDATES if os.path.exists(p)), None)
    if not path:
        print("未找到 VIX CSV，用法: python3 vix_seed.py <csv路径>")
        return
    rows = parse_csv(path)
    n = await db.save_daily_bars(SYMBOL, rows)
    total = await db.get_daily_bars(SYMBOL)
    print(f"CSV {len(rows)} 根，本次新增 {n} 根（INSERT IGNORE 幂等）；"
          f"{SYMBOL} 库内共 {len(total)} 根，区间 {total[0]['date']} ~ {total[-1]['date']}")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 5: 全部测试通过**

Run: `python3 -m pytest tests/test_daily_bars.py tests/test_alert_engine.py tests/test_alert_api.py -q`
Expected: 全 PASS（既有 + 新 4）。
Run: `python3 -m pyflakes daily_bars.py vix_seed.py`
Expected: 零警告（vix_seed 为新文件须零警告；daily_bars 与 HEAD 对比无新增警告）。

---

### Task 2: alert_engine 两条新规则 + server 白名单

**Files:**
- Modify: `alert_engine.py`（DEFAULT_PARAMS L20-25、eval_extreme_guard 之后加两函数、evaluate L198-200 接线）
- Modify: `server.py:2075`（_RULE_KEYS）
- Test: `tests/test_alert_engine.py`、`tests/test_alert_api.py`

- [ ] **Step 1: 写失败测试**（追加到 tests/test_alert_engine.py；注意既有 `engine_env` fixture 预写 `_costs={"NVDA":200}`、`_ind()` 无 week52_high 键——新测试用 `dict(_ind(), week52_high=...)` 扩展）

```python
# ---------------- V2 双规则（仅 QQQ）：止盈 + 深跌回投 ----------------

def _qqq_cost(monkeypatch, cost=100.0):
    monkeypatch.setattr(ae, "_costs", {"QQQ": cost})
    monkeypatch.setattr(ae, "_costs_at", time.monotonic())


def test_vix_take_profit_triggers(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_cost(monkeypatch)
    state = {}
    evs = ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state,
                                  dict(_ind(), asof="2026-10-02"))
    assert len(evs) == 1 and evs[0]["rule"] == "vix_take_profit"
    assert evs[0]["level"] == "warning"
    assert evs[0]["body"]["profit_pct"] == 25.0
    assert evs[0]["body"]["vix_pctile"] == 15.0


def test_vix_take_profit_needs_both_conditions(engine_env, monkeypatch):
    state = {}
    # 浮盈达标但 VIX 分位高（市场未自满）→ 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 35.0)
    _qqq_cost(monkeypatch)
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state, _ind()) == []
    # VIX 分位低但浮盈不足 → 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    assert ae.eval_vix_take_profit("QQQ", 115.0, ae.DEFAULT_PARAMS, state, _ind()) == []


def test_vix_take_profit_missing_vix_and_gate(engine_env, monkeypatch):
    # VIX 分位缺失（拉取失败退化）→ 不触发
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: None)
    _qqq_cost(monkeypatch)
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, {}, _ind()) == []
    # 非 QQQ 门禁（报告结论不外推）
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    assert ae.eval_vix_take_profit("NVDA", 250.0, ae.DEFAULT_PARAMS, {}, _ind()) == []
    # 无成本 → 不触发
    monkeypatch.setattr(ae, "_costs", {})
    assert ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, {}, _ind()) == []


def test_vix_take_profit_resets_out_of_band(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_cost(monkeypatch)
    state = {}
    ae.eval_vix_take_profit("QQQ", 125.0, ae.DEFAULT_PARAMS, state, _ind())
    assert "main" in state["vix_take_profit"]
    # 浮盈回落到带外（<19.5%）→ 冷却清除，重新具备触发资格
    ae.eval_vix_take_profit("QQQ", 119.0, ae.DEFAULT_PARAMS, state, _ind())
    assert "main" not in state["vix_take_profit"]


def test_deep_dip_triggers_and_resets(engine_env, monkeypatch):
    ind = dict(_ind(), week52_high=200.0)
    state = {}
    evs = ae.eval_deep_dip("QQQ", 168.0, ae.DEFAULT_PARAMS, state, ind)
    assert len(evs) == 1 and evs[0]["rule"] == "deep_dip"
    assert evs[0]["level"] == "info"
    assert evs[0]["body"]["drawdown_pct"] == -16.0
    assert evs[0]["body"]["week52_high"] == 200.0
    # 回撤收窄到带外（>-14.5%）→ 冷却清除
    ae.eval_deep_dip("QQQ", 171.2, ae.DEFAULT_PARAMS, state, ind)
    assert "main" not in state["deep_dip"]


def test_deep_dip_gate_and_missing_week52(engine_env, monkeypatch):
    # week52_high 缺失（历史不足）→ 不触发
    assert ae.eval_deep_dip("QQQ", 100.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=None)) == []
    # 非 QQQ 门禁
    assert ae.eval_deep_dip("NVDA", 100.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=200.0)) == []
    # 回撤未达标（-14%）→ 不触发
    assert ae.eval_deep_dip("QQQ", 172.0, ae.DEFAULT_PARAMS, {},
                            dict(_ind(), week52_high=200.0)) == []


def test_evaluate_dispatches_v2_rules(engine_env, monkeypatch):
    """evaluate 接线验证：QQQ 一次求值两条新规则生效。"""
    ind = dict(_ind(), week52_high=100.0, asof="2026-10-02")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: ind)
    monkeypatch.setattr(ae.daily_bars, "get_vix_percentile", lambda a=None: 15.0)
    _qqq_cost(monkeypatch)
    evs = asyncio.run(ae.evaluate("QQQ", 60.0))
    rules = {e["rule"] for e in evs}
    assert "deep_dip" in rules            # 距 52 周高回撤 -40% 达标
    assert "vix_take_profit" not in rules  # 浮盈 -40% 未达标
```

追加到 tests/test_alert_api.py（对齐既有规则保存测试的 `_client(monkeypatch)` 打桩模式，先 Read 该文件 L155-200 找到保存用例照抄结构）：

```python
def test_rule_save_accepts_v2_keys(monkeypatch):
    """_RULE_KEYS 白名单放行 vix_take_profit/deep_dip 新键。"""
    client = _client(monkeypatch)
    r = client.post("/api/alert/rule/QQQ", json={
        "params": {"vix_take_profit": {"enabled": True, "profit_pct": 25.0,
                                       "vix_pctile_max": 25.0},
                   "deep_dip": {"enabled": True, "dip_pct": 18.0}},
        "enabled": True})
    assert r.status_code == 200 and r.json()["ok"] is True
    # 未知键 400 回归
    r2 = client.post("/api/alert/rule/QQQ", json={
        "params": {"vix_take_profit2": {"enabled": True}}, "enabled": True})
    assert r2.status_code == 400
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_engine.py tests/test_alert_api.py -q`
Expected: 新增用例 FAIL（eval_vix_take_profit 不存在 / API 400 因键不在白名单），既有 PASS。

- [ ] **Step 3: 实现 alert_engine.py**

3a. DEFAULT_PARAMS（L20-25）追加两键：

```python
    "vix_take_profit": {"enabled": True, "profit_pct": 20.0, "vix_pctile_max": 20.0},
    "deep_dip": {"enabled": True, "dip_pct": 15.0},
```

3b. `eval_extreme_guard` 之后追加两函数：

```python
def eval_vix_take_profit(symbol: str, price: float, params: dict, state: dict,
                         ind: dict | None) -> list[dict]:
    """V2 止盈（回测实证）：浮盈≥阈值 且 VIX 处 2 年低分位，双条件缺一不可。

    浮盈高+波动率奇低=涨幅大且市场自满，风险积累；VIX 分位是防卖飞过滤器。
    仅 QQQ 生效（回测结论不外推）；VIX 分位缺失（拉取失败/落后基准日）
    不触发（报告同款 NaN 语义）。@author yys"""
    if symbol != "QQQ":
        return []
    cfg = params.get("vix_take_profit", {})
    if not cfg.get("enabled", True):
        return []
    cost = _costs.get(symbol)
    if not cost:
        return []
    vix_pctl = daily_bars.get_vix_percentile((ind or {}).get("asof"))
    if vix_pctl is None:
        return []
    profit_pct = (price / cost - 1.0) * 100.0
    want = float(cfg.get("profit_pct", 20.0))
    now = time.time()
    st = state.setdefault("vix_take_profit", {})
    if profit_pct < want - 0.5:
        st.pop("main", None)     # 浮盈离开触发带 → 冷却重置（可再触发）
        return []
    if profit_pct >= want and vix_pctl <= float(cfg.get("vix_pctile_max", 20.0)) \
            and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "vix_take_profit",
            "QQQ 浮盈达标且 VIX 处 2 年低分位，建议止盈",
            "warning",
            {"profit_pct": round(profit_pct, 2), "vix_pctile": round(vix_pctl, 1),
             "cost": cost, "price": price})]
    return []


def eval_deep_dip(symbol: str, price: float, params: dict, state: dict,
                  ind: dict | None) -> list[dict]:
    """V2 深跌回投（回测实证）：距 52 周高回撤≥阈值 → 底部区域提示。

    仅 QQQ 生效；与 extreme_guard 口径独立（52 周高 vs RSI/乖离），
    同轮并存属设计现状。@author yys"""
    if symbol != "QQQ":
        return []
    cfg = params.get("deep_dip", {})
    if not cfg.get("enabled", True) or not ind:
        return []
    week52 = ind.get("week52_high")
    if week52 is None:
        return []
    dd = (price / week52 - 1.0) * 100.0
    dip = float(cfg.get("dip_pct", 15.0))
    now = time.time()
    st = state.setdefault("deep_dip", {})
    if dd > -(dip - 0.5):
        st.pop("main", None)     # 回撤收窄离开触发带 → 冷却重置
        return []
    if dd <= -dip and _cooldown_ok(st, "main", now):
        _set_cooldown(st, "main", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "deep_dip",
            "QQQ 距 52 周高点回撤达标，深跌回投窗口",
            "info",
            {"drawdown_pct": round(dd, 2), "week52_high": round(week52, 2),
             "price": price})]
    return []
```

3c. `evaluate()` L200 `events += eval_extreme_guard(...)` 之后追加：

```python
        events += eval_vix_take_profit(symbol, price, params, state, ind)
        events += eval_deep_dip(symbol, price, params, state, ind)
```

- [ ] **Step 4: server.py 白名单**（L2075）

```python
_RULE_KEYS = ("price_plan", "atr_stop", "trend_filter", "extreme_guard",
              "vix_take_profit", "deep_dip")
```

- [ ] **Step 5: 全部测试通过**

Run: `python3 -m pytest tests/test_alert_engine.py tests/test_alert_api.py tests/test_daily_bars.py tests/test_backtest.py -q`
Expected: 全 PASS。
Run: `python3 -m pyflakes alert_engine.py server.py`
Expected: 与 HEAD 对比零新增警告。

---

### Task 3: 前端提醒中心两张规则卡

**Files:**
- Modify: `static/portfolio.html`（L1633-1645 RULE_META/RULE_ORDER/DEFAULT_RULE_PARAMS、规则卡渲染 elif 链 L1772-1780、保存收集 L1828-1840）

- [ ] **Step 1: Read 现有规则卡实现**

Read `static/portfolio.html` L1625-1850 全段，理解：RULE_META/RULE_ORDER/DEFAULT_RULE_PARAMS 结构、`ruleDraft` 渲染函数中 `k === 'price_plan' / 'atr_stop' / 'extreme_guard'` 各分支的 DOM 模板（checkbox id 形如 `re_<key>`、数字输入 id 形如 `rp_<param>`、`pick(base, k, default)` 回退函数）、保存时 params 收集结构。

- [ ] **Step 2: 数据结构追加**（L1633-1645）

```javascript
const RULE_META = {
  price_plan:    {name: '价格档位计划', desc: '成本价锚定档位%，跌=加仓/涨=止盈提示'},
  atr_stop:      {name: 'ATR 吊灯止损', desc: '价跌破 22日高 − k×ATR22 时提醒'},
  trend_filter:  {name: 'MA200 趋势过滤', desc: '趋势状态标记（用于回测回撤对照）'},
  extreme_guard: {name: 'RSI 极值防护', desc: 'rsi_low 超卖阈值 / bias_low 低位偏离%'},
  vix_take_profit: {name: 'VIX 低分位止盈', desc: '浮盈≥profit_pct% 且 VIX≤2年 vix_pctile_max 分位（仅QQQ，双条件缺一不可）'},
  deep_dip:      {name: '深跌回投窗口', desc: '距 52 周高回撤≥dip_pct% 提示（仅QQQ）'},
};
const RULE_ORDER = ['price_plan', 'atr_stop', 'trend_filter', 'extreme_guard',
                    'vix_take_profit', 'deep_dip'];
```
DEFAULT_RULE_PARAMS 同步追加（对齐 atr_stop 的数字参数模式）：
```javascript
  vix_take_profit: {enabled: true, profit_pct: 20, vix_pctile_max: 20},
  deep_dip: {enabled: true, dip_pct: 15},
```

- [ ] **Step 3: 卡渲染分支**（照 extreme_guard 的数字输入 DOM 模式，在 elif 链末尾追加两分支）

vix_take_profit 卡：checkbox `re_vix_take_profit` + 数字输入 `rp_profit_pct`、`rp_vix_pctile_max`（label「浮盈阈值%」「VIX分位≤%」）；deep_dip 卡：checkbox `re_deep_dip` + 数字输入 `rp_dip_pct`（label「回撤阈值%」）。具体 DOM 以 Step 1 读到的 atr_stop/extreme_guard 卡结构为准同构实现。

- [ ] **Step 4: 保存收集**（L1828-1840 params 收集对象追加）

```javascript
    vix_take_profit: {enabled: document.getElementById('re_vix_take_profit').checked,
                      profit_pct: num('rp_profit_pct',
        pick(base.vix_take_profit, 'profit_pct', DEFAULT_RULE_PARAMS.vix_take_profit.profit_pct)),
                      vix_pctile_max: num('rp_vix_pctile_max',
        pick(base.vix_take_profit, 'vix_pctile_max', DEFAULT_RULE_PARAMS.vix_take_profit.vix_pctile_max))},
    deep_dip: {enabled: document.getElementById('re_deep_dip').checked,
               dip_pct: num('rp_dip_pct',
        pick(base.deep_dip, 'dip_pct', DEFAULT_RULE_PARAMS.deep_dip.dip_pct))},
```
（`num`/`pick` 为既有工具函数；若实际签名不同以 Step 1 读到的为准。）

- [ ] **Step 5: 验证**

- 提取 script 块 `node --check` 通过（python3 正则提取 `<script>...</script>` 全部内容写入临时 .js 再 check）
- grep 确认新 id（re_vix_take_profit/re_deep_dip/rp_profit_pct/rp_vix_pctile_max/rp_dip_pct）在渲染与收集两侧成对出现
- grep 确认 RULE_ORDER 六键与 RULE_META 六键一致
- `python3 -m pytest tests/test_alert_api.py -q` 仍全 PASS（未动后端）

---

### Task 4: 部署与线上验收

**Files:** 无代码修改（执行部署与验收；发现问题回上游 task 修）

- [ ] **Step 1: 全量基线**

Run: `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py tests/test_alert_api.py tests/test_backtest.py tests/test_daily_bars.py tests/test_quotes_hub.py tests/test_longport_push.py tests/test_mailer.py -q`
Expected: 348 + 本项目新增（Task 1 +4 / Task 2 +8 左右）全 PASS，0 failed。

- [ ] **Step 2: VIX 种子导入线上库**（本地执行即写线上库）

Run: `python3 vix_seed.py /tmp/opencode/backtest/kline_csv/VIX_US_day_10y.csv`
Expected: 输出「CSV 2516 根，本次新增 2516 根；VIX 库内共 2516 根，区间 2016-10-03 ~ 2026-10-02」。
重跑一次验证幂等：新增 0 根。
（服务器路径 fallback 已内置；若本地 CSV 缺失，先 `scp root@<server-ip>:/root/kline_csv/VIX_US_day_10y.csv /tmp/opencode/backtest/kline_csv/`。）

- [ ] **Step 3: 部署**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='QQQ定投VIX止盈回测报告.html' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii && sleep 5 && systemctl is-active qdii"
```
（回测报告 HTML 不部署。）
Expected: active。

- [ ] **Step 4: 线上验收**

在服务器执行（token 取法与既往一致）：
1. `journalctl -u qdii --since '2 min ago' | grep -iE 'error|traceback'` → 无
2. 启动即刷验证：`journalctl -u qdii --since '2 min ago' | grep -E '日K刷新|VIX'` → 出现「日K刷新开始: 14 只」且 VIX 无 skipped 日志（若 VIX.US 拉取失败记 skipped empty_or_failed——此时分位退化不触发止盈，服务不炸；记录符号问题待查，可试 `.VIX.US` 变体）
3. `curl -s -b "session_token=$TOKEN" http://127.0.0.1:8888/api/alert/rules` → defaults 含 vix_take_profit/deep_dip 两键默认值
4. `mysql ... -e "SELECT symbol, COUNT(*) FROM daily_bars WHERE symbol='VIX' GROUP BY symbol"` → 2516（或 Step 2 实际根数）
5. kline 回归：`curl -s -b "session_token=$TOKEN" 'http://127.0.0.1:8888/api/kline/QQQ?tf=day&range=3'` → ok:true 四周期抽查
6. QQQ evaluate 无异常（journalctl 无「提醒求值异常」）

Expected: 全部通过。

- [ ] **Step 5: 汇报**

汇报验收结果与遗留（如 VIX.US 长桥符号失败则单列）。
