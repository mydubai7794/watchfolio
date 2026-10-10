# 策略提醒市值过滤 + 外部 Agent 持仓接口 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提醒引擎只评估持仓市值 ≥ 阈值（默认 500 USD，可配置）的美股；新增 API Key 鉴权的 `GET /api/agent/portfolio` 供外部 agent 拉取持仓/收益/现金/盈亏。

**Architecture:** 过滤收口在 `alert_engine.evaluate_round`（轮询与长桥 push 双入口统一）；agent 端点读 `portfolio_cache` 零额外行情请求，middleware 放行 `/api/agent/` 前缀、端点内自鉴权（`hmac.compare_digest`）。

**Tech Stack:** Python 3 / FastAPI / pytest（monkeypatch 桩 DB，测试不连库不起服务）。

**Spec:** `docs/superpowers/specs/2026-10-05-alert-filter-agent-api-design.md`

**测试基线命令**（在仓库根 `/root/workspace/project/qdii` 执行）：
```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py -q
```

**已知背景（实施者必读）：**
- 全程中文注释/提交信息；新增函数 docstring 带尾部标记 `@author yys`；禁止 `// TODO` 与伪代码。
- 禁止在代码/注释/文档中出现任何外部交易所品牌名等违禁词（描述数据源只用系统内名称：腾讯/新浪/Yahoo/FT/twelvedata/长桥/24h数据源）。
- 测试不起服务、不连 DB：全部用 monkeypatch 桩 `server.db` / `ae.db`。
- 代码中不得出现任何占位符；本计划中给出的代码即最终代码。

---

### Task 1: 提醒引擎市值过滤（alert_engine.py）

**Files:**
- Modify: `alert_engine.py`（`_costs` → `_positions`，`_load_costs` → `_load_positions`，`evaluate_round` 加过滤）
- Test: `tests/test_alert_engine.py`（fixture 改造 + 新增 5 个用例）

- [ ] **Step 1: 写失败测试**

在 `tests/test_alert_engine.py` 末尾追加（注意：fixture 与 helper 的改造在 Step 3，此步先加用例）：

```python
# ---------------- 市值阈值过滤（evaluate_round 收口） ----------------

def _positions_env(monkeypatch, positions, config_value="500"):
    """预写持仓缓存与 sys_config 桩（config_value 传 None 模拟配置缺失）。"""
    monkeypatch.setattr(ae, "_positions", positions)
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())

    async def fake_config(key):
        return config_value
    monkeypatch.setattr(ae.db, "get_config", fake_config)


def test_round_filters_small_position(engine_env, monkeypatch):
    """市值 < 阈值 → 不评估不落库：2 股 × 149 = 298 < 500，跌破止损也无事件。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 2.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert evs == []


def test_round_keeps_large_position(engine_env, monkeypatch):
    """市值 ≥ 阈值 → 正常评估：10 股 × 149 = 1490 ≥ 500，atr_stop 触发。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 10.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)


def test_round_threshold_boundary_equal(engine_env, monkeypatch):
    """恰好等于阈值 → 保留（≥ 判定）：1 股 × 500 = 500。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 1.0}}, "500")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 500.0}))
    assert evs != []


def test_round_zero_threshold_disables_filter(engine_env, monkeypatch):
    """阈值 0 → 不过滤：2 股 × 149 = 298 仍评估。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 2.0}}, "0")
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)


def test_round_missing_config_defaults_500(engine_env, monkeypatch):
    """配置缺失 → 默认 500：3 股 × 149 = 447 < 500 被过滤。"""
    _positions_env(monkeypatch, {"NVDA": {"cost": 200.0, "qty": 3.0}}, None)
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert evs == []
```

- [ ] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_alert_engine.py -k "round_" -v
```
预期：5 个新用例 FAIL（`_positions` 属性不存在，monkeypatch.setattr 抛 AttributeError）。

- [ ] **Step 3: 实现 alert_engine.py 改造**

3a. 模块级缓存（原 29-30 行）替换为：

```python
_positions: dict[str, dict] = {}   # symbol → {"cost": 成本价, "qty": 数量}
_positions_at = 0.0
```

3b. `_load_costs`（原 50-58 行）整体替换为：

```python
async def _load_positions() -> dict[str, dict]:
    """持仓成本+数量缓存（60s 刷新，供市值阈值过滤与成本锚定）。"""
    global _positions, _positions_at
    if time.monotonic() - _positions_at > 60 or not _positions:
        holdings = await db.get_holdings(enabled_only=True)
        _positions = {h["symbol"]: {"cost": float(h["cost_price"] or 0),
                                    "qty": float(h["quantity"] or 0)}
                      for h in holdings if h["market"] == "US"}
        _positions_at = time.monotonic()
    return _positions
```

3c. `eval_price_plan` 内（原 82 行 `cost = _costs.get(symbol)`）替换为：

```python
    cost = _positions.get(symbol, {}).get("cost")
```

3d. `eval_vix_take_profit` 内（原 182 行同款）替换为：

```python
    cost = _positions.get(symbol, {}).get("cost")
```

3e. `evaluate_round`（原 286-303 行）整体替换为：

```python
async def evaluate_round(prices: dict[str, float]) -> list[dict]:
    """一轮全量求值（轮询循环/push 回调统一入口）。

    市值阈值过滤：持仓市值（现价×数量）低于 ALERT_MIN_MARKET_VALUE
    （sys_config，USD，默认 500）的标的跳过求值——不评估、不提醒、不落库；
    设 0 或负值关闭过滤。"""
    global COOLDOWN_SECONDS
    threshold = 500.0
    try:
        hours = float(await db.get_config("ALERT_COOLDOWN_HOURS") or 4)
        COOLDOWN_SECONDS = hours * 3600.0
        threshold = float(await db.get_config("ALERT_MIN_MARKET_VALUE") or 500)
    except Exception:
        logger.warning("冷却/阈值配置加载失败，沿用当前 %sh / %.0f USD",
                       COOLDOWN_SECONDS / 3600, threshold)
    await _load_positions()
    events = []
    for sym, price in prices.items():
        pos = _positions.get(sym)
        if price is None or pos is None:
            continue
        if threshold > 0 and float(price) * pos["qty"] < threshold:
            continue
        try:
            events += await evaluate(sym, float(price))
        except Exception as e:
            logger.warning("提醒求值异常 %s: %s", sym, e)
    return events
```

- [ ] **Step 4: 同步改造 tests/test_alert_engine.py 既有桩**

4a. fixture `engine_env` 中（原 26-38 行）三处替换：

```python
    async def fake_holdings(enabled_only=True):
        return [{"symbol": "NVDA", "market": "US", "cost_price": 200.0,
                 "quantity": 100.0}]
    monkeypatch.setattr(ae.db, "get_holdings", fake_holdings)
```

（原 `monkeypatch.setattr(ae.db, "get_config", fake_config)` 返回 `"0"` 保持不变——冷却 0 即时过期、市值阈值 0 关闭过滤，存量用例行为不变。）

缓存预写两行替换为：

```python
    monkeypatch.setattr(ae, "_positions", {"NVDA": {"cost": 200.0, "qty": 100.0}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
```

4b. helper `_qqq_cost`（原 208-211 行）替换为：

```python
def _qqq_positions(monkeypatch, cost=100.0, qty=100.0):
    """预写 QQQ 持仓缓存（覆盖 engine_env 的 NVDA 缓存，QQQ 专用用例用）。"""
    monkeypatch.setattr(ae, "_positions", {"QQQ": {"cost": cost, "qty": qty}})
    monkeypatch.setattr(ae, "_positions_at", time.monotonic())
```

并全文将调用 `_qqq_cost(monkeypatch)` / `_qqq_cost(monkeypatch, cost=…)`（如有）改为 `_qqq_positions(monkeypatch)`；将 `monkeypatch.setattr(ae, "_costs", {})`（原 246 行）改为 `monkeypatch.setattr(ae, "_positions", {})`。

- [ ] **Step 5: 跑 alert_engine 全量测试确认通过**

```bash
python3 -m pytest tests/test_alert_engine.py -q
```
预期：全部 PASS（存量 + 新增 5 个）。

- [ ] **Step 6: Commit**

```bash
git add alert_engine.py tests/test_alert_engine.py
git commit -m "feat(alert): 提醒引擎市值阈值过滤(sys_config ALERT_MIN_MARKET_VALUE,默认500USD,0关闭)"
```

---

### Task 2: 外部 Agent 持仓接口（server.py）

**Files:**
- Modify: `server.py`（`_is_auth_path` 放行 `/api/agent/`；新增 `import hmac`；新增端点）
- Test: `tests/test_server_auth.py`（末尾追加测试类）

- [ ] **Step 1: 写失败测试**

在 `tests/test_server_auth.py` 末尾追加：

```python
class TestAgentPortfolioApi:
    """GET /api/agent/portfolio：API Key 鉴权三态与响应结构。

    middleware 放行 /api/agent/ 前缀（无需浏览器 session），
    端点内自鉴权（X-API-Key 头）。
    """

    _base_cache = {
        "ok": True, "session": "regular", "trade_date": "2026-10-02",
        "fx_usdhkd": 7.8,
        "summary": {"total_value": 1234.5, "holdings_value": 1000.0,
                    "cash_usd": 234.5, "cost_basis": 900.0,
                    "day_pnl": 10.0, "day_pnl_pct": 0.8, "close_pnl": 8.0,
                    "overnight_pnl": 2.0, "total_pnl": 100.0,
                    "total_pnl_pct": 10.0},
        "positions": [{"symbol": "NVDA", "name": "测试", "market": "US",
                       "currency": "USD", "quantity": 2.0,
                       "cost_price": 100.0, "price": 110.0, "value": 220.0,
                       "today_pnl": 5.0, "position_pnl": 20.0,
                       "position_pnl_pct": 10.0, "cost_usd": 200.0,
                       "stale": False, "ext": "regular"}],
        "cash": [{"currency": "USD", "amount": 234.5}],
        "errors": [],
    }

    def _patch_key(self, monkeypatch, key):
        async def fake_config(k):
            return key if k == "AGENT_API_KEY" else None
        monkeypatch.setattr(server.db, "get_config", fake_config)

    def teardown_method(self):
        server.portfolio_cache = {"ok": False, "positions": [], "summary": {}, "errors": []}
        server.market_cache = []

    def test_404_when_key_unconfigured(self, monkeypatch):
        """AGENT_API_KEY 未配置 → 404（视为禁用，不暴露端点存在性）。"""
        self._patch_key(monkeypatch, None)
        r = client.get("/api/agent/portfolio", headers={"X-API-Key": "x"})
        assert r.status_code == 404

    def test_401_wrong_key(self, monkeypatch):
        """key 不匹配 → 401。"""
        self._patch_key(monkeypatch, "right-key")
        r = client.get("/api/agent/portfolio", headers={"X-API-Key": "wrong-key"})
        assert r.status_code == 401

    def test_200_valid_key_and_structure(self, monkeypatch):
        """key 匹配 → 200，结构完整（summary/holdings/cash/fx/cny 键齐全）。"""
        self._patch_key(monkeypatch, "right-key")
        server.portfolio_cache = dict(self._base_cache)
        server.market_cache = [{"key": "USDCNH", "price": 7.12}]
        r = client.get("/api/agent/portfolio", headers={"X-API-Key": "right-key"})
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["session"] == "regular"
        assert data["trade_date"] == "2026-10-02"
        assert data["generated_at"]
        for k in ("total_value", "holdings_value", "cash_usd", "cost_basis",
                  "day_pnl", "close_pnl", "overnight_pnl",
                  "total_pnl", "total_pnl_pct"):
            assert k in data["summary"]
        h = data["holdings"][0]
        for k in ("symbol", "name", "market", "currency", "quantity",
                  "cost_price", "price", "value", "today_pnl",
                  "position_pnl", "position_pnl_pct", "cost_usd",
                  "stale", "ext"):
            assert k in h
        assert data["cash"] == [{"currency": "USD", "amount": 234.5}]
        assert data["fx"]["usdhkd"] == 7.8
        assert data["fx"]["usdcnh"] == 7.12

    def test_cny_recalculated_with_live_usdcnh(self, monkeypatch):
        """cny 与 overview 同逻辑：用最新看板 USDCNH 实时重算（不随快照拖旧）。"""
        self._patch_key(monkeypatch, "right-key")
        server.portfolio_cache = dict(self._base_cache)
        server.market_cache = [{"key": "USDCNH", "price": 7.1}]
        server._cny_cfg["invested"] = 14000.0
        server._cny_cfg["expense"] = 0.0
        try:
            r = client.get("/api/agent/portfolio", headers={"X-API-Key": "right-key"})
            cny = r.json()["cny"]
            assert cny is not None
            assert cny["rate"] == 7.1
            assert cny["value"] == round(1234.5 * 7.1, 2)
        finally:
            server._cny_cfg["invested"] = 0.0
            server._cny_cfg["expense"] = 0.0
```

- [ ] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_server_auth.py::TestAgentPortfolioApi -v
```
预期：至少 3 个用例 FAIL（middleware 未放行 `/api/agent/` → 401 未登录；或端点不存在 404，均与断言不符）。例外：`test_401_wrong_key` 可能因 middleware 的 401 与断言状态码巧合而意外通过——以其余用例 FAIL 为准。

- [ ] **Step 3: 实现 server.py 三处改动**

3a. import 区（第 3-12 行标准库块内，`import hashlib` 位置附近按字母序）加入：

```python
import hmac
```

3b. `_is_auth_path`（server.py:118-127）加一行放行（保持其余行不动）：

```python
        or path.startswith("/api/agent/")     # 外部 agent 接口：端点内 API Key 自鉴权
```

3c. 在 `portfolio_daily`（约 server.py:2393-2401）之后、`holding_list` 之前插入端点：

```python
@app.get("/api/agent/portfolio")
async def agent_portfolio(request: Request):
    """外部 agent 持仓快照（X-API-Key 鉴权，读缓存零额外行情请求）。

    供外部 agent 做持仓鉴定/再平衡分析；sys_config.AGENT_API_KEY 未配置
    → 404（视为禁用，不暴露端点存在性）；key 不匹配 → 401（常量时间比较）。
    cny 与 overview 同逻辑：用最新看板 USDCNH + 本金配置实时重算，
    不受 600s 慢轮快照拖累。@author yys
    """
    try:
        api_key = await db.get_config("AGENT_API_KEY") or ""
    except Exception:
        api_key = ""
    if not api_key:
        return JSONResponse(status_code=404, content={"ok": False, "message": "Not Found"})
    provided = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(provided.encode(), api_key.encode()):
        return JSONResponse(status_code=401, content={"ok": False, "message": "无效的 API Key"})
    data = dict(portfolio_cache)
    usdcnh = next((m.get("price") for m in (market_cache or [])
                   if m.get("key") == "USDCNH" and m.get("price")), None)
    cny = pf.build_cny_summary((data.get("summary") or {}).get("total_value"),
                               usdcnh, _cny_cfg["invested"], _cny_cfg["expense"])
    return {
        "ok": True,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session": data.get("session"),
        "trade_date": data.get("trade_date"),
        "summary": data.get("summary") or {},
        "holdings": [
            {"symbol": p.get("symbol"), "name": p.get("name", ""),
             "market": p.get("market"), "currency": p.get("currency"),
             "quantity": p.get("quantity"), "cost_price": p.get("cost_price"),
             "price": p.get("price"), "value": p.get("value"),
             "today_pnl": p.get("today_pnl"),
             "position_pnl": p.get("position_pnl"),
             "position_pnl_pct": p.get("position_pnl_pct"),
             "cost_usd": p.get("cost_usd"), "stale": p.get("stale", False),
             "ext": p.get("ext")}
            for p in (data.get("positions") or [])],
        "cash": data.get("cash") or [],
        "fx": {"usdhkd": data.get("fx_usdhkd"), "usdcnh": usdcnh},
        "cny": cny,
    }
```

- [ ] **Step 4: 跑新增测试确认通过**

```bash
python3 -m pytest tests/test_server_auth.py::TestAgentPortfolioApi -v
```
预期：4 个用例全 PASS。

- [ ] **Step 5: Commit**

```bash
git add server.py tests/test_server_auth.py
git commit -m "feat(api): 外部agent持仓接口GET /api/agent/portfolio(X-API-Key鉴权,读缓存零额外请求)"
```

---

### Task 3: 接口文档

**Files:**
- Create: `docs/api-agent-portfolio.md`

- [ ] **Step 1: 写文档**

```markdown
# 外部 Agent 持仓接口

`GET /api/agent/portfolio` — 供外部 agent（持仓鉴定/再平衡分析）拉取当前
持仓、收益、现金、盈亏快照。数据与持仓页同源（读服务端缓存，零额外行情
请求），为只读接口。

## 鉴权

请求头携带静态 API Key：

```
X-API-Key: <AGENT_API_KEY>
```

- Key 存放于服务端 `sys_config` 表 `AGENT_API_KEY` 项；未配置时接口返回
  404（视为禁用）。
- Key 不匹配返回 401。更换 Key = 更新该配置项，即时生效（无缓存）。

配置方法（服务器 MySQL）：

```sql
INSERT INTO sys_config (cfg_key, cfg_value, remark)
VALUES ('AGENT_API_KEY', '<32位随机串>', '外部agent持仓接口密钥')
ON DUPLICATE KEY UPDATE cfg_value=VALUES(cfg_value);
```

## 请求示例

```bash
curl -H "X-API-Key: <key>" http://<your-domain>/api/agent/portfolio
```

## 响应结构

```json
{
  "ok": true,
  "generated_at": "2026-10-05 09:30:12",
  "session": "regular",
  "trade_date": "2026-10-02",
  "summary": {
    "total_value": 12345.67,
    "holdings_value": 11000.0,
    "cash_usd": 1345.67,
    "cost_basis": 9800.0,
    "day_pnl": 120.5,
    "day_pnl_pct": 0.98,
    "close_pnl": 80.0,
    "overnight_pnl": 40.5,
    "total_pnl": 2545.67,
    "total_pnl_pct": 26.0
  },
  "holdings": [
    {
      "symbol": "NVDA",
      "name": "英伟达",
      "market": "US",
      "currency": "USD",
      "quantity": 10.0,
      "cost_price": 90.0,
      "price": 105.0,
      "value": 1050.0,
      "today_pnl": 30.0,
      "position_pnl": 150.0,
      "position_pnl_pct": 16.67,
      "cost_usd": 900.0,
      "stale": false,
      "ext": "regular"
    }
  ],
  "cash": [{"currency": "USD", "amount": 1000.0}],
  "fx": {"usdhkd": 7.8, "usdcnh": 7.12},
  "cny": {
    "rate": 7.12, "invested": 14000.0, "invested_gross": 14000.0,
    "expense": 0.0, "value": 14596.0, "pnl": 596.0, "pnl_pct": 4.26
  }
}
```

## 字段说明

### 顶层

| 字段 | 类型 | 说明 |
|------|------|------|
| ok | bool | 恒 true（错误走 401/404/500 状态码） |
| generated_at | string | 响应生成时间（北京时间） |
| session | string | 美股时段：regular/pre/post/closed |
| trade_date | string | 一场口径交易日（凌晨归上一交易场） |
| summary | object | 组合汇总（USD） |
| holdings | array | 持仓明细 |
| cash | array | 现金账户（账户×币种原始金额） |
| fx | object | 汇率快照 |
| cny | object/null | 人民币累计口径（本金未配置/汇率缺失为 null） |

### summary（单位 USD）

| 字段 | 说明 |
|------|------|
| total_value | 总资产（= holdings_value + cash_usd） |
| holdings_value | 持仓市值（不含现金） |
| cash_usd | 现金折算 USD（HKD 按 USDHKD 折算） |
| cost_basis | 持仓成本 |
| day_pnl / day_pnl_pct | 当日盈亏（实时累计，含夜盘） |
| close_pnl | 收盘定格盈亏 |
| overnight_pnl | 夜盘增量盈亏（恒等式：day_pnl = close_pnl + overnight_pnl） |
| total_pnl / total_pnl_pct | 持仓总盈亏（现值−成本） |

### holdings[]

| 字段 | 说明 |
|------|------|
| symbol / name / market / currency | 标的代码/名称/市场（US、HK、HKFUND）/计价币种 |
| quantity | 数量/份额 |
| cost_price | 持仓成本价（原币种） |
| price | 最新价（原币种；stale 时为 0） |
| value | 市值（USD，港股/基金已折算） |
| today_pnl | 当日盈亏（USD） |
| position_pnl / position_pnl_pct | 持仓盈亏（USD / %） |
| cost_usd | 持仓成本（USD） |
| stale | true = 行情缺失，value=0、盈亏为 null |
| ext | 价格时段：regular/pre/post/none |

### fx / cny

- `fx.usdhkd`：USD/HKD（静态折算 7.5-8.5 区间校验，异常回退兜底值）
- `fx.usdcnh`：离岸 USD/CNH 最新快照（看板缓存，可能为 null）
- `cny`：净投入口径累计盈亏——`value = total_value × usdcnh`，
  `pnl = value − (invested_gross − expense)`；本金来自 sys_config
  （cny_invested / expense_cny）

## 关联配置

| sys_config 键 | 说明 |
|---------------|------|
| AGENT_API_KEY | 本接口鉴权密钥（未配置=接口禁用） |
| ALERT_MIN_MARKET_VALUE | 提醒引擎市值阈值（USD，默认 500，0=不过滤）；与本接口无关，供 agent 顺带了解提醒口径 |
```

- [ ] **Step 2: Commit**

```bash
git add docs/api-agent-portfolio.md
git commit -m "docs: 外部agent持仓接口文档(鉴权/示例/字段口径)"
```

> 注意：写文档前先在服务器上执行 `mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e "SHOW CREATE TABLE sys_config\G"` 核对 sys_config 的真实列名（cfg_key/cfg_value 还是 key/value），文档中的 INSERT 语句必须与真实 DDL 一致；如列名不同，按真实列名修正文档后再提交。

---

### Task 4: 基线回归

- [ ] **Step 1: 全量基线测试**

```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py -q
```
预期：全部 PASS，无回退（基线 238 passed + 本次新增用例）。

- [ ] **Step 2: 语法自检**

```bash
python3 -m py_compile server.py alert_engine.py && python3 -c "import server" 2>&1 | head -5
```
预期：无输出（import server 因缺 DB 连接可能挂起/报连接错误均可接受，只要不是语法错误——若挂起 3 秒以上 Ctrl-C 即可）。

---

### Task 5: 部署与线上配置（主 Agent 执行，不委派）

- [ ] rsync 发布 + `systemctl restart qdii`（按 AGENTS.md 部署命令）
- [ ] 线上 MySQL 写配置：
  - `AGENT_API_KEY` = `python3 -c "import secrets; print(secrets.token_hex(16))"` 生成
  - `ALERT_MIN_MARKET_VALUE` = `500`
- [ ] 线上验证：无 key 404 / 错 key 401 / 对 key 200 且 `ok:true`、holdings 非空；`systemctl is-active qdii`；`journalctl -u qdii --since '2 min ago'` 无新增异常
