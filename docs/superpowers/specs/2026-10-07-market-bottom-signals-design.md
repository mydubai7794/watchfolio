# 市场底部信号：个股技术面预警 + 宏观估值看板 设计文档

日期：2026-10-07
状态：已批准（用户确认「全都要」，DCF 因子明确去除）
作者：yys

## 1. 背景与目标

用户提出一组「抄底/估值」信号（巴菲特指标、席勒 CAPE、美债收益率曲线、
200 日均线乖离、恐慌贪婪指数、斐波那契回撤、DCF 估值、触碰 200 均线），
要求并入现有系统。经澄清：

- **DCF 因子取消**（合理价需人工假设，用户明确不要）；
- 个股技术面（200 均线乖离 / 触碰年线 / 斐波那契回撤）进**现有预警引擎 + K线画线**；
- 宏观四指标（巴菲特 / CAPE / 收益率曲线利差 / 恐慌贪婪）做**纯看板展示**
  （不进 alerts 铃声流，不接预警）。

分期：一期个股侧（零新增请求），二期宏观侧（日频 ~5 请求/天）。

## 2. 现状锚点（实现前必读）

| 事实 | 位置 |
|------|------|
| 指标缓存 INDICATORS: {symbol: {ma50, ma200, rsi14, atr22, hhv22, stop_level, week52_high, enough_history, asof, last_close}} | daily_bars.py:24, compute_indicators |
| 规则签名统一 `(symbol, price, params, state, ind) -> list[event]`，冷却状态机 `_cooldown_ok/_set_cooldown`（具名键 main），重置带 RESET_BAND | alert_engine.py |
| 现有 extreme_guard 的 bias 是 **MA50** 口径（alert_engine.py:148），MA200 乖离是全新信号，无重复 | alert_engine.py:148 |
| 规则白名单 `_RULE_KEYS` + 数值校验 `_NUMERIC_PARAM_SPECS`，PUT /api/alert/rules/{symbol} 入库前拦截坏包 | server.py:2096-2127 |
| `/api/kline/{symbol}`：tf=day 带 indicators 单值快照；payload 有 rsi/macd/kdj 序列；截尾逻辑 keep<n 时同步截序列 | server.py:2217-2275 |
| 前端 klDrawMain 的 `lines` 数组画横线（成本/档位/止损/22日高，虚线+右侧标签），均线折线需另画 polyline | static/portfolio.html:2105 |
| 市场看板 renderMarket 从 /api/portfolio/overview 的 data.market 渲染 mkt-card + drawSpark | static/portfolio.html:623 |
| 后台任务注册处（create_task 列表） | server.py:163-199 |
| 每日循环错峰先例：ledger_fx 22:35 | server.py ledger_fx_loop |

## 3. 一期：个股技术面（预警 + K线）

### 3.1 daily_bars.py 指标扩展

`compute_indicators(bars)` 返回 dict 新增两项（历史不足为 None，
零新增请求，数据全部来自库内日K）：

```
"ma200_bias": (last_close - ma200) / ma200 * 100.0   # ma200 None 时 None
"fib": {
    "high": H,          # 252 日窗口内最高 high（含当日）
    "low": L,           # 252 日窗口内最低 low，且要求 L 的日期在 H 之前（先涨后跌才叫回撤）
    "depth": (H - last_close) / (H - L),   # 当前回撤深度 0~1+；L 日期晚于 H（下跌延续段）时整个 fib 置 None
    "levels": {"382": H-0.382*(H-L), "500": H-0.5*(H-L), "618": H-0.618*(H-L)},
}
```

- 新常量 `FIB_WINDOW = 252`（与 WEEK52_WINDOW 同源口径，两者独立常量互不影响）。
- 窗口不足 252 根时按现有 week52_high 惯例：用现有长度窗口（`closes[-252:]` 切片自然短），
  但 depth 仅在 `enough_history`（≥200 根）时有意义——fib 计算不额外设门槛，由下游规则用 enough_history 门控。
- H/L 用 high/low 序列（不是 close），L 的日期索引必须 < H 的日期索引，否则 fib=None（下跌波段不适用回撤口径）。

### 3.2 alert_engine.py 新增 2 类规则

进 `DEFAULT_PARAMS` 白名单：

```python
"ma200_zone": {"enabled": True, "bias_low": -10.0, "band_pct": 2.0},
"fib_retrace": {"enabled": True, "depth": 0.5},
```

**eval_ma200_zone(symbol, price, params, state, ind)**，两个独立具名冷却键：

- 门控：`enabled`、`ind` 存在、`ind.enough_history`、`ind.ma200` 非 None（短历史静默禁用，同 atr_stop 惯例）。
- 子信号 `bias`（深度乖离，warning）：`bias = (price - ma200)/ma200*100 ≤ bias_low` →
  「{symbol} 现价较200日线乖离 {bias:+.1f}%，历史级低估区」；body 带 bias_pct/ma200/price。
  离开带重置：`bias > bias_low + 0.5`（百分点边际，参照 deep_dip 的 0.5 band 模式）。
- 子信号 `touch`（贴近年线，info）：`|bias| ≤ band_pct` →
  「{symbol} 贴近200日线（乖离 {bias:+.1f}%），长线支撑测试」。
  离开带重置：`|bias| > band_pct + 0.5`。
- 冷却：两个子信号各自 `_set_cooldown(st, "bias"/"touch", now, COOLDOWN_SECONDS)`。

**eval_fib_retrace(symbol, price, params, state, ind)**：

- 门控：`enabled`、`ind`、`ind.enough_history`、`ind.fib` 非 None。
- 触发：`fib.depth ≥ depth` → info「{symbol} 本轮涨幅已回撤 {depth*100:.0f}%（斐波那契 0.5 位下方）」；
  body 带 depth/high/low/levels 三档价/price。
- 离开带重置：`depth < depth_cfg - 0.05`。
- 冷却键 `main`。

`evaluate()` 中追加两行调用（与现有五规则并列）。

### 3.3 server.py 参数校验扩展

- `_RULE_KEYS = tuple(alert_engine.DEFAULT_PARAMS)` 自动派生（server.py:2081），
  DEFAULT_PARAMS 加新键即自动进白名单，**无需手改**。
- `bias_low` 已有全局规格 `(None, None, False)`（extreme_guard 在用，server.py:2092），
  ma200_zone 复用同键名，**规格不动**。
- `_NUMERIC_PARAM_SPECS` 仅新增两个键：
  - `band_pct`: (0, 20, True)
  - `depth`: (0.3, 0.9, False)（下界含 0.3）

### 3.4 /api/kline/{symbol} 增强（仅 tf=day）

payload 新增 `overlays`（缓存内全量、随截尾同步切片）：

```json
"overlays": {
  "ma50_series": [...],     // 与 bars 等长，None 占位（前 49 根）
  "ma200_series": [...],
  "fib": {"high": H, "low": L, "levels": {...}, "depth": d}   // 横线价位，全量算一次不受截尾影响
}
```

- 序列用现有 `sma()` + `_round_series(., 2)` 生成；非 day 周期 overlays 为空 dict `{}`。
- 截尾分支（keep<n）同步 `out["overlays"] = {"ma50_series": ...[cut:], "ma200_series": ...[cut:], "fib": 原}`。

### 3.5 前端 kline 画线（static/portfolio.html）

- `klDrawMain`：蜡烛画完后，若 `d.overlays` 存在且 tf=day：
  - ma50_series / ma200_series 以 polyline 叠加（颜色 MA50=#E6A23C 已被成本线占用 → MA50 用 #9B59B6 紫、MA200 用 #2A6CF0 蓝，线宽 1.2，None 断线跳段，复用 klDrawInd 的 line 闭包模式）；
  - fib 三档价位追加进现有 `lines` 数组（标签 `Fib38.2/Fib50/Fib61.8`，颜色 #8E9ABF 统一浅灰蓝虚线）；
  - fib 的 high/low 不强制纳入 y 轴范围（避免压缩主图），仅 levels 三档线参与 lines 的 min/max 收缩（现有 lines.forEach 已做）。
- `klInfoLine`：day 周期追加「Fib回撤 {depth*100|0}%」一项（depth 非 None 时）。
- 均线图例：主图左上角小字 `MA50(紫) MA200(蓝)`，仅 day 显示。

## 4. 二期：宏观估值看板

### 4.1 macro_fetcher.py（新文件）

四个指标的日频抓取与解析，全部直连优先、失败走代理池（复用现有 proxy helper），
再失败当日放弃（次日再试），**沿用上次落库值展示，不放大请求**。

| 指标 name | 源与解析 | 频率 |
|-----------|---------|------|
| `buffett` | FRED fredgraph.csv：`WILL5000PRFC`（周频）÷ `GDP`（季频年化）×100%；CSV 两个请求，取各自最后一个非 `.` 值 | 日 |
| `cape` | multpl.com `/shiller-pe/table-json`（数组最近在前，取[0]）；失败回退解析 HTML 表格首行 | 日 |
| `yield_2s10s` | FRED `DGS10` − `DGS2`（两个 CSV，各自最新值相减，单位 pct） | 日 |
| `fear_greed` | CNN dataviz graphdata JSON（须浏览器 UA），取 `fear_greed.score`（0-100）与 `fear_greed.rating`；**失败回退自建合成**：`100 − daily_bars.VIX 分位`（get_vix_percentile()，None 则放弃），extra 标注 `source: "fallback"` | 日 |

- 请求头 UA 参照现有 fetcher 的浏览器伪装惯例；超时 15s。
- FRED CSV 空值行（`.`）跳过；数值 float 解析失败跳行。
- 函数命名：`fetch_buffett() / fetch_cape() / fetch_yield_spread() / fetch_fear_greed()`，
  各返回 `{name, asof: "YYYY-MM-DD", value: float, extra: dict}` 或 None。

### 4.2 database.py 新表 macro_indicators

```sql
CREATE TABLE IF NOT EXISTS macro_indicators (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(32) NOT NULL,
  asof DATE NOT NULL,
  value DOUBLE NOT NULL,
  extra TEXT,                -- JSON：cape 的 rating、fear_greed 的 source/rating、buffett 的分子分母等
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_name_asof (name, asof)
)
```

- CRUD：`upsert_macro_indicator(name, asof, value, extra)`（REPLACE 语义幂等）、
  `get_macro_latest()`（四项最新一行）、`get_macro_history(name, days=365)`。
- 建表挂进现有 init（与 init_alert_tables 同模式）。

### 4.3 server.py 后台循环 + API

- `macro_daily_loop()`：启动即跑一次，此后每日北京时间 **22:40**（与 ledger_fx 22:35 错峰）。
  四指标串行、间隔 2s；单项失败 log warning 不中断。
- 注册进 startup 的 create_task 列表（server.py:163-199 区域）。
- `GET /api/macro`（session 鉴权同其它端点）：返回
  `{ok, indicators: [{name, value, asof, extra, history: [[ts, value], ...]}], status: {name: "cheap|neutral|expensive|risk"}}`。
  status 由后端按阈值判（前端只上色不判逻辑）：

| name | cheap（绿） | neutral（灰） | expensive/risk（红） |
|------|------------|---------------|---------------------|
| buffett | <80 | 80~120 | >120 |
| cape | <15 | 15~25 | >25 |
| yield_2s10s | >1 | 0~1 | <0（倒挂=风险） |
| fear_greed | <25（极度恐慌=机会） | 25~55 | >55（贪婪） |

注：卡片副标题写明语义（如「<25 极度恐慌（机会视角）」），避免红绿直觉混淆。

### 4.4 前端「市场估值」区块

- 位置：市场看板 card 之后新起一个 card（title「市场估值」），内部 2×2 网格四张迷你卡。
- 每卡：中文名+英文小字、当前值（fear_greed 整数、yield 2 位、buffett 整数+%、cape 1 位）、
  状态色圆点（cheap=绿 #00A870 / neutral=灰 / expensive=红 #E5484D）、asof 日期、
  近一年 spark（复用 drawSpark 纯价格序列模式，无时间戳只显价格）。
- 数据源 `/api/macro`，随市场看板同一刷新节奏拉取（前端 60s 轮询已有处追加或并入 overview 刷新函数均可，取改动最小者）。
- 四项任一 history 为空时该卡显示「--」占位不报错。

## 5. 测试计划

- `tests/test_alert_engine.py` 追加：
  - eval_ma200_zone：bias 触发/冷却期内不重复/离开带重置/touch 与 bias 独立冷却/短历史(enough_history=False)禁用/参数覆盖 bias_low；
  - eval_fib_retrace：depth≥0.5 触发/重置带/fib=None（L 晚于 H 的下跌段）禁用/enough_history 门控；
  - _merged_params 合并新默认键（防漏注册）。
- `daily_bars` fib 纯函数用例：上涨后回撤段 depth 正确、下跌延续段 None、levels 三档价。
- kline overlays 用例：day 带 ma 序列且与 bars 等长、range 截尾后同步切片、非 day 为空 dict。
- macro_fetcher 解析用例（fixture 字符串，不发网络）：FRED CSV 含 `.` 空行、multpl JSON、CNN JSON 字段路径、VIX 回退路径。
- `_validate_rule_params` 新键校验用例（越界/非数值拒绝）。
- 基线命令：`python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py tests/test_alert_api.py`（新增用例并入对应文件，全绿为准）。

## 6. 请求量与风控评估

- 一期：0 新增外部请求（全部库内日K推导）。
- 二期：5 个 HTTP/天（FRED×3 CSV + multpl×1 + CNN×1），失败当日放弃不重试放大。
- 命名纪律：FRED/multpl/CNN 为本系统新增数据源名称（进数据源矩阵），无违禁词。
- macro 表日增 ≤4 行/天，一年 ~1.5K 行，无需清理策略。

## 7. 明确不做

- DCF 估值因子（用户取消）。
- 宏观指标接 alerts 预警流（用户选择纯看板）。
- 斐波那契自动 zigzag 波段识别（用固定 252 日窗口 + 先低后高结构判定，保持可解释）。
