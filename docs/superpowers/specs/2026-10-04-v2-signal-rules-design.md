# V2 双规则接入提醒引擎（QQQ 止盈 + 深跌回投）设计

日期：2026-10-04 · 状态：已批准（用户裁定：仅 QQQ / CSV 历史 + 长桥日增量 / 实时求值）

## 1. 背景与依据

《QQQ 定投 × VIX 止盈策略回测报告》（repo 根目录 `QQQ定投VIX止盈回测报告.html`）回测实证（2007-08 ~ 2026-09，1000 周根）：

- **V2 采用**：止盈（浮盈≥20% 且 VIX 处 2 年 20% 低分位，双条件缺一不可）+ 深跌回投（距 52 周高 -15%）→ 终值/XIRR/回撤三项全面占优（19 年仅 5 次止盈、4 笔大额回投全落底部区域）
- S1 下跌加倍 / S2 纯 dip 池 / V1 VIX 高位加投 / T 系列全部被数据否定，不接入
- 风险披露：结论仅回测 QQQ，不外推 → **两条规则硬编码仅 QQQ 生效**

现有提醒引擎（alert_engine.py）四类规则：price_plan / atr_stop / trend_filter / extreme_guard。本次新增两类规则键，沿用同一求值/冷却/落库架构。

## 2. 数据层

### 2.1 VIX 日K一次性导入（种子）

- 来源：`/root/kline_csv/VIX_US_day_10y.csv`（服务器）或 `/tmp/opencode/backtest/kline_csv/VIX_US_day_10y.csv`（本地），格式 `date,close` 两列，2016-10-03 ~ 2026-10-02 共 2516 根
- 落表：复用 `daily_bars` 表，symbol=`VIX`，open/high/low=close 填充、volume=0（分位只看 close；指标 ma50/rsi 等对 VIX 无意义但无害）
- 载体：`vix_seed.py` CLI（ledger_seed 同款幂等模式，INSERT IGNORE，可安全重放）；路径 fallback 服务器路径→本地路径；**CSV 文件不进 repo**
- database.py 连的即线上库（***REMOVED***:3914），本地跑一次即完成线上导入；部署后服务器也可重放修数

### 2.2 VIX 日增量（长桥）

- `daily_refresh_loop` 组 symbol 列表时固定追加 `VIX`（不依赖持仓），lp_symbol→`VIX.US`（用户回测已验证长桥可拉 VIX 日K）
- 额度：VIX 计 1 只/月（14/100），可接受
- 增量失败容错：VIX 分位退化 None → 止盈不触发（报告同款 NaN 语义），服务不炸

### 2.3 52 周高

QQQ 现有 daily_bars 1000 根（≈4 年）现算，无需新数据。

## 3. 指标层（daily_bars.py）

### 3.1 compute_indicators 扩展

- 新增 `week52_high`：近 252 个交易日（含当日）收盘价最高值；bars < 252 根 → None（deep_dip 不触发）
- 进 INDICATORS 缓存随每日刷新重算，无额外请求

### 3.2 VIX 分位缓存（模块级）

- `VIX_PCTL` + `VIX_ASOF`：当日 VIX 收盘在近 504 个交易日（含当日，2 年）收盘中的百分位（≤p% 的占比×100）；refresh_symbol('VIX') 成功后重算
- **有效性判定**：`VIX_ASOF < QQQ 最新日K日期` → get_vix_percentile() 返回 None（两者同为长桥增量，QQQ 更新而 VIX 未更新说明 VIX 拉取失败；两者同步滞后如周末则判定有效）
- 504 根不足时用现有根数算（CSV 有 2516 根，常态不会缺）

## 4. 规则层（alert_engine.py）

### 4.1 DEFAULT_PARAMS 新增两键

```python
"vix_take_profit": {"enabled": True, "profit_pct": 20.0, "vix_pctile_max": 20.0},
"deep_dip":        {"enabled": True, "dip_pct": 15.0},
```

### 4.2 eval_vix_take_profit（warning 级）

- 门禁：`symbol != "QQQ"` 直接返回（报告结论不外推，注释说明）
- 条件：浮盈 `price/cost - 1`（cost 来自既有 `_costs` 缓存）≥ profit_pct **且** `daily_bars.get_vix_percentile()` ≤ vix_pctile_max；VIX 分位 None 或 cost 缺失 → 不触发
- 事件：title「QQQ 浮盈达标且 VIX 处低分位，建议止盈」，body 含 profit/vix_pctile/cost/price
- 状态机：触发后 fired + 冷却 COOLDOWN_SECONDS（默认 4h）；浮盈回落到 `profit_pct - 0.5` 以下 → 清 fired（离开带重置，模仿 price_plan RESET_BAND 按百分点）

### 4.3 eval_deep_dip（info 级）

- 门禁：同上仅 QQQ
- 条件：`price/week52_high - 1 ≤ -dip_pct`（week52_high 来自 ind，None 不触发）
- 事件：title「QQQ 距 52 周高回撤达标，深跌回投窗口」，body 含 drawdown/week52_high/price
- 状态机：冷却同上；回撤收窄到 `-(dip_pct - 0.5)` 以内 → 清 fired
- 与 extreme_guard 独立并存（口径不同：52 周高 vs RSI/乖离），同轮可同时触发属设计现状（与 BABA 矛盾提醒同裁定）

### 4.4 接线

- `evaluate()` 追加两次 eval 调用；server `_RULE_KEYS` 白名单加两键（规则保存校验放行）；alert_backtest 不扩展（报告即回测结论，YAGNI）

## 5. 节奏与展示

- 挂现有实时求值（轮询/push 驱动）：price 实时，VIX 分位/52 周高为日级缓存——日级数据不引入前视；人工确认执行，比报告周收盘确认更及时
- 前端提醒中心（static/portfolio.html）：规则配置卡两张（enabled/profit_pct/vix_pctile_max/dip_pct 可调）+ alerts 列表 rule 名称中文映射两条
- mailer 聚合通用，无需改动

## 6. 测试

- tests/test_daily_bars.py：week52_high（含 <252 根 None）、VIX 分位计算（已知序列精确断言）、CSV 解析→rows（date,close 两列容错）
- tests/test_alert_engine.py：两条规则触发/双条件缺一不可/VIX None 不触发/冷却/离开带重置/非 QQQ 门禁
- tests/test_alert_api.py：_RULE_KEYS 放行新键（保存合法配置 200、未知键 400 回归）
- 前端无自动化，Task 部署后人工/线上验收

## 7. 验收（部署后）

- 基线全绿（337+新增）；CSV 导入后 `SELECT COUNT(*) FROM daily_bars WHERE symbol='VIX'` = 2516
- 长桥 VIX.US 日增量在 journalctl 出现且成功（首次真实拉取，若 401/权限失败 → 分位退化不触发，记录待查）
- kline 端点回归四周期正常；`/api/alert/rules` 含两新键默认值；线上 QQQ evaluate 无异常日志
