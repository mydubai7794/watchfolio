# 组合净值（TWR）模块设计

- 日期：2026-10-09
- 状态：已确认（brainstorming 产出）
- 范围：剔除入金影响的「实际股票增值收益率」+ 净值走势曲线

## 1. 背景与问题

用户入金 ¥20,000（≈$3,000）后，两处展示失真：

| 展示位 | 现口径 | 失真机理 |
|--------|--------|---------|
| CNY 卡片「人民币累计」 | `(总资产×USDCNH − 净投入)/净投入`（server.py build_cny_summary 调用处） | `sys_config.cny_invested` 未同步时折算总值 +¥21,000 直接计为盈亏，pnl_pct 虚抬约 24pct；即使补记账，简单收益率分母突变、历史不可比 |
| 走势曲线（30/90/all） | `portfolio_ticks.total_value_usd`（含现金） | 入金令曲线台阶式抬升 $3,000，视觉等同「暴涨」 |

根因：简单收益率/绝对市值口径无法区分「本金流入」与「投资增值」。顶部
`total_pnl_pct`（持仓/成本口径）不受入金影响，本次不动。

## 2. 目标

1. 新增 **TWR（时间加权收益率，USD 口径）**：入金按当时净值折算份额、不虚增
   收益，反映纯投资增值能力（不含汇率波动）。
2. 走势图新增 **净值曲线**（市值/净值双模式并列，原口径保留）。
3. 以 ledger 事件为唯一真相源**全历史重放**：零状态、零新表；补录/勘误事件后
   重算即自动修正。

## 3. 非目标

- 不改 `cny_invested` 手动维护机制（TWR 口径免疫此问题）。
- 不做 CNY 口径 TWR（含汇损版本）——如需后续可在净值 USD 序列上叠汇率因子。
- 不写缓存/新表（重放为内存级运算，数据量：日线数百点 + 事件约 70 笔）。
- portfolio 循环写路径零改动（不感知 ledger，维持模块隔离）。

## 4. 口径与算法

### 4.1 单位净值法（事件流重放）

- 基期：ledger 首笔 `inflow` 日（当前为 2026-06-26）。基期 NAV=1.0；
  份额₀ = 基期现金流前最近 tick 的 `total_value_usd` + 基期当日入金
  `usd_equivalent`（基期前遗留资产视同期初投入，曲线无断层）。
- 每个日线点：`NAV_t = total_value_t / shares_t`。
- 重放规则：daily ticks 与现金流按日期合并；同日**先按昨日 NAV 调份额、再取
  当日末 NAV**（现金流发生前估值，日线粒度近似，误差极小）。

### 4.2 现金流映射（金额一律用事件 `usd_equivalent`）

| 事件类型 | 现金流 | 说明 |
|---------|--------|------|
| `inflow` | `+usd_equivalent` | 份额 += 金额/当时 NAV——入金不再虚增收益 |
| `transfer` | `−usd_equivalent` | 转出组合 |
| `expense` | `−usd_equivalent` | 消费赎回 |
| `fee` | `−usd_equivalent` | 通道费独立扣减，保守计流出 |
| `fx` | 不计 | 组合内部币种结构变化；inflow 已是换汇后到账值 |
| `reward` | 不计 | 无本金来源的资产增加 → 推高 NAV（视作收益） |
| `realized`/`balance` | 不计 | 备忘/对账类 |

### 4.3 输出

`{series: [{date, nav}], twr_pct, ann_pct, days}`：
- `twr_pct = (最新 NAV − 1) × 100`
- `ann_pct = ((1+TWR)^(365/days) − 1) × 100`；`days < 30` 时置 None（防噪音放大）
- 净值保留 4 位小数。

## 5. 后端设计

- **纯函数** `build_nav_series(daily_rows, events) -> dict`：放 `ledger.py`
  （与 `build_report` 同文件同风格）。输入：`get_portfolio_daily(3650)` +
  `get_ledger_events()`，均为既有 CRUD。
- **新 API** `GET /api/portfolio/nav?range=30/90/all`（server.py，与
  `/api/portfolio/daily` 平级，session 鉴权）：返回 4.3 结构 + `ok`。
- **overview 扩展**：`/api/portfolio/overview` 的 `cny` 块新增 `twr_pct`
  （实时 NAV = 当前含现金总资产 / 重放至今份额）；`cny` 整体降级时 TWR 独立
  降级（前端 `--`）。

## 6. 前端设计（static/portfolio.html）

- **CNY 卡片**：副行尾部追加 ` · TWR ±x.xx%`（沿用涨跌色）；title tooltip
  追加「年化 ±x.xx% · 时间加权(剔除入金)」。
- **走势图**：区间切换旁加「市值 | 净值」模式切换；净值模式画 NAV 曲线
  （1.00 基准虚线），悬浮显示日期 + 净值；沿用现有图表实现，无构建。

## 7. 测试计划

单测（仿 test_ledger.py 纯函数风格，新文件 tests/test_nav.py 或并入
test_ledger.py）：

1. 入金日前后 NAV 连续（核心：入金不虚增收益）
2. 出金/消费按当时 NAV 减份额
3. reward 不计现金流 → 推高 NAV
4. fee 计负现金流
5. 基期前遗留资产并入期初份额
6. days<30 年化为 None
7. 无现金流时 NAV 随市值等比
8. 空 events / 空 ticks 边界降级

API 测试（仿 test_server_auth.py）：`/api/portfolio/nav` 未授权 401；授权后
200 且结构完整（series/twr_pct/ann_pct/days）。

回归：现有基线 400 passed 不破。

## 8. 验收标准

1. 入金 ¥20,000 当日净值曲线无台阶跳变，TWR 不因入金抬升。
2. CNY 卡片同时展示简单口径（原样）与 TWR，两者口径标注清晰。
3. 走势图「市值/净值」双模式可切换，原市值模式行为不变。
4. ledger 补录历史入金事件后，净值曲线与 TWR 重算自动修正。

## 9. 边界与风险

- **现金流完整性前提**：TWR 正确性依赖 ledger 入金/出金/消费事件全量记录
  （用户已确认习惯满足）。漏记入金 → 净值虚高；可在记账页对账位人工互验。
- **日线粒度近似**：同日多笔现金流按昨日 NAV 折算份额，日内波动误差远小于
  1pct 量级，可接受；如未来需要精确，可切换 ticks 原始粒度重放（接口不变）。
- **基期前资产**：视同期初投入处理， NAV 序列自基期起连续无断层。
