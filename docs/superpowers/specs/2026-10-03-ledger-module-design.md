# 记账模块（Ledger）设计

**日期**：2026-10-03
**状态**：待用户审阅
**数字基线**：本地私有基线文档（v3 盘点，不入库；发布版脱敏，真实金额/渠道明细不出现于本仓库）

## 1. 背景与目标

v3 盘点已人工闭环全部资金链（多笔入金、点差、净盈亏恒等式分毫不差，具体基线值见本地私有文档，发布版脱敏）。本设计把该口径产品化：

1. 逐笔资金事件入库，系统实时计算点差、通道成本排名、盈亏恒等式；
2. 入金邮件自动入账（复用 email_trader 现有 30min IMAP 轮询，零新增轮询）；
3. 月结单解析→入库流程沉淀为可复用 skill。

**不做的事**：不做前端手动录入表单（AI 经 API 是唯一录入通道）；不改 trade_records 语义；除 ECB 日线（每日 1 次）外不新增任何外部请求源。

## 2. 数据模型

### ledger_events（单表事件流）

```sql
CREATE TABLE IF NOT EXISTS ledger_events (
  id INT AUTO_INCREMENT PRIMARY KEY,
  event_date DATE NOT NULL,
  event_type VARCHAR(20) NOT NULL,  -- inflow/fx/fee/reward/expense/transfer/realized/balance
  account VARCHAR(20) NOT NULL,     -- ccb/cmb/cib/za_bank/za_sec/hsbc/zf/external
  channel VARCHAR(40),              -- 通道: 银行购汇/柜台/SWIFT电汇/券商换汇/支付渠道等（值域示例见 ledger_seed.example.py）
  direction CHAR(1) NOT NULL,       -- '+'/'-'
  amount_ccy VARCHAR(5) NOT NULL,   -- HKD/USD/CNY（付出币种）
  amount DECIMAL(14,2) NOT NULL,    -- 付出金额
  recv_ccy VARCHAR(5),              -- 换汇类：收到币种
  recv_amount DECIMAL(14,2),        -- 换汇类：实际得到金额
  usd_equivalent DECIMAL(14,2),     -- USD 等值（inflow/expense/transfer/realized 必填）
  cny_amount DECIMAL(14,2),         -- CNY 口径（inflow 必填；邮件自动入账可 NULL 待补）
  mid_rate DECIMAL(12,6),           -- 换汇类：ECB 当日中间价快照（对币种：HKD→usd_hkd，CNY→usd_cny）
  note VARCHAR(200),
  ref_no VARCHAR(40) NOT NULL UNIQUE,  -- 幂等键：结单流水号 或 EMAIL-<MessageID> 或 日期-账户-序号
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4;
```

- 点差不存派生列：报表 API 由 `mid_rate` + 两边金额实时推导（单一事实源）。
- `cny_amount IS NULL AND event_type='inflow'` 定义为 **pending 事件**（邮件自动入账但结单未补全），报表提示计数。

### ledger_fx_rates（ECB 日线缓存）

```sql
CREATE TABLE IF NOT EXISTS ledger_fx_rates (
  rate_date DATE NOT NULL UNIQUE,
  usd_cny DECIMAL(12,6) NOT NULL,
  usd_hkd DECIMAL(12,6) NOT NULL,
  fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4;
```

- 拉取：每日一次（北京时间约 22:35，ECB 定盘后），`api.frankfurter.dev` 直连→代理容错；失败次日顺延补拉（可回填历史区间），严禁短周期重试放大请求。
- 消费方：邮件自动入账取当日 mid_rate、月结单 enrich、报表历史点差复核。

## 3. API（复用现有 session 鉴权）

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/ledger/events` | POST | 批量 upsert（按 ref_no 幂等；同 ref_no 重复提交=覆盖更新），AI 唯一录入入口 |
| `/api/ledger/events` | GET | 明细查询：`type/account/year` 筛选 |
| `/api/ledger/report` | GET | 全部实时计算，见 §4 |

## 4. 计算口径（逐条映射 v3 文档；基线值已脱敏，实际值见本地私有文档）

| 指标 | 公式 | 基线（脱敏） |
|---|---|---|
| 总投入 | Σ inflow.cny_amount | <总投入基线> |
| USD 化入账 | Σ inflow.usd_equivalent | <USD入账基线> |
| 加权购汇成本 | 总投入 ÷ USD 化入账 | <均成本基线> |
| 单笔点差 | (mid_rate 理论应得 − 实际得到) ÷ 理论应得 | 全部 fx 笔明细 |
| 点差合计 | Σ（负点差=收益） | <点差基线> |
| 通道排名 | fx 事件 GROUP BY channel，按量加权平均点差 | 各通道点差率排序 |
| 显性费用 | Σ fee.usd_equivalent | <费用基线> |
| 奖励合计 | Σ reward | <奖励基线> |
| 已实现盈亏 | Σ realized.usd_equivalent | <已实现基线> |
| 消耗 | Σ expense.usd_equivalent | <消耗基线> |
| 保活转出 | transfer 至 external 的面值回流 | <转出基线> |
| 汇损 | Σ inflow.usd_equivalent × (当前 USDCNH − 均成本) | <汇损基线> |
| 增值 | (当前总资产 + 保活转出面值) − (USD 化入账 − 消耗) | <增值基线> |
| **恒等式** | 增值×当前汇率 − |汇损| − 消耗×当前汇率 = 净盈亏 | **<净盈亏基线> ±0.5 CNY** |

- 「当前总资产」取现有 portfolio 汇总（持仓+现金）；「当前 USDCNH/USDHKD」取系统现有行情快照（fx_spark/market_ticks），不新增请求。
- HKD→USD 折算统一 7.846（v3 静态口径）；转出面值按转出口径折算。

## 5. 入金邮件自动入账

- email_trader 现有 30min IMAP 轮询内新增一类解析：**入金/收款通知**（收款人 HSBC/众安侧到账邮件）→ 自动生成 `ledger_events(inflow)`，`ref_no = EMAIL-<MessageID>` 幂等。
- 自动补全：`mid_rate` 取 ledger_fx_rates 当日值；`usd_equivalent` 按 mid_rate 折算；`cny_amount` 邮件含原币金额则记，否则 NULL（pending，月结单补全）。
- **实现前置**：先采样邮箱中的入金邮件版式；若到账邮件不可识别/不存在，降级为「仅结单录入」，不阻塞主体功能。
- 已有成交邮件解析逻辑不动，仅新增邮件类型分支。

## 6. 前端（portfolio.html 新增「记账」tab）

1. 总览卡片：净盈亏 + 恒等式树状分解（增值/汇损/消耗）；
2. 通道成本排名表 + 点差明细表；
3. 事件明细表（type/月份筛选）；
4. 对账位：ledger 推算现金余额 vs `cash_accounts` 系统值差异高亮；pending 事件计数（下次 0.68 类差异自动暴露）。

## 7. 种子数据与验证

- v3 全部事件一次性入库（构成明细见本地私有基线文档第二~八节；发布版脱敏）。
- 新增 `tests/test_ledger.py`：报表断言必须精确等于 v3 基线（§4 右列全部数值；恒等式容差 ±1 CNY 为汇率舍入余量）。基线数值发布版已脱敏，见 tests/ 内实际断言。
- 基线 198（2026-10-03 实测，含 2 个时间敏感测试修复）→ 预计 ~216。
- 部署后线上验证：`GET /api/ledger/report` 与 v3 文档逐项比对。

## 8. 修数四项（随本模块一并执行，已拍板；数值已脱敏，见本地私有基线文档）

| 对象 | 改动 | 依据 |
|---|---|---|
| cash_accounts HKD | 改为**跨账户合并桶口径** | 多账户留存相加（原写单账户值会少记另一账户留存） |
| cash_accounts USD | 按用户口述实数修正 | 跨渠道实数核对 |
| holdings NVDA cost_price | 按赠股 0 成本折算修正 | 赠股成本口径（v3 原值有笔误） |
| sys_config cny_invested | 改为全额口径 | 口径A（消耗类为 expense 非投入）；修后须等于 Σ inflow.cny_amount（纳入测试断言） |

## 9. Skill 交付物

`~/.claude/skills/qdii-ledger-ingest/SKILL.md`（用 skill-creator 生成）：

- 触发词：处理月结单 / 录入结单 / ledger ingest；
- SOP：PDF → pymupdf 文本提取 → 各行版式解析规则表（ZA/HSBC securities/HSBC One/致富）→ 新事件识别（对比库内最后事件日期，防重复）→ POST `/api/ledger/events` → `GET /api/ledger/report` 回归核对（恒等式闭环 + 对账位差异报告）→ 输出差异清单待用户确认；
- 内置：ref_no 生成规则、pending 补全流程、ECB 补拉脚本调用方式。

## 10. 工作流（终态）

```
日常：入金邮件 ──30min──> 自动 inflow 事件（pending）
每月：结单 PDF ──skill──> 解析新事件 + 补全 pending ──POST──> 报表/恒等式/对账自动更新
```
