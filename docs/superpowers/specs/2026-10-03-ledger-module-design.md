# 记账模块（Ledger）设计

**日期**：2026-10-03
**状态**：待用户审阅
**数字基线**：`账户成本盈亏盘点_20261003.md`（v3，全部数字可由 8 份结单 + HSBC 邮件 + ECB 参考价 + 腾讯日线独立复算）

## 1. 背景与目标

v3 盘点已人工闭环全部资金链（11 笔入金、点差 43.32 USD、净盈亏 +7,281.54 CNY 恒等式分毫不差）。本设计把该口径产品化：

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
  event_type VARCHAR(20) NOT NULL,  -- inflow/fx/fee/reward/expense/transfer/realized
  account VARCHAR(20) NOT NULL,     -- ccb/cmb/cib/za_bank/za_sec/hsbc/zf/external
  channel VARCHAR(40),              -- 建行购汇汇出/汇丰柜台/众安App换U/兴业SWIFT/招行原币/致富/支付宝
  direction CHAR(1) NOT NULL,       -- '+'/'-'
  amount_ccy VARCHAR(5) NOT NULL,   -- HKD/USD/CNY
  amount DECIMAL(14,2) NOT NULL,    -- 事件币种金额
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

## 4. 计算口径（逐条映射 v3 文档）

| 指标 | 公式 | v3 基线值 |
|---|---|---|
| 总投入 | Σ inflow.cny_amount | 84,270.00 |
| USD 化入账 | Σ inflow.usd_equivalent | 12,478.33 |
| 加权购汇成本 | 总投入 ÷ USD 化入账 | 6.7533 |
| 单笔点差 | (mid_rate 理论应得 − 实际得到) ÷ 理论应得 | 11+6 笔明细 |
| 点差合计 | Σ（负点差=收益，如同事代换 −1.80%） | 43.32 USD |
| 通道排名 | fx 事件 GROUP BY channel，按量加权平均点差 | 汇丰柜台 0.41~0.43% 最贵 |
| 显性费用 | Σ fee.usd_equivalent | 13.14 USD |
| 奖励合计 | Σ reward | ≈161.01 USD |
| 已实现盈亏 | Σ realized.usd_equivalent | −36.68 USD |
| 消耗 | Σ expense.usd_equivalent（电费） | 36.73 USD |
| 保活转出 | transfer 至 external 的面值回流 | 17.76 HKD |
| 汇损 | Σ inflow.usd_equivalent × (当前 USDCNH − 均成本) | −589.05（@6.7061） |
| 增值 | (当前总资产 + 保活转出面值) − (USD 化入账 − 消耗) | +1,210.38 USD |
| **恒等式** | 增值×当前汇率 − |汇损| − 消耗×当前汇率 = 净盈亏 | **+7,281.54 ±0.5 CNY** |

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

- v3 全部事件一次性入库约 55 笔（11 inflow + 17 fx[购汇10+同事代换1+账户内6] + ~7 fee + ~13 reward + 1 expense + 3 transfer + 6 realized，明细以 v3 文档第二~七节为准）。
- 新增 `tests/test_ledger.py`：报表断言必须精确等于 v3 基线（§4 右列全部数字；恒等式容差 ±0.5 CNY 为汇率舍入余量）。
- 基线 198 → 预计 ~210。
- 部署后线上验证：`GET /api/ledger/report` 与 v3 文档逐项比对。

## 8. 修数四项（随本模块一并执行，已拍板）

| 对象 | 现值 | 改为 | 依据 |
|---|---|---|---|
| cash_accounts HKD | 70.48 | 32.52 | 众安 8/9 月流水闭环 |
| cash_accounts USD | 371.11 | 371.79 | 用户口述实数（145.80+225.99） |
| holdings NVDA cost_price | 222.4038 | 213.98 | 赠股按 0 成本：(229.80+51.97+0)/1.3169 |
| sys_config cny_invested | 84560 | 84270 | 口径A（电费为 expense 非投入）；修后须等于 Σ inflow.cny_amount（纳入测试断言） |

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
