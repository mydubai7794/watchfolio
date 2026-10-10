---
name: ledger-ingest
description: Watchfolio 持仓网站记账模块（ledger）录入技能。当用户给出银行结单 PDF/App 截图文字/邮件通知/口述流水，要求录入记账、入账、补账、对账、核对余额、查点差或查投入成本时使用。触发词：记账、录入、入账、结单录入、ledger、对账、总投入、点差、通道成本。录入通道为服务器 POST /api/ledger/events（幂等），录入后自动跑 /api/ledger/report 对账并汇报差异。
---

# 记账录入（ledger ingest）

## 通道与鉴权

- API：`http://127.0.0.1:8888`（服务本机执行，ssh 过去 curl；服务器地址见你本地的 AGENTS.local.md）
- token：`ssh root@<server-ip> 'set -a; . /opt/qdii/qdii.env; set +a; mysql -h127.0.0.1 -P$QDII_DB_PORT -u$QDII_DB_USER -p"$QDII_DB_PASS" $QDII_DB_NAME -N -e "SELECT token FROM sessions ORDER BY expires DESC LIMIT 1"'`
- 请求带 cookie：`-b "session_token=$TOKEN"`

## 录入流程（每次必须完整执行）

1. **查重**：`GET /api/ledger/events?type=<类型>`，按 ref_no 判断是否已录（幂等 upsert，重复提交安全但不留垃圾）。
2. **构造事件**（字段口径见下），`POST /api/ledger/events`，body：`{"events":[...]}`。
3. **对账验证**：`GET /api/ledger/report`，检查：
   - `balances.diff` 各币种 ≈ 0（±1 容差；新结单出余额快照后必须重新提交 balance 事件）
   - `identity.identity_check_cny == identity.net_cny`（恒等式自洽）
4. **汇报**：把 summary 变化（总投入/加权成本/点差/净盈亏）与上一次的差异汇报给用户。

## 字段口径（错口径=错账，逐条遵守）

事件 8 类：`inflow` 入金、`fx` 换汇、`fee` 费用、`reward` 奖励/派息/利息、`expense` 消耗（生活缴费类）、`transfer` 转账、`realized` 已实现盈亏、`balance` 余额快照。

账户枚举：`za_bank` 众安银行、`za_sec` 众安证券、`hsbc` 汇丰、`zf` 致富、`ccb/cmb/cib` 建行/招行/兴业（购汇端）、`external` 外部。币种仅 `CNY/HKD/USD`。

- `event_date`：YYYY-MM-DD（资金落账日，非结单日）。
- `direction`：`+` 入账 / `-` 出账。realized 盈亏直接用正负金额 + 对应 direction。
- `fx` 双边：`amount_ccy/amount`=卖出币种与数量，`recv_ccy/recv_amount`=买入币种与数量；`mid_rate`=ECB 当日中间价（单位付币所得收币）。取值：`curl -s https://api.frankfurter.dev/v1/latest?base=CNY&symbols=USD,HKD`（或历史日期 `.../2026-10-05?...`；无当日取前一工作日）。**点差不手填**，报表从 mid_rate 推导。
- `usd_equivalent`：该笔 USD 计价。HKD→USD 固定 **7.846**；CNY→USD 用当日实价（`fx_spark` 或行情缓存 USDCNH）。
- `cny_amount`：仅 inflow 必填（人民币实际投入）。
- `ref_no` 幂等键，命名规则：
  - 结单：`STMT-<账户>-<YYYYMM>-<序号>`
  - 邮件实证：`ZA-<邮件内交易编号>`
  - 口述/截图：`USR-<YYYYMMDD>-<金额><币种>`
- `balance` 快照：结单上「账户×币种」期末余额，amount=余额值，direction=`+`，每期结单都提交（报表按账户×币种取最新）。
- `expense` 消耗事件**必须同步** sys_config：`UPDATE sys_config SET value=ROUND(value+<新增消耗折CNY>,2) WHERE \`key\`='expense_cny'`（持仓页「人民币累计」净投入口径依赖此值，两边由 ledger 页 identity.invest_net_cny 对账校验）。
- `channel`：换汇通道名（汇丰柜台/众安App换U/建行购汇汇出/招行原币+众安换U/兴业SWIFT/致富换汇），沿用既有命名，新通道先与用户确认。
- `note`：写证据来源（结单页码/邮件编号/口述），月结单冲销差异（回赠/利息）单独记 reward/fee 而非改旧账。

## 邮件采样对账（辅助）

服务器项目目录 `/opt/qdii` 有 `email_trader.fetch_za_notice_emails(days)`（ZA Bank 换汇/转出/派息通知）。发现新邮件版式时：核对邮件里交易编号→ref_no、金额→双边字段，再走录入流程。

## 红线

- 服务器调试：一律 ssh + journalctl + curl，禁止本地起服务（线上唯一实例，双实例放大外部请求触发风控）。
- 改口径前先跑 `python3 -m pytest tests/test_ledger.py`（本地可跑）。
- 金额精度：分币种保留两位小数；mid_rate 保留 6 位。
- 月度差异 >1% 或恒等式不闭合时，停止录入并输出诊断，等用户确认。
