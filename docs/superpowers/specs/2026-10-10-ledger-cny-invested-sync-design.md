# ledger 录入派生值同步（cny_invested 服务端化）设计

- 日期：2026-10-10
- 状态：已确认（brainstorming 产出）
- 范围：`POST /api/ledger/events` 入库后自动同步 `sys_config.cny_invested`，闭合「口述入账 → 全链路落账」手动录入通道

## 1. 背景与问题

用户提出「众安入金/换汇邮件自动入账」需求，评估后**否决**，转向「统一手动
录入 + 流程化补全」：

| 否决理由 | 说明 |
|---------|------|
| 覆盖率天花板低 | 汇丰入金走短信（IMAP 拿不到）；兴业→汇丰、交行→众安等链路付款侧邮件均不在系统内；自动通道只覆盖「众安通知」一环，双轨并存徒增「哪些录了哪些没录」对账负担 |
| 收益薄维护贵 | 省的只是一句口述；代价是解析器维护 + 营销邮件混流 + 版式改版误录风险 |
| 手动基建已有 80% | qdii-ledger-ingest skill 已覆盖查重/事件构造/ECB mid_rate/对账/汇报 |

真正缺口只有一个：**录 inflow 后 `sys_config.cny_invested` 不跟随**——skill
仅约定 expense→expense_cny 手工同步，inflow 侧缺失，导致持仓页「人民币累计」
卡片（invested_gross/net，server.py 实时读 sys_config）与 ledger 报表
（Σ inflow.cny_amount）脱节。

## 2. 目标

1. `POST /api/ledger/events` 批量 upsert 完成后，服务端自动重算并覆盖写
   `sys_config.cny_invested`，公式与报表侧逐字一致。
2. 响应体回显 `derived_sync`（before/after），供录入 AI 核对与汇报。
3. skill 删除可能的手工同步歧义，明确「inflow 无需手工 UPDATE」。

## 3. 非目标

- **不做邮件自动入账**（见 §1 评估）。
- **不收编 expense_cny 同步**：报表侧 `identity.expense_cny =
  Σ expense.usd_equivalent × 当前 USDCNH`（ledger.py:99，市值口径），而
  sys_config.expense_cny 是逐笔发生时折 CNY 的**历史成本累计**；事件表未存
  折算汇率，无法从事件表重算推导。强行覆盖=改口径。维持 skill 手工累加步骤
  （SKILL.md 第 39 行）原样。
- **不加 `balance_after` 字段**：balance 快照走普通事件已支持，口述场景由
  AI 从 report.balances 推算值选补（skill 可选步骤）。
- **不扩账户枚举**：付款行（如交行）不进 `_LEDGER_ACCOUNTS`，channel 沿用
  现有命名、note 写实证（如「交行汇款」）——用户已确认。

## 4. 口径与公式

```
cny_invested = ROUND(SUM(cny_amount) WHERE event_type='inflow', 2)   # 不分 direction
```

- 与报表侧 `total_cny = Σ inflow.cny_amount`（ledger.py:64）**逐字一致**，
  负向冲正（direction='-' 的 inflow）自然抵减。
- 依据：2026-10-03 ledger spec 已定「修后须等于 Σ inflow.cny_amount」，
  本设计让 sys_config 实现跟上既有口径，不引入新口径。
- **更新 nav-twr spec §3 决定**：该 spec「不改 cny_invested 手动维护机制
  （TWR 口径免疫此问题）」——当时无痛点故不动；本次为闭合手动录入通道
  将同步服务端化，TWR 侧不受影响（重放事件源不变）。

## 5. 后端设计

- **database.py 新增** `sync_ledger_cny_invested() -> dict`：
  `SELECT ROUND(SUM(cny_amount),2)` → 与 `get_config("cny_invested")`
  before 对比 → 覆盖写（set_config，若无则新增与 seed 同款 upsert）→
  返回 `{"before": float, "after": float}`。空值防御仿 server.py:1660
  （`float(x or 0)`）。
- **server.py** `ledger_events_post`（2796）upsert 循环后无条件调用同步
  （重算覆盖幂等，多算无害；修正历史 inflow 也自动收敛），响应追加：
  `"derived_sync": {"cny_invested": {...}}`；同时将 after 写入模块级
  `_cny_cfg["invested"]`，持仓页 CNY 卡片**零延迟**生效（不等下一持仓轮
  低频刷新）。

## 6. skill 更新（两份同步改）

`~/.claude/skills/qdii-ledger-ingest/SKILL.md`（运行时）与项目内
`.claude/skills/ledger-ingest/SKILL.md`（入库副本）：

1. 字段口径补一句：inflow 入库后服务端自动同步
   `sys_config.cny_invested`（=Σ inflow.cny_amount），**无需手工 UPDATE**；
   响应 `derived_sync` 可核对；before≠after 且本批无 inflow → 历史被
   纠正，汇报时提请用户注意。
2. 通道备注示例：付款行不进账户枚举，channel 沿用既有命名，note 写实证
   （如「交行汇款」）。
3. 口述场景可选步骤：从 `GET /api/ledger/report` 的 balances 推算值追加
   balance 快照事件，令 balances.diff 提前归零（不等结单）。

## 7. 测试计划

tests/test_ledger.py 新增（仿现有 POST 端点测试风格）：

1. POST inflow 批 → `get_config("cny_invested")` == Σ cny_amount（±0.01）。
2. 同批重放 → 值不变（幂等）。
3. 同 ref_no 修正 cny_amount → 跟随新 Σ。
4. POST fx/expense/balance → cny_invested 不变。
5. 响应含 derived_sync（before/after 结构完整）。

回归：现有基线全过 + `python3 scripts/check_secrets.py`。

## 8. 验收标准

口述「交行汇款 ¥10,000 至众安并换成 $1,491.48」一次录入后：

1. ledger_events 落 inflow+fx 两条，cny_invested 自动 +10000.00，
   derived_sync 回显正确。
2. 持仓页 CNY 卡片 invested_gross/net 立即跟随（零延迟）。
3. ledger 报表 total_cny_invested 与 sys_config.cny_invested 恒等。
4. 重放/修正/删除重录任一场景，两侧自动收敛无重复计。

## 9. 边界与风险

- **历史偏差纠正跳变**：若 sys_config 现值与 Σ 有历史脏偏差，任意一次
  POST 会将其纠正为 Σ（可能跳变）。上线后首次录入前先 GET
  /api/ledger/report 人工核对一次；有偏差由用户确认后接受首次 POST 对齐。
- **顺序失败窗口**：事件 upsert 成功但同步失败 → 响应 500，AI 重试整批；
  重放时事件覆盖更新（幂等无害）+ 同步重算补上，无累计缺口。
- **删事件场景**：直接改库删 inflow 不会触发同步（无 POST 发生）；
  补救=任意一次 POST 或手工 UPDATE。可接受（删事件本身是超常规操作）。
