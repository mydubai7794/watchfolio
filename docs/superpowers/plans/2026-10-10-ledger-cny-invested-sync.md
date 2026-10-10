# ledger cny_invested 服务端同步 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /api/ledger/events` 入库后自动重算并覆盖写 `sys_config.cny_invested`（=Σ inflow.cny_amount），响应回显 `derived_sync`，持仓页 CNY 卡片零延迟跟随。

**Architecture:** database.py 新增重算函数 `sync_ledger_cny_invested()`（重算覆盖式，幂等天然成立）；server.py `ledger_events_post` 尾部无条件调用并直写 `_cny_cfg` 缓存；skill 两份修订（inflow 免手工 UPDATE）。expense_cny 维持 skill 手工累加（历史成本口径不可从事件表推导，spec §3）。

**Tech Stack:** FastAPI + aiomysql（既有 db 层）、pytest（TestClient + monkeypatch 风格）。

**Spec:** `docs/superpowers/specs/2026-10-10-ledger-cny-invested-sync-design.md`

**关键上下文（执行前必读）：**
- 报表口径锚点：ledger.py:64 `total_cny = sum(cny_amount for inflow)`——**不分 direction**，SQL 必须同口径。
- `set_config`（database.py:644）的 ON DUPLICATE 会把 remark 覆盖为 VALUES(remark)，空 remark 清注释 → 值相同必须跳写，写入时必须带 remark。
- `_cny_cfg` 是 server.py:1454 模块级缓存 `{"invested": 0.0, "expense": 0.0}`，端点读它算 CNY 卡片；同步后直写保证零延迟。
- 现有测试 `test_post_events_upsert`（tests/test_ledger.py:314）未 fake 新同步函数，挂钩后必须补 monkeypatch，否则测试会连真库。
- 红线：禁止本地起服务；pytest 本地可跑；提交前 `python3 scripts/check_secrets.py`。

---

### Task 1: database.sync_ledger_cny_invested（纯逻辑，TDD）

**Files:**
- Modify: `database.py`（`upsert_ledger_event` 之后，约 1320 行处插入）
- Test: `tests/test_ledger.py`（文件尾部追加；顶部补 `from decimal import Decimal`——若已有则跳过）

- [ ] **Step 1: 写失败测试**

在 `tests/test_ledger.py` 文件末尾追加：

```python
# ---------------------------------------------------------------------------
# sync_ledger_cny_invested（cny_invested 服务端重算同步）
# ---------------------------------------------------------------------------

from decimal import Decimal

import database


class TestSyncCnyInvested:
    def _patch(self, monkeypatch, sum_value, config_value):
        calls = {}

        async def fake_sum(*a, **k):
            return [{"s": sum_value}]

        async def fake_get(key):
            return config_value

        async def fake_set(key, value, remark=""):
            calls.update(key=key, value=value, remark=remark)
            return 1

        monkeypatch.setattr(database, "execute_dict", fake_sum)
        monkeypatch.setattr(database, "get_config", fake_get)
        monkeypatch.setattr(database, "set_config", fake_set)
        return calls

    def test_sync_writes_when_diff(self, monkeypatch):
        calls = self._patch(monkeypatch, Decimal("680.00"), "0")
        out = run(database.sync_ledger_cny_invested())
        assert out == {"before": 0.0, "after": 680.0}
        assert calls["key"] == "cny_invested"
        assert calls["value"] == "680.00"
        assert "自动同步" in calls["remark"]

    def test_sync_skips_when_equal(self, monkeypatch):
        calls = self._patch(monkeypatch, Decimal("680.00"), "680.00")
        out = run(database.sync_ledger_cny_invested())
        assert out == {"before": 680.0, "after": 680.0}
        assert calls == {}

    def test_sync_empty_table(self, monkeypatch):
        calls = self._patch(monkeypatch, None, "500")
        out = run(database.sync_ledger_cny_invested())
        assert out == {"before": 500.0, "after": 0.0}
        assert calls["value"] == "0.00"

    def test_sync_replays_converge(self, monkeypatch):
        # 重放修正：config 旧值 500，Σ 已变 1180 → 覆盖为新 Σ（幂等收敛）
        calls = self._patch(monkeypatch, Decimal("1180.00"), "500")
        out = run(database.sync_ledger_cny_invested())
        assert out == {"before": 500.0, "after": 1180.0}
        assert calls["value"] == "1180.00"
```

（`run(coro)` 为本文件模块级助手，tests/test_ledger.py:22 已确认存在。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_ledger.py::TestSyncCnyInvested -v`
Expected: FAIL — `AttributeError: module 'database' has no attribute 'sync_ledger_cny_invested'`

- [ ] **Step 3: 实现**

`database.py` 中 `ledger_event_exists` 函数（约 1322 行）之后插入：

```python
async def sync_ledger_cny_invested() -> dict:
    """重算并覆盖写 sys_config.cny_invested（=Σ inflow.cny_amount）。@author yys

    重算覆盖式天然幂等：重放/同 ref_no 修正/部分失败重试均自动收敛，
    无需判「新插入还是更新」。公式与报表侧 ledger.py total_cny 逐字一致
    （不分 direction，负向冲正自然抵减）。值相同跳过写入——set_config 的
    ON DUPLICATE 会把 remark 覆盖为 VALUES(remark)。
    """
    rows = await execute_dict(
        "SELECT ROUND(SUM(cny_amount), 2) AS s FROM ledger_events "
        "WHERE event_type = 'inflow'")
    target = float(rows[0]["s"] or 0) if rows else 0.0
    before = float(await get_config("cny_invested") or 0)
    if abs(target - before) >= 0.005:
        await set_config(
            "cny_invested", f"{target:.2f}",
            remark="人民币累计投入本金（=Σ inflow.cny_amount，ledger POST 自动同步）")
    return {"before": before, "after": target}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_ledger.py::TestSyncCnyInvested -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add database.py tests/test_ledger.py
git commit -m "feat: ledger新增sync_ledger_cny_invested重算同步（=Σ inflow.cny_amount幂等覆盖）"
```

---

### Task 2: server 端点挂钩 + derived_sync 响应（TDD）

**Files:**
- Modify: `server.py:2796-2826`（`ledger_events_post`）
- Test: `tests/test_ledger.py`（修补现有 `test_post_events_upsert` + 新增派生同步测试）

- [ ] **Step 1: 修补现有测试 + 写失败测试**

(a) `tests/test_ledger.py` 的 `test_post_events_upsert`（约 314 行）内、`monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)` 之后补一行（防挂钩后连真库）：

```python
        async def fake_sync():
            return {"before": 0.0, "after": 0.0}

        monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)
        monkeypatch.setattr(server.db, "sync_ledger_cny_invested", fake_sync)
```

(b) `TestLedgerApi` 类内追加：

```python
    def test_post_events_derived_sync(self, monkeypatch):
        async def fake_upsert(**ev):
            return 1

        async def fake_sync():
            return {"before": 500.0, "after": 1180.0}

        monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)
        monkeypatch.setattr(server.db, "sync_ledger_cny_invested", fake_sync)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session())
        monkeypatch.setattr(server, "_cny_cfg", {"invested": 500.0, "expense": 0.0})
        client.cookies.set("session_token", "t-ledger-4")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-08", "event_type": "inflow",
                 "account": "za_bank", "direction": "+", "amount_ccy": "CNY",
                 "amount": 10000, "ref_no": "TEST-SYNC-1",
                 "usd_equivalent": 1491.48, "cny_amount": 10000}]})
            assert r.status_code == 200
            data = r.json()
            assert data["ok"] is True and data["applied"] == 1
            assert data["derived_sync"]["cny_invested"] == {"before": 500.0, "after": 1180.0}
            assert server._cny_cfg["invested"] == 1180.0
        finally:
            client.cookies.clear()
```

（`monkeypatch.setattr(server, "_cny_cfg", {...})` 整体替换 dict，端点写入落在替换对象上、测试结束自动还原，不污染模块级缓存。）

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_ledger.py::TestLedgerApi -v`
Expected: `test_post_events_derived_sync` FAIL — `KeyError: 'derived_sync'`；其余 PASS

- [ ] **Step 3: 实现**

`server.py` `ledger_events_post` 尾部（原 `return {"ok": True, "applied": len(events)}`，约 2826 行）替换为：

```python
    # 派生值同步：重算 cny_invested=Σ inflow.cny_amount 覆盖写（幂等），
    # 直写 _cny_cfg 令持仓页 CNY 卡片零延迟生效（不等下一持仓轮低频刷新）
    sync = await db.sync_ledger_cny_invested()
    _cny_cfg["invested"] = sync["after"]
    return {"ok": True, "applied": len(events),
            "derived_sync": {"cny_invested": sync}}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_ledger.py -v`
Expected: 全部 passed（含修补后的 `test_post_events_upsert` 与新测试）

- [ ] **Step 5: Commit**

```bash
git add server.py tests/test_ledger.py
git commit -m "feat: ledger事件POST挂钩cny_invested自动同步+derived_sync回显（CNY卡片零延迟）"
```

---

### Task 3: skill 两份修订

**Files:**
- Modify: `/root/.claude/skills/qdii-ledger-ingest/SKILL.md`（运行时版，不入 git）
- Modify: `.claude/skills/ledger-ingest/SKILL.md`（入库副本，入 git）

两份的修订内容一致（各 3 处；行号以全局版为准，项目版对应段落文字相同）：

- [ ] **Step 1: 修订字段口径（cny_amount 行，全局版约 33 行）**

原：

```markdown
- `cny_amount`：仅 inflow 必填（人民币实际投入）。
```

改为：

```markdown
- `cny_amount`：仅 inflow 必填（人民币实际投入）。inflow 入库后服务端自动同步 `sys_config.cny_invested`（恒等 Σ inflow.cny_amount，POST 响应 `derived_sync.cny_invested` 回显 before/after），**无需手工 UPDATE**；若 before≠after 且本批无 inflow = 历史值被纠正，汇报时提请用户注意。
```

- [ ] **Step 2: 修订对账验证（录入流程步骤 3 的 balances 条目，全局版约 19 行）**

原：

```markdown
   - `balances.diff` 各币种 ≈ 0（±1 容差；新结单出余额快照后必须重新提交 balance 事件）
```

改为：

```markdown
   - `balances.diff` 各币种 ≈ 0（±1 容差；新结单出余额快照后必须重新提交 balance 事件；口述场景可选：用 report 的 `balances.ledger` 推算值补一条 balance 快照——ref_no 同源加 `-bal` 后缀——令 diff 提前归零，不补则等下一期结单定格）
```

- [ ] **Step 3: 修订 note 口径（全局版约 41 行）**

原：

```markdown
- `note`：写证据来源（结单页码/邮件编号/口述），月结单冲销差异（回赠/利息）单独记 reward/fee 而非改旧账。
```

改为：

```markdown
- `note`：写证据来源（结单页码/邮件编号/口述），月结单冲销差异（回赠/利息）单独记 reward/fee 而非改旧账。付款行实证写这里（如「交行汇款」）——付款行不进账户枚举、不单列账户。
```

- [ ] **Step 4: Commit（仅项目内副本）**

```bash
git add .claude/skills/ledger-ingest/SKILL.md
git commit -m "docs: 记账skill更新——inflow服务端自动同步说明+口述余额快照可选步骤+付款行note口径"
```

---

### Task 4: 回归 + 部署 + 线上核对

**Files:** 无代码改动（验证任务）

- [ ] **Step 1: 本地全量回归**

```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py tests/test_alert_api.py tests/test_daily_bars.py tests/test_longport_push.py tests/test_macro_fetcher.py tests/test_ledger.py tests/test_nav.py
```

Expected: 全部 passed（基线 441 + 本次新增 5）

- [ ] **Step 2: 提交门禁**

```bash
python3 scripts/check_secrets.py
```

Expected: `检出 0 处`

- [ ] **Step 3: 部署（rsync + 重启，命令模板见 AGENTS.md 部署节；服务器地址取 AGENTS.local.md）**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='qdii.env' --exclude='*.local.md' --exclude='.mcp.json' \
  --exclude='static/download' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

- [ ] **Step 4: 线上健康与存量核对（spec §9 风险处置）**

```bash
ssh root@<server-ip> "systemctl is-active qdii && journalctl -u qdii --since '2 min ago' | tail -20"
```

Expected: active，无异常栈。

存量核对（首次录入前，核对 Σ 与 sys_config 现值是否一致）：

```bash
ssh root@<server-ip> "set -a; . /opt/qdii/qdii.env; set +a; mysql -h127.0.0.1 -P\$QDII_DB_PORT -u\$QDII_DB_USER -p\"\$QDII_DB_PASS\" \$QDII_DB_NAME -N -e \"SELECT ROUND(SUM(cny_amount),2) FROM ledger_events WHERE event_type='inflow'; SELECT value FROM sys_config WHERE \\\`key\\\`='cny_invested';\""
```

两值一致 → 零跳变，收工；不一致 → **历史偏差**：停下汇报用户，确认后由下一次任意 POST 自动对齐（或手工 UPDATE 对齐后再部署验证）。

- [ ] **Step 5: 端到端验收（spec §8，用测试性质口述样例核对响应结构，不落真实账）**

带 session cookie POST 一条与线上既有账无关的 `ref_no="VERIFY-20261010"` inflow 后立即删除并 POST 同步一次使 cny_invested 收敛回原值：

```bash
# 1) 记录当前 sys_config.cny_invested
# 2) POST {"events":[{event_date:"2026-10-10", event_type:"inflow", account:"za_bank",
#    direction:"+", amount_ccy:"CNY", amount:1, ref_no:"VERIFY-20261010",
#    usd_equivalent:0.15, cny_amount:1}]}
#    → 断言响应含 derived_sync.cny_invested，after=原值+1.00
# 3) mysql DELETE FROM ledger_events WHERE ref_no='VERIFY-20261010'
# 4) 重复 POST 步骤 2 同一 ref_no → cny_invested 重算回原值（DELETE 后重放收敛验证）
# 5) 持仓页 /api/portfolio/overview 的 cny 块 invested_gross 已跟随（不等持仓轮）
```

Expected: 步骤 2 after=原值+1.00；步骤 4 后 cny_invested 回原值；overview cny 块立即反映。验证完毕汇报。

---

## Self-Review 记录

1. **Spec 覆盖**：§5 后端→Task 1/2；§6 skill→Task 3（3 处修订逐一对应）；§7 测试→Task 1（4 测）+ Task 2（1 测 + 现有测试修补）；§8 验收→Task 4 Step 5；§9 边界（顺序失败窗口=重放收敛，Task 1 测试 4 覆盖；历史偏差→Task 4 Step 4 处置）。无遗漏。
2. **占位符**：无 TBD/TODO；`<server-ip>` 为 AGENTS.local.md 环境值（仓库惯例），非计划占位。
3. **类型一致**：`sync_ledger_cny_invested() -> dict{"before","after"}` 在 Task 1 实现、Task 2 fake 与端点消费三处一致；`derived_sync` 响应键名与 skill 文案、Task 2 断言一致。
