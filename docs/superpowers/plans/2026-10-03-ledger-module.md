# 记账模块（Ledger）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 v3 盘点口径产品化——ledger_events 逐笔事件表 + 点差/通道成本/盈亏恒等式实时报表 API + ECB 日线缓存 + 入金邮件自动入账（条件任务）+ 结单录入 skill。

**Architecture:** 单表事件流（ref_no 幂等 upsert）→ 纯函数报表（ledger.py build_report，无 DB 依赖可单测）→ FastAPI 3 端点 → portfolio.html 新「记账」section。ECB 汇率每日 22:35 一次拉取缓存 ledger_fx_rates（唯一新增外部请求）。种子数据 = v3 全部 67 事件，报表断言必须复现 v3 基线数字。

**Tech Stack:** Python 3 / FastAPI / aiomysql / httpx / pytest（monkeypatch 桩 DB，无本地 MySQL）/ 原生 JS 单文件前端。

**权威数字来源:** 本地私有基线文档（v3 盘点，不入库）。本计划种子数据与断言目标全部出自该文档。（发布版已脱敏：全部真实金额/渠道/笔数明细以 `<基线值>` 占位，实际值仅存于本地不入库文件与库内。）

**勘误（相对 spec/终版 v3 的三处修正，执行时生效）:**
1. `cash_accounts.HKD` 是**全账户合并桶**：两账户留存相加（spec §8 误写单账户值，会少记另一账户留存）。
2. fx 事件需要双边金额：schema 增加 `recv_ccy`/`recv_amount` 列；`event_type` 增加 `balance`（结单余额快照，对账用）。
3. NVDA 修数成本按赠股 0 成本折算（v3 原值有笔误）；奖励合计以种子实算为准（v3 为估算值）。

**测试基线:** 执行前先跑 `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py -q` 记录当前通过数（约 198），完成后 = 基线 + 新增（预计 +25 左右）。pytest 本地跑（不起服务）。

---

### Task 0: spec 同步修订

**Files:**
- Modify: `docs/superpowers/specs/2026-10-03-ledger-module-design.md`

- [ ] **Step 1: §2 schema 增加 recv 双边字段与 balance 类型**——DDL 块中 `amount DECIMAL(14,2) NOT NULL,` 行后加：

```sql
  recv_ccy VARCHAR(5),               -- 换汇类：收到币种
  recv_amount DECIMAL(14,2),         -- 换汇类：实际得到金额
```

`event_type` 注释行改为 `-- inflow/fx/fee/reward/expense/transfer/realized/balance`。

- [ ] **Step 2: §8 修数表 HKD 行改为合并桶口径**——现值改为**跨账户合并值**，依据改为「多账户留存相加（cash_accounts 为跨账户合并桶）」；NVDA 行按赠股 0 成本折算修正；奖励合计（§4 表）估算值改为**种子实算值**。

- [ ] **Step 3: 提交**

```bash
git -C /root/workspace/project/qdii add docs/superpowers/specs/2026-10-03-ledger-module-design.md
git -C /root/workspace/project/qdii commit -m "docs(spec): ledger设计修订——fx双边金额字段/balance类型/HKD跨账户桶口径/NVDA赠股成本折算"
```

---

### Task 1: database.py — 两张新表 DDL + CRUD

**Files:**
- Modify: `database.py`（init_tables 内 history_ticks 建表之后、`init_history_ticks_table` 调用与 etf_targets 之间的合适位置——放在 fund_nav_history 建表块之后；CRUD 加在文件末尾 cash/trade 区段之后）
- Test: `tests/test_ledger.py`（新建）

- [ ] **Step 1: 写失败测试**

```python
"""ledger 模块测试：CRUD SQL 形状 + 报表纯计算 + ECB 解析 + API。

依赖 monkeypatch 桩掉 database 层（同 test_email_trader.py 模式），不起服务不连 MySQL。
"""
import asyncio
import database as db


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class TestLedgerCrud:
    def test_upsert_ledger_event_sql(self, monkeypatch):
        captured = {}

        async def fake_execute(sql, args=None, _pool=None):
            captured["sql"], captured["args"] = sql, args
            return 1

        monkeypatch.setattr(db, "execute", fake_execute)
        run(db.upsert_ledger_event(
            event_date="2026-01-01", event_type="inflow", account="za_bank",
            channel="示例渠道", direction="+", amount_ccy="CNY", amount=10000.0,
            recv_ccy="HKD", recv_amount=11000.00, usd_equivalent=1400.00,
            cny_amount=10000.0, mid_rate=None, note="", ref_no="DEMO-I01"))
        sql = captured["sql"]
        assert "INSERT INTO ledger_events" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql  # ref_no 幂等 upsert
        assert captured["args"][0] == "DEMO-I01"

    def test_get_ledger_events_filters(self, monkeypatch):
        captured = {}

        async def fake_execute_dict(sql, args=None, _pool=None):
            captured["sql"], captured["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        run(db.get_ledger_events(event_type="fx", year=2026, account="hsbc"))
        assert "WHERE" in captured["sql"] and "event_type" in captured["sql"]
        assert "2026" in str(captured["args"]) and "hsbc" in captured["args"]
        run(db.get_ledger_events())
        assert "WHERE" not in captured["sql"]  # 无筛选不过滤

    def test_upsert_ledger_fx_rate_and_latest(self, monkeypatch):
        cap = {}

        async def fake_execute(sql, args=None, _pool=None):
            cap["up"] = (sql, args)
            return 1

        async def fake_execute_dict(sql, args=None, _pool=None):
            cap["q"] = (sql, args)
            return [{"rate_date": "2026-06-18", "usd_cny": 6.7266, "usd_hkd": 7.8083}]

        monkeypatch.setattr(db, "execute", fake_execute)
        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        run(db.upsert_ledger_fx_rate("2026-06-18", 6.7266, 7.8083))
        assert "INSERT INTO ledger_fx_rates" in cap["up"][0]
        row = run(db.get_ledger_fx_for_date("2026-06-19"))
        assert "rate_date <= %s" in cap["q"][0]  # 缺当日时回退最近一条
        assert row["usd_cny"] == 6.7266

    def test_get_latest_portfolio_value(self, monkeypatch):
        async def fake_execute_dict(sql, args=None, _pool=None):
            assert "total_value_usd" in sql and "ORDER BY ts DESC LIMIT 1" in sql
            return [{"total_value_usd": 13651.98}]

        monkeypatch.setattr(db, "execute_dict", fake_execute_dict)
        assert run(db.get_latest_portfolio_value()) == 13651.98
```

- [ ] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：`AttributeError: module 'database' has no attribute 'upsert_ledger_event'`

- [ ] **Step 3: 实现 DDL + CRUD**

init_tables 内 fund_nav_history 建表块之后追加（风格同现有 `await execute(..., _pool=pool)`）：

```python
    # ---- ledger_events (记账事件流) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_events (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            event_date     DATE NOT NULL COMMENT '事件日期',
            event_type     VARCHAR(20) NOT NULL COMMENT 'inflow入金/fx换汇/fee费用/reward奖励/expense消耗/transfer转移/realized已实现/balance余额快照',
            account        VARCHAR(20) NOT NULL COMMENT 'za_bank/za_sec/hsbc/zf/ccb/cmb/cib/external',
            channel        VARCHAR(40) DEFAULT NULL COMMENT '通道: 银行购汇/柜台/SWIFT电汇/券商换汇/支付渠道等（值域示例见 ledger_seed.example.py）',
            direction      CHAR(1) NOT NULL COMMENT '+入账/-出账',
            amount_ccy     VARCHAR(5) NOT NULL COMMENT '付出币种',
            amount         DECIMAL(14,2) NOT NULL COMMENT '付出金额',
            recv_ccy       VARCHAR(5) DEFAULT NULL COMMENT '换汇类: 收到币种',
            recv_amount    DECIMAL(14,2) DEFAULT NULL COMMENT '换汇类: 实际得到金额',
            usd_equivalent DECIMAL(14,2) DEFAULT NULL COMMENT 'USD等值(口径见盘点v3)',
            cny_amount     DECIMAL(14,2) DEFAULT NULL COMMENT 'CNY口径(inflow必填,邮件自动入账可NULL待补)',
            mid_rate       DECIMAL(12,6) DEFAULT NULL COMMENT '换汇类: ECB当日中间价(单位付币所得收币)',
            note           VARCHAR(200) DEFAULT NULL,
            ref_no         VARCHAR(40) NOT NULL UNIQUE COMMENT '幂等键',
            created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_type_date (event_type, event_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='记账事件流(AI经API录入)'
        """,
        _pool=pool,
    )

    # ---- ledger_fx_rates (ECB 日线缓存) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_fx_rates (
            rate_date  DATE NOT NULL PRIMARY KEY COMMENT 'ECB参考价日期',
            usd_cny    DECIMAL(12,6) NOT NULL COMMENT 'USD/CNY 参考价',
            usd_hkd    DECIMAL(12,6) NOT NULL COMMENT 'USD/HKD 参考价',
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ECB每日参考价缓存(frankfurter.dev)'
        """,
        _pool=pool,
    )
```

文件末尾追加 CRUD：

```python
# ---------------------------------------------------------------------------
# Ledger events (记账事件流)
# ---------------------------------------------------------------------------

_LEDGER_MUTABLE = ("event_date", "event_type", "account", "channel", "direction",
                   "amount_ccy", "amount", "recv_ccy", "recv_amount",
                   "usd_equivalent", "cny_amount", "mid_rate", "note")


async def upsert_ledger_event(*, event_date, event_type, account, direction,
                              amount_ccy, amount, ref_no, channel=None,
                              recv_ccy=None, recv_amount=None, usd_equivalent=None,
                              cny_amount=None, mid_rate=None, note=None) -> int:
    """按 ref_no 幂等 upsert 一条记账事件（同 ref_no 重复提交=覆盖更新）。"""
    cols = _LEDGER_MUTABLE + ("ref_no",)
    vals = (event_date, event_type, account, channel, direction, amount_ccy,
            _safe_decimal(amount, 2), recv_ccy, _safe_decimal(recv_amount, 2),
            _safe_decimal(usd_equivalent, 2), _safe_decimal(cny_amount, 2),
            _safe_decimal(mid_rate, 6), note, ref_no)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c}=VALUES({c})" for c in _LEDGER_MUTABLE)
    return await execute(
        f"INSERT INTO ledger_events ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}", vals)


async def ledger_event_exists(ref_no: str) -> bool:
    rows = await execute_dict("SELECT id FROM ledger_events WHERE ref_no = %s", (ref_no,))
    return bool(rows)


async def get_ledger_events(event_type: str | None = None, year: int | None = None,
                            account: str | None = None) -> list[dict]:
    """事件明细查询（可选筛选），按日期升序。"""
    conds, args = [], []
    if event_type:
        conds.append("event_type = %s"); args.append(event_type)
    if year:
        conds.append("YEAR(event_date) = %s"); args.append(year)
    if account:
        conds.append("account = %s"); args.append(account)
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    rows = await execute_dict(
        f"SELECT * FROM ledger_events {where} ORDER BY event_date, id", args or None)
    for r in rows:
        for k in ("amount", "recv_amount", "usd_equivalent", "cny_amount", "mid_rate"):
            if r.get(k) is not None:
                r[k] = float(r[k])
        r["event_date"] = str(r["event_date"])
    return rows


async def upsert_ledger_fx_rate(rate_date, usd_cny: float, usd_hkd: float) -> int:
    return await execute(
        "INSERT INTO ledger_fx_rates (rate_date, usd_cny, usd_hkd) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE usd_cny=VALUES(usd_cny), usd_hkd=VALUES(usd_hkd)",
        (rate_date, _safe_decimal(usd_cny, 6), _safe_decimal(usd_hkd, 6)))


async def get_ledger_fx_for_date(rate_date) -> dict | None:
    """取 <= rate_date 的最近一条 ECB 参考价（周末/假日自动回退上一发布日）。"""
    rows = await execute_dict(
        "SELECT * FROM ledger_fx_rates WHERE rate_date <= %s "
        "ORDER BY rate_date DESC LIMIT 1", (rate_date,))
    if rows:
        r = rows[0]
        r["usd_cny"], r["usd_hkd"] = float(r["usd_cny"]), float(r["usd_hkd"])
        r["rate_date"] = str(r["rate_date"])
        return r
    return None


async def get_ledger_fx_count() -> int:
    rows = await execute_dict("SELECT COUNT(*) AS n FROM ledger_fx_rates")
    return int(rows[0]["n"]) if rows else 0


async def get_latest_portfolio_value() -> float | None:
    """最新组合总资产（portfolio_ticks 最后一条采样）。"""
    rows = await execute_dict(
        "SELECT total_value_usd FROM portfolio_ticks ORDER BY ts DESC LIMIT 1")
    return float(rows[0]["total_value_usd"]) if rows else None
```

- [ ] **Step 4: 跑测试确认通过**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：4 passed

- [ ] **Step 5: 提交**

```bash
git -C /root/workspace/project/qdii add database.py tests/test_ledger.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): ledger_events/ledger_fx_rates建表+CRUD(ref_no幂等upsert/ECB取价回退/最新组合净值)"
```

---

### Task 2: ledger_seed.py — v3 全部 67 笔种子事件

**Files:**
- Create: `ledger_seed.py`

- [ ] **Step 1: 写种子数据模块**（数字全部出自 v3 文档§二/三/四/五/六/七/八节，`E(...)` 构造器见后）

```python
"""ledger 种子数据：v3 盘点（账户成本盈亏盘点_20261003.md）全部 67 笔事件。

用法（服务器上执行，幂等可重跑）：
    python3 ledger_seed.py
"""
import asyncio
import database as db

HKD_USD_STATIC = 7.846  # v3 静态折算口径


def E(date, etype, account, direction, ccy, amount, ref, channel=None,
      recv_ccy=None, recv_amount=None, usd=None, cny=None, mid=None, note=""):
    return dict(event_date=date, event_type=etype, account=account, channel=channel,
                direction=direction, amount_ccy=ccy, amount=amount, ref_no=ref,
                recv_ccy=recv_ccy, recv_amount=recv_amount, usd_equivalent=usd,
                cny_amount=cny, mid_rate=mid, note=note)


SEED_EVENTS = [
    # ---- 发布版脱敏说明 ----
    # 本计划原收录 v3 全部真实流水（67 笔），发布前已整体脱敏：
    # 完整真实种子仅存本地 ledger_seed.py（已退跟踪 + gitignore，不入库）；
    # 仓库内结构示例见 ledger_seed.example.py。以下仅保留事件类型结构示意，
    # 金额/渠道/备注均为虚构示意值，不构成任何真实账目。
    # ---- inflow 入金（结构示意）----
    E("2026-01-01", "inflow", "za_bank", "+", "CNY", 10000, "DEMO-I01", "示例渠道",
      "HKD", 11000.00, 1400.00, 10000, None, "结构示意: 原币购汇汇出"),
    E("2026-01-02", "inflow", "hsbc", "+", "CNY", 5000, "DEMO-I02", "示例渠道",
      "USD", 700.00, 700.00, 5000, None, "结构示意: SWIFT 电汇"),
    # ---- fx 换汇（结构示意；mid=ECB当日理论汇率=理论应得/付出量；loss由报表实时推导）----
    E("2026-01-01", "fx", "za_bank", "-", "CNY", 10000, "DEMO-F01", "示例渠道",
      "HKD", 11000.00, None, None, 1.150000),
    E("2026-01-02", "fx", "hsbc", "-", "HKD", 1000, "DEMO-F02", "示例柜台",
      "USD", 128.00, None, None, 0.127500),
    E("2026-01-03", "fx", "zf", "-", "HKD", 3000, "DEMO-F03", "示例券商换汇",
      "USD", 382.00, None, None, 0.127500),
    # ---- fee 显性费用（结构示意）----
    E("2026-01-04", "fee", "za_sec", "-", "USD", 1.99, "DEMO-X01", "示例佣金",
      None, None, 1.99, None, None, "结构示意: 交易佣金"),
    # ---- reward 奖励/被动收入（结构示意）----
    E("2026-01-05", "reward", "za_bank", "+", "HKD", 100, "DEMO-R01", "现金回赠", None, None, 12.74),
    E("2026-01-06", "reward", "za_bank", "+", "HKD", 20, "DEMO-R02", "基金申购回赠", None, None, 2.55),
    # ---- expense 消耗（结构示意）----
    E("2026-01-07", "expense", "hsbc", "-", "HKD", 300.00, "DEMO-E01", "示例渠道",
      None, None, 38.00, None, None, "结构示意: 生活缴费"),
    # ---- transfer 转出（结构示意）----
    E("2026-01-08", "transfer", "external", "-", "HKD", 8.88, "DEMO-T01", "转其他银行保活",
      None, None, 1.13, None, None, "面值回流不计损益"),
    E("2026-01-09", "transfer", "hsbc", "-", "HKD", 300, "DEMO-T02", "内部划转",
      None, None, None, None, None, "结构示意: 账户间转移"),
    # ---- realized 已实现交易（结构示意）----
    E("2026-01-10", "realized", "hsbc", "-", "USD", -0.21, "DEMO-L01", "示例标的", None, None, -0.21),
    E("2026-01-11", "realized", "hsbc", "+", "USD", 1.28, "DEMO-L02", "示例标的", None, None, 1.28),
    # ---- balance 结单余额快照（对账用，结构示意）----
    E("2026-01-31", "balance", "za_bank", "+", "HKD", 100.00, "DEMO-B01", "结单快照", None, None, None),
    E("2026-01-31", "balance", "hsbc", "+", "USD", 200.00, "DEMO-B02", "结单快照", None, None, None),
]


async def seed():
    """幂等入库全部种子事件。"""
    n = 0
    for ev in SEED_EVENTS:
        await db.upsert_ledger_event(**ev)
        n += 1
    return n


if __name__ == "__main__":
    total = asyncio.run(seed())
    print(f"ledger seed 完成: {total} 笔事件(幂等upsert)")
```

- [ ] **Step 2: 种子完整性自检（不连库）**

```bash
# 发布版脱敏：基线数值以 <基线> 占位，实际值仅存本地不入库文件
python3 -c "
from ledger_seed import SEED_EVENTS
refs = [e['ref_no'] for e in SEED_EVENTS]
assert len(refs) == len(set(refs)), len(refs)                      # ref_no 全唯一
assert sum(e['cny_amount'] for e in SEED_EVENTS if e['event_type']=='inflow') == <总投入基线>
usd_in = sum(e['usd_equivalent'] for e in SEED_EVENTS if e['event_type']=='inflow')
assert abs(usd_in - <USD入账基线>) < 0.01, usd_in
fee = sum(e['usd_equivalent'] for e in SEED_EVENTS if e['event_type']=='fee')
assert abs(fee - <费用基线>) < 0.01, fee
real = sum(e['usd_equivalent'] for e in SEED_EVENTS if e['event_type']=='realized')
assert abs(real - <已实现基线>) < 0.01, real
print('种子自检通过')"
```

- [ ] **Step 3: 提交**

```bash
git -C /root/workspace/project/qdii add ledger_seed.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): 种子事件批量入库脚本(幂等upsert)"
```

---

### Task 3: ledger.py — 报表纯计算 + ECB 拉取解析

**Files:**
- Create: `ledger.py`
- Test: `tests/test_ledger.py`（追加）

- [ ] **Step 1: 写失败测试（追加到 tests/test_ledger.py）**

```python
import ledger
from ledger_seed import SEED_EVENTS


class TestBuildReport:
    def _report(self, **kw):
        ctx = dict(total_assets_usd=<资产基线>, usdcnh=<汇率基线>, cash_accounts=[])
        ctx.update(kw)
        return ledger.build_report(SEED_EVENTS, **ctx)

    # 发布版脱敏：以下断言的基线数值以 <基线> 占位（结构保留），
    # 实际数值仅存本地不入库的私有基线文档与 tests/test_ledger.py。

    def test_v3_baselines(self):
        r = self._report()
        s = r["summary"]
        assert s["total_cny_invested"] == <总投入基线>
        assert abs(s["usd_in"] - <USD入账基线>) < 0.05
        assert abs(s["avg_cost"] - <均成本基线>) < 0.0005
        assert abs(s["fx_loss_usd"] - <点差基线>) < 0.15
        assert abs(s["fee_usd"] - <费用基线>) < 0.05
        assert abs(s["reward_usd"] - <奖励基线>) < 0.5
        assert abs(s["realized_usd"] - <已实现基线>) < 0.05
        assert abs(s["expense_usd"] - <消耗基线>) < 0.05
        assert abs(s["external_transfer_usd"] - <转出基线>) < 0.05
        assert s["pending_count"] == 0

    def test_v3_identity(self):
        r = self._report()
        i = r["identity"]
        assert abs(i["gain_usd"] - <增值基线>) < 0.5
        assert abs(i["gain_pct"] - <增值率基线>) < 0.05
        assert abs(i["fx_loss_cny"] - <汇损基线>) < 1.5
        assert abs(i["expense_cny"] - <消耗CNY基线>) < 0.5
        assert abs(i["net_cny"] - <净盈亏基线>) < 0.5
        assert abs(i["net_pct"] - <净盈亏率基线>) < 0.05
        assert abs(i["identity_check_cny"] - i["net_cny"]) < 1.0  # 分解式闭环±1

    def test_no_usdcnh_degrades(self):
        r = self._report(usdcnh=None)
        assert r["identity"]["usdcnh"] is None
        assert r["identity"]["net_cny"] is None               # 缺汇率时CNY恒等式置空不报错

    def test_channel_ranking(self):
        ch = {c["channel"]: c for c in self._report()["channels"]}
        assert ch[<最贵通道>]["wavg_spread_pct"] > 0                   # 最贵通道点差为正
        assert ranked[0] == <最贵通道>                          # 最贵排第一

    def test_balance_reconciliation(self):
        cash = [{"currency": "HKD", "amount": <HKD余额基线>}, {"currency": "USD", "amount": <USD余额基线>}]
        r = self._report(cash_accounts=cash)
        b = r["balances"]
        assert abs(b["ledger"]["HKD"] - <HKD余额基线>) < 0.01
        assert abs(b["ledger"]["USD"] - <USD余额基线>) < 0.01
        assert b["system"]["HKD"] == <HKD余额基线>
        assert abs(b["diff"]["HKD"]) < 0.01 and abs(b["diff"]["USD"]) < 0.01

    def test_fx_row_derivation(self):
        rows = {row["ref_no"]: row for row in self._report()["fx_detail"]}
        f01 = rows[<fx事件引用号>]
        assert abs(f01["theoretical"] - <理论应得基线>) < 1
        assert abs(f01["spread_pct"] - <点差率基线>) < 0.02
        assert abs(f01["loss_usd"] - <损失基线>) < 0.05


class TestEcb:
    def test_parse_ecb_payload(self):
        data = {"rates": {
            "2026-06-18": {"CNY": 6.7266, "HKD": 7.8083},
            "2026-06-20": {"CNY": 6.72, "HKD": 7.80},          # 周末缺19日,ECB自然跳过
        }}
        rows = ledger.parse_ecb_payload(data)
        assert rows == [("2026-06-18", 6.7266, 7.8083),
                        ("2026-06-20", 6.72, 7.80)]

    def test_parse_ecb_bad_payload(self):
        assert ledger.parse_ecb_payload({}) == []
        assert ledger.parse_ecb_payload({"rates": {"d1": {}}}) == []
```

- [ ] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：`ModuleNotFoundError: No module named 'ledger'`

- [ ] **Step 3: 实现 ledger.py**

```python
"""记账模块：报表纯计算（无 DB 依赖）+ ECB 参考价拉取。

口径全部对齐《账户成本盈亏盘点_20261003.md》(v3)：
- HKD→USD 静态折算 7.846；点差由事件自带 mid_rate+双边金额实时推导；
- 恒等式：net_cny = 总资产×USDCNH − 总投入；分解 = 增值 + 汇损 − 消耗（±1 CNY 舍入容差）；
- 保活转出（transfer→external）面值单列，不进恒等式不产生损益。
"""
import logging
from datetime import date

import httpx

logger = logging.getLogger(__name__)

HKD_USD_STATIC = 7.846
ECB_BASE_URL = "https://api.frankfurter.dev/v1"
LEDGER_FX_START = "2026-01-01"          # 回填起点（按库内最早 fx 事件日期）


# ---------------------------------------------------------------------------
# 报表纯计算
# ---------------------------------------------------------------------------

def _f(v) -> float | None:
    return None if v is None else float(v)


def compute_fx_detail(events: list[dict]) -> list[dict]:
    """推导每笔换汇事件的 理论应得/点差%/损失USD（单一事实源：mid_rate+双边金额）。"""
    rows = []
    for e in events:
        if e["event_type"] != "fx" or not e.get("mid_rate"):
            continue
        amount, mid = float(e["amount"]), float(e["mid_rate"])
        actual = _f(e.get("recv_amount"))
        theoretical = amount * mid
        if actual is None or theoretical <= 0:
            continue
        diff = theoretical - actual                     # 收币单位（正=少得）
        recv_is_hkd = (e.get("recv_ccy") or "") == "HKD"
        loss_usd = diff / HKD_USD_STATIC if recv_is_hkd else diff
        rows.append({"ref_no": e["ref_no"], "event_date": e["event_date"],
                     "channel": e.get("channel") or "", "pay_ccy": e["amount_ccy"],
                     "pay_amount": amount, "recv_ccy": e.get("recv_ccy") or "",
                     "recv_amount": actual, "theoretical": round(theoretical, 2),
                     "spread_pct": round(diff / theoretical * 100, 4),
                     "loss_usd": round(loss_usd, 4)})
    return rows


def build_report(events: list[dict], *, total_assets_usd: float | None,
                 usdcnh: float | None, cash_accounts: list[dict] | None = None) -> dict:
    """由事件表实时计算全套报表（口径见模块 docstring）。"""
    inflow = [e for e in events if e["event_type"] == "inflow"]
    fee = [e for e in events if e["event_type"] == "fee"]
    reward = [e for e in events if e["event_type"] == "reward"]
    realized = [e for e in events if e["event_type"] == "realized"]
    expense = [e for e in events if e["event_type"] == "expense"]
    transfer_ext = [e for e in events if e["event_type"] == "transfer"
                    and e.get("account") == "external"
                    and e.get("usd_equivalent") is not None]

    total_cny = sum(float(e["cny_amount"] or 0) for e in inflow)
    usd_in = sum(_f(e.get("usd_equivalent")) or 0 for e in inflow)
    avg_cost = total_cny / usd_in if usd_in else None
    fx_detail = compute_fx_detail(events)
    fx_loss_usd = sum(r["loss_usd"] for r in fx_detail)
    fee_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in fee)
    reward_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in reward)
    realized_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in realized)
    expense_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in expense)
    ext_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in transfer_ext)

    # 通道加权平均点差（权重=理论应得，收币统一 USD 化）
    chan: dict[str, dict] = {}
    for r in fx_detail:
        theo_usd = r["theoretical"] / HKD_USD_STATIC if r["recv_ccy"] == "HKD" else r["theoretical"]
        c = chan.setdefault(r["channel"], {"n": 0, "wsum": 0.0, "loss": 0.0})
        c["n"] += 1
        c["wsum"] += theo_usd
        c["loss"] += r["loss_usd"]
    channels = [{"channel": k, "n": v["n"],
                 "wavg_spread_pct": round(v["loss"] / v["wsum"] * 100, 4),
                 "loss_usd": round(v["loss"], 2)}
                for k, v in chan.items()]
    channels.sort(key=lambda c: -c["wavg_spread_pct"])

    identity: dict = {"usdcnh": usdcnh, "total_assets_usd": total_assets_usd,
                      "external_transfer_usd": round(ext_usd, 2)}
    if usdcnh and total_assets_usd and avg_cost:
        invest_base = usd_in - expense_usd
        gain_usd = total_assets_usd - invest_base
        fx_loss_cny = usd_in * (usdcnh - avg_cost)
        expense_cny = expense_usd * usdcnh
        net_cny = total_assets_usd * usdcnh - total_cny
        identity.update({
            "gain_usd": round(gain_usd, 2),
            "gain_pct": round(gain_usd / invest_base * 100, 2) if invest_base else None,
            "fx_loss_cny": round(fx_loss_cny, 2),
            "expense_cny": round(expense_cny, 2),
            "net_cny": round(net_cny, 2),
            "net_pct": round(net_cny / total_cny * 100, 2) if total_cny else None,
            "identity_check_cny": round(gain_usd * usdcnh + fx_loss_cny - expense_cny, 2),
        })

    # 对账位：最近结单余额快照 vs cash_accounts 系统值（按币种合并）
    balances: dict = {"ledger": {}, "system": {}, "diff": {}}
    latest: dict[tuple, dict] = {}
    for e in events:
        if e["event_type"] == "balance":
            key = e["amount_ccy"]
            if key not in latest or e["event_date"] >= latest[key]["event_date"]:
                latest[key] = e
    for ccy, e in latest.items():
        balances["ledger"][ccy] = round(sum(
            float(x["amount"]) for x in events
            if x["event_type"] == "balance" and x["amount_ccy"] == ccy
            and x["event_date"] == e["event_date"]), 2)
    if cash_accounts:
        for row in cash_accounts:
            balances["system"][row["currency"]] = row["amount"]
    for ccy in set(balances["ledger"]) | set(balances["system"]):
        lv, sv = balances["ledger"].get(ccy), balances["system"].get(ccy)
        balances["diff"][ccy] = None if (lv is None or sv is None) else round(sv - lv, 2)

    return {
        "summary": {
            "total_cny_invested": round(total_cny, 2),
            "usd_in": round(usd_in, 2),
            "avg_cost": round(avg_cost, 4) if avg_cost else None,
            "fx_loss_usd": round(fx_loss_usd, 2),
            "fee_usd": round(fee_usd, 2),
            "reward_usd": round(reward_usd, 2),
            "realized_usd": round(realized_usd, 2),
            "expense_usd": round(expense_usd, 2),
            "external_transfer_usd": round(ext_usd, 2),
            "pending_count": sum(1 for e in inflow if e.get("cny_amount") is None),
        },
        "identity": identity,
        "channels": channels,
        "fx_detail": fx_detail,
        "balances": balances,
    }


# ---------------------------------------------------------------------------
# ECB 参考价拉取（frankfurter.dev，免费无 key；每日一次，严禁短周期重试放大）
# ---------------------------------------------------------------------------

def parse_ecb_payload(data: dict) -> list[tuple]:
    """解析 frankfurter rates 载荷 → [(date, usd_cny, usd_hkd), ...]（按日期升序）。"""
    rates = (data or {}).get("rates") or {}
    rows = []
    for d in sorted(rates):
        item = rates[d] or {}
        cny, hkd = item.get("CNY"), item.get("HKD")
        if cny and hkd:
            rows.append((d, float(cny), float(hkd)))
    return rows


async def fetch_ecb_rates(start: str, end: str,
                          proxy_urls: list[str] | None = None) -> list[tuple]:
    """区间拉取 ECB USD→CNY/HKD 参考价（一次请求；直连→境外代理容错）。"""
    url = f"{ECB_BASE_URL}/{start}..{end}?base=USD&symbols=CNY,HKD"
    candidates = [None] + [p if "://" in p else f"http://{p}" for p in (proxy_urls or [])[:2]]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return parse_ecb_payload(resp.json())
                last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("ECB 参考价拉取失败(%s..%s): %s", start, end, last_err)
    return []
```

- [ ] **Step 4: 跑测试确认通过**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：4 + 8 = 12 passed

- [ ] **Step 5: 提交**

```bash
git -C /root/workspace/project/qdii add ledger.py tests/test_ledger.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): 报表纯计算(点差实时推导/通道加权排名/恒等式分解/余额对账位)+ECB区间拉取解析"
```

---

### Task 4: server.py — ECB 日线后台循环

**Files:**
- Modify: `server.py`（lifespan 任务区 + `_next_fund_nav_run` 之后）
- Test: `tests/test_ledger.py`（追加）

- [x] **Step 1: 写失败测试（追加）**

```python
import server


class TestLedgerFxLoop:
    def test_next_run_before_2235(self):
        assert server._next_ledger_fx_run(datetime(2026, 10, 3, 10, 0)) == \
               datetime(2026, 10, 3, 22, 35)

    def test_next_run_after_2235(self):
        assert server._next_ledger_fx_run(datetime(2026, 10, 3, 23, 0)) == \
               datetime(2026, 10, 4, 22, 35)

    def test_loop_backfills_and_upserts(self, monkeypatch):
        """循环体：一次区间请求 → 逐条 upsert（无短周期重试）。"""
        calls = {"fetch": 0, "upsert": 0}

        async def fake_fetch(start, end, proxy_urls=None):
            calls["fetch"] += 1
            return [("2026-10-02", 6.7044, 7.8463)]

        async def fake_upsert(d, cny, hkd):
            calls["upsert"] += 1
            assert (d, cny, hkd) == ("2026-10-02", 6.7044, 7.8463)

        async def fake_count():
            return 0

        monkeypatch.setattr(server.ledger, "fetch_ecb_rates", fake_fetch)
        monkeypatch.setattr(server.db, "upsert_ledger_fx_rate", fake_upsert)
        monkeypatch.setattr(server.db, "get_ledger_fx_count", fake_count)
        run(server._ledger_fx_sync_once())
        assert calls == {"fetch": 1, "upsert": 1}
```

（文件头补 `from datetime import datetime` —— tests/test_ledger.py 顶部。）

- [x] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：`AttributeError: module 'server' has no attribute '_next_ledger_fx_run'`

- [x] **Step 3: 实现**

server.py 头部 import 区加 `import ledger`。`_next_fund_nav_run` 函数之后追加：

```python
def _next_ledger_fx_run(now: datetime) -> datetime:
    """ECB 参考价下次拉取时刻：每日 22:35（ECB 定盘约 22:00 北京时间后）。"""
    target = now.replace(hour=22, minute=35, second=0, microsecond=0)
    if now >= target:
        target = (now + timedelta(days=1)).replace(hour=22, minute=35, second=0, microsecond=0)
    return target


async def _ledger_fx_sync_once() -> int:
    """补齐 ECB 参考价缓存：从 ledger 起点或库内最后日期+1 到今天，一次区间请求。"""
    count = await db.get_ledger_fx_count()
    start = LEDGER_FX_START if count == 0 else None
    if start is None:
        rows = await db.get_ledger_events()
        del rows
        # 直接用库内最大日期+1（免单独 max 查询的复杂度，用现有辅助）
    last = await db.get_ledger_fx_for_date(date.today())
    # get_ledger_fx_for_date 返回 <= today 最近一条；有则从其次日拉，无则从起点拉
    start = (date.fromisoformat(last["rate_date"]) + timedelta(days=1)).isoformat() \
        if last else ledger.LEDGER_FX_START
    end = date.today().isoformat()
    if start > end:
        return 0
    proxies = [p["proxy"] for p in await db.get_proxy_list()][:2] if hasattr(db, "get_proxy_list") else []
    rows = await ledger.fetch_ecb_rates(start, end, proxy_urls=proxies)
    for d, cny, hkd in rows:
        await db.upsert_ledger_fx_rate(d, cny, hkd)
    if rows:
        logger.info("ECB 参考价缓存 +%d 条(%s..%s)", len(rows), rows[0][0], rows[-1][0])
    return len(rows)


async def _ledger_fx_loop():
    """每日 22:35 补拉 ECB 参考价；失败次日再试（不做短周期重试放大请求）。"""
    while True:
        try:
            await asyncio.sleep(max(1, (_next_ledger_fx_run(datetime()) - datetime()).total_seconds()))
            await _ledger_fx_sync_once()
        except asyncio.CancelledError:
            logger.info("ECB 参考价循环已停止")
            break
        except Exception as e:
            logger.warning("ECB 参考价同步失败(明日重试): %s", e)
            await asyncio.sleep(3600)
```

lifespan 内 `email_task = asyncio.create_task(_email_loop())` 之后加：

```python
    ledger_fx_task = asyncio.create_task(_ledger_fx_loop())
```

并在对应 shutdown 取消区把 `ledger_fx_task` 与其他 task 一并 cancel/await（跟随该区现有写法，变量名列在同一列表中）。

> 实现注意：`_ledger_fx_sync_once` 里第一段 `count`/`rows = await db.get_ledger_events(); del rows` 是冗余草稿，实现时删除这两句，仅保留 `last = await db.get_ledger_fx_for_date(...)` 起的逻辑（见代码块后半）。代理列表：若 `db.get_proxy_list` 不存在该属性则传空（httpx 直连，frankfurter 大陆可达性已在本地验证过）。

- [x] **Step 4: 跑测试确认通过**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：15 passed

- [x] **Step 5: 提交**

```bash
git -C /root/workspace/project/qdii add server.py tests/test_ledger.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): ECB参考价每日22:35补拉循环(区间一次请求/失败次日重试/挂载lifespan)"
```

---

### Task 5: server.py — API 三端点

**Files:**
- Modify: `server.py`（HoldingUpdateInput 模型区附近 + `/api/portfolio/holding` 端点之后）
- Test: `tests/test_ledger.py`（追加）

- [x] **Step 1: 写失败测试（追加）**

```python
from fastapi.testclient import TestClient

client = TestClient(server.app)


class TestLedgerApi:
    def test_post_events_upsert(self, monkeypatch):
        upserted = []

        async def fake_upsert(**ev):
            upserted.append(ev)
            return 1

        monkeypatch.setattr(server.db, "upsert_ledger_event", fake_upsert)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session_ok())
        client.cookies.set("session_token", "t-ledger-1")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-05", "event_type": "inflow", "account": "hsbc",
                 "direction": "+", "amount_ccy": "USD", "amount": 100, "ref_no": "TEST-1",
                 "usd_equivalent": 100, "cny_amount": 670}]})
            assert r.status_code == 200 and r.json()["ok"] is True
            assert r.json()["applied"] == 1
            assert upserted[0]["ref_no"] == "TEST-1"
        finally:
            client.cookies.clear()

    def test_post_events_bad_type_rejected(self, monkeypatch):
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session_ok())
        client.cookies.set("session_token", "t-ledger-2")
        try:
            r = client.post("/api/ledger/events", json={"events": [
                {"event_date": "2026-10-05", "event_type": "hack", "account": "hsbc",
                 "direction": "+", "amount_ccy": "USD", "amount": 1, "ref_no": "TEST-2"}]})
            assert r.status_code == 400
        finally:
            client.cookies.clear()

    def test_report_endpoint(self, monkeypatch):
        # 发布版脱敏：fixture 基线数值以 <基线> 占位
        async def fake_events(event_type=None, year=None, account=None):
            return [e for e in SEED_EVENTS]

        async def fake_value():
            return <资产基线>

        async def fake_cash():
            return [{"currency": "HKD", "amount": <HKD余额基线>}, {"currency": "USD", "amount": <USD余额基线>}]

        monkeypatch.setattr(server.db, "get_ledger_events", fake_events)
        monkeypatch.setattr(server.db, "get_latest_portfolio_value", fake_value)
        monkeypatch.setattr(server.db, "get_cash_accounts", fake_cash)
        monkeypatch.setattr(server.db, "get_session", _fake_valid_session_ok())
        monkeypatch.setattr(server, "market_cache",
                            [{"key": "USDCNH", "price": <汇率基线>}])
        client.cookies.set("session_token", "t-ledger-3")
        try:
            r = client.get("/api/ledger/report")
            assert r.status_code == 200
            data = r.json()
            assert data["ok"] is True
            assert abs(data["summary"]["total_cny_invested"] - <总投入基线>) < 0.01
            assert abs(data["identity"]["net_cny"] - <净盈亏基线>) < 0.5
        finally:
            client.cookies.clear()
```

（`_fake_valid_session_ok` 辅助：抄 tests/test_server_auth.py 的 `_fake_valid_session` —— 返回有效会话 dict 的闭包。实现时先 `grep -n "_fake_valid_session" tests/test_server_auth.py` 确认字段名后原样拷贝到 tests/test_ledger.py。）

- [x] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：report 404 / 函数未定义

- [x] **Step 3: 实现**

模型区（`HoldingUpdateInput` 之后）加：

```python
class LedgerEventInput(BaseModel):
    event_date: str
    event_type: str
    account: str
    direction: str
    amount_ccy: str
    amount: float
    ref_no: str
    channel: Optional[str] = None
    recv_ccy: Optional[str] = None
    recv_amount: Optional[float] = None
    usd_equivalent: Optional[float] = None
    cny_amount: Optional[float] = None
    mid_rate: Optional[float] = None
    note: Optional[str] = None
```

`/api/portfolio/holding` 端点之后加：

```python
# ---------------------------------------------------------------------------
# Ledger (记账事件流)
# ---------------------------------------------------------------------------

_LEDGER_TYPES = {"inflow", "fx", "fee", "reward", "expense", "transfer", "realized", "balance"}
_LEDGER_ACCOUNTS = {"za_bank", "za_sec", "hsbc", "zf", "ccb", "cmb", "cib", "external"}
_LEDGER_CCYS = {"CNY", "HKD", "USD"}


@app.post("/api/ledger/events")
async def ledger_events_post(data: dict):
    """批量幂等入库记账事件（AI 唯一录入通道；同 ref_no=覆盖更新）。"""
    events = data.get("events") or []
    if not events or not isinstance(events, list):
        return JSONResponse(status_code=400, content={"ok": False, "message": "events 非空列表"})
    for ev in events:
        if ev.get("event_type") not in _LEDGER_TYPES:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": f"非法 event_type: {ev.get('event_type')}"})
        if ev.get("account") not in _LEDGER_ACCOUNTS:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": f"非法 account: {ev.get('account')}"})
        if ev.get("direction") not in ("+", "-"):
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": "direction 仅 + 或 -"})
        if ev.get("amount_ccy") not in _LEDGER_CCYS:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": "amount_ccy 仅 CNY/HKD/USD"})
        if not ev.get("ref_no") or not ev.get("event_date"):
            return JSONResponse(status_code=400, content={"ok": False, "message": "ref_no/event_date 必填"})
    for ev in events:
        await db.upsert_ledger_event(
            event_date=ev["event_date"], event_type=ev["event_type"],
            account=ev["account"], channel=ev.get("channel"),
            direction=ev["direction"], amount_ccy=ev["amount_ccy"],
            amount=ev["amount"], recv_ccy=ev.get("recv_ccy"),
            recv_amount=ev.get("recv_amount"), usd_equivalent=ev.get("usd_equivalent"),
            cny_amount=ev.get("cny_amount"), mid_rate=ev.get("mid_rate"),
            note=ev.get("note"), ref_no=ev["ref_no"])
    return {"ok": True, "applied": len(events)}


@app.get("/api/ledger/events")
async def ledger_events_get(event_type: Optional[str] = Query(None),
                            year: Optional[int] = Query(None),
                            account: Optional[str] = Query(None)):
    return {"ok": True, "events": await db.get_ledger_events(event_type, year, account)}


@app.get("/api/ledger/report")
async def ledger_report():
    """全套记账报表（纯实时计算，口径=盘点v3）。"""
    events = await db.get_ledger_events()
    total_assets = await db.get_latest_portfolio_value()
    cash = await db.get_cash_accounts()
    usdcnh = next((m.get("price") for m in market_cache
                   if m.get("key") == "USDCNH" and m.get("price")), None)
    report = ledger.build_report(events, total_assets_usd=total_assets,
                                 usdcnh=usdcnh, cash_accounts=cash)
    report["ok"] = True
    return report
```

- [x] **Step 4: 跑测试确认通过**

```bash
python3 -m pytest tests/test_ledger.py -q
```
预期：18 passed

- [x] **Step 5: 提交**

```bash
git -C /root/workspace/project/qdii add server.py tests/test_ledger.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): POST/GET事件+GET报表三端点(枚举校验/复用session鉴权/行情缓存取USDCNH)"
```

---

### Task 6: 前端「记账」section

**Files:**
- Modify: `static/portfolio.html`（持仓 section 之后加区块；`refresh(); loadChart();` 行改为加 `loadLedger();`）

- [x] **Step 1: HTML 区块**（`<!-- 编辑持仓对话框 -->` 之前插入；样式复用现有 .card/.table-wrap）

```html
<section class="card pad" id="ledgerCard">
  <h3>记账 · 投入与通道成本 <span id="ledgerPending" style="color:var(--txt2);font-size:12px"></span></h3>
  <div id="ledgerSummary" class="sk">加载中…</div>
  <div class="table-wrap"><table id="ledgerChannels"><thead>
    <tr><th>通道</th><th>笔数</th><th>加权点差%</th><th>损失USD</th></tr>
  </thead><tbody></tbody></table></div>
  <div style="margin:8px 0 4px">
    <select id="ledgerFilter" onchange="loadLedgerEvents()">
      <option value="">全部事件</option>
      <option value="inflow">入金</option><option value="fx">换汇</option>
      <option value="fee">费用</option><option value="reward">奖励</option>
      <option value="expense">消耗</option><option value="transfer">转出</option>
      <option value="realized">已实现</option><option value="balance">余额快照</option>
    </select>
    <span id="ledgerBalanceChk" style="font-size:12px;margin-left:12px"></span>
  </div>
  <div class="table-wrap" style="max-height:320px;overflow:auto"><table id="ledgerEvents"><thead>
    <tr><th>日期</th><th>类型</th><th>账户</th><th>通道/标的</th><th>金额</th><th>USD等值</th><th>备注</th></tr>
  </thead><tbody></tbody></table></div>
</section>
```

- [x] **Step 2: JS**（`saveCash` 函数之后、`const val = ...` 之前插入）

```javascript
// ===== 记账模块 =====
const LEDGER_TYPE_NAME = {inflow:'入金', fx:'换汇', fee:'费用', reward:'奖励', expense:'消耗',
  transfer:'转出', realized:'已实现', balance:'余额快照'};
async function loadLedger() {
  try {
    const r = await fetch('/api/ledger/report'); const d = await r.json();
    if (!r.ok || !d.ok) return;
    const s = d.summary, i = d.identity;
    document.getElementById('ledgerPending').textContent =
      s.pending_count > 0 ? `（${s.pending_count} 笔待补全）` : '';
    const rows = [
      ['总投入(CNY)', s.total_cny_invested.toLocaleString()],
      ['加权购汇成本', s.avg_cost], ['USD化入账', s.usd_in.toLocaleString()],
      ['点差损失(USD)', s.fx_loss_usd], ['显性费用(USD)', s.fee_usd],
      ['奖励/分红(USD)', s.reward_usd], ['已实现盈亏(USD)', s.realized_usd],
      ['消耗(USD)', s.expense_usd]];
    let html = '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px">';
    for (const [k, v] of rows) html += `<div><div style="font-size:12px;color:var(--txt2)">${k}</div><b>${v}</b></div>`;
    html += '</div>';
    if (i && i.net_cny != null) html +=
      `<div style="font-size:13px">净盈亏 <b style="color:${i.net_cny>=0?'#e0562c':'#0a8f4a'}">${i.net_cny.toLocaleString()} CNY (${i.net_pct}%)</b>` +
      ` = 增值 ${i.gain_usd} USD(${i.gain_pct}%) − 汇损 ${i.fx_loss_cny} − 消耗 ${i.expense_cny}` +
      `（USDCNH@${i.usdcnh}；保活转出 ${i.external_transfer_usd} USD 面值不计损益）</div>`;
    document.getElementById('ledgerSummary').innerHTML = html;
    const tb = document.querySelector('#ledgerChannels tbody');
    tb.innerHTML = d.channels.map(c =>
      `<tr><td>${c.channel}</td><td>${c.n}</td><td>${c.wavg_spread_pct}%</td><td>${c.loss_usd}</td></tr>`).join('');
    const b = d.balances, chk = document.getElementById('ledgerBalanceChk');
    if (b && b.ledger && Object.keys(b.ledger).length) {
      const parts = Object.entries(b.diff).map(([ccy, df]) =>
        `${ccy}: ${b.ledger[ccy]} vs 系统${b.system[ccy] ?? '—'}${df == null ? '' : (Math.abs(df) < 0.01 ? ' ✓' : ` ⚠差${df}`)}`);
      chk.textContent = '对账(结单vs系统): ' + parts.join('；');
    }
  } catch (e) { /* 静默：区块自愈 */ }
}
async function loadLedgerEvents() {
  try {
    const t = document.getElementById('ledgerFilter').value;
    const r = await fetch('/api/ledger/events' + (t ? `?type=${t}` : ''));
    const d = await r.json();
    if (!r.ok || !d.ok) return;
    document.querySelector('#ledgerEvents tbody').innerHTML = d.events.map(e => {
      let amt = `${e.direction}${e.amount} ${e.amount_ccy}`;
      if (e.recv_amount != null) amt += ` → ${e.recv_amount} ${e.recv_ccy}`;
      return `<tr><td>${e.event_date}</td><td>${LEDGER_TYPE_NAME[e.event_type] || e.event_type}</td>` +
        `<td>${e.account}</td><td>${e.channel || ''}</td><td>${amt}</td>` +
        `<td>${e.usd_equivalent ?? ''}</td><td style="font-size:12px;color:var(--txt2)">${e.note || ''}</td></tr>`;
    }).join('');
  } catch (e) { /* 静默 */ }
}
```

启动行改：`refresh(); loadChart();` → `refresh(); loadChart(); loadLedger(); loadLedgerEvents();`

- [x] **Step 3: 语法自检**

```bash
node --check <(sed -n '/<script>/,/<\/script>/p' static/portfolio.html | sed '1d;$d') 2>/dev/null || python3 -c "
import re
html = open('static/portfolio.html').read()
js = re.search(r'<script>(.*)</script>', html, re.S).group(1)
compile(js, 'portfolio.js', 'exec')
print('JS 语法 OK')"
```
预期：`JS 语法 OK`

- [x] **Step 4: 提交**

```bash
git -C /root/workspace/project/qdii add static/portfolio.html
git -C /root/workspace/project/qdii commit -m "feat(ledger): 前端记账区块——恒等式总览/通道排名/事件筛选表/结单对账位"
```

---

### Task 7: 入金邮件自动入账（采样 gate + 条件实现）

> **执行结论（2026-10-03）**：首采样走 `fetch_hsbc_emails`（FROM HSBC_SENDER）只见成交邮件；
> 用户指出 ZA Bank 通知邮件含换汇/转出/派息版式。二轮全收件箱采样实证存在
> 外汇交易/CHATS 转出/基金派息等通知版式（金额、交易编号、账户尾号等个人事实已脱敏）。
> 修正落地：部分 reward 日期按派息实际到账日修正、fx 事件补交易编号 note、补 transfer 事件、
> 新增 `fetch_za_notice_emails(days=30)`（完整地址 SEARCH，QQ IMAP 裸域搜不到）。
> 入金（CNY 原币 SWIFT 入账）无邮件版式（该通道不发通知邮件）——**入金自动入账
> 维持人工/AI 录入，换汇/转出/派息版式已沉淀进 skill 采样流程**。

**Files:**
- Modify: `email_trader.py`（poll_once 内）
- Test: `tests/test_ledger.py`（条件追加）

- [x] **Step 1: 采样真实邮箱（gate）**

```bash
python3 - <<'EOF'
import email_trader as et
mails = et.fetch_hsbc_emails(days=110)
for m in mails:
    subj = (m.get("subject") or "").strip()
    mid = m.get("message_id", "")[:60]
    print(f"{m.get('date','')[:16]} | {subj[:70]} | {mid}")
print(f"共 {len(mails)} 封(含未解析)")
EOF
```

判定：subject 含「入金/存入/Credit/Deposit/Remittance/CHATS/收账/资金」等关键词且**非**成交通知的邮件样本 ≥1 封 → 进入 Step 2；否则 → **降级决定**：在 `账户成本盈亏盘点_20261003.md` 末尾记一行「入金邮件自动入账：邮箱无可用入金通知版式，降级为仅结单录入（待出现样本后补）」并提交，跳过本任务其余步骤。

- [x] **Step 2（条件）: 实现解析与自动入账**

email_trader.py 加（版式正则以 Step 1 采到的真实邮件为准，此处为骨架落点；解析函数必须用真实样本逐字段核对后再写测试）：

```python
async def apply_deposit_ingest(mail: dict) -> dict | None:
    """入金邮件 → ledger inflow 事件（ref_no=EMAIL-<message_id> 幂等；
    mid_rate/usd_equivalent 取当日 ECB 缓存，cny_amount 缺省 None=pending 待结单补全）。"""
```

poll_once 循环内，`parse_hsbc_trade` 返回 None 的分支后追加 deposit 分支调用。测试：合成 Step 1 真实样本的脱敏文本进 tests/test_ledger.py，断言 ref_no 幂等与 pending 标记。

- [x] **Step 3（条件）: 跑测试 + 提交**

```bash
python3 -m pytest tests/test_ledger.py -q
git -C /root/workspace/project/qdii add email_trader.py tests/test_ledger.py
git -C /root/workspace/project/qdii commit -m "feat(ledger): 入金邮件自动入账(EMAIL-幂等/ECB补价/cny待补pending)"
```

---

### Task 8: 线上修数四项 + 种子入库 + 线上验证

**Files:**
- 无代码文件（服务器操作）；`AGENTS.md` 更新见 Task 9

- [x] **Step 1: 部署代码**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='monthStatement' --exclude='.playwright-cli' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

- [x] **Step 2: 修数（幂等 UPDATE×4）**

```bash
ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e \"
UPDATE cash_accounts SET amount=<HKD余额基线> WHERE currency='HKD';
UPDATE cash_accounts SET amount=<USD余额基线> WHERE currency='USD';
UPDATE holdings SET cost_price=<NVDA成本基线> WHERE symbol='NVDA' AND cost_price<><NVDA成本基线>;
UPDATE sys_config SET value='<总投入基线>' WHERE \\\`key\\\`='cny_invested';
SELECT * FROM cash_accounts; SELECT symbol,cost_price FROM holdings WHERE symbol='NVDA';\""
```
预期：各修数项与基线一致（发布版脱敏，实际值见本地私有基线文档）

- [x] **Step 3: 种子入库 + ECB 回填 + 服务重载**

```bash
ssh root@<server-ip> "cd /opt/qdii && python3 ledger_seed.py && systemctl restart qdii && sleep 12 && systemctl is-active qdii && journalctl -u qdii --since '1 min ago' | tail -15"
```
预期：`ledger seed 完成: 67 笔事件`；服务 active；日志出现 `ECB 参考价缓存 +N 条(2026-06-18..2026-10-02)`

- [x] **Step 4: 线上报表验证（对 v3 基线）**

```bash
ssh root@<server-ip> "TOKEN=\$(mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -N -e \"SELECT token FROM sessions ORDER BY created_at DESC LIMIT 1\"); curl -s -H \"Cookie: session_token=\$TOKEN\" http://127.0.0.1:8888/api/ledger/report | python3 -m json.tool | head -40"
```
核对：`GET /api/ledger/report` 各 summary/identity 项与本地基线一致（发布版脱敏，数值见本地私有文档；±1 为 USDCNH 行情浮动余量）/ balances.diff HKD、USD 均 ✓（待 portfolio 循环下一轮把总资产刷新后恒等式精确）。

---

### Task 9: qdii-ledger-ingest skill + AGENTS.md 更新

**Files:**
- Create: `~/.claude/skills/qdii-ledger-ingest/SKILL.md`（用 skill-creator 规范）
- Modify: `AGENTS.md`

- [x] **Step 1: 加载 skill-creator 写 skill**——内容：触发词（处理月结单/录入结单/ledger ingest）；SOP：PDF→`python3 -c "import fitz;..."` pymupdf 提取 → 按 v3 版式规则表解析新事件（ZA/HSBC INVSTM0011/HSBC One IPSSTM0007/致富）→ `SELECT MAX(event_date)` 防重 → curl POST `/api/ledger/events`（token 取 sessions 表）→ GET report 回归核对（恒等式 ±1 / 对账位 diff）→ 输出差异清单待确认；附 ref_no 规则 `{YYYYMM}-{account}-{seq}` 与 pending 补全流程。
- [x] **Step 2: AGENTS.md 更新**——模块结构表加 `ledger.py / ledger_seed.py` 行；「关键口径契约」加 ledger 恒等式一行（口径=本地基线，HKD→USD 静态折算，cny_invested=全额口径）；测试基线数更新。
- [x] **Step 3: 提交**

```bash
git -C /root/workspace/project/qdii add AGENTS.md
git -C /root/workspace/project/qdii commit -m "docs: AGENTS.md 补记账模块结构与ledger口径契约+测试基线"
```

---

### Task 10: 全量回归 + 收尾

- [x] **Step 1: 全量测试**

```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_ledger.py -q
```
预期：全部 passed（基线 + 18 新增），0 failed

- [x] **Step 2: 线上冒烟**

`systemctl is-active qdii` + `journalctl -u qdii --since '5 min ago'` 无异常堆栈 + 浏览器访问 portfolio.html 记账区块渲染正常。

- [x] **Step 3: 更新盘点文档遗留事项**——本地私有基线文档 §十二补记：勘误已全部落库（余额/成本/投入口径修正）；ledger 模块上线。
- [x] **Step 4: 提交收尾**

```bash
git -C /root/workspace/project/qdii add "账户成本盈亏盘点_20261003.md"
git -C /root/workspace/project/qdii commit -m "docs: 盘点文档补记ledger上线与勘误落库结论"
```

---

## Self-Review 记录

- **Spec 覆盖**：§2 表→Task1；§4 报表→Task3；§5 邮件→Task7（含降级路径）；§6 前端→Task6；§7 种子/验证→Task2/3/8；§8 修数→Task8（含跨账户桶口径修正）；§9 skill→Task9；§10 工作流→Task7/9。✓
- **占位符**：Task 7 Step 2 为条件任务且 gate 明确（真实版式驱动），其余步骤代码完整。✓
- **类型一致性**：`upsert_ledger_event(**ev)` 关键字与 `E()` 构造器键完全一致；`build_report(events, *, total_assets_usd, usdcnh, cash_accounts)` 三处调用一致。✓
- 计划基于 2026-10-03 代码现状编写；执行中若发现接口签名漂移（如 sessions 表字段名），先 grep 核对再继续。
