# 已清仓持仓与卖点标记 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 清仓标的沉入「已清仓」折叠分区（周期回放推导已实现盈亏 + 卖出后涨跌幅），K线弹层标记买卖点并列出每笔至今涨跌幅。

**Architecture:** `trade_records` 全量回放（holdings 数量作锚点反推期初仓位）纯函数推导周期，不落新表；overview 端点注入 `closed` 块（60s 缓存，不进 SSE 载荷）；kline 端点 payload 追加 `trades`；前端主表过滤 qty>0、新增已清仓分区、K线弹层画 B/S 标记 + 成交列表。零新增外部请求。

**Tech Stack:** Python 3 / FastAPI / aiomysql / 原生 JS 单文件前端 / pytest（monkeypatch 桩 DB，零连库）。

**Spec:** `docs/superpowers/specs/2026-10-11-closed-positions-and-trade-marks-design.md`

**约定：**
- 行号为写作时基线，编辑时以锚文本为准（`grep -n` 重新定位）。
- 每个任务独立可交付、独立提交；测试命令均在仓库根 `/root/workspace/project/qdii` 执行。
- 新增 Python 代码 `@author yys`；禁止 TODO/伪代码。
- 基线回归命令（AGENTS.md 约定）：
  `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py tests/test_alert_api.py tests/test_daily_bars.py tests/test_longport_push.py tests/test_macro_fetcher.py tests/test_ledger.py tests/test_nav.py -q`（当前 441 passed；本计划完成后再 +N）。

---

### Task 1: `ledger.build_closed_positions` 纯函数（TDD）

**Files:**
- Test: `tests/test_closed_positions.py`（新建）
- Modify: `ledger.py`（import 行 + 文件尾部追加）

- [ ] **Step 1.1: 写失败测试（新建测试文件，含 9 个用例）**

新建 `tests/test_closed_positions.py`，内容：

```python
"""已清仓周期推导测试：build_closed_positions 回放口径（spec
2026-10-11 §3）+ trade_records 查询 SQL 形状 + API closed/kline.trades。

纯计算部分无 DB 依赖；API 部分打桩 db 层（同 test_alert_api.py 模式），
不起服务不连 MySQL。
"""
import asyncio
import time
from datetime import date, datetime

import database as db
import ledger
import server
from fastapi.testclient import TestClient


def run(coro):
    return asyncio.run(coro)


def _tr(sym, side, qty, price, dt, **kw):
    """构造 trade_records 形状行；email_date/created_at 传 datetime。"""
    row = {"symbol": sym, "side": side, "quantity": qty, "price": price,
           "email_date": dt, "created_at": dt, "name": "", "market": "US"}
    row.update(kw)
    return row


def _h(sym, qty, cost=0.0, name="", market="US"):
    """构造 holdings 形状行。"""
    return {"symbol": sym, "quantity": qty, "cost_price": cost,
            "name": name, "market": market}


# ---------------------------------------------------------------------------
# 纯计算：build_closed_positions
# ---------------------------------------------------------------------------

def test_single_cycle_exact():
    """单周期：买10@100 → 卖10@120，realized=200/exit_avg=120/36天。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1 and rows[0].get("no_records") is not True
    r = rows[0]
    assert r["symbol"] == "MU"
    assert r["exit_date"] == "2026-02-10"
    assert r["hold_days"] == 36
    assert r["exit_qty"] == 10
    assert r["exit_avg"] == 120.0
    assert r["realized_pnl"] == 200.0
    assert r["realized_pct"] == 20.0
    assert r["approx"] is False


def test_partial_sells_then_close():
    """部分减仓后清仓：买10@100 → 卖4@110 → 卖6@120，realized=160/均价116。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 4, 110, datetime(2026, 1, 20)),
              _tr("MU", "SELL", 6, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1
    assert rows[0]["exit_qty"] == 10
    assert rows[0]["exit_avg"] == 116.0
    assert rows[0]["realized_pnl"] == 160.0
    assert rows[0]["realized_pct"] == 16.0


def test_reentry_two_cycles():
    """再入场：两轮归零 → 两个周期，输出按 exit_date 倒序。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 110, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 10, 130, datetime(2026, 3, 1)),
              _tr("MU", "SELL", 5, 140, datetime(2026, 3, 15)),
              _tr("MU", "SELL", 5, 150, datetime(2026, 4, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 2
    assert rows[0]["exit_date"] == "2026-04-01"      # 倒序：新周期在前
    assert rows[0]["realized_pnl"] == 150.0          # (700+750)-1300
    assert rows[0]["exit_avg"] == 145.0
    assert rows[1]["exit_date"] == "2026-01-20"
    assert rows[1]["realized_pnl"] == 100.0
    assert rows[1]["exit_avg"] == 110.0


def test_anchor_initial_position():
    """期初锚点：无买入记录直接卖5@100，cost=80 → q0=5，realized=100，approx。"""
    trades = [_tr("AMD", "SELL", 5, 100, datetime(2026, 1, 10))]
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)])
    assert len(rows) == 1
    assert rows[0]["realized_pnl"] == 100.0          # 500 - 5×80
    assert rows[0]["realized_pct"] == 25.0
    assert rows[0]["approx"] is True


def test_anchor_negative_clamped():
    """锚点负值钳制：记录净量5 但 holdings=0 → q0 钳 0、周期打 approx 不抛异常。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 5, 50, datetime(2026, 3, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1                            # 首周期闭合；尾部未闭合丢弃
    assert rows[0]["realized_pnl"] == 200.0
    assert rows[0]["approx"] is True


def test_open_position_no_cycle():
    """未闭合：qty>0（部分减仓后仍持有）→ 不产生周期。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 5, 110, datetime(2026, 1, 20))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 5)])
    assert rows == []


def test_placeholder_no_records():
    """占位行：qty==0 且无任何成交记录 → 单条 no_records 行，字段全 None。"""
    rows = ledger.build_closed_positions([], [_h("F", 0, name="福特")])
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "F" and r["no_records"] is True
    assert r["exit_date"] is None and r["exit_avg"] is None
    assert r["realized_pnl"] is None and r["hold_days"] is None


def test_email_date_none_falls_back_created_at():
    """email_date 为 NULL → 回退 created_at 参与回放（日期与排序口径）。"""
    trades = [_tr("MU", "BUY", 10, 100, None, created_at=datetime(2026, 1, 5, 9, 0)),
              _tr("MU", "SELL", 10, 120, None, created_at=datetime(2026, 1, 20, 9, 0))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1
    assert rows[0]["exit_date"] == "2026-01-20"
    assert rows[0]["realized_pnl"] == 200.0


def test_orphan_symbol_holdings_row_deleted():
    """孤儿 symbol：holdings 行已删但有完整成交 → 周期照常推导，名称取末条记录。"""
    trades = [_tr("PCG", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("PCG", "SELL", 10, 120, datetime(2026, 2, 10),
                  name="PG&E", market="US")]
    rows = ledger.build_closed_positions(trades, [])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "PCG"
    assert rows[0]["name"] == "PG&E"
    assert rows[0]["realized_pnl"] == 200.0
```

- [ ] **Step 1.2: 运行确认失败**

Run: `python3 -m pytest tests/test_closed_positions.py -q`
Expected: FAIL（`AttributeError: module 'ledger' has no attribute 'build_closed_positions'`）

- [ ] **Step 1.3: 实现 ledger.py 纯函数**

`ledger.py` 第 10 行 import 改为：

```python
from datetime import date, datetime
```

文件尾部追加（`build_nav_series` 之后）：

```python
# ---------------------------------------------------------------------------
# 已清仓周期推导（trade_records 回放，纯计算无 IO）
# ---------------------------------------------------------------------------

def _dt_or_none(v):
    """宽容取 datetime：datetime 原样 / date 补零点 / ISO 字符串截前 10 解析。"""
    if v is None or isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    try:
        return datetime.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _replay_closed_cycles(sym: str, meta: dict, cur_qty: float,
                          recs: list[dict]) -> list[dict]:
    """单 symbol 数量轨迹回放（build_closed_positions 内部辅助）。

    锚点反推期初：q0 = cur_qty − Σ(BUY) + Σ(SELL)；q0>0 = email 记录前的
    手工录入期初（期初成本 q0×cost_price 近似，首周期 approx）；q0<0（手工
    改量/删行失真）钳 0 且首周期打 approx。数量每次归零 → 周期闭合：
    realized = Σ卖额 − (Σ买额 + 期初成本)。@author yys
    """
    net = sum(float(t["quantity"]) * (1 if t["side"] == "BUY" else -1)
              for t in recs)
    q0 = cur_qty - net
    anchor_approx = q0 > 1e-9
    if q0 < -1e-9:
        q0 = 0.0
        anchor_approx = True
    qty = q0
    first_cycle = q0 <= 0            # q0>0 时首个周期已由期初开启
    basis = q0 * float(meta.get("cost_price") or 0) if q0 > 0 else 0.0
    start = _dt_or_none(recs[0].get("email_date") or recs[0].get("created_at")) \
        if q0 > 0 else None
    sell_amt = sell_qty = 0.0
    cyc_approx = anchor_approx
    out: list[dict] = []
    for t in recs:
        q, p = float(t["quantity"]), float(t["price"])
        dt = _dt_or_none(t.get("email_date") or t.get("created_at"))
        if t["side"] == "BUY":
            if start is None:        # 新周期首笔（上一周期已闭合）
                cyc_approx = anchor_approx if first_cycle else False
                first_cycle = False
                start, basis, sell_amt, sell_qty = dt, 0.0, 0.0, 0.0
            qty += q
            basis += q * p
        else:                        # SELL
            qty -= q
            sell_amt += q * p
            sell_qty += q
            if qty < -1e-9:          # 轨迹失真防御：钳制归零
                qty = 0.0
                cyc_approx = True
            if abs(qty) <= 1e-9:
                qty = 0.0
                realized = sell_amt - basis
                exit_avg = sell_amt / sell_qty if sell_qty > 0 else None
                out.append({
                    "symbol": sym,
                    "name": meta.get("name") or "",
                    "market": meta.get("market") or "",
                    "exit_date": dt.date().isoformat() if dt else None,
                    "hold_days": (dt.date() - start.date()).days
                                 if dt and start else None,
                    "exit_qty": round(sell_qty, 4),
                    "exit_avg": round(exit_avg, 4) if exit_avg else None,
                    "realized_pnl": round(realized, 2),
                    "realized_pct": round(realized / basis * 100, 2)
                                    if basis > 0 else None,
                    "approx": bool(cyc_approx),
                })
                # 周期状态复位（first_cycle 消耗语义：期初已开启则此处不再置位）
                start, basis, sell_amt, sell_qty = None, 0.0, 0.0, 0.0
                cyc_approx = False
    return out


def build_closed_positions(trades: list[dict], holdings: list[dict]) -> list[dict]:
    """由 trade_records 回放推导已清仓周期列表（round trip）+ 无成交占位行。

    口径（spec 2026-10-11 §3）：锚点反推期初数量、数量归零切周期、
    realized = Σ卖额 − Σ买额 − 期初成本（期初成本 = q0 × 当前 cost_price 近似）。
    trades 须按时间升序（get_all_trades 的 COALESCE 排序已保证）。
    返回：周期行按 exit_date 倒序在前；qty==0 且无周期的 holdings 行输出
    占位行（no_records=True、盈亏字段 None）沉底。@author yys
    """
    by_symbol: dict[str, list[dict]] = {}
    for t in trades:
        by_symbol.setdefault(t["symbol"], []).append(t)

    rows: list[dict] = []
    placeholders: list[dict] = []
    for h in holdings:
        sym = h["symbol"]
        recs = by_symbol.pop(sym, [])
        cur_qty = float(h["quantity"] or 0)
        cycles = _replay_closed_cycles(sym, h, cur_qty, recs)
        if cycles:
            rows.extend(cycles)
        elif cur_qty == 0:
            placeholders.append({
                "symbol": sym, "name": h.get("name") or "",
                "market": h.get("market") or "",
                "exit_date": None, "hold_days": None, "exit_qty": None,
                "exit_avg": None, "realized_pnl": None, "realized_pct": None,
                "approx": False, "no_records": True})

    # holdings 行已删除（历史手动清理）但有成交的 symbol：按 q0=0 回放，
    # 名称/市场取末条记录
    for sym, recs in by_symbol.items():
        if not recs:
            continue
        last = recs[-1]
        meta = {"name": last.get("name") or "", "market": last.get("market") or "",
                "cost_price": 0}
        rows.extend(_replay_closed_cycles(sym, meta, 0.0, recs))

    rows.sort(key=lambda r: r["exit_date"] or "", reverse=True)
    return rows + placeholders
```

- [ ] **Step 1.4: 运行确认通过**

Run: `python3 -m pytest tests/test_closed_positions.py -q`
Expected: 9 passed

- [ ] **Step 1.5: 提交**

```bash
git add ledger.py tests/test_closed_positions.py
git commit -m "feat: ledger新增build_closed_positions已清仓周期回放推导（锚点反推期初/归零切周期/近似打标）"
```

---

### Task 2: `database.get_all_trades` / `get_trades_by_symbol`

**Files:**
- Modify: `database.py`（`get_recent_trades` 之后，约 1293 行）
- Test: `tests/test_closed_positions.py`（追加）

- [ ] **Step 2.1: 追加失败测试（SQL 形状，同 test_ledger.py TestLedgerCrud 模式）**

在 `tests/test_closed_positions.py` 追加：

```python
# ---------------------------------------------------------------------------
# database 查询 SQL 形状（打桩 execute_dict，零连库）
# ---------------------------------------------------------------------------

class TestTradeQueries:
    def test_get_all_trades_sql(self, monkeypatch):
        cap = {}

        async def fake_ed(sql, args=None, _pool=None):
            cap["sql"], cap["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_ed)
        run(db.get_all_trades())
        assert "FROM trade_records" in cap["sql"]
        assert "COALESCE(email_date, created_at)" in cap["sql"]   # NULL 日期回退
        assert "ORDER BY" in cap["sql"]
        assert cap["args"] is None

    def test_get_trades_by_symbol_sql(self, monkeypatch):
        cap = {}

        async def fake_ed(sql, args=None, _pool=None):
            cap["sql"], cap["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_ed)
        run(db.get_trades_by_symbol("MU"))
        assert "FROM trade_records" in cap["sql"]
        assert "WHERE symbol = %s" in cap["sql"]
        assert "COALESCE(email_date, created_at)" in cap["sql"]
        assert cap["args"] == ("MU",)
```

- [ ] **Step 2.2: 运行确认失败**

Run: `python3 -m pytest tests/test_closed_positions.py::TestTradeQueries -q`
Expected: FAIL（`AttributeError: ... has no attribute 'get_all_trades'`）

- [ ] **Step 2.3: 实现 database.py 两个查询**

在 `database.py` 的 `get_recent_trades`（约 1285-1293 行）之后追加：

```python
async def get_all_trades() -> list[dict]:
    """trade_records 全量（时间升序，已清仓周期回放用）。

    排序 COALESCE(email_date, created_at)：email_date 缺失的记录按落库时间
    兜底参与排序，保证回放时间线单调。@author yys
    """
    rows = await execute_dict(
        "SELECT * FROM trade_records "
        "ORDER BY COALESCE(email_date, created_at), id")
    for r in rows:
        r["quantity"] = float(r["quantity"])
        r["price"] = float(r["price"])
    return rows


async def get_trades_by_symbol(symbol: str) -> list[dict]:
    """单 symbol 成交记录（时间升序，K线卖点标记/成交列表用）。@author yys"""
    rows = await execute_dict(
        "SELECT * FROM trade_records WHERE symbol = %s "
        "ORDER BY COALESCE(email_date, created_at), id", (symbol,))
    for r in rows:
        r["quantity"] = float(r["quantity"])
        r["price"] = float(r["price"])
    return rows
```

- [ ] **Step 2.4: 运行确认通过**

Run: `python3 -m pytest tests/test_closed_positions.py -q`
Expected: 11 passed

- [ ] **Step 2.5: 提交**

```bash
git add database.py tests/test_closed_positions.py
git commit -m "feat: database新增get_all_trades/get_trades_by_symbol（COALESCE时间升序）"
```

---

### Task 3: server.py closed 上下文缓存 + overview 注入

**Files:**
- Modify: `server.py`（`_nav_ctx_cache` 区域之后约 2619 行加缓存；`portfolio_overview` 内 `recent_trades` 块后约 2556 行注入）
- Test: `tests/test_closed_positions.py`（追加）

- [ ] **Step 3.1: 追加失败测试**

在 `tests/test_closed_positions.py` 追加：

```python
# ---------------------------------------------------------------------------
# API：overview closed 块（TestClient 打桩，同 test_alert_api._client 模式）
# ---------------------------------------------------------------------------

def _client(monkeypatch):
    async def fake_get_session(t):
        return {"username": "u", "expires": time.time() + 3600}
    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    c = TestClient(server.app)
    c.cookies.set("session_token", "tok")
    return c


def _reset_closed_cache(monkeypatch):
    monkeypatch.setattr(server, "_closed_ctx_cache",
                        {"ts": 0.0, "trades": None, "holdings": None})


def test_overview_closed_block(monkeypatch):
    """overview 注入 closed 块：周期行结构/数值正确（回放口径）。"""
    async def fake_trades():
        return [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
                _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]

    async def fake_holdings(enabled_only=False):
        return [_h("MU", 0)]

    monkeypatch.setattr(server.db, "get_all_trades", fake_trades)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    closed = r.json().get("closed")
    assert closed and closed[0]["symbol"] == "MU"
    assert closed[0]["realized_pnl"] == 200.0
    assert closed[0]["exit_avg"] == 120.0
    assert closed[0]["exit_date"] == "2026-02-10"


def test_overview_closed_db_error_degrades(monkeypatch):
    """closed 上下文 DB 异常 → 降级空数组，不 500（同 recent_trades 模式）。"""
    async def boom():
        raise RuntimeError("db down")

    async def fake_holdings(enabled_only=False):
        return []

    monkeypatch.setattr(server.db, "get_all_trades", boom)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    assert r.json().get("closed") == []
```

- [ ] **Step 3.2: 运行确认失败**

Run: `python3 -m pytest tests/test_closed_positions.py::test_overview_closed_block tests/test_closed_positions.py::test_overview_closed_db_error_degrades -q`
Expected: FAIL（`AttributeError: module 'server' has no attribute '_closed_ctx_cache'`；第二个用例因 `closed` 键不存在断言失败）

- [ ] **Step 3.3: 实现 server.py**

3a. 在 `_get_nav_ctx` 函数之后（约 2619 行，`@app.get("/api/portfolio/nav")` 之前）追加：

```python
# 已清仓上下文缓存（60s：全量 trades + holdings；周期重放纯内存微秒级，
# 缓存只为省 overview 高频轮询下的重复 DB 读）
_closed_ctx_cache = {"ts": 0.0, "trades": None, "holdings": None}


async def _get_closed_ctx() -> tuple[list[dict], list[dict]]:
    """已清仓周期重放上下文：全量成交 + 全量持仓（60s 缓存，新成交 60s 内
    eventual 生效）。@author yys"""
    now = time.time()
    if _closed_ctx_cache["trades"] is not None and \
            now - _closed_ctx_cache["ts"] < 60:
        return _closed_ctx_cache["trades"], _closed_ctx_cache["holdings"]
    trades, holdings = await db.get_all_trades(), await db.get_holdings()
    _closed_ctx_cache.update(ts=now, trades=trades, holdings=holdings)
    return trades, holdings
```

3b. 在 `portfolio_overview` 内 `recent_trades` try/except 块之后（约 2556 行，`return data` 之前）追加：

```python
    # 已清仓周期列表（trade_records 回放推导；留痕数据随 60s 缓存 eventual
    # 生效，不进 SSE 推送载荷——前端沿用 recent_trades 保活合并模式）
    try:
        _trades, _holdings = await _get_closed_ctx()
        data["closed"] = ledger.build_closed_positions(_trades, _holdings)
    except Exception:
        data["closed"] = []
```

- [ ] **Step 3.4: 运行确认通过**

Run: `python3 -m pytest tests/test_closed_positions.py -q`
Expected: 13 passed

- [ ] **Step 3.5: 提交**

```bash
git add server.py tests/test_closed_positions.py
git commit -m "feat: overview注入closed已清仓周期块（60s上下文缓存，DB异常降级空数组）"
```

---

### Task 4: server.py kline payload 增加 trades

**Files:**
- Modify: `server.py`（`kline` 端点缓存未命中分支，约 2441-2448 行）
- Modify: `tests/test_alert_api.py`（kline 相关用例补 `get_trades_by_symbol` 桩，防真实连库）
- Test: `tests/test_closed_positions.py`（追加）

- [ ] **Step 4.1: 追加失败测试**

在 `tests/test_closed_positions.py` 追加：

```python
def test_kline_trades_payload(monkeypatch):
    """kline 响应含 trades 数组（date=YYYY-MM-DD/side/price/quantity）。"""
    async def fake_bars(sym, limit=None):
        return [{"date": date(2026, 10, 1), "open": 1, "high": 2, "low": 0.5,
                 "close": 1.5, "volume": 10}]

    async def fake_rule(sym):
        return None

    async def fake_holdings(enabled_only=True):
        return []

    async def fake_trades(sym):
        return [{"symbol": "QQQ", "side": "BUY", "quantity": 10, "price": 100.0,
                 "email_date": datetime(2026, 9, 30, 10, 0),
                 "created_at": datetime(2026, 9, 30, 11, 0)}]

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_trades_by_symbol", fake_trades)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    d = client.get("/api/kline/QQQ?tf=day&range=1000").json()
    assert d["trades"] == [{"date": "2026-09-30", "side": "BUY",
                            "price": 100.0, "quantity": 10.0}]
```

- [ ] **Step 4.2: 运行确认失败**

Run: `python3 -m pytest tests/test_closed_positions.py::test_kline_trades_payload -q`
Expected: FAIL（`KeyError: 'trades'` 或断言 `None != [...]`）

- [ ] **Step 4.3: 实现 kline 端点 trades**

`server.py` kline 端点缓存未命中分支内，`payload.update(_kl_series(bars))` 之后、`_KLINE_CACHE[cache_key] = ...` 之前（约 2447 行）插入：

```python
        # 成交记录（买卖点标记/成交列表用；日期归一 YYYY-MM-DD 对齐日K轴，
        # email_date 缺失回退 created_at，随 (symbol,tf) 60s 缓存一并落）
        try:
            _trows = await db.get_trades_by_symbol(symbol)
        except Exception:
            _trows = []

        def _tdate(r):
            d = r.get("email_date") or r.get("created_at")
            return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]

        payload["trades"] = [
            {"date": _tdate(r), "side": r["side"], "price": float(r["price"]),
             "quantity": float(r["quantity"])} for r in _trows]
```

（`_trows` 查询失败吞异常为空数组：K线主数据可用性优先，成交标记缺失可降级。）

- [ ] **Step 4.4: 修补 test_alert_api.py 既有 kline 用例（防真实连库）**

新 db 调用会使 `tests/test_alert_api.py` 所有打 `/api/kline/*` 的用例尝试真实连库。在该文件 import 区（`import server` 之后，`def _client` 之前）追加 autouse fixture，为全文件默认打桩（个别用例仍可用自己的 monkeypatch.setattr 覆盖）：

```python
@pytest.fixture(autouse=True)
def _stub_trades_by_symbol(monkeypatch):
    """kline 端点新增 get_trades_by_symbol 调用：全文件默认桩空，
    防既有用例真实连库（需真实成交的用例自行 setattr 覆盖）。@author yys"""
    async def fake_trades(sym):
        return []
    monkeypatch.setattr(server.db, "get_trades_by_symbol", fake_trades)
```

确认该文件已 `import pytest`（是，其测试均用 monkeypatch fixture），无需新增 import。

- [ ] **Step 4.5: 运行确认通过（含既有 kline 用例不回归）**

Run: `python3 -m pytest tests/test_closed_positions.py tests/test_alert_api.py -q`
Expected: 全部 passed（test_closed_positions 14 + test_alert_api 原有用例数不变不挂）

- [ ] **Step 4.6: 提交**

```bash
git add server.py tests/test_closed_positions.py tests/test_alert_api.py
git commit -m "feat: kline端点payload追加trades成交数组（60s缓存随存，alert_api补桩防连库）"
```

---

### Task 5: 前端——主表过滤 + 已清仓分区

**Files:**
- Modify: `static/portfolio.html`（HTML 区块 + CSS + renderPositions/renderClosed/SSE 合并）

前端无测试基建（仓库惯例：前端改动靠 Task 7 线上浏览器验证），本任务步骤为精确编辑。

- [ ] **Step 5.1: HTML——持仓明细 section 之后插入已清仓分区**

在「持仓明细」`</section>`（约 473 行，`<!-- 持仓明细 -->` 区块的闭合标签）之后插入：

```html
  <!-- 已清仓分区：qty==0 标的（周期回放推导，默认折叠） -->
  <section class="card pad" id="closedCard" hidden>
    <div class="card-head closed-head" id="closedHead">
      <div class="card-title">已清仓 <span class="pill tag" id="closedCount">0</span></div>
      <div class="card-note">卖出后视角 · 点击展开/收起</div>
    </div>
    <div class="table-wrap" id="closedBody" hidden>
      <table class="holdings">
        <thead><tr>
          <th>名称/代码</th><th>清仓时间</th><th>持有天数</th><th>清仓均价</th>
          <th>已实现盈亏</th><th>现价</th><th>卖出后涨跌幅</th><th></th>
        </tr></thead>
        <tbody id="closedTable"></tbody>
      </table>
      <div class="mobile-list" id="closedMobile"></div>
    </div>
  </section>
```

- [ ] **Step 5.2: CSS——折叠手势 + 近似标签样式**

在 `.holdings tbody tr:hover td` 规则（约 150 行）之后追加：

```css
.closed-head { cursor: pointer; user-select: none; }
.approx-tag { font-style: normal; font-size: 11px; color: var(--txt3); margin-left: 4px; }
```

- [ ] **Step 5.3: JS——renderPositions 主表过滤 qty>0**

`renderPositions` 内（约 824 行）：

```js
  const positions = (d.positions || []).slice();
```

改为：

```js
  // 主表只渲染持仓中（qty>0）；qty==0 已清仓行移交 renderClosed 分区
  const positions = (d.positions || []).filter(p => (p.quantity || 0) > 0);
```

- [ ] **Step 5.4: JS——renderClosed 函数 + 折叠状态**

在 `renderPositions` 函数结束后（`renderOverview` 之前，约 886 行）插入：

```js
// ---- 已清仓分区：closed 周期行 + positions 现价 join（卖出后涨跌幅）----
// 折叠状态为内存态（刷新回默认收起）；现价随 overview/SSE positions 实时刷新
let closedOpen = false;

function renderClosed(d) {
  const rows = d.closed || [];
  const card = document.getElementById('closedCard');
  if (!rows.length) { card.hidden = true; return; }
  card.hidden = false;
  document.getElementById('closedCount').textContent = rows.length;
  document.getElementById('closedBody').hidden = !closedOpen;
  const priceMap = {};
  (d.positions || []).forEach(p => { if (p.price) priceMap[p.symbol] = p.price; });
  const tbody = document.getElementById('closedTable');
  const mlist = document.getElementById('closedMobile');
  tbody.innerHTML = ''; mlist.innerHTML = '';
  rows.forEach(c => {
    const noRec = !!c.no_records;
    const price = priceMap[c.symbol];
    const postChg = (price && c.exit_avg) ? (price - c.exit_avg) / c.exit_avg * 100 : null;
    const pnlHtml = noRec ? '--'
      : sign(c.realized_pnl) + '$' + fmt(Math.abs(c.realized_pnl))
        + ' <em class="' + cls(c.realized_pct) + '">(' + sign(c.realized_pct) + fmt(c.realized_pct) + '%)</em>'
        + (c.approx ? '<i class="approx-tag">成本近似</i>' : '');
    const tr = document.createElement('tr');
    tr.innerHTML = `<td><div>${esc(c.name || c.symbol)}${noRec ? '<span class="pill tag tag-stale">无成交记录</span>' : ''}</div><div class="sym"><span class="sym-link" data-sym="${esc(c.symbol)}" title="查看日K">${esc(c.symbol)}</span></div></td>
      <td>${c.exit_date || '--'}</td>
      <td>${c.hold_days != null ? c.hold_days : '--'}</td>
      <td>${c.exit_avg != null ? fmt(c.exit_avg) : '--'}</td>
      <td class="${cls(c.realized_pnl)}">${pnlHtml}</td>
      <td>${price ? fmt(price) : '--'}</td>
      <td class="${cls(postChg)}">${postChg == null ? '--' : sign(postChg) + fmt(postChg) + '%'}</td>
      <td></td>`;
    tbody.appendChild(tr);
    const mc = document.createElement('div');
    mc.className = 'm-card';
    mc.innerHTML = `<div class="m-top"><span class="m-name">${esc(c.name || c.symbol)}${noRec ? '<span class="pill tag tag-stale">无成交记录</span>' : ''}</span>
        <span class="m-quote ${cls(postChg)}">${postChg == null ? '--' : sign(postChg) + fmt(postChg) + '%'}</span></div>
      <div class="m-meta">
        <span><span class="sym-link" data-sym="${esc(c.symbol)}" title="查看日K">${esc(c.symbol)}</span></span>
        <span class="m-div"></span>
        <span>${c.exit_date || '--'}${c.hold_days != null ? ' · ' + c.hold_days + '天' : ''} · 均价 ${c.exit_avg != null ? fmt(c.exit_avg) : '--'}</span>
        <span class="m-div"></span>
        <span>已实现 <b class="${cls(c.realized_pnl)}">${noRec ? '--' : sign(c.realized_pnl) + '$' + fmt(Math.abs(c.realized_pnl))}${c.approx ? '（成本近似）' : ''}</b></span>
      </div>`;
    mlist.appendChild(mc);
  });
}

document.getElementById('closedHead').addEventListener('click', () => {
  closedOpen = !closedOpen;
  document.getElementById('closedBody').hidden = !closedOpen;
});
```

- [ ] **Step 5.5: JS——renderOverview 挂接 + SSE 保活合并**

5a. `renderOverview` 内 `renderPositions(d);`（约 984 行）之后追加一行：

```js
  renderClosed(d);
```

5b. SSE overview 事件合并（约 2668 行 `recent_trades` 保活之后）追加：

```js
    if (d.closed === undefined && lastOverview && lastOverview.closed)
      d.closed = lastOverview.closed;
```

- [ ] **Step 5.6: 后端回归（前端纯静态改动不影响 pytest，跑基线确认无意外）**

Run: `python3 -m pytest tests/test_closed_positions.py tests/test_server_auth.py -q`
Expected: passed

- [ ] **Step 5.7: 提交**

```bash
git add static/portfolio.html
git commit -m "feat: 前端已清仓折叠分区（周期行+现价join卖出后涨跌幅+无记录占位），主表过滤qty>0"
```

---

### Task 6: 前端——K线弹层买卖点标记 + 成交列表

**Files:**
- Modify: `static/portfolio.html`（弹层 HTML + CSS + klRender/klDrawMain/klApply/renderKlTrades）

- [ ] **Step 6.1: HTML——弹层内 klRsi 之后插入成交列表容器**

K线弹层内 `<canvas id="klRsi"></canvas>`（约 543 行）与 `<div class="kline-info" id="klInfo"></div>`（约 544 行）之间插入：

```html
    <div class="kl-trades" id="klTrades" hidden></div>
```

- [ ] **Step 6.2: CSS——成交列表行样式**

在 Task 5.2 追加的规则之后继续追加：

```css
.kl-trades { max-height: 132px; overflow-y: auto; margin-top: 8px;
  border-top: 1px solid var(--bg); padding-top: 8px; }
.kt-row { display: flex; gap: 12px; align-items: center; font-size: 12px;
  padding: 3px 0; color: var(--txt2); }
.kt-side { display: inline-block; min-width: 14px; text-align: center;
  font-weight: 600; border-radius: 3px; padding: 0 3px; }
.kt-side.buy { color: var(--up); border: 1px solid var(--up); }
.kt-side.sell { color: var(--down); border: 1px solid var(--down); }
```

- [ ] **Step 6.3: JS——klRender 透传 trades（仅日K画标记）**

`klRender` 内 klDrawMain 调用（约 2212-2217 行）：

```js
  klDrawMain(bars, d.marks || {}, (() => {
    if (KL.tf !== 'day' || !d.overlays) return null;
    return {ma50_series: (d.overlays.ma50_series || []).slice(start, end),
            ma200_series: (d.overlays.ma200_series || []).slice(start, end),
            fib: d.overlays.fib || {}};
  })());
```

改为（追加第 4 参）：

```js
  klDrawMain(bars, d.marks || {}, (() => {
    if (KL.tf !== 'day' || !d.overlays) return null;
    return {ma50_series: (d.overlays.ma50_series || []).slice(start, end),
            ma200_series: (d.overlays.ma200_series || []).slice(start, end),
            fib: d.overlays.fib || {}};
  })(), KL.tf === 'day' ? (d.trades || []) : []);
```

- [ ] **Step 6.4: JS——klDrawMain 画 B/S 标记**

`klDrawMain` 签名（约 2237 行）：

```js
function klDrawMain(bars, marks, ov) {
```

改为：

```js
function klDrawMain(bars, marks, ov, trades) {
```

在 MA 均线叠加块结束后（`ctx.fillText('MA200', pad.l + 44, pad.t + 4);` 与「底部日期标签」注释之间，约 2299 行）插入：

```js
  // 买卖点标记（仅日K传入非空 trades）：B▲ 蜡烛下方红 / S▼ 蜡烛上方绿
  // （买=看涨红、卖=看跌绿，站内红涨绿跌配色）；窗口外/无匹配蜡烛只进列表
  if (trades && trades.length) {
    const idxByDate = {};
    bars.forEach((b, i) => { idxByDate[String(b.date).slice(0, 10)] = i; });
    ctx.textAlign = 'center'; ctx.font = F600_13;
    trades.forEach(t => {
      const i = idxByDate[String(t.date).slice(0, 10)];
      if (i === undefined) return;
      const cx = x(i);
      if (t.side === 'BUY') {
        const py = Math.min(y(+bars[i].low) + 16, H - pad.b - 4);
        ctx.fillStyle = UP_COLOR;
        ctx.beginPath();
        ctx.moveTo(cx, py - 9); ctx.lineTo(cx - 4, py - 2); ctx.lineTo(cx + 4, py - 2);
        ctx.closePath(); ctx.fill();
        ctx.fillText('B', cx, py + 5);
      } else if (t.side === 'SELL') {
        const py = Math.max(y(+bars[i].high) - 16, pad.t + 10);
        ctx.fillStyle = DOWN_COLOR;
        ctx.beginPath();
        ctx.moveTo(cx, py + 9); ctx.lineTo(cx - 4, py + 2); ctx.lineTo(cx + 4, py + 2);
        ctx.closePath(); ctx.fill();
        ctx.fillText('S', cx, py - 6);
      }
    });
    ctx.textAlign = 'left';
  }
```

- [ ] **Step 6.5: JS——renderKlTrades 成交列表 + klApply/openKline 挂接**

6a. 在 `klApply` 函数之前（约 2160 行）插入：

```js
/**
 * 成交列表：每笔至今涨跌幅（BUY=(现价−买价)/买价、SELL 同理）。
 * 现价取 lastOverview positions 按 symbol join，退化最近日K收盘价。@author yys
 */
function renderKlTrades(d) {
  const box = document.getElementById('klTrades');
  const trades = d.trades || [];
  if (!trades.length) { box.hidden = true; box.innerHTML = ''; return; }
  let price = null;
  ((lastOverview && lastOverview.positions) || []).some(
    p => p.symbol === KL.sym && p.price && (price = p.price));
  if (!price && KL.data.day && (KL.data.day.bars || []).length)
    price = +KL.data.day.bars[KL.data.day.bars.length - 1].close;
  box.hidden = false;
  box.innerHTML = trades.map(t => {
    const chg = price ? (price - t.price) / t.price * 100 : null;
    return '<div class="kt-row">'
      + '<span class="kt-side ' + (t.side === 'BUY' ? 'buy">买' : 'sell">卖') + '</span>'
      + '<span>' + esc(String(t.date)) + '</span>'
      + '<span>' + fmt(t.price) + ' × ' + fmt(t.quantity, 0) + '</span>'
      + '<span class="' + cls(chg) + '">' + (chg == null ? '--' : sign(chg) + fmt(chg) + '%') + '</span>'
      + '</div>';
  }).join('');
}
```

6b. `klApply` 内 `klRender();`（约 2172 行）之后追加：

```js
  renderKlTrades(d);
```

6c. `openKline` 内 `klClearCanvas();`（约 2120 行）之后追加（清上一标的残留列表）：

```js
  const _kt = document.getElementById('klTrades');
  if (_kt) { _kt.hidden = true; _kt.innerHTML = ''; }
```

- [ ] **Step 6.6: 后端回归确认**

Run: `python3 -m pytest tests/test_closed_positions.py -q`
Expected: 14 passed

- [ ] **Step 6.7: 提交**

```bash
git add static/portfolio.html
git commit -m "feat: K线弹层买卖点标记（B▲/S▼红涨绿跌）+成交列表（每笔至今涨跌幅）"
```

---

### Task 7: 全量回归 + secrets 门禁 + 部署线上验证 + 文档口径

**Files:**
- Modify: `AGENTS.md`（关键口径契约追加一条）

- [ ] **Step 7.1: 全量基线回归**

Run: `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py tests/test_alert_api.py tests/test_daily_bars.py tests/test_longport_push.py tests/test_macro_fetcher.py tests/test_ledger.py tests/test_nav.py tests/test_closed_positions.py -q`
Expected: 全部 passed（基线 441 + 新增 14 = 455）

- [ ] **Step 7.2: secrets 门禁**

Run: `python3 scripts/check_secrets.py`
Expected: 通过（无泄露告警）

- [ ] **Step 7.3: AGENTS.md 关键口径契约追加**

在「关键口径契约（改动前必读）」清单末尾（「净值 TWR 口径」条目之后）追加：

```markdown
- **已清仓口径（2026-10-11）**：清仓标的 holdings 行保留（qty=0 不删），
  前端主表只渲染 qty>0，qty==0 沉「已清仓」折叠分区；周期由
  `ledger.build_closed_positions`（trade_records 全量回放 + holdings 数量
  锚点反推期初 q0，归零切周期）纯计算推导，60s `_closed_ctx_cache`，
  overview 端点注入 `closed` 块（不进 SSE 载荷，前端保活合并同
  recent_trades）；realized = Σ卖额 − Σ买额 − 期初成本（期初 = q0 × 当前
  cost_price 近似，approx 打标）；卖出后涨跌幅前端用 positions 现价 join
  (现价−exit_avg)/exit_avg；K线 `/api/kline` payload 带 trades（日期
  YYYY-MM-DD 对齐日K轴），弹层日K画 B▲红/S▼绿 标记 + 成交列表（每笔至今
  涨跌幅）。零新增外部请求（清仓标的 enabled 保持 1 搭腾讯批总线）。
```

- [ ] **Step 7.4: 提交**

```bash
git add AGENTS.md
git commit -m "docs: AGENTS.md已清仓口径契约（周期回放/近似打标/K线标记）"
```

- [ ] **Step 7.5: 部署 + 线上验证（真实命令见 AGENTS.local.md）**

```bash
# rsync 部署 + 重启（IP 以 AGENTS.local.md 为准，同 AGENTS.md 部署命令模板）
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='qdii.env' --exclude='*.local.md' --exclude='.mcp.json' \
  --exclude='static/download' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

验证点（`systemctl is-active qdii` + `journalctl -u qdii --since '2 min ago'` 无异常后，带 session cookie curl，token 取法见 AGENTS.md）：

1. `curl -s -b "session_token=<tok>" http://<server-ip>:8888/api/portfolio/overview | python3 -m json.tool | grep -A5 '"closed"'` → closed 块存在，周期行数值合理；
2. 任一有成交的 symbol：`curl -s -b "session_token=<tok>" "http://<server-ip>:8888/api/kline/MU?tf=day&range=1000" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('trades',[])))"` → 非零；
3. 浏览器：持仓明细主表无零值行；「已清仓」分区默认折叠、徽标数量正确、展开后现价/卖出后涨跌幅有值（无行情标的显示 --）；点击已清仓 symbol 打开 K线 → 日K上有 B/S 标记、底部成交列表每笔带涨跌幅；再入场多周期多行。
4. `python3 -m pytest` 不适用于线上；线上仅验证运行时无 `WARNING ... closed` 类异常刷屏。

---

## 自审记录（writing-plans Self-Review）

1. **Spec 覆盖**：§3 口径 → Task 1；§4.1-4.2 → Task 1/2；§4.3 overview/SSE → Task 3；§4.3 kline trades → Task 4；§5.1 分区/过滤/保活 → Task 5；§5.2 标记/列表 → Task 6；§7 测试 → Task 1-4 内嵌；§6 边界（无记录占位/孤儿 symbol/钳制）→ Task 1 用例 5/7/9；部署验证 → Task 7。无缺口。
2. **占位符扫描**：全部步骤含完整代码/命令/期望输出，无 TBD/TODO/“适当处理”。
3. **类型一致性**：`build_closed_positions(trades, holdings)` 出参字段名（exit_date/hold_days/exit_qty/exit_avg/realized_pnl/realized_pct/approx/no_records）与 Task 3 测试、Task 5 renderClosed 消费一致；kline `trades` 元素字段（date/side/price/quantity）与 Task 6 标记/列表消费一致；`_closed_ctx_cache`/`_get_closed_ctx` 命名在 Task 3 定义与测试引用一致。
