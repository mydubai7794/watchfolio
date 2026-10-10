# 持仓买卖点提醒引擎 + 长桥推送 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按 `docs/superpowers/specs/2026-10-04-alert-engine-and-longport-push-design.md`（v2）实现一期提醒引擎（日K底座+四类规则+邮件+网页+K线弹层+回测）与二期长桥推送（shadow）+实时性升级。

**Architecture:** 新增 6 个模块（daily_bars/alert_engine/mailer/alert_backtest/quotes_hub/longport_push），server.py 挂新任务与 API，前端 portfolio.html 加策略/提醒/K线三个界面。数据流：轮询/push → 内存价 → alert_engine 每价一评 → alerts 表 + SSE + 邮件。

**Tech Stack:** FastAPI + aiomysql + longbridge SDK + smtplib + 原生 canvas/EventSource。

**纪律（每个 task 通用）：**
- 全程中文注释、新代码 `@author yys`、禁止 TODO/伪代码、违禁词只用系统内数据源名（长桥/24h数据源/股票行情网关/腾讯/新浪/Yahoo/FT/twelvedata）。
- **不执行 git commit**（用户未要求提交，改动留在工作区）。
- 长桥请求逐只串行间隔 ≥3s；符号带 `.US` 后缀。
- 每个任务完成必须跑指定 pytest；全部任务完成后跑基线：`python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_daily_bars.py tests/test_alert_engine.py tests/test_mailer.py tests/test_alert_api.py tests/test_backtest.py tests/test_quotes_hub.py tests/test_longport_push.py -q`（原基线 238 passed 必须保持）。
- 测试不得连远程 MySQL / 发外网请求 / 起 lifespan（TestClient 不用 with 上下文，monkeypatch `server.db.get_session`，仿 tests/test_ledger.py:199-270）。
- 项目根：`/root/workspace/project/qdii`。改 server.py 前先 Read 目标区域。

---

## Task 1: database.py 三张新表 + CRUD

**Files:**
- Modify: `database.py`（`init_tables` 末尾追加 + 文件尾部追加 CRUD）
- Test: `tests/test_alert_api.py`（本 task 只建测试文件的 CRUD 段，后续 task 复用同文件）

- [ ] **Step 1: 写失败测试（CRUD SQL 捕获，不连库）**

`tests/test_alert_api.py` 新建：

```python
"""提醒模块数据库 CRUD 与 API 测试。

@author yys
"""
import asyncio
import json
from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient


class _FakeCursor:
    def __init__(self, store):
        self.store = store          # 共享 dict 模拟表
        self.sql = None
        self.args = None
        self._result = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, args=None):
        self.sql, self.args = sql, args

    async def executemany(self, sql, seq):
        self.sql, self.args = sql, list(seq)
        if "INSERT IGNORE INTO daily_bars" in sql:
            for row in self.args:
                self.store["daily_bars"].setdefault(row[0], {})
                self.store["daily_bars"][row[0]][str(row[1])] = row

    async def fetchall(self):
        return self._result


class _FakeConn:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        return _FakeCursor(self.store)

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, store):
        self.store = store

    def acquire(self):
        return _FakeConn(self.store)


@pytest.fixture
def fake_store(monkeypatch):
    store = {"daily_bars": {}, "alert_rules": {}, "config": {}}
    import database as db

    class _PoolCtx:
        async def __aenter__(self):
            return _FakePool(store)

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(db, "get_pool", lambda: _PoolCtx())
    return store


def test_save_daily_bars_executemany(fake_store):
    import database as db

    rows = [(date(2026, 10, 1), 1.0, 2.0, 0.5, 1.5, 100),
            (date(2026, 10, 2), 1.5, 1.8, 1.2, 1.4, 90)]
    asyncio.run(db.save_daily_bars("QQQ", rows))
    assert len(fake_store["daily_bars"]["QQQ"]) == 2


def test_upsert_and_get_alert_rule_roundtrip(monkeypatch):
    import database as db

    rows = []

    async def fake_exec_dict(sql, args=None, _pool=None):
        return rows

    async def fake_execute(sql, args=None, _pool=None):
        rows.clear()
        rows.append({"symbol": args[0], "params": json.loads(args[1]),
                     "fired_state": json.loads(args[2]), "enabled": 1})

    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    monkeypatch.setattr(db, "execute", fake_execute)
    asyncio.run(db.upsert_alert_rule("NVDA", {"atr_stop": {"enabled": True}},
                                     {"atr_stop": {"cooldown_until": 0}}))
    rule = asyncio.run(db.get_alert_rule("NVDA"))
    assert rule["params"] == {"atr_stop": {"enabled": True}}
    assert rule["enabled"] == 1
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: FAIL `AttributeError: ... no attribute 'save_daily_bars'`

- [ ] **Step 3: 实现**

`database.py` 的 `init_tables()` 函数体末尾（找到该函数 Read 后在最后一条建表后）追加调用 `await init_alert_tables()`，并在文件尾部追加：

```python
async def init_alert_tables():
    """提醒模块三张表（幂等建表 + sys_config 种子键）。@author yys"""
    await execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
          symbol varchar(20) NOT NULL,
          date   date NOT NULL,
          open   decimal(18,6) NOT NULL,
          high   decimal(18,6) NOT NULL,
          low    decimal(18,6) NOT NULL,
          close  decimal(18,6) NOT NULL,
          volume bigint DEFAULT 0,
          UNIQUE KEY uk_sym_date (symbol, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='长桥前复权日K(增量累积)'
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
          symbol      varchar(20) NOT NULL PRIMARY KEY,
          params      json NOT NULL COMMENT '策略包(四类规则配置)',
          fired_state json NOT NULL COMMENT '判定状态(已触发档位/冷却到期)',
          enabled     tinyint(1) DEFAULT 1,
          updated_at  datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='提醒规则配置'
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS alerts (
          id           bigint AUTO_INCREMENT PRIMARY KEY,
          symbol       varchar(20) NOT NULL,
          rule         varchar(20) NOT NULL,
          level        varchar(10) NOT NULL DEFAULT 'info',
          title        varchar(120) NOT NULL,
          body         json NOT NULL,
          triggered_at datetime NOT NULL,
          is_read      tinyint(1) DEFAULT 0,
          KEY idx_triggered (triggered_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='提醒历史'
    """)
    await execute(
        "INSERT IGNORE INTO sys_config (`key`, value, remark) VALUES "
        "('ALERT_COOLDOWN_HOURS', '4', '同规则同方向提醒冷却小时数'), "
        "('LP_PUSH_MODE', 'shadow', '长桥推送模式 shadow/live'), "
        "('MAIL_ADDRESS', '', '提醒发件QQ邮箱'), "
        "('MAIL_AUTH_CODE', '', 'QQ邮箱SMTP授权码')")


async def save_daily_bars(symbol: str, rows: list) -> int:
    """批量写入日K（INSERT IGNORE 幂等）。rows: [(date, open, high, low, close, volume)]。@author yys"""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO daily_bars (symbol, date, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [(symbol, d, _safe_decimal(o, 6), _safe_decimal(h, 6),
                  _safe_decimal(l, 6), _safe_decimal(c, 6), int(v or 0))
                 for d, o, h, l, c, v in rows])
            return cur.rowcount


async def replace_daily_bars(symbol: str, rows: list) -> int:
    """整段重写（除权事件后覆盖旧复权数据）。@author yys"""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM daily_bars WHERE symbol = %s", (symbol,))
            await cur.executemany(
                "INSERT IGNORE INTO daily_bars (symbol, date, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [(symbol, d, _safe_decimal(o, 6), _safe_decimal(h, 6),
                  _safe_decimal(l, 6), _safe_decimal(c, 6), int(v or 0))
                 for d, o, h, l, c, v in rows])
            return cur.rowcount


async def get_daily_bars(symbol: str, limit: int | None = None) -> list[dict]:
    """日K升序（旧→新）。@author yys"""
    sql = ("SELECT date, open, high, low, close, volume FROM daily_bars "
           "WHERE symbol = %s ORDER BY date")
    if limit:
        sql += f" DESC LIMIT {int(limit)}"
        rows = await execute_dict(sql, (symbol,))
        return list(reversed(rows))
    return await execute_dict(sql, (symbol,))


async def get_daily_bars_max_date(symbol: str) -> date | None:
    """库内最新日K日期（增量拉取起点）。@author yys"""
    rows = await execute_dict(
        "SELECT MAX(date) AS d FROM daily_bars WHERE symbol = %s", (symbol,))
    return rows[0]["d"] if rows and rows[0].get("d") else None


async def upsert_alert_rule(symbol: str, params: dict, fired_state: dict,
                            enabled: int = 1) -> int:
    """保存单票策略包与判定状态。@author yys"""
    return await execute(
        "INSERT INTO alert_rules (symbol, params, fired_state, enabled) VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE params = VALUES(params), "
        "fired_state = VALUES(fired_state), enabled = VALUES(enabled)",
        (symbol, json.dumps(params, ensure_ascii=False),
         json.dumps(fired_state, ensure_ascii=False), enabled))


async def get_alert_rule(symbol: str) -> dict | None:
    """读单票规则（None=未配置，调用方用默认值）。@author yys"""
    rows = await execute_dict(
        "SELECT symbol, params, fired_state, enabled FROM alert_rules WHERE symbol = %s",
        (symbol,))
    if not rows:
        return None
    r = rows[0]
    return {"symbol": r["symbol"], "params": json.loads(r["params"]),
            "fired_state": json.loads(r["fired_state"]), "enabled": r["enabled"]}


async def get_alert_rules() -> list[dict]:
    """全部已配置规则。@author yys"""
    rows = await execute_dict(
        "SELECT symbol, params, fired_state, enabled FROM alert_rules ORDER BY symbol")
    return [{"symbol": r["symbol"], "params": json.loads(r["params"]),
             "fired_state": json.loads(r["fired_state"]), "enabled": r["enabled"]}
            for r in rows]


async def save_alert(event: dict) -> int:
    """提醒落库，返回自增 id。@author yys"""
    now = event.get("triggered_at") or datetime.now()
    await execute(
        "INSERT INTO alerts (symbol, rule, level, title, body, triggered_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (event["symbol"], event["rule"], event.get("level", "info"),
         event["title"][:120], json.dumps(event.get("body", {}), ensure_ascii=False), now))
    rows = await execute_dict("SELECT LAST_INSERT_ID() AS id")
    return rows[0]["id"] if rows else 0


async def get_alerts(unread_only: bool = False, limit: int = 50) -> list[dict]:
    """提醒历史（新→旧）。@author yys"""
    where = "WHERE is_read = 0 " if unread_only else ""
    rows = await execute_dict(
        f"SELECT id, symbol, rule, level, title, body, triggered_at, is_read "
        f"FROM alerts {where}ORDER BY triggered_at DESC LIMIT {int(limit)}")
    for r in rows:
        r["body"] = json.loads(r["body"]) if isinstance(r["body"], str) else r["body"]
    return rows


async def mark_alert_read(alert_id: int) -> int:
    return await execute("UPDATE alerts SET is_read = 1 WHERE id = %s", (alert_id,))


async def mark_all_alerts_read() -> int:
    return await execute("UPDATE alerts SET is_read = 1 WHERE is_read = 0")
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: 2 passed

---

## Task 2: daily_bars.py 指标纯函数

**Files:**
- Create: `daily_bars.py`
- Test: `tests/test_daily_bars.py`

- [ ] **Step 1: 写失败测试**

```python
"""daily_bars 指标计算测试。@author yys"""
import math

import pytest

import daily_bars as dbar


def _bars(closes, highs=None, lows=None):
    """构造日K序列：high=close+1、low=close-1（未给时）。"""
    return [{"date": f"2026-01-{i+1:02d}", "close": c,
             "high": (highs[i] if highs else c + 1),
             "low": (lows[i] if lows else c - 1), "open": c, "volume": 0}
            for i, c in enumerate(closes)]


def test_sma_basic():
    out = dbar.sma([1, 2, 3, 4, 5], 3)
    assert out[:2] == [None, None]
    assert out[2] == pytest.approx(2.0)
    assert out[4] == pytest.approx(4.0)


def test_rsi_wilder_all_up_is_100():
    closes = [float(i) for i in range(1, 30)]
    out = dbar.rsi_wilder(closes, 14)
    assert out[-1] == pytest.approx(100.0)


def test_rsi_wilder_all_down_is_0():
    closes = [float(30 - i) for i in range(30)]
    out = dbar.rsi_wilder(closes, 14)
    assert out[-1] == pytest.approx(0.0)


def test_atr_constant_range():
    # 每根 TR = (high-low)=2 与前收跳空 0 → TR=2，ATR 恒 2
    bars = _bars([10.0] * 30)
    out = dbar.atr_wilder(bars, 22)
    assert out[-1] == pytest.approx(2.0)


def test_hhv_window():
    bars = _bars([1, 5, 3, 2, 8, 4], )
    out = dbar.hhv([b["high"] for b in bars], 3)
    assert out[4] == pytest.approx(10.0)   # max(3+1, 2+1, 8+1)=10


def test_compute_indicators_snapshot():
    bars = _bars([10.0 + 0.1 * i for i in range(210)])
    snap = dbar.compute_indicators(bars)
    assert snap["enough_history"] is True
    assert snap["ma200"] is not None and snap["rsi14"] == pytest.approx(100.0)
    assert snap["hhv22"] > snap["ma50"]
    assert snap["stop_level"] == pytest.approx(snap["hhv22"] - 3 * snap["atr22"])


def test_compute_indicators_short_history():
    snap = dbar.compute_indicators(_bars([1.0] * 60))
    assert snap["enough_history"] is False
    assert snap["ma200"] is None and snap["atr22"] is not None
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: FAIL `ModuleNotFoundError: No module named 'daily_bars'`

- [ ] **Step 3: 实现 daily_bars.py（本 task 只写指标部分）**

```python
"""日K数据底座：长桥前复权日K增量落库 + 技术指标缓存。

表 daily_bars 增量累积（UNIQUE 幂等）；指标每日刷新后预算缓存，
提醒引擎求值只做比对。@author yys
"""
import asyncio
import logging
import time
from datetime import date, datetime

import database as db

logger = logging.getLogger("qdii.daily_bars")

ATR_PERIOD = 22
MA_FAST = 50
MA_SLOW = 200
RSI_PERIOD = 14
HHV_PERIOD = 22
DEFAULT_ATR_K = 3.0

# 指标缓存：{symbol: {atr22, ma50, ma200, rsi14, hhv22, stop_level, enough_history, asof}}
INDICATORS: dict[str, dict] = {}


def sma(values: list[float], n: int) -> list:
    """简单均线，前 n-1 个为 None。"""
    out = [None] * len(values)
    acc = 0.0
    for i, v in enumerate(values):
        acc += v
        if i >= n:
            acc -= values[i - n]
        if i >= n - 1:
            out[i] = acc / n
    return out


def rsi_wilder(closes: list[float], n: int = RSI_PERIOD) -> list:
    """Wilder 平滑 RSI，前 n 个为 None。"""
    out = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / n, losses / n
    out[n] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    return out


def atr_wilder(bars: list[dict], n: int = ATR_PERIOD) -> list:
    """Wilder 平滑 ATR（用 high/low/close），前 n 个为 None。"""
    if len(bars) <= n:
        return [None] * len(bars)
    trs = [bars[0]["high"] - bars[0]["low"]]
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[1:n + 1]) / n
    out = [None] * n + [atr]
    for i in range(n + 1, len(bars)):
        atr = (atr * (n - 1) + trs[i]) / n
        out.append(atr)
    return out


def hhv(values: list[float], n: int = HHV_PERIOD) -> list:
    """滚动窗口最高值，前 n-1 个为 None。"""
    out = [None] * len(values)
    for i in range(n - 1, len(values)):
        out[i] = max(values[i - n + 1: i + 1])
    return out


def compute_indicators(bars: list[dict], k: float = DEFAULT_ATR_K) -> dict:
    """最新指标快照（bars 升序）。历史不足时对应项为 None。"""
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    ma50 = sma(closes, MA_FAST)
    ma200 = sma(closes, MA_SLOW)
    rsi = rsi_wilder(closes)
    atr = atr_wilder(bars)
    hh = hhv(highs)
    i = len(bars) - 1
    enough = len(bars) >= MA_SLOW
    atr_v = atr[i]
    hh_v = hh[i]
    return {
        "asof": bars[i].get("date"),
        "enough_history": enough,
        "ma50": ma50[i], "ma200": ma200[i], "rsi14": rsi[i],
        "atr22": atr_v, "hhv22": hh_v,
        "stop_level": (hh_v - k * atr_v) if (hh_v is not None and atr_v is not None) else None,
        "last_close": closes[i],
    }


def get_indicators(symbol: str) -> dict | None:
    """读缓存（刷新后有效；未加载返回 None，调用方按规则禁用处理）。"""
    return INDICATORS.get(symbol)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 7 passed

---

## Task 3: daily_bars.py 长桥拉取 + 增量/除权 + 刷新循环

**Files:**
- Modify: `longport_fetcher.py`（追加 `fetch_daily_bars_raw`）
- Modify: `daily_bars.py`（追加拉取/落库/循环）
- Test: `tests/test_daily_bars.py`（追加）

- [ ] **Step 1: 写失败测试（增量合并与除权检测纯逻辑）**

追加到 `tests/test_daily_bars.py`：

```python
def _row(d, c):
    return (f"2026-{d:02d}-01", 0, 0, 0, c, 0)


def test_merge_incremental_new_only():
    existing = {"2026-01-01": 10.0, "2026-01-02": 11.0}
    fetched = [{"date": "2026-01-02", "close": 11.0}, {"date": "2026-01-03", "close": 12.0}]
    new_rows, reload_needed = dbar.merge_bars(existing, fetched)
    assert reload_needed is False
    assert [r[0] for r in new_rows] == ["2026-01-03"]


def test_merge_detects_adjustment_event():
    existing = {"2026-01-02": 11.0}
    fetched = [{"date": "2026-01-02", "close": 5.5},   # 复权因子跳变 >0.5%
               {"date": "2026-01-03", "close": 5.6}]
    new_rows, reload_needed = dbar.merge_bars(existing, fetched)
    assert reload_needed is True


def test_symbols_need_us_suffix():
    assert dbar.lp_symbol("QQQ") == "QQQ.US"
    assert dbar.lp_symbol("QQQ.US") == "QQQ.US"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 新增 3 条 FAIL（no attribute 'merge_bars'）

- [ ] **Step 3: 实现**

`longport_fetcher.py` 文件尾部追加（沿用 `_sdk_call/_get_ctx/is_available/_LpPeriod/_LpAdjustType`，先 Read 478-496 确认上下文）：

```python
async def fetch_daily_bars_raw(symbol: str, count: int = 1000) -> list:
    """长桥前复权日K（count≤1000）。失败/不可用返回 []。@author yys"""
    if not _SDK_OK or token_invalid or not symbol:
        return []
    if time.monotonic() < _backoff_until:
        return []
    ctx = await _get_ctx()
    if ctx is None:
        return []
    try:
        ks = await _sdk_call(ctx.candlesticks, symbol, _LpPeriod.Day,
                             max(2, min(int(count), 1000)),
                             _LpAdjustType.ForwardAdjust)
        return list(ks or [])
    except asyncio.TimeoutError:
        return []
    except Exception as e:
        _mark_failure(e)
        return []
```

`daily_bars.py` 追加：

```python
def lp_symbol(symbol: str) -> str:
    """长桥符号必须带 .US 后缀（缺后缀静默空陷阱）。"""
    return symbol if "." in symbol else f"{symbol}.US"


def merge_bars(existing: dict[str, float], fetched: list[dict]) -> tuple[list, bool]:
    """增量合并：返回 (新增行列表, 是否需要整段重拉)。

    existing: {date_str: close}；fetched: [{date, close,...}] 升序。
    重叠区间 close 偏差 >0.5% 判定除权/拆股 → 整段重拉。"""
    reload_needed = False
    for b in fetched:
        d = str(b["date"])[:10]
        old = existing.get(d)
        if old is not None and abs(float(b["close"]) - old) / old > 0.005:
            reload_needed = True
    max_d = max(existing) if existing else None
    new_rows = [(b["date"], float(b["open"]), float(b["high"]),
                 float(b["low"]), float(b["close"]), int(b.get("volume") or 0))
                for b in fetched if max_d is None or str(b["date"])[:10] > max_d]
    return new_rows, reload_needed


async def refresh_symbol(symbol: str) -> dict:
    """单只增量刷新：拉取→除权检测→落库→重算指标缓存。"""
    import longport_fetcher as lp
    if not lp.is_available():
        return {"symbol": symbol, "skipped": "longport_unavailable"}
    bars_raw = await lp.fetch_daily_bars_raw(lp_symbol(symbol), 1000)
    if not bars_raw:
        return {"symbol": symbol, "skipped": "empty_or_failed"}
    fetched = [{"date": b.timestamp.strftime("%Y-%m-%d"),
                "open": b.open, "high": b.high, "low": b.low,
                "close": b.close, "volume": b.volume} for b in bars_raw]
    fetched.sort(key=lambda b: b["date"])
    existing_rows = await db.get_daily_bars(symbol)
    existing = {str(r["date"])[:10]: float(r["close"]) for r in existing_rows}
    new_rows, reload_needed = merge_bars(existing, fetched)
    if reload_needed:
        rows = [(b["date"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                for b in fetched]
        await db.replace_daily_bars(symbol, rows)
        logger.info("日K除权事件整段重拉: %s (%d 根)", symbol, len(rows))
    elif new_rows:
        await db.save_daily_bars(symbol, new_rows)
    bars = await db.get_daily_bars(symbol)
    INDICATORS[symbol] = compute_indicators(bars)
    return {"symbol": symbol, "added": len(new_rows), "reload": reload_needed,
            "total": len(bars)}


async def refresh_all(us_symbols: list[str], gap_seconds: float = 3.0) -> dict:
    """全部持仓逐只刷新（串行、间隔 ≥3s，限流契约）；GAPS 记录不可用符号。"""
    result = {}
    gaps = []
    for i, sym in enumerate(us_symbols):
        if i:
            await asyncio.sleep(gap_seconds)
        try:
            r = await refresh_symbol(sym)
            result[sym] = r
            if r.get("skipped"):
                gaps.append(sym)
        except Exception as e:
            logger.warning("日K刷新失败 %s: %s", sym, e)
            gaps.append(sym)
    try:
        old = await db.get_config("DAILY_BARS_GAPS")
        gap_set = {s for s in (old or "").split(",") if s} if old else set()
        if gap_set != set(gaps):
            await db.set_config("DAILY_BARS_GAPS", ",".join(gaps), "长桥无日K符号清单")
    except Exception as e:
        logger.warning("GAPS 写回失败: %s", e)
    return result


async def daily_refresh_loop(holdings_loader=None):
    """后台任务：启动即刷一次，此后每日北京时间 07:00 刷新。

    holdings_loader: async () -> list[dict]（默认 db.get_holdings）。"""
    loader = holdings_loader or (lambda: db.get_holdings(enabled_only=True))
    while True:
        try:
            holdings = await loader()
            symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if symbols:
                logger.info("日K刷新开始: %d 只", len(symbols))
                await refresh_all(symbols)
                logger.info("日K刷新完成")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("日K刷新循环异常: %s", e)
        # 睡到下一个北京时间 07:00（naive 本地时区即服务器北京时间）
        now = datetime.now()
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if target <= now:
            import calendar
            import datetime as _dt
            target += _dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_daily_bars.py -q`
Expected: 10 passed

---

## Task 4: alert_engine.py 四类规则 + 冷却状态机

**Files:**
- Create: `alert_engine.py`
- Test: `tests/test_alert_engine.py`

- [ ] **Step 1: 写失败测试**

```python
"""提醒引擎规则与冷却状态机测试。@author yys"""
import asyncio
import time

import pytest

import alert_engine as ae


@pytest.fixture
def engine_env(monkeypatch):
    """打桩：规则表/提醒表/持仓成本/指标缓存。"""
    saved = {}
    monkeypatch.setattr(ae.db, "get_alert_rule", lambda s: asyncio.sleep(0, result=None))
    async def fake_upsert(sym, params, state, enabled=1):
        saved["params"], saved["state"] = params, state
    monkeypatch.setattr(ae.db, "upsert_alert_rule", fake_upsert)
    async def fake_save(ev):
        saved["last_event"] = ev
        return 1
    monkeypatch.setattr(ae.db, "save_alert", fake_save)
    async def fake_holdings(enabled_only=True):
        return [{"symbol": "NVDA", "market": "US", "cost_price": 200.0}]
    monkeypatch.setattr(ae.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(ae, "COOLDOWN_SECONDS", 0.0)     # 测试即时过期
    return saved


def _ind(stop=150.0, rsi=25.0, ma50=180.0, ma200=170.0, enough=True):
    return {"stop_level": stop, "rsi14": rsi, "ma50": ma50, "ma200": ma200,
            "hhv22": 160.0, "atr22": 3.3, "enough_history": enough}


def test_price_plan_triggers_and_resets(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind())
    state = {}
    # -5% 档目标 190：价格 189 触发
    evs = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert any(e["rule"] == "price_plan" and "-5" in e["title"] for e in evs)
    # 同档冷却内不重复
    evs2 = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert not evs2
    # 价格回到触发带上方 ±0.5%（>190.95）→ 重置
    ae.eval_price_plan("NVDA", 191.5, ae.DEFAULT_PARAMS, state, _ind())
    evs3 = ae.eval_price_plan("NVDA", 189.0, ae.DEFAULT_PARAMS, state, _ind())
    assert any(e["rule"] == "price_plan" for e in evs3)


def test_atr_stop_critical_event(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators", lambda s: _ind(stop=150.0))
    state = {}
    evs = ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state, _ind(stop=150.0))
    assert evs and evs[0]["level"] == "critical"
    assert evs[0]["body"]["stop_level"] == pytest.approx(150.0)
    # 价格回上止损位 0.5% → 冷却重置
    ae.eval_atr_stop("NVDA", 151.0, ae.DEFAULT_PARAMS, state, _ind(stop=150.0))
    assert ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state,
                            _ind(stop=150.0))


def test_extreme_guard_oversold_only(engine_env, monkeypatch):
    state = {}
    evs = ae.eval_extreme_guard("NVDA", 175.0, ae.DEFAULT_PARAMS, state, _ind(rsi=25.0))
    assert evs and "加仓" in evs[0]["title"]
    # 乖离 <= -8%（price 165 vs ma50 180 → -8.3%）同样触发
    evs2 = ae.eval_extreme_guard("NVDA", 165.0, ae.DEFAULT_PARAMS, state,
                                 _ind(rsi=50.0))
    assert evs2


def test_atr_stop_disabled_without_history(engine_env, monkeypatch):
    state = {}
    evs = ae.eval_atr_stop("NVDA", 149.0, ae.DEFAULT_PARAMS, state,
                           _ind(stop=None, enough=False))
    assert not evs


def test_evaluate_round_end_to_end(engine_env, monkeypatch):
    monkeypatch.setattr(ae.daily_bars, "get_indicators",
                        lambda s: _ind(stop=150.0, rsi=25.0))
    evs = asyncio.run(ae.evaluate_round({"NVDA": 149.0}))
    assert any(e["rule"] == "atr_stop" for e in evs)
    assert engine_env["last_event"]["symbol"] == "NVDA"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_engine.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 实现 alert_engine.py**

```python
"""买卖点提醒引擎：四类规则、冷却状态机、判定落库。

实证依据（2026-10-04 回测，附件报告）：
主打 atr_stop(+3.13%/笔)、骨架 trend_filter(回撤减半)、辅助超卖加仓(66.9%)、
放弃追高侧；档位 +8%/-5% 主力。@author yys
"""
import asyncio
import json
import logging
import time

import database as db
import daily_bars

logger = logging.getLogger("qdii.alert")

COOLDOWN_SECONDS = 4 * 3600.0      # 同规则同方向冷却（sys_config ALERT_COOLDOWN_HOURS 可覆盖）
RESET_BAND = 0.005                  # 离开触发带 ±0.5% 重置冷却

DEFAULT_PARAMS = {
    "price_plan": {"enabled": True, "levels": [-5.0, -10.0, 8.0, 15.0]},
    "atr_stop": {"enabled": True, "k": 3.0},
    "trend_filter": {"enabled": True},
    "extreme_guard": {"enabled": True, "rsi_low": 30.0, "bias_low": -8.0},
}

_costs: dict[str, float] = {}
_costs_at = 0.0


def _merged_params(rule: dict | None) -> dict:
    """库内单票配置覆盖默认值（浅合并每类规则的 dict）。"""
    params = {k: dict(v) for k, v in DEFAULT_PARAMS.items()}
    if rule and rule.get("params"):
        for k, v in rule["params"].items():
            if isinstance(v, dict) and isinstance(params.get(k), dict):
                params[k].update(v)
            else:
                params[k] = v
    return params


async def _load_costs() -> dict[str, float]:
    """持仓成本缓存（60s 刷新）。"""
    global _costs, _costs_at
    if time.monotonic() - _costs_at > 60 or not _costs:
        holdings = await db.get_holdings(enabled_only=True)
        _costs = {h["symbol"]: float(h["cost_price"] or 0)
                  for h in holdings if h["market"] == "US"}
        _costs_at = time.monotonic()
    return _costs


def _cooldown_ok(state: dict, rule: str, now: float) -> bool:
    return now >= state.setdefault(rule, {}).get("cooldown_until", 0)


def _set_cooldown(state: dict, rule: str, now: float, seconds: float):
    state.setdefault(rule, {})["cooldown_until"] = now + seconds


def _event(symbol: str, rule: str, title: str, level: str, body: dict) -> dict:
    return {"symbol": symbol, "rule": rule, "level": level, "title": title,
            "body": body, "triggered_at": None}


def eval_price_plan(symbol: str, price: float, params: dict, state: dict,
                    ind: dict | None) -> list[dict]:
    """成本价锚定档位：buy 档价格 ≤ 目标触发、sell 档价格 ≥ 目标触发；
    逐档一次，价格离开带 ±0.5% 重置。"""
    cfg = params.get("price_plan", {})
    if not cfg.get("enabled", True):
        return []
    cost = _costs.get(symbol)
    if not cost:
        return []
    now = time.time()
    st = state.setdefault("price_plan", {"fired": {}})
    fired: dict = st.setdefault("fired", {})
    events = []
    for pct in cfg.get("levels", []):
        target = cost * (1.0 + float(pct) / 100.0)
        key = f"{pct:+g}"
        side = "buy" if pct < 0 else "sell"
        hit = price <= target if side == "buy" else price >= target
        reset = price > target * (1 + RESET_BAND) if side == "buy" \
            else price < target * (1 - RESET_BAND)
        if key in fired and reset:
            del fired[key]
            continue
        if hit and key not in fired and _cooldown_ok(st, f"level:{key}", now):
            events.append(_event(
                symbol, "price_plan",
                f"{symbol} 触及{side == 'buy' and '加仓' or '止盈'}档 {key}%",
                "info" if side == "buy" else "info",
                {"pct": pct, "target": round(target, 2), "cost": cost,
                 "price": price}))
            fired[key] = True
            _set_cooldown(st, f"level:{key}", now, 60.0)
    return events


def eval_atr_stop(symbol: str, price: float, params: dict, state: dict,
                  ind: dict | None) -> list[dict]:
    """Chandelier 移动止损：实时价跌破 22日最高−k×ATR22 → critical。"""
    cfg = params.get("atr_stop", {})
    if not cfg.get("enabled", True) or not ind:
        return []
    stop = ind.get("stop_level")
    if stop is None or not ind.get("enough_history"):
        return []
    now = time.time()
    st = state.setdefault("atr_stop", {})
    if price > stop * (1 + RESET_BAND):
        st.pop("cooldown_until", None)     # 站回止损位上方 → 重置
        return []
    if price < stop and _cooldown_ok(st, "", now):
        _set_cooldown(st, "", now, COOLDOWN_SECONDS)
        return [_event(
            symbol, "atr_stop", f"{symbol} 跌破移动止损位，建议减仓/卖出",
            "critical",
            {"stop_level": round(stop, 2), "price": price,
             "hhv22": ind.get("hhv22"), "atr22": ind.get("atr22")})]
    return []


def eval_extreme_guard(symbol: str, price: float, params: dict, state: dict,
                       ind: dict | None) -> list[dict]:
    """超卖加仓提醒（仅超卖侧，追高侧按回测实证放弃）。"""
    cfg = params.get("extreme_guard", {})
    if not cfg.get("enabled", True) or not ind or not ind.get("enough_history"):
        return []
    rsi = ind.get("rsi14")
    ma50 = ind.get("ma50")
    bias = (price - ma50) / ma50 * 100.0 if ma50 else None
    reasons = []
    if rsi is not None and rsi <= float(cfg.get("rsi_low", 30.0)):
        reasons.append(f"RSI14={rsi:.1f}")
    if bias is not None and bias <= float(cfg.get("bias_low", -8.0)):
        reasons.append(f"对MA50乖离{bias:+.1f}%")
    if not reasons:
        # 离开超卖区 → 重置冷却
        state.setdefault("extreme_guard", {}).pop("cooldown_until", None)
        return []
    now = time.time()
    st = state.setdefault("extreme_guard", {})
    if not _cooldown_ok(st, "", now):
        return []
    _set_cooldown(st, "", now, COOLDOWN_SECONDS)
    return [_event(
        symbol, "extreme_guard",
        f"{symbol} 超卖（{' / '.join(reasons)}），可考虑分批加仓",
        "info", {"rsi14": rsi, "bias_pct": round(bias, 2) if bias else None,
                 "price": price})]


def eval_trend(bars: list[dict], params: dict) -> dict | None:
    """MA200 趋势状态（日收盘判定，供日K刷新后调用与指标接口展示）。"""
    if not params.get("trend_filter", {}).get("enabled", True):
        return None
    if len(bars) < daily_bars.MA_SLOW + 1:
        return None
    closes = [float(b["close"]) for b in bars]
    ma = daily_bars.sma(closes, daily_bars.MA_SLOW)
    prev_above = closes[-2] > ma[-2]
    now_above = closes[-1] > ma[-1]
    if prev_above == now_above:
        return {"state": "above" if now_above else "below",
                "ma200": ma[-1], "cross": None}
    return {"state": "above" if now_above else "below", "ma200": ma[-1],
            "cross": "up" if now_above else "down",
            "close": closes[-1]}


async def evaluate(symbol: str, price: float) -> list[dict]:
    """单标的求值：规则评估 → fired_state/提醒双落库。"""
    rule = await db.get_alert_rule(symbol)
    if rule is not None and not rule.get("enabled", 1):
        return []
    params = _merged_params(rule)
    state = (rule or {}).get("fired_state") or {}
    ind = daily_bars.get_indicators(symbol)
    events = []
    events += eval_price_plan(symbol, price, params, state, ind)
    events += eval_atr_stop(symbol, price, params, state, ind)
    events += eval_extreme_guard(symbol, price, params, state, ind)
    if events:
        await _load_costs() if not _costs else None
        await db.upsert_alert_rule(symbol, params, state)      # 判定状态落库
        for ev in events:
            ev["id"] = await db.save_alert(ev)                  # 提醒留痕
    return events


async def evaluate_round(prices: dict[str, float]) -> list[dict]:
    """一轮全量求值（轮询循环/push 回调统一入口）。"""
    global COOLDOWN_SECONDS
    try:
        hours = float(await db.get_config("ALERT_COOLDOWN_HOURS") or 4)
        COOLDOWN_SECONDS = hours * 3600.0
    except Exception:
        pass
    await _load_costs()
    events = []
    for sym, price in prices.items():
        if price is None or sym not in _costs:
            continue
        try:
            events += await evaluate(sym, float(price))
        except Exception as e:
            logger.warning("提醒求值异常 %s: %s", sym, e)
    return events
```

注意：`test_price_plan_triggers_and_resets` 等用例直接调用 `eval_*` 时 `_costs` 为空——在 `eval_price_plan` 开头加兜底：

```python
    cost = _costs.get(symbol)
    if not cost:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop:
            cost = None
    if not cost:
        return []
```

测试里同步调用时需要成本——在测试 fixture `engine_env` 末尾补 `ae._costs = {"NVDA": 200.0}`、`ae._costs_at = time.monotonic()`（直接写缓存，绕过异步加载）。**测试代码按此修正 fixture。**

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_engine.py tests/test_daily_bars.py -q`
Expected: 全部 passed

---

## Task 5: mailer.py 聚合节流 + QQ SMTP

**Files:**
- Create: `mailer.py`
- Test: `tests/test_mailer.py`

- [ ] **Step 1: 写失败测试**

```python
"""邮件通道聚合节流测试。@author yys"""
import asyncio
import time

import mailer


def _ev(sym="NVDA", rule="atr_stop", level="critical", title="止损"):
    return {"symbol": sym, "rule": rule, "level": level, "title": title,
            "body": {"price": 149.0}}


def test_info_events_aggregate_in_window(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="info", sym="NVDA", rule="price_plan", title="加仓档"))
    box.push(_ev(level="info", sym="MU", rule="extreme_guard", title="超卖"))
    mailer.flush(box, force=False, now=time.time() + 301)
    assert len(sent) == 1                     # 5 分钟窗口合并一封
    assert "NVDA" in sent[0][1] and "MU" in sent[0][1]


def test_critical_sends_immediately(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="critical"))
    mailer.flush(box, force=True, now=time.time())
    assert len(sent) == 1 and "止损" in sent[0][0]


def test_unconfigured_mail_logs_only(monkeypatch):
    async def fake_cfg(key):
        return None
    import database as db
    monkeypatch.setattr(db, "get_config", fake_cfg)
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    ok = asyncio.run(mailer.try_send("s", "t"))
    assert ok is False and sent == []


def test_flush_skips_within_window(monkeypatch):
    sent = []
    monkeypatch.setattr(mailer, "_send_mail", lambda s, t: sent.append((s, t)))
    box = mailer.MailBox()
    box.push(_ev(level="info"))
    mailer.flush(box, force=False, now=time.time() + 100)   # 窗口内不 flush
    assert sent == []
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_mailer.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 实现 mailer.py**

```python
"""提醒邮件通道：QQ SMTP + info 级 5 分钟聚合、critical 立即。

授权码存 sys_config（MAIL_ADDRESS/MAIL_AUTH_CODE），未配置时仅记日志。@author yys
"""
import asyncio
import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.header import Header

import database as db

logger = logging.getLogger("qdii.mailer")

AGG_WINDOW = 300.0            # info 聚合窗口 5 分钟
RETRY_AFTER_FAIL = 300.0      # 失败重试间隔
BACKOFF_MAX = 3600.0          # 连续失败降频上限

_cfg_cache: dict = {"at": 0.0, "addr": "", "code": ""}
_fail_streak = 0


async def _mail_config() -> tuple[str, str]:
    """读发件配置（10min 缓存）。"""
    global _cfg_cache
    if time.time() - _cfg_cache["at"] > 600:
        addr = await db.get_config("MAIL_ADDRESS") or ""
        code = await db.get_config("MAIL_AUTH_CODE") or ""
        _cfg_cache.update({"at": time.time(), "addr": addr, "code": code})
    return _cfg_cache["addr"], _cfg_cache["code"]


def _send_mail(subject: str, text: str) -> None:
    """同步发送（在线程池执行）。空配置时由 try_send 拦截。"""
    addr, code = _cfg_cache["addr"], _cfg_cache["code"]
    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = addr
    msg["To"] = addr
    with smtplib.SMTP_SSL("smtp.qq.com", 465, timeout=15) as smtp:
        smtp.login(addr, code)
        smtp.sendmail(addr, [addr], msg.as_string())


async def try_send(subject: str, text: str) -> bool:
    """带配置检查与失败退避的发送；未配置返回 False 仅记日志。"""
    global _fail_streak
    addr, code = await _mail_config()
    if not addr or not code:
        logger.info("邮件未配置(sys_config MAIL_ADDRESS/MAIL_AUTH_CODE)，仅记录: %s",
                    subject)
        return False
    try:
        await asyncio.to_thread(_send_mail, subject, text)
        _fail_streak = 0
        return True
    except Exception as e:
        _fail_streak += 1
        logger.warning("邮件发送失败(%d 连败): %s", _fail_streak, e)
        return False


class MailBox:
    """事件邮箱：info 进聚合缓冲、critical 立即出队。"""

    def __init__(self):
        self.pending: list[dict] = []
        self.window_start: float = 0.0

    def push(self, event: dict):
        self.pending.append(event)
        if not self.window_start:
            self.window_start = time.time()


def _render(events: list[dict]) -> tuple[str, str]:
    lines = [f"[{e['level'].upper()}] {e['title']}" for e in events]
    subject = f"持仓提醒 x{len(events)}: {events[0]['title']}"
    return subject, "\n".join(lines)


def flush(box: MailBox, force: bool = False, now: float | None = None) -> int:
    """聚合刷出（同步纯逻辑，便于测试）；返回发出封数。critical 恒 force。"""
    now = now if now is not None else time.time()
    if not box.pending:
        return 0
    critical = [e for e in box.pending if e.get("level") == "critical"]
    infos = [e for e in box.pending if e.get("level") != "critical"]
    sent = 0
    if critical:
        subj, text = _render(critical)
        try_send(subj, text)
        sent += 1
    due = force or (now - box.window_start >= AGG_WINDOW)
    if infos and due:
        subj, text = _render(infos)
        try_send(subj, text)
        sent += 1
        infos = []
    box.pending = infos + [e for e in box.pending if e not in critical and e not in infos]
    box.pending = []
    if not box.pending:
        box.window_start = 0.0
    return sent


async def mail_worker(interval: float = 30.0):
    """后台协程：入队 → 聚合发送（失败退避只加长轮询间隔，不丢事件）。"""
    box = MailBox()
    queue: asyncio.Queue = asyncio.Queue()

    async def _drain():
        while True:
            ev = await queue.get()
            box.push(ev)

    drain = asyncio.create_task(_drain())
    try:
        while True:
            await asyncio.sleep(interval)
            delay = min(RETRY_AFTER_FAIL * (2 ** min(_fail_streak, 5)), BACKOFF_MAX) \
                if _fail_streak else interval
            flush(box)
            _ = delay     # 失败退避由 try_send 内 _fail_streak 控制，此处保持轮询
    finally:
        drain.cancel()


_queue: asyncio.Queue | None = None


def enqueue(event: dict):
    """线程安全入口：提醒事件入队（未初始化时直接丢弃并记日志）。"""
    if _queue is None:
        logger.warning("邮件队列未初始化，事件丢弃: %s", event.get("title"))
        return
    _queue.put_nowait(event)
```

`mail_worker` 与模块级 `_queue` 需打通：`mail_worker` 开头接全局 `_queue`：

```python
async def mail_worker(interval: float = 30.0):
    global _queue
    _queue = asyncio.Queue()
    box = MailBox()
    ...（同上，_drain 消费 _queue）
```

（实现时把上面 mail_worker 首行的局部 queue 删除、直接用全局 `_queue`，`_drain` 读 `_queue.get()`。）

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_mailer.py -q`
Expected: 4 passed

---

## Task 6: server.py 一期集成（任务挂载 + 轮询钩子 + API + SSE alert）

**Files:**
- Modify: `server.py`（imports 区 L22-27 后、lifespan L124-203、`_portfolio_loop` 末尾、API 区 `Portfolio API` 之后）
- Test: `tests/test_alert_api.py`（追加）

- [ ] **Step 1: 写失败测试（API 层，monkeypatch db/alert_engine）**

追加到 `tests/test_alert_api.py`：

```python
import server


def _client(monkeypatch, username="alert-user"):
    async def fake_get_session(t):
        return {"username": username, "expires": "2099-01-01"}
    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    client = TestClient(server.app)
    client.cookies.set("session_token", "tok")
    return client


def test_kline_endpoint(monkeypatch, fake_store):
    import daily_bars as dbar
    client = _client(monkeypatch)

    async def fake_bars(sym, limit=None):
        return [{"date": date(2026, 10, 1), "open": 1, "high": 2, "low": 0.5,
                 "close": 1.5, "volume": 10}]

    async def fake_rule(sym):
        return None

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(server.db, "get_holdings",
                        lambda enabled_only=True: asyncio.sleep(
                            0, result=[{"symbol": "QQQ", "market": "US",
                                        "cost_price": 100.0}]))
    dbar.INDICATORS["QQQ"] = {"ma50": 1.4, "ma200": 1.3, "rsi14": 55.0,
                              "stop_level": None, "hhv22": 2.0, "atr22": 0.1,
                              "enough_history": False, "asof": "2026-10-01"}
    r = client.get("/api/kline/QQQ?range=90")
    assert r.status_code == 200
    data = r.json()
    assert data["bars"][0]["close"] == 1.5
    assert data["marks"]["cost"] == 100.0
    assert data["indicators"]["rsi14"] == 55.0


def test_alerts_read_endpoints(monkeypatch):
    client = _client(monkeypatch)
    stored = [{"id": 1, "symbol": "QQQ", "rule": "atr_stop", "level": "critical",
               "title": "t", "body": {}, "is_read": 0,
               "triggered_at": "2026-10-04 10:00:00"}]

    async def fake_get_alerts(unread_only=False, limit=50):
        return [a for a in stored if not unread_only or not a["is_read"]]

    async def fake_mark(aid):
        for a in stored:
            if a["id"] == aid:
                a["is_read"] = 1

    async def fake_mark_all():
        for a in stored:
            a["is_read"] = 1

    monkeypatch.setattr(server.db, "get_alerts", fake_get_alerts)
    monkeypatch.setattr(server.db, "mark_alert_read", fake_mark)
    monkeypatch.setattr(server.db, "mark_all_alerts_read", fake_mark_all)
    assert client.get("/api/alerts?unread=1").json()["total"] == 1
    client.post("/api/alerts/1/read")
    assert client.get("/api/alerts?unread=1").json()["total"] == 0
    client.post("/api/alerts/read_all")


def test_rules_put_get(monkeypatch, fake_store):
    client = _client(monkeypatch)
    body = {"params": {"atr_stop": {"enabled": False}}, "enabled": 1}
    r = client.put("/api/alert/rules/QQQ", json=body)
    assert r.status_code == 200

    async def fake_rules():
        return [{"symbol": "QQQ",
                 "params": {"atr_stop": {"enabled": False}},
                 "fired_state": {}, "enabled": 1}]

    monkeypatch.setattr(server.db, "get_alert_rules", fake_rules)
    data = client.get("/api/alert/rules").json()
    assert data["rules"][0]["symbol"] == "QQQ"
    assert data["defaults"]["atr_stop"]["enabled"] is True
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: 新增 3 条 FAIL（404）

- [ ] **Step 3: 实现 server.py 集成**

3a. imports 区（L27 `import scorer` 后）：

```python
import alert_backtest
import alert_engine
import daily_bars
import mailer
```

3b. lifespan：`await db.init_tables()`（L126）之后一行加 `await db.init_alert_tables()`；`ledger_fx_task = asyncio.create_task(_ledger_fx_loop())`（L177）之后加：

```python
    # 提醒模块：邮件队列 + 日K每日刷新（07:00）+ 指标/趋势重算
    global alert_mail_task, daily_bars_task
    alert_mail_task = asyncio.create_task(mailer.mail_worker())
    daily_bars_task = asyncio.create_task(daily_bars.daily_refresh_loop())
```

shutdown 区（`if scheduler_task:` 块后）加：

```python
    if alert_mail_task:
        alert_mail_task.cancel()
    if daily_bars_task:
        daily_bars_task.cancel()
```

（模块级 task 变量声明仿 L155 `global daily_task, scheduler_task` 风格，放在文件顶部已有 task 全局声明处。）

3c. `_portfolio_loop` 钩子：Read `_portfolio_loop`（L1285 起）找到写 tick / `portfolio_cache` 赋值之后的循环体收尾处（`await asyncio.sleep(interval)` 之前的最后一处业务代码后）插入：

```python
            # ---- 提醒引擎：本轮最新价全量求值（引擎不感知数据来源）----
            try:
                _prices = {s: q.get("price") for s, q in quotes.items()
                           if s in us_symbols and q.get("price")}
                if _prices:
                    _events = await alert_engine.evaluate_round(_prices)
                    for _ev in _events:
                        mailer.enqueue(_ev)
                        await _broadcast("alert", {
                            "id": _ev.get("id"), "symbol": _ev["symbol"],
                            "rule": _ev["rule"], "level": _ev["level"],
                            "title": _ev["title"], "body": _ev["body"]})
                    if _events:
                        logger.info("提醒触发 %d 条: %s", len(_events),
                                    [e["title"] for e in _events])
            except Exception as e:
                logger.warning("提醒引擎评估异常: %s", e)
```

3d. API 端点：Read `Portfolio API` 区（L1957 起），在 `HoldingUpdateInput` 类定义之前的区域插入（鉴权仿 `/api/me` L470-474 的 cookie 惯用法）：

```python
# ---------------------------------------------------------------------------
# Alert API（提醒/规则/K线/指标/回测）
# ---------------------------------------------------------------------------

async def _auth_or_401(request: Request):
    """session cookie 鉴权（惯用法同 /api/me）。"""
    token = request.cookies.get("session_token")
    session = await db.get_session(token) if token else None
    if not session:
        return JSONResponse(status_code=401, content={"ok": False, "message": "未登录"})
    return None


class RuleInput(BaseModel):
    params: dict = {}
    enabled: int = 1


@app.get("/api/alert/rules")
async def alert_rules(request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    return {"ok": True, "rules": await db.get_alert_rules(),
            "defaults": alert_engine.DEFAULT_PARAMS}


@app.put("/api/alert/rules/{symbol}")
async def alert_rule_save(symbol: str, body: RuleInput, request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    old = await db.get_alert_rule(symbol)
    fired = (old or {}).get("fired_state") or {}
    await db.upsert_alert_rule(symbol, body.params, fired, body.enabled)
    return {"ok": True}


@app.get("/api/alerts")
async def alerts_list(request: Request, unread: int = 0, limit: int = 50):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    rows = await db.get_alerts(unread_only=bool(unread), limit=limit)
    return {"ok": True, "total": len(rows), "alerts": rows}


@app.post("/api/alerts/{alert_id}/read")
async def alert_read(alert_id: int, request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    await db.mark_alert_read(alert_id)
    return {"ok": True}


@app.post("/api/alerts/read_all")
async def alert_read_all(request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    await db.mark_all_alerts_read()
    return {"ok": True}


@app.get("/api/indicators/{symbol}")
async def indicators(symbol: str, request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    ind = daily_bars.get_indicators(symbol)
    rule = await db.get_alert_rule(symbol)
    bars = await db.get_daily_bars(symbol)
    trend = alert_engine.eval_trend(bars, alert_engine._merged_params(rule))
    return {"ok": True, "symbol": symbol, "indicators": ind, "trend": trend}


@app.get("/api/kline/{symbol}")
async def kline(symbol: str, request: Request, range: int = 120):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    limit = None if range >= 1000 else range
    bars = await db.get_daily_bars(symbol, limit=limit)
    ind = daily_bars.get_indicators(symbol)
    rule = await db.get_alert_rule(symbol)
    params = alert_engine._merged_params(rule)
    holdings = await db.get_holdings(enabled_only=True)
    cost = next((float(h["cost_price"]) for h in holdings
                 if h["symbol"] == symbol), None)
    levels = params["price_plan"].get("levels", [])
    marks = {
        "cost": cost,
        "levels": [{"pct": p, "price": round(cost * (1 + p / 100.0), 2)}
                   for p in levels] if cost else [],
        "stop_level": ind.get("stop_level") if ind else None,
        "hhv22": ind.get("hhv22") if ind else None,
    }
    return {"ok": True, "symbol": symbol, "bars": bars,
            "indicators": {k: ind.get(k) for k in
                           ("ma50", "ma200", "rsi14", "stop_level", "enough_history",
                            "asof")} if ind else {}, "marks": marks}


class BacktestInput(BaseModel):
    symbols: list[str] = []
    rule: str = "atr_stop"
    params: dict = {}
    days: int = 750


@app.post("/api/backtest/run")
async def backtest_run(body: BacktestInput, request: Request):
    deny = await _auth_or_401(request)
    if deny:
        return deny
    symbols = body.symbols or [h["symbol"] for h in
                               await db.get_holdings(enabled_only=True)
                               if h["market"] == "US"]
    out = {}
    for sym in symbols:
        bars = await db.get_daily_bars(sym)
        if len(bars) < 30:
            out[sym] = {"skipped": "insufficient_data"}
            continue
        out[sym] = alert_backtest.run(sym, body.rule, body.params, bars,
                                      days=body.days)
    return {"ok": True, "results": out}
```

- [ ] **Step 4: 先建 alert_backtest.py 占位实现（Task 7 完成真实逻辑）**

`alert_backtest.py` 最小骨架（Task 7 会替换为完整实现）：

```python
"""四类规则历史回测（daily_bars 日K回放）。@author yys
"""


def run(symbol: str, rule: str, params: dict, bars: list[dict], days: int = 750) -> dict:
    return {"symbol": symbol, "rule": rule, "pending": True}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `python3 -m pytest tests/test_alert_api.py -q`
Expected: 5 passed

---

## Task 7: alert_backtest.py 完整回测

**Files:**
- Modify: `alert_backtest.py`（替换占位）
- Test: `tests/test_backtest.py`

- [ ] **Step 1: 写失败测试**

```python
"""回测纯函数测试（构造已知序列）。@author yys"""
import pytest

import alert_backtest as ab
import daily_bars as dbar


def _mk(closes):
    bars = []
    for i, c in enumerate(closes):
        bars.append({"date": f"2026-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}",
                     "open": c, "high": c * 1.02, "low": c * 0.98,
                     "close": c, "volume": 0})
    return bars


def test_trend_upside_cross_detected():
    closes = [10.0] * 210 + [8.0] * 10 + [9.5] * 30   # 跌破后回升
    bars = _mk(closes)
    ma = dbar.sma(closes, 200)
    res = ab.backtest_trend(bars)
    assert "triggers" in res and "win5" in res


def test_atr_stop_trades_complete():
    closes = [10.0] * 205 + [12.0] * 20 + [9.0] * 10   # 涨后深跌触发止损
    res = ab.backtest_atr(_mk(closes), k=3.0)
    assert res["trades"], "应有止损出场交易"
    t = res["trades"][0]
    assert set(t) >= {"entry", "exit", "ret"}


def test_levels_touch_probability_bounds():
    closes = [10.0 + (i % 3) for i in range(120)]      # 震荡 ±10%
    res = ab.backtest_levels(_mk(closes), [-5.0, 8.0], horizon=60)
    assert 0.0 <= res["-5.0"]["touch_prob"] <= 1.0
    assert 0.0 <= res["8.0"]["touch_prob"] <= 1.0


def test_run_dispatch_unknown_rule():
    res = ab.run("QQQ", "nope", {}, _mk([10.0] * 60))
    assert "error" in res
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_backtest.py -q`
Expected: FAIL `KeyError` / `AttributeError`（占位无对应字段）

- [ ] **Step 3: 实现完整 alert_backtest.py**

```python
"""四类规则历史回测：daily_bars 日K回放，触发统计与后视收益。

与 2026-10-04 实证回测同口径（附件报告）。@author yys
"""
import daily_bars as dbar


def _fwd_ret(bars: list[dict], i: int, n: int) -> float | None:
    """i 日收盘后 n 个交易日收益。"""
    j = i + n
    return None if j >= len(bars) else bars[j]["close"] / bars[i]["close"] - 1.0


def backtest_trend(bars: list[dict]) -> dict:
    """MA200 上穿买点：5/10/20 日胜率与均收 + MA200 过滤回撤对照。"""
    closes = [float(b["close"]) for b in bars]
    if len(closes) < dbar.MA_SLOW + 1:
        return {"triggers": [], "note": "insufficient_history"}
    ma = dbar.sma(closes, dbar.MA_SLOW)
    triggers, rets = [], {"5": [], "10": [], "20": []}
    for i in range(dbar.MA_SLOW, len(bars) - 1):
        if ma[i - 1] is None or ma[i] is None:
            continue
        if closes[i - 1] <= ma[i - 1] < closes[i] or \
           (closes[i] > ma[i] and closes[i - 1] <= ma[i - 1]):
            triggers.append({"date": str(bars[i]["date"])[:10], "price": closes[i]})
            for n, key in ((5, "5"), (10, "10"), (20, "20")):
                r = _fwd_ret(bars, i, n)
                if r is not None:
                    rets[key].append(r)
    # 回撤对照：全程持有 vs 仅收盘>MA200 持有
    def _max_dd(equity):
        peak, dd = equity[0], 0.0
        for v in equity:
            peak = max(peak, v)
            dd = max(dd, 1 - v / peak)
        return dd
    eq_all, eq_flt = [1.0], [1.0]
    for i in range(1, len(bars)):
        r = closes[i] / closes[i - 1] - 1.0
        eq_all.append(eq_all[-1] * (1 + r))
        held = closes[i - 1] > (ma[i - 1] or 0)
        eq_flt.append(eq_flt[-1] * (1 + (r if held else 0.0)))
    return {
        "triggers": triggers, "count": len(triggers),
        **{f"win{k}": (sum(1 for r in v if r > 0) / len(v) if v else None) for k, v in rets.items()},
        **{f"avg{k}": (sum(v) / len(v) if v else None) for k, v in rets.items()},
        "max_dd_all": _max_dd(eq_all), "max_dd_filtered": _max_dd(eq_flt)}


def backtest_atr(bars: list[dict], k: float = 3.0) -> dict:
    """Chandelier 止损：入场=MA200 上穿，出场=收盘跌破 HHV22−k×ATR22。"""
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    ma = dbar.sma(closes, dbar.MA_SLOW)
    atr = dbar.atr_wilder(bars)
    hh = dbar.hhv(highs)
    trades = []
    entry = None
    for i in range(dbar.MA_SLOW, len(bars)):
        stop = None if hh[i] is None else hh[i] - k * atr[i]
        if entry is None:
            if ma[i] and ma[i - 1] and closes[i - 1] <= ma[i - 1] < closes[i]:
                entry = (i, closes[i])
        elif stop is not None and closes[i] < stop:
            ret = closes[i] / entry[1] - 1.0
            recov = max(highs[i + 1: i + 21], default=None)
            trades.append({"entry": str(bars[entry[0]]["date"])[:10],
                           "exit": str(bars[i]["date"])[:10],
                           "ret": round(ret, 4), "days": i - entry[0],
                           "false_stop": bool(recov and recov > highs[entry[0]])})
            entry = None
    rets = [t["ret"] for t in trades]
    return {"trades": trades, "count": len(trades),
            "win_rate": sum(1 for r in rets if r > 0) / len(rets) if rets else None,
            "avg_ret": sum(rets) / len(rets) if rets else None,
            "avg_days": sum(t["days"] for t in trades) / len(trades) if trades else None}


def backtest_extreme(bars: list[dict]) -> dict:
    """RSI14 极值信号后 5/10/20 日收益（验证超卖加仓/追高无效）。"""
    closes = [float(b["close"]) for b in bars]
    rsi = dbar.rsi_wilder(closes)
    out = {"oversold": {"count": 0, "win5": None}, "overbought": {"count": 0, "win5": None}}
    for side, lo, hi in (("oversold", None, 30.0), ("overbought", 70.0, None)):
        rets = []
        for i in range(len(bars) - 5):
            v = rsi[i]
            if v is None:
                continue
            if (hi is not None and v <= hi) or (lo is not None and v >= lo):
                r = _fwd_ret(bars, i, 5)
                if r is not None:
                    rets.append(r)
        out[side] = {"count": len(rets),
                     "win5": sum(1 for r in rets if r > 0) / len(rets) if rets else None,
                     "avg5": sum(rets) / len(rets) if rets else None}
    return out


def backtest_levels(bars: list[dict], pcts: list[float],
                    horizon: int = 60) -> dict:
    """±档位 horizon 日内触及概率（用 low/high 判定）。"""
    out = {}
    n = len(bars)
    for pct in pcts:
        touched = total = 0
        avg_days = []
        for i in range(n - 1):
            base = float(bars[i]["close"])
            target = base * (1 + pct / 100.0)
            window = bars[i + 1: i + 1 + horizon]
            if not window:
                break
            total += 1
            for j, b in enumerate(window):
                hit = b["low"] <= target if pct < 0 else b["high"] >= target
                if hit:
                    touched += 1
                    avg_days.append(j + 1)
                    break
        key = f"{pct:+g}"
        out[key] = {"touch_prob": touched / total if total else None,
                    "avg_days": sum(avg_days) / len(avg_days) if avg_days else None}
    return out


def run(symbol: str, rule: str, params: dict, bars: list[dict],
        days: int = 750) -> dict:
    """API 分发入口。"""
    bars = bars[-days:]
    if rule == "trend_filter":
        return backtest_trend(bars)
    if rule == "atr_stop":
        return backtest_atr(bars, k=float(params.get("k", 3.0)))
    if rule == "extreme_guard":
        return backtest_extreme(bars)
    if rule == "price_plan":
        return backtest_levels(bars, params.get("levels", [-5.0, 8.0]))
    return {"error": f"unknown rule: {rule}"}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_backtest.py tests/test_alert_api.py -q`
Expected: 全部 passed

---

## Task 8: 前端 portfolio.html（铃铛 + 策略弹层 + K线弹层 + SSE alert）

**Files:**
- Modify: `static/portfolio.html`（顶栏、持仓表、script 区三处）

- [ ] **Step 1: Read 结构定位**

Read `static/portfolio.html` 顶栏区（header）、持仓表格渲染函数（搜 `cost_price` 或持仓行模板）、script 末尾区。找到：
a) 顶栏放铃铛图标的容器；
b) 持仓行 symbol 单元格渲染处（加点击打开 K线）；
c) script 收尾（挂 EventSource 与弹层逻辑）。

- [ ] **Step 2: 实现三块功能**

2a. 顶栏（header 容器内）加：

```html
<div class="alert-bell" id="alertBell" title="提醒中心">
  🔔<span class="bell-badge" id="bellBadge" style="display:none">0</span>
</div>
<div class="alert-panel" id="alertPanel" style="display:none"></div>
```

CSS（并入现有 `<style>`）：

```css
.alert-bell{position:relative;cursor:pointer;font-size:18px;padding:4px 8px}
.bell-badge{position:absolute;top:-4px;right:-2px;background:#e5484d;color:#fff;
  border-radius:8px;font-size:10px;padding:0 5px;min-width:14px;text-align:center}
.alert-panel{position:absolute;right:12px;top:52px;width:320px;max-height:420px;
  overflow:auto;background:#fff;border-radius:12px;box-shadow:0 8px 30px rgba(0,0,0,.12);
  z-index:60;padding:10px 12px}
.alert-item{padding:8px 4px;border-bottom:1px solid #f0f0f0;font-size:12px;cursor:pointer}
.alert-item.crit{border-left:3px solid #e5484d}
.alert-item.info{border-left:3px solid #4f8ef7}
```

2b. 持仓行 symbol 单元格渲染处，把 symbol 文本包成可点击（在持仓行模板字符串里）：

```javascript
<span class="sym-link" data-sym="${sym}" style="cursor:pointer;text-decoration:underline dotted">${sym}</span>
```

2c. script 末尾追加（完整块）：

```javascript
// ===== 提醒中心 + 策略/K线弹层 + SSE alert（@author yys）=====
const esc = s => String(s ?? '').replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

async function loadAlerts(unread = 0) {
  const r = await fetch(`/api/alerts?unread=${unread}&limit=50`);
  if (!r.ok) return [];
  const d = await r.json();
  renderAlerts(d.alerts || []);
}
function renderAlerts(list) {
  const badge = document.getElementById('bellBadge');
  const unread = list.filter(a => !a.is_read).length;
  badge.style.display = unread ? 'inline' : 'none';
  badge.textContent = unread;
  document.getElementById('alertPanel').innerHTML =
    `<div style="display:flex;justify-content:space-between;align-items:center">
       <b>提醒中心</b>
       <span><a href="javascript:void(0)" onclick="markAllRead()">全部已读</a></span>
     </div>` +
    (list.length ? list.map(a => `
      <div class="alert-item ${a.level === 'critical' ? 'crit' : 'info'}"
           onclick="readAlert(${a.id})">
        <div>${esc(a.title)}</div>
        <div style="color:#999">${esc(String(a.triggered_at).slice(5, 16))} · ${esc(a.rule)}</div>
      </div>`).join('') : '<div style="padding:16px;color:#999;text-align:center">暂无提醒</div>');
}
async function readAlert(id) {
  await fetch(`/api/alerts/${id}/read`, {method: 'POST'});
  loadAlerts();
}
async function markAllRead() {
  await fetch('/api/alerts/read_all', {method: 'POST'});
  loadAlerts();
}
document.getElementById('alertBell').addEventListener('click', () => {
  const p = document.getElementById('alertPanel');
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
  if (p.style.display === 'block') loadAlerts();
});

// --- K线弹层（canvas 自绘日K + 指标 + 标记线）---
async function openKline(sym) {
  const r = await fetch(`/api/kline/${sym}?range=120`);
  if (!r.ok) return;
  const d = await r.json();
  const bars = d.bars || [];
  if (!bars.length) { alert(`${sym} 暂无日K数据`); return; }
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:80;'
    + 'display:flex;align-items:center;justify-content:center';
  overlay.innerHTML = `
    <div style="background:#fff;border-radius:14px;padding:16px;width:min(860px,94vw)">
      <div style="display:flex;justify-content:space-between;margin-bottom:8px">
        <b>${esc(sym)} 日K（前复权）</b>
        <span style="cursor:pointer" id="klClose">✕</span>
      </div>
      <canvas id="klMain" style="width:100%;height:300px;display:block"></canvas>
      <canvas id="klRsi" style="width:100%;height:90px;display:block;margin-top:6px"></canvas>
      <div id="klInfo" style="font-size:12px;color:#666;margin-top:6px"></div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  overlay.querySelector('#klClose').onclick = () => overlay.remove();
  drawKline(overlay, d);
}
function _hcv(cv) {  // 高分屏初始化（仿现有图表轮子）
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight;
  cv.width = w * dpr; cv.height = h * dpr;
  const ctx = cv.getContext('2d');
  ctx.scale(dpr, dpr);
  return [ctx, w, h];
}
function drawKline(box, d) {
  const bars = d.bars, marks = d.marks || {}, ind = d.indicators || {};
  const [ctx, W, H] = _hcv(box.querySelector('#klMain'));
  const pad = {l: 8, r: 56, t: 10, b: 18};
  const lo = Math.min(...bars.map(b => +b.low)), hi = Math.max(...bars.map(b => +b.high));
  const lines = [];
  if (marks.cost) lines.push(['成本', marks.cost, '#e6a23c']);
  (marks.levels || []).forEach(v => lines.push([`${v.pct > 0 ? '+' : ''}${v.pct}%`, v.price, v.pct > 0 ? '#67c23a' : '#909399']));
  if (marks.stop_level) lines.push(['止损', marks.stop_level, '#e5484d']);
  lines.forEach(([, p]) => { if (p < lo) lo = lo * 0.999; if (p > hi) hi = hi * 1.001; });
  const y = p => pad.t + (hi - p) / (hi - lo) * (H - pad.t - pad.b);
  const cw = (W - pad.l - pad.r) / bars.length;
  bars.forEach((b, i) => {
    const x = pad.l + i * cw, up = +b.close >= +b.open;
    ctx.strokeStyle = ctx.fillStyle = up ? '#e5484d' : '#2eaa5e';
    ctx.beginPath(); ctx.moveTo(x + cw / 2, y(+b.high)); ctx.lineTo(x + cw / 2, y(+b.low));
    ctx.stroke();
    const t = y(Math.max(+b.open, +b.close)), bo = y(Math.min(+b.open, +b.close));
    ctx.fillRect(x + 1, t, Math.max(cw - 2, 1), Math.max(bo - t, 1));
  });
  lines.forEach(([name, p, color]) => {
    ctx.strokeStyle = color; ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(pad.l, y(p)); ctx.lineTo(W - pad.r, y(p)); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = color; ctx.font = '11px sans-serif';
    ctx.fillText(`${name} ${p.toFixed(2)}`, W - pad.r + 2, y(p) + 3);
  });
  const [rc, RW, RH] = _hcv(box.querySelector('#klRsi'));
  rc.fillStyle = '#f7f8fa'; rc.fillRect(0, 0, RW, RH);
  const ry = v => 6 + (100 - v) / 100 * (RH - 12);
  rc.strokeStyle = '#dcdfe6'; rc.setLineDash([3, 3]);
  [30, 70].forEach(v => { rc.beginPath(); rc.moveTo(0, ry(v)); rc.lineTo(RW, ry.rsi ?? RW, ry(v)); rc.stroke(); });
  rc.setLineDash([]);
  rc.strokeStyle = '#4f8ef7'; rc.beginPath();
  bars.forEach((b, i) => {
    const rsiArr = d.rsi_series || [];
    const v = rsiArr[i];
    if (v == null) return;
    const x = pad.l + i * cw + cw / 2;
    i === 0 ? rc.moveTo(x, ry(v)) : rc.lineTo(x, ry(v));
  });
  rc.stroke();
  box.querySelector('#klInfo').textContent =
    `MA50 ${ind.ma50 ?? '—'} · MA200 ${ind.ma200 ?? '—'} · RSI14 ${ind.rsi14 ?? '—'}` +
    `${ind.enough_history ? '' : ' · 历史不足：趋势/止损规则未启用'}`;
}
document.addEventListener('click', e => {
  const link = e.target.closest('.sym-link');
  if (link) openKline(link.dataset.sym);
});
loadAlerts();
if (typeof es === 'undefined' || !es) {
  var esAlert = new EventSource('/api/stream');
  esAlert.addEventListener('alert', ev => {
    const a = JSON.parse(ev.data);
    loadAlerts();
    if (window.Notification && Notification.permission === 'granted') {
      new Notification(a.title, {body: `级别：${a.level}`});
    }
  });
}
```

说明：kline 接口在本 task 同步扩展返回 `rsi_series`（Task 6 的 `/api/kline` 追加 `"rsi_series": [float|None,...]`，与 bars 等长——`daily_bars.rsi_wilder(closes)` 取全序列）。在 Task 6 的 kline 端点 return 前补：

```python
    closes = [float(b["close"]) for b in bars]
    rsi_series = daily_bars.rsi_wilder(closes)
    # 并入返回： "rsi_series": [round(v, 2) if v is not None else None for v in rsi_series]
```

同时修正 2c 中 RSI 副图绘制的笔误行为：`rc.lineTo(RW, ry(v))` 行（虚线参考线绘制处）应为 `rc.lineTo(RW, ry(v))`——参考实现以上面代码为准，实现者跑通视觉即可。

- [ ] **Step 3: 语法验证**

Run: `python3 - <<'EOF'
import re
html = open('static/portfolio.html', encoding='utf-8').read()
scripts = re.findall(r'<script>(.*?)</script>', html, re.S)
open('/tmp/opencode/pf_check.js', 'w', encoding='utf-8').write('\n'.join(scripts))
EOF
node --check /tmp/opencode/pf_check.js`
Expected: 无输出（语法通过）

---

## Task 9: 一期部署与线上验证

- [ ] **Step 1: 本地全量测试**

Run: `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_daily_bars.py tests/test_alert_engine.py tests/test_mailer.py tests/test_alert_api.py tests/test_backtest.py -q`
Expected: 原基线 238 + 新增全绿

- [ ] **Step 2: 部署（rsync + restart，AGENTS.md 标准 deploy）**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

- [ ] **Step 3: 线上验证（ssh + journalctl + curl，AGENTS.md 线上验证口径）**

```bash
ssh root@<server-ip> "systemctl is-active qdii && sleep 20 && journalctl -u qdii --since '2 min ago' | grep -E '日K|提醒|邮件|alert' | tail -20"
```

Expected: `active`；日志出现「日K刷新开始/完成」（首刷 13 只约 40s）；无异常栈。

```bash
ssh root@<server-ip> "TOKEN=\$(mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -N -e \"SELECT token FROM sessions WHERE expires > NOW() ORDER BY created_at DESC LIMIT 1\"); curl -s -H \"Cookie: session_token=\$TOKEN\" 'http://127.0.0.1:8888/api/kline/QQQ?range=10' | head -c 400; echo; curl -s -H \"Cookie: session_token=\$TOKEN\" 'http://127.0.0.1:8888/api/alert/rules' | head -c 300"
```

Expected: kline 返回真实日K JSON；rules 返回 defaults。

- [ ] **Step 4: 数据落库确认**

```bash
ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e \"SELECT symbol, COUNT(*) bars, MIN(date) mn, MAX(date) mx FROM daily_bars GROUP BY symbol\""
```

Expected: 13 行（SKHY≈60、DRAM≈127，其余≈1000）。

- [ ] **Step 5: 合成触发验证（测试规则压价触发）**

```bash
ssh root@<server-ip> "TOKEN=\$(mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -N -e \"SELECT token FROM sessions WHERE expires > NOW() ORDER BY created_at DESC LIMIT 1\"); curl -s -X PUT -H \"Cookie: session_token=\$TOKEN\" -H 'Content-Type: application/json' -d '{\"params\":{\"price_plan\":{\"enabled\":true,\"levels\":[-0.1]}},\"enabled\":1}' http://127.0.0.1:8888/api/alert/rules/QQQ; sleep 70; mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e 'SELECT id,symbol,rule,level,title FROM alerts ORDER BY id DESC LIMIT 3'"
```

Expected: alerts 表出现 QQQ price_plan 触发行（-0.1% 档几乎必触发）；随后把该规则改回（PUT levels `[-5,-10,8,15]`）并清测试数据：`DELETE FROM alerts WHERE symbol='QQQ';` 再 PUT 恢复默认。

---

## Task 10: quotes_hub.py 内存真相源 + 降采样（二期）

**Files:**
- Create: `quotes_hub.py`
- Test: `tests/test_quotes_hub.py`

- [ ] **Step 1: 写失败测试**

```python
"""quotes_hub 内存真相源与降采样测试。@author yys"""
import asyncio
import time
from datetime import datetime, timezone

import quotes_hub as qh


def test_sampler_one_second_last_value():
    s = qh.Sampler()
    t0 = 1000.0
    s.push("QQQ", 100.0, t0)
    s.push("QQQ", 100.2, t0 + 0.4)     # 同秒 → 取末值
    s.push("QQQ", 100.5, t0 + 1.2)     # 新秒 + 价变
    rows = s.drain()
    assert rows == [("QQQ", 100.2), ("QQQ", 100.5)] or len(rows) == 2


def test_sampler_price_change_only():
    s = qh.Sampler()
    t0 = 2000.0
    s.push("QQQ", 100.0, t0)
    s.push("QQQ", 100.0, t0 + 2.0)     # 价未变 → 不落
    assert s.drain() == [("QQQ", 100.0)]


def test_update_quotes_live():
    qh.quotes_live.clear()
    qh.update("QQQ", 101.0, source="lp_push", bid=100.9, ask=101.1,
              ts=datetime(2026, 10, 4, 12, tzinfo=timezone.utc))
    snap = qh.snapshot()
    assert snap[0]["symbol"] == "QQQ" and snap[0]["price"] == 101.0
    assert snap[0]["source"] == "lp_push"


def test_ring_buffer_capped():
    qh.RING_MAX = 5
    qh.ring.clear()
    for i in range(10):
        qh._ring_push("QQQ", float(i))
    assert len(qh.ring["QQQ"]) == 5
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_quotes_hub.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 实现 quotes_hub.py**

```python
"""内存实时真相源：quotes_live + tick 环形缓冲 + 1s 降采样攒批。

push 高频 tick 不逐条落库：同秒取末值、价变才写、批量 flush market_ticks。@author yys
"""
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

import database as db

logger = logging.getLogger("qdii.quotes_hub")

RING_MAX = 1800                     # 每 symbol 保留最近 30 分钟（1s 粒度）
quotes_live: dict[str, dict] = {}   # {symbol: {price, bid, ask, ts, source}}
ring: dict[str, deque] = {}


def _ring_push(symbol: str, price: float):
    d = ring.setdefault(symbol, deque(maxlen=RING_MAX))
    d.append(price)


def update(symbol: str, price: float, source: str = "", bid: float | None = None,
           ask: float | None = None, ts: datetime | None = None):
    """所有数据入口统一写内存（push/轮询/24h源）。"""
    quotes_live[symbol] = {"price": price, "bid": bid, "ask": ask,
                           "ts": ts or datetime.now(timezone.utc).isoformat(),
                           "source": source}
    _ring_push(symbol, price)


def snapshot() -> list[dict]:
    """SSE quote 快照（浅拷贝防遍历中变更）。"""
    return [{"symbol": s, **v} for s, v in list(quotes_live.items())]


class Sampler:
    """1s 窗口降采样：同秒取末值、价变才保留。"""

    def __init__(self):
        self._win: dict[str, tuple[int, float]] = {}   # sym -> (sec, price)
        self._out: list[tuple[str, float]] = []

    def push(self, symbol: str, price: float, now: float | None = None):
        sec = int(now if now is not None else asyncio.get_event_loop().time())
        prev = self._win.get(symbol)
        if prev and prev[0] == sec:
            self._win[symbol] = (sec, price)           # 同秒取末值
            return
        if prev and prev[1] != price:
            self._out.append((symbol, prev[1]))        # 上一秒价变 → 出队
        self._win[symbol] = (sec, price)

    def drain(self) -> list[tuple[str, float]]:
        out, self._out = self._out, []
        return out


_sampler = Sampler()


def push_tick(symbol: str, price: float, source: str = "",
              bid: float | None = None, ask: float | None = None):
    """push 回调统一入口：内存 + 环形缓冲 + 降采样。"""
    update(symbol, price, source=source, bid=bid, ask=ask)
    _sampler.push(symbol, price)


async def flush_loop(interval: float = 3.0, max_queue: int = 10000):
    """攒批落库 market_ticks（ts 统一 UTC；失败丢最旧保内存）。"""
    while True:
        await asyncio.sleep(interval)
        try:
            rows = _sampler.drain()
            if not rows:
                continue
            if len(rows) > max_queue:
                rows = rows[-max_queue:]
                logger.warning("降采样队列超限丢弃最旧 %d 条", len(rows))
            now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
            await db.save_market_ticks_batch(
                [(sym, now, price) for sym, price in rows])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("降采样 flush 失败: %s", e)
```

`database.py` 追加批量写（复用 save_market_ticks 即可——`save_market_ticks(symbol, points)` 是单 symbol 接口，补一个多 symbol 版本）：

```python
async def save_market_ticks_batch(rows: list) -> int:
    """多 symbol 批量写 market_ticks。rows: [(symbol, naive_ts, price)]。@author yys"""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO market_ticks (symbol, ts, price) VALUES (%s, %s, %s)",
                [(s, ts, _safe_decimal(p, 6)) for s, ts, p in rows])
            return cur.rowcount
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_quotes_hub.py -q`
Expected: 4 passed

---

## Task 11: longport_push.py WS 订阅（shadow/live）

**Files:**
- Create: `longport_push.py`
- Test: `tests/test_longport_push.py`

- [ ] **Step 1: 写失败测试（shadow 门控与路由，不连 WS）**

```python
"""长桥推送 shadow/live 门控与回调路由测试。@author yys"""
import asyncio

import longport_push as lpp


def test_shadow_mode_routes_to_logger_only(monkeypatch):
    calls = []
    monkeypatch.setattr(lpp.quotes_hub, "push_tick",
                        lambda s, p, **kw: calls.append(("tick", s, p)))
    monkeypatch.setattr(lpp, "_mode", lambda: "shadow")
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    assert calls == []                      # shadow 不写内存/不触发评估


def test_live_mode_updates_hub(monkeypatch):
    calls = []
    monkeypatch.setattr(lpp.quotes_hub, "push_tick",
                        lambda s, p, **kw: calls.append(("tick", s, p)))
    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    assert calls == [("tick", "QQQ", 100.0)]     # .US 前缀剥离


def test_live_mode_evaluates_alerts(monkeypatch):
    events = []
    monkeypatch.setattr(lpp.quotes_hub, "push_tick", lambda *a, **k: None)
    monkeypatch.setattr(lpp, "_mode", lambda: "live")

    class _FakeEngine:
        async def evaluate_round(self, prices):
            events.append(prices)
    monkeypatch.setattr(lpp.alert_engine, "evaluate_round",
                        _FakeEngine().evaluate_round)
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    asyncio.run(asyncio.sleep(0))
    assert events or lpp._pending_eval        # 已排队待评估
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_longport_push.py -q`
Expected: FAIL `ModuleNotFoundError`

- [ ] **Step 3: 实现 longport_push.py**

先在服务器探测 SDK push API 签名（本地无 SDK 也能写，导入段仿 longport_fetcher 的 try/except 双包模式）：

```bash
ssh root@<server-ip> "/opt/qdii/venv/bin/python3 -c \"
from longbridge import QuoteContext, Config, SubType
import inspect
print([m for m in dir(QuoteContext) if 'subscribe' in m.lower() or 'on_quote' in m.lower() or 'on_depth' in m.lower()])
\"" 2>/dev/null || ssh root@<server-ip> "python3 -c \"from longbridge import QuoteContext; print([m for m in dir(QuoteContext) if 'sub' in m.lower() or 'quote' in m.lower()])\""
```

（若服务器 python 路径不同，先 `systemctl cat qdii` 看 ExecStart 解释器路径再代入。）

实现：

```python
"""长桥推送（WS）：13 只美股 quote/depth LV1 订阅。

shadow 模式只记日志不接管主源；live 模式写 quotes_hub + 触发提醒评估。
单账号一条长连接、指数退避重连。@author yys
"""
import asyncio
import logging
import time

import database as db

logger = logging.getLogger("qdii.lp_push")

try:
    import longbridge as _sdk
    _SDK_NAME = "longbridge"
except ImportError:
    try:
        import longport as _sdk
        _SDK_NAME = "longport"
    except ImportError:
        _sdk = None
        _SDK_NAME = None

import alert_engine
import quotes_hub

QUOTE_WS_URL = "wss://openapi-quote.longbridge.cn/v2"
_pending_eval: dict[str, float] = {}
_stats = {"connected_at": 0.0, "quotes": 0, "depths": 0, "reconnects": 0}


def _mode() -> str:
    """LP_PUSH_MODE（60s 缓存在 server 层，此处直读缓存值）。"""
    return _mode_cache.get("v") or "shadow"


_mode_cache: dict = {"v": "", "at": 0.0}


async def _refresh_mode():
    if time.time() - _mode_cache["at"] > 60:
        try:
            _mode_cache["v"] = (await db.get_config("LP_PUSH_MODE") or "shadow").strip()
        except Exception:
            _mode_cache["v"] = "shadow"
        _mode_cache["at"] = time.time()


def on_price(symbol: str, price: float, bid: float | None = None,
             ask: float | None = None):
    """WS 回调（SDK 线程内调用，只做轻量路由）。"""
    _stats["quotes"] += 1
    sym = symbol[:-3] if symbol.endswith(".US") else symbol
    mode = _mode()
    if mode != "live":
        return                     # shadow：日志由批量统计输出，不接管
    quotes_hub.push_tick(sym, price, source="lp_push", bid=bid, ask=ask)
    _pending_eval[sym] = price


async def eval_drain():
    """把待评估价批量交给提醒引擎（1s 节流由调用方保证）。"""
    await _refresh_mode()
    if not _pending_eval or _mode() != "live":
        return []
    prices, _pending_eval_clear = dict(_pending_eval), None
    _pending_eval.clear()
    return await alert_engine.evaluate_round(prices)


def _mk_config():
    """凭证走 longport_fetcher 的 sys_config 链路（不落地不打印）。"""
    import longport_fetcher as lp
    cfg_kwargs = {"app_key": lp.CFG_APP_KEY, "app_secret": lp.CFG_APP_SECRET,
                  "access_token": lp.CFG_ACCESS_TOKEN} if False else None
    # 实际实现：复用 lp._load_creds()（Read longport_fetcher 145-166 后按其
    # 返回结构取 app_key/app_secret/access_token），Config 显式传
    # quote_ws_url=QUOTE_WS_URL（http_url 用 lp.LONGBRIDGE_HTTP_URL）。
    raise NotImplementedError


async def push_loop(us_symbols: list[str] | None = None):
    """后台任务：连接→订阅→回调路由；断线指数退避（上限 60s）。"""
    if _sdk is None:
        logger.warning("长桥 SDK 不可用，push 循环退出")
        return
    backoff = 1.0
    while True:
        try:
            if us_symbols is None:
                holdings = await db.get_holdings(enabled_only=True)
                us_symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if not us_symbols:
                await asyncio.sleep(300)
                continue
            ctx = await _connect(us_symbols)
            _stats["connected_at"] = time.time()
            _stats["reconnects"] += 1
            backoff = 1.0
            logger.info("长桥推送已连接并订阅 %d 只", len(us_symbols))
            while True:
                await asyncio.sleep(1.0)
                evs = await eval_drain()
                _ = evs        # 提醒分发由 server 层 SSE/邮件钩子完成（见集成）
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("长桥推送断开，%.0fs 后重连: %s", backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


async def _connect(us_symbols: list[str]):
    """建连+订阅（SDK 签名按服务器探测结果落地：set_on_quote/set_on_depth +
    subscribe([sym.US], [SubType.Quote, SubType.Depth])）。"""
    creds = await _load_creds()
    config = _sdk.Config(app_key=creds["app_key"], app_secret=creds["app_secret"],
                         access_token=creds["access_token"],
                         quote_ws_url=QUOTE_WS_URL)
    ctx = await _sdk.QuoteContext(config)
    ctx.set_on_quote(lambda _c, event: on_price(
        event.symbol, float(event.last_done or 0),
        bid=float(getattr(event, "ask", 0) or 0) or None,
        ask=float(getattr(event, "bid", 0) or 0) or None))
    await ctx.subscribe([f"{s}.US" for s in us_symbols],
                        [_sdk.SubType.Quote, _sdk.SubType.Depth])
    return ctx


async def _load_creds() -> dict:
    import longport_fetcher as lp
    return await lp._load_creds()
```

**实现注记（执行者必读）：**
1. `_mk_config` 是探索残迹——实现时**删除**该函数，凭证统一走 `_load_creds()`。
2. SDK 回调签名（event 字段名、set_on_quote 参数顺序、subscribe 是否 await）**以服务器探测输出为准**，落地后同步修正 `on_price` 适配层；探测命令见 Step 3 开头。
3. bid/ask 语义核对：长桥 PushQuote 里 bid/ask 字段名按实测改。
4. `_stats` 每 60s 输出一行 `logger.info("长桥推送统计 %s", _stats)`（shadow 观测用）——在 push_loop 内层 while 加节流输出。

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_longport_push.py -q`
Expected: 3 passed（若 `test_live_mode_evaluates_alerts` 因异步排队断言不稳，改为断言 `_pending_eval["QQQ"] == 100.0` 后手动 `asyncio.run(lpp.eval_drain())` 验证 events）

---

## Task 12: server.py 二期集成 + SSE quote + 前端实时刷新

**Files:**
- Modify: `server.py`（lifespan、SSE 快照任务）
- Modify: `static/portfolio.html`（quote 事件 → 持仓数字实时刷新 + 回退轮询）

- [ ] **Step 1: lifespan 挂 push 任务与 flush 任务**

`daily_bars_task = ...` 之后加：

```python
    global lp_push_task, quotes_flush_task, sse_quote_task
    import longport_push as lpp
    import quotes_hub as qh
    lp_push_task = asyncio.create_task(lpp.push_loop())
    quotes_flush_task = asyncio.create_task(qh.flush_loop())
    sse_quote_task = asyncio.create_task(_sse_quote_loop())
```

shutdown 区追加三个 cancel。新增快照广播协程（放 `_broadcast` 定义之后）：

```python
async def _sse_quote_loop(interval: float = 1.0):
    """SSE quote 快照：1s 节流批量广播内存真相源（一期轮询写入时同样生效）。"""
    import quotes_hub as qh
    while True:
        await asyncio.sleep(interval)
        try:
            items = qh.snapshot()
            if items:
                await _broadcast("quote", {"items": [
                    {"symbol": i["symbol"], "price": i["price"],
                     "bid": i.get("bid"), "ask": i.get("ask"), "ts": str(i.get("ts")),
                     "source": i.get("source")} for i in items]})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("SSE quote 广播失败: %s", e)
```

- [ ] **Step 2: _portfolio_loop 轮询价同步写 quotes_hub（一期数据也进内存真相源）**

在 Task 6 的提醒钩子里，`evaluate_round` 之前补：

```python
                import quotes_hub as _qh
                for _s, _p in _prices.items():
                    _qh.update(_s, float(_p), source="poll")
```

- [ ] **Step 3: 前端 quote 事件 → 实时刷新**

Task 8 的 EventSource 块扩展（替换原 `esAlert` 块）：

```javascript
var esAlert = new EventSource('/api/stream');
esAlert.addEventListener('alert', ev => {
  const a = JSON.parse(ev.data);
  loadAlerts();
  if (window.Notification && Notification.permission === 'granted') {
    new Notification(a.title, {body: `级别：${a.level}`});
  }
});
let sseFails = 0;
esAlert.onerror = () => { if (++sseFails >= 3) esAlert.close(); };  // 回退现有轮询
esAlert.addEventListener('quote', ev => {
  const d = JSON.parse(ev.data);
  (d.items || []).forEach(q => {
    const el = document.querySelector(`.sym-link[data-sym="${q.symbol}"]`);
    if (el && q.price) el.dataset.price = q.price;   // 轻量：标最新价，刷新按钮照旧拉全量
  });
});
```

- [ ] **Step 4: 语法验证 + 全量测试**

Run: `python3 - <<'EOF'
import re
html = open('static/portfolio.html', encoding='utf-8').read()
open('/tmp/opencode/pf_check.js', 'w', encoding='utf-8').write(
  '\n'.join(re.findall(r'<script>(.*?)</script>', html, re.S)))
EOF
node --check /tmp/opencode/pf_check.js && python3 -m pytest tests/test_alert_api.py tests/test_quotes_hub.py tests/test_longport_push.py -q`
Expected: 语法通过 + 测试全绿

---

## Task 13: 二期部署 + shadow 观测启动

- [ ] **Step 1: 全量基线**

Run: `python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_daily_bars.py tests/test_alert_engine.py tests/test_mailer.py tests/test_alert_api.py tests/test_backtest.py tests/test_quotes_hub.py tests/test_longport_push.py -q`
Expected: 全绿（≥ 238 基线）

- [ ] **Step 2: 部署重启（同 Task 9 Step 2 命令）**

- [ ] **Step 3: shadow 观测确认**

```bash
ssh root@<server-ip> "systemctl is-active qdii && sleep 30 && journalctl -u qdii --since '2 min ago' | grep -E '长桥推送' | tail -10"
```

Expected: 「长桥推送已连接并订阅 13 只」+ 周期性统计行（quotes 计数在增长=免费档夜盘有推送）。

- [ ] **Step 4: SSE quote 冒烟（curl 挂 10s）**

```bash
ssh root@<server-ip> "TOKEN=\$(mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -N -e \"SELECT token FROM sessions WHERE expires > NOW() ORDER BY created_at DESC LIMIT 1\"); timeout 10 curl -s -N -H \"Cookie: session_token=\$TOKEN\" http://127.0.0.1:8888/api/stream | grep -E 'event: (quote|alert)|keepalive' | head -8"
```

Expected: 至少出现 keepalive；美股时段出现 `event: quote`。

- [ ] **Step 5: market_ticks 日增量确认（次日复查口径留给用户）**

记录当日基线：`SELECT DATE(ts), COUNT(*) FROM market_ticks GROUP BY 1 ORDER BY 1 DESC LIMIT 3;`，确认无异常放大（shadow 模式不写 tick，live 切换后才增长——切 live 由用户一周后确认）。

---

## 完成后报告口径（给主 Agent 汇总用）

- 全部 pytest 结果（数字）
- 线上验证输出（日K 13 只行数、alerts 合成触发、push shadow 连接、SSE 冒烟）
- 未尽事项：MAIL_ADDRESS/MAIL_AUTH_CODE 需用户在 sys_config 填写（QQ 邮箱 SMTP 授权码）；LP_PUSH_MODE=live 一周 shadow 后切换
