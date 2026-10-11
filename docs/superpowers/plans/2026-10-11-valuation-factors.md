# 估值因子层（PE/PB/PEG/股息率）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 决策上下文层补估值面：长桥 static_info 日更（eps_ttm/bps/股息率，券商已换算 USD/ADS 口径）+ EDGAR companyconcept 周更（EPS 同比增速）→ `factors[symbol].valuation` 注入双端点 + 日K弹层展示。

**Architecture:** 新模块 `valuation.py`（归一化/增速解析/轮编排），挂 `edgar_daily_loop` 尾部（零新增循环）；落库只存慢变原始值（pe/pb/peg 注入时用现价实时算）；ETF 门控（长桥 ETF 字段是噪声，实测 QQQ eps=-88）。

**Tech Stack:** 长桥 SDK `ctx.static_info`（复用 `_get_ctx/_sdk_call/_mark_*`）、SEC companyconcept API（复用 `_edgar_get` 2s 节流/UA/锁）、MySQL upsert、pytest（15 文件基线 **495 passed** 不得回归）。

**Spec:** `docs/superpowers/specs/2026-10-11-valuation-factors-design.md`（实测锚点 §7 必读）。

**对 spec 的两处执行期修正（已审定）：**
1. `holding_valuations` 表**去掉 reason 列**——reason 由注入时从行数据+asset_type 推导，避免日更（eps/bps）与周更（growth）双节奏 upsert 互相覆盖。
2. upsert 拆两个函数：`upsert_holding_valuation_static`（日更字段）/`upsert_holding_valuation_growth`（周更字段），各自 ON DUPLICATE KEY UPDATE 仅本组列。

---

### Task 1: database.py — holding_valuations 表 + CRUD

**Files:**
- Modify: `database.py`（init_tables 内建表，参照 insider_tx 建表位置与缩进；CRUD 放 `get_insider_accessions` 之后 :1074 附近）
- Test: `tests/test_valuation.py`（新建）

- [ ] **Step 1: 写失败测试**（fake pool 模式完全参照 `tests/test_edgar_db.py` 的现有 fake pool/monkeypatch 写法）

```python
"""估值因子层测试：纯函数 + DB CRUD + 轮编排（全部打桩零网络零 DB 连接）。@author yys"""
import asyncio
import json
import pytest
from types import SimpleNamespace

import database as db
import valuation
import longport_fetcher
import edgar_fetcher


# ---------------------------------------------------------------------------
# DB CRUD（fake pool 模式参照 tests/test_edgar_db.py）
# ---------------------------------------------------------------------------

def test_upsert_valuation_static_and_get(monkeypatch):
    """日更字段 upsert 不触碰 growth 列；周更 upsert 仅更新 growth。"""
    calls = []

    async def fake_exec(sql, params=None):
        calls.append((sql, params))

    async def fake_dict(sql, params=None):
        return [{"symbol": "NVDA", "eps_ttm": 8.0, "bps": 9.5,
                 "growth_pct": 66.67, "dividend_yield": 0.52,
                 "updated_at": "2026-10-11 16:00:00"}]

    monkeypatch.setattr(db, "execute", fake_exec)
    monkeypatch.setattr(db, "execute_dict", fake_dict)
    asyncio.run(db.upsert_holding_valuation_static(
        "NVDA", 8.0, 9.5, 0.52))
    asyncio.run(db.upsert_holding_valuation_growth("NVDA", 66.67))
    # static upsert 只更新 eps/bps/dy 三列
    assert "eps_ttm=VALUES(eps_ttm)" in calls[0][0]
    assert "growth_pct" not in calls[0][0].split("ON DUPLICATE")[1]
    # growth upsert 只更新 growth 列
    assert "growth_pct=VALUES(growth_pct)" in calls[1][0]
    assert "eps_ttm" not in calls[1][0].split("ON DUPLICATE")[1]
    rows = asyncio.run(db.get_valuations())
    assert rows["NVDA"]["eps_ttm"] == 8.0
    assert rows["NVDA"]["growth_pct"] == 66.67
```

- [ ] **Step 2: 跑测试确认失败**：`python3 -m pytest tests/test_valuation.py -q` → FAIL（`upsert_holding_valuation_static` 不存在）

- [ ] **Step 3: 实现**（database.py）

建表（init_tables 内 insider_tx 建表语句之后）：

```python
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS holding_valuations (
              symbol         VARCHAR(16)   NOT NULL,
              eps_ttm        DECIMAL(16,6) NULL,
              bps            DECIMAL(16,6) NULL,
              growth_pct     DECIMAL(10,4) NULL COMMENT '最近两个完整财年EPS同比%（原币）',
              dividend_yield DECIMAL(8,4)  NULL,
              updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (symbol)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='估值因子原始值（长桥日更+EDGAR周更增速）'
        """)
```

CRUD（get_insider_accessions 后）：

```python
async def upsert_holding_valuation_static(symbol: str, eps_ttm, bps,
                                          dividend_yield) -> int:
    """日更 upsert（长桥 static_info 口径）：仅 eps/bps/dividend_yield 三列，
    不触碰 growth_pct（周更节奏，防覆盖）。@author yys"""
    return await execute(
        "INSERT INTO holding_valuations (symbol, eps_ttm, bps, dividend_yield) "
        "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
        "eps_ttm=VALUES(eps_ttm), bps=VALUES(bps), "
        "dividend_yield=VALUES(dividend_yield)",
        (symbol, _safe_decimal(eps_ttm, 6), _safe_decimal(bps, 6),
         _safe_decimal(dividend_yield, 4)))


async def upsert_holding_valuation_growth(symbol: str, growth_pct) -> int:
    """周更 upsert（EDGAR companyconcept 增速）：仅 growth 列。@author yys"""
    return await execute(
        "INSERT INTO holding_valuations (symbol, growth_pct) VALUES (%s,%s) "
        "ON DUPLICATE KEY UPDATE growth_pct=VALUES(growth_pct)",
        (symbol, _safe_decimal(growth_pct, 4)))


async def get_valuations() -> dict[str, dict]:
    """全表估值行 → {symbol: row}（≤20 行无过滤必要）。@author yys"""
    rows = await execute_dict(
        "SELECT symbol, eps_ttm, bps, growth_pct, dividend_yield, "
        "DATE_FORMAT(updated_at, '%%Y-%%m-%%d %%H:%%i:%%s') updated_at "
        "FROM holding_valuations")
    return {r["symbol"]: r for r in rows}
```

注意 `_safe_decimal` 已存在（save_insider_tx 在用）；None 入参时其现有行为（返回 None）即可，若实现发现 None 会抛异常则加 `None if v is None else` 防御。

- [ ] **Step 4: 跑测试通过**：`python3 -m pytest tests/test_valuation.py -q` → PASS
- [ ] **Step 5: Commit**：`git add database.py tests/test_valuation.py && git commit -m "feat(db): holding_valuations 表与双节奏 upsert CRUD（估值因子层Task1）"`

---

### Task 2: 长桥 fetch_static_info + EDGAR fetch_companyconcept

**Files:**
- Modify: `longport_fetcher.py`（`fetch_quote_map` 后面加，约 :473 后）
- Modify: `edgar_fetcher.py`（`fetch_form4_text` 后加；`SUBMISSIONS_URL` 常量区加 URL 常量）
- Test: `tests/test_longport.py`、`tests/test_edgar_fetcher.py`（扩展）

- [ ] **Step 1: 写失败测试**

test_longport.py 追加（打桩模式参照该文件现有 `_get_ctx/_sdk_call` 测试写法）：

```python
def test_fetch_static_info_success_and_failure(monkeypatch):
    import longport_fetcher as lp
    from types import SimpleNamespace

    async def fake_ctx():
        return SimpleNamespace()

    async def fake_call(fn, *args):
        assert args == (["NVDA.US", "BABA.US"],)
        return [SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0)]

    monkeypatch.setattr(lp, "_SDK_OK", True)
    monkeypatch.setattr(lp, "token_invalid", False)
    monkeypatch.setattr(lp, "_backoff_until", 0.0)
    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)
    monkeypatch.setattr(lp, "_sdk_call", fake_call)
    infos = asyncio.run(lp.fetch_static_info(["NVDA", "BABA"]))
    assert infos and infos[0].symbol == "NVDA.US"

    async def fake_call_fail(fn, *args):
        raise RuntimeError("boom")

    monkeypatch.setattr(lp, "_sdk_call", fake_call_fail)
    assert asyncio.run(lp.fetch_static_info(["NVDA"])) == []
```

test_edgar_fetcher.py 追加（打桩模式参照 `_patch_client`/`test_edgar_get_throttle_and_backoff`；若直接测 URL 拼接可简化为断言 `COMPANYCONCEPT_URL` 格式化结果含 10 位补零 CIK + `_edgar_get` 被以该 URL 调用）：

```python
def test_fetch_companyconcept_url_padding(monkeypatch):
    captured = []

    async def fake_get(url):
        captured.append(url)
        return "{}"

    monkeypatch.setattr(ef, "_edgar_get", fake_get)
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))
    r = asyncio.run(ef.fetch_companyconcept(1045810))
    assert r == "{}"
    assert captured == [
        "https://data.sec.gov/api/xbrl/companyconcept/"
        "CIK0001045810/us-gaap/EarningsPerShareDiluted.json"]

    async def fake_none(url):
        return None
    monkeypatch.setattr(ef, "_edgar_get", fake_none)
    assert asyncio.run(ef.fetch_companyconcept(2120882)) is None
```

- [ ] **Step 2: 跑测试确认失败**（两文件各自 FAIL：函数不存在）
- [ ] **Step 3: 实现**

longport_fetcher.py（`fetch_quote_map` 之后；与 `_quote_raw` 同款防御链）：

```python
async def fetch_static_info(symbols: list[str]) -> list:
    """批量标的静态信息（eps_ttm/bps/dividend_yield，券商已换算 USD/ADS
    口径）。失败/不可用/退避期返回 []（ETF 字段为噪声由上层 asset_type
    门控）。@author yys"""
    if not _SDK_OK or token_invalid or not symbols:
        return []
    if time.monotonic() < _backoff_until:
        return []
    ctx = await _get_ctx()
    if ctx is None:
        return []
    try:
        infos = await _sdk_call(ctx.static_info,
                                [f"{s}.US" for s in symbols])
        _mark_success()
        return infos or []
    except asyncio.TimeoutError:
        return []
    except Exception as e:
        _mark_failure(e)
        return []
```

edgar_fetcher.py（常量区 SUBMISSIONS_URL 后 + fetch_form4_text 后）：

```python
COMPANYCONCEPT_URL = ("https://data.sec.gov/api/xbrl/companyconcept/"
                      "CIK{cik:010d}/us-gaap/{concept}.json")
```

```python
async def fetch_companyconcept(cik: int,
                               concept: str = "EarningsPerShareDiluted"
                               ) -> str | None:
    """单概念 XBRL 序列（年度 EPS 同比增速用；~10-45KB）。404/失败 None。@author yys"""
    return await _edgar_get(COMPANYCONCEPT_URL.format(cik=cik, concept=concept))
```

- [ ] **Step 4: 跑测试通过**（两文件新增用例 PASS + 原有用例不回归）
- [ ] **Step 5: Commit**：`git add longport_fetcher.py edgar_fetcher.py tests/test_longport.py tests/test_edgar_fetcher.py && git commit -m "feat(fetcher): 长桥static_info批量封装与EDGAR companyconcept单概念拉取（估值因子层Task2）"`

---

### Task 3: valuation.py — 纯计算（归一化 + 增速解析）

**Files:**
- Create: `valuation.py`
- Test: `tests/test_valuation.py`（追加）

- [ ] **Step 1: 写失败测试**

```python
# ---------------------------------------------------------------------------
# 纯函数：normalize_static_info / parse_eps_growth（实测锚定 fixture）
# ---------------------------------------------------------------------------

def test_normalize_static_info():
    infos = [
        SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0033, bps=9.5014,
                        dividend_yield=0.52),
        SimpleNamespace(symbol="BABA.US", eps_ttm=4.3942, bps=64.3653,
                        dividend_yield=1.03),
        SimpleNamespace(symbol="SMH.US", eps_ttm=0, bps=0, dividend_yield=0),
    ]
    out = valuation.normalize_static_info(infos)
    assert out["NVDA"] == {"eps_ttm": 8.0033, "bps": 9.5014,
                           "dividend_yield": 0.52}
    assert out["BABA"]["eps_ttm"] == 4.3942
    assert out["SMH"]["eps_ttm"] == 0.0          # 保留原值，门控在上层
    assert "QQQ" not in out                      # 静默空（未返回标的）自然缺
    assert valuation.normalize_static_info(None) == {}
    # Decimal 与 None 防御
    from decimal import Decimal
    out2 = valuation.normalize_static_info([
        SimpleNamespace(symbol="X.US", eps_ttm=Decimal("1.5"), bps=None,
                        dividend_yield=None)])
    assert out2["X"] == {"eps_ttm": 1.5, "bps": None, "dividend_yield": None}


NVDA_CC = json.dumps({"units": {"USD/shares": [
    {"start": "2023-01-30", "end": "2024-01-28", "val": 1.19,
     "form": "10-K", "fp": "FY", "filed": "2024-02-21"},
    {"start": "2024-01-29", "end": "2025-01-26", "val": 2.94,
     "form": "10-K", "fp": "FY", "filed": "2025-02-26"},
    {"start": "2024-01-29", "end": "2025-01-26", "val": 2.94,
     "form": "10-K", "fp": "FY", "filed": "2025-02-26"},   # 重复申报行
    {"start": "2025-01-27", "end": "2026-01-25", "val": 4.90,
     "form": "10-K", "fp": "FY", "filed": "2026-02-25"},
    {"start": "2025-01-27", "end": "2026-04-26", "val": 3.5,
     "form": "10-Q", "fp": "Q1", "filed": "2026-05-28"},   # 季度值须被过滤
]}})

BABA_CC = json.dumps({"units": {
    "CNY/shares": [
        {"start": "2023-04-01", "end": "2024-03-31", "val": 6.94,
         "form": "20-F", "fp": "FY", "filed": "2024-06-01"},
        {"start": "2024-04-01", "end": "2025-03-31", "val": 6.7,
         "form": "20-F", "fp": "FY", "filed": "2025-06-01"},
        {"start": "2025-04-01", "end": "2026-03-31", "val": 5.5,
         "form": "20-F", "fp": "FY", "filed": "2026-06-01"},
    ],
    "USD/shares": [
        {"start": "2024-04-01", "end": "2025-03-31", "val": 0.92,
         "form": "20-F", "fp": "FY", "filed": "2025-06-01"},
        {"start": "2025-04-01", "end": "2026-03-31", "val": 0.80,
         "form": "20-F", "fp": "FY", "filed": "2026-06-01"},
    ]}})

CCJ_CC = json.dumps({"units": {"USD/shares": [
    {"start": "2024-01-01", "end": "2024-12-31", "val": 0.34,
     "form": "40-F", "fp": "FY", "filed": "2025-03-01"},
    {"start": "2025-01-01", "end": "2025-12-31", "val": 0.42,
     "form": "40-F", "fp": "FY", "filed": "2026-03-01"},
]}})


def test_parse_eps_growth():
    # NVDA：重复行去重 + 季度混入过滤 → 4.90/2.94-1
    assert valuation.parse_eps_growth(NVDA_CC) == pytest.approx(66.667, abs=0.01)
    # BABA：主币种 = 行数最多的 unit（CNY 3 行 > USD 2 行）→ 原币同比
    assert valuation.parse_eps_growth(BABA_CC) == pytest.approx(-17.91, abs=0.01)
    # 40-F 申报人有效
    assert valuation.parse_eps_growth(CCJ_CC) == pytest.approx(23.53, abs=0.01)
    # 财年不足两个 / v1=0 / 坏 JSON / None
    one_fy = json.dumps({"units": {"USD/shares": [
        {"start": "2025-01-01", "end": "2025-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(one_fy) is None
    zero_base = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 0,
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(zero_base) is None
    assert valuation.parse_eps_growth("not json") is None
    assert valuation.parse_eps_growth(None) is None
    # 同财年重复行取 filed 最大（后申报修正值优先）
    corrected = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.2,
         "form": "10-K/A", "fp": "FY", "filed": "2025-04-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 2.4,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(corrected) == pytest.approx(100.0, abs=0.01)
```

注意：`10-K/A` 不在 form 白名单（保守：修正申报不参与，取 filed 最大的白名单行）——若实现时想让 `10-K/A` 参与，测试 fixture 里 corrected 的第二行 form 应改 `10-K` 并另立用例；本计划按保守口径：**form 严格 `in ("10-K", "20-F", "40-F")`**，corrected 用例改用两行 10-K：

```python
    corrected = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.2,
         "form": "10-K", "fp": "FY", "filed": "2025-04-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 2.4,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
```

- [ ] **Step 2: 跑测试确认失败**（valuation 模块不存在）
- [ ] **Step 3: 实现 valuation.py**

```python
"""估值因子层：长桥 static_info 日更（eps_ttm/bps/股息率）+ EDGAR
companyconcept 周更（EPS 同比增速）→ holding_valuations 落库。

口径契约（2026-10-11 实测锚定，详见 specs/2026-10-11-valuation-factors-design.md）：
- 长桥 eps_ttm/bps 为券商换算后 USD/ADS 口径（BABA 无需换算）；
- ETF 静态字段是噪声（QQQ eps=-88/SMH 全 0），asset_type 门控拦截；
- 增速取 companyconcept 主币种（行数最多 unit）最近两个完整财年同比，
  原币比值货币无关；pe/pb/peg 不落库，注入时用现价实时算。@author yys
"""
import asyncio
import json
import logging
from datetime import datetime

import database as db
import edgar_fetcher
import longport_fetcher

logger = logging.getLogger("qdii.valuation")

GROWTH_INTERVAL_SEC = 6 * 86400      # 增速周更节流（>6 天必跑）
_FY_FORMS = ("10-K", "20-F", "40-F")


def normalize_static_info(infos) -> dict:
    """长桥 SecurityStaticInfo 列表 → {sym: {eps_ttm, bps, dividend_yield}}。
    Decimal→float；0/负值保留原值（ETF 噪声门控在上层）。@author yys"""
    out = {}
    for q in infos or []:
        sym = str(getattr(q, "symbol", "")).replace(".US", "", 1)
        if not sym:
            continue

        def _f(name):
            v = getattr(q, name, None)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        out[sym] = {"eps_ttm": _f("eps_ttm"), "bps": _f("bps"),
                    "dividend_yield": _f("dividend_yield")}
    return out


def parse_eps_growth(text) -> float | None:
    """companyconcept JSON → 最近两个完整财年 EPS 同比%（主币种 unit）。

    行过滤：form ∈ {10-K,20-F,40-F} 且 fp=FY 且跨年（start 年≠end 年，
    防季度值混入）；同财年多行（重复申报）取 filed 最大行；财年不足
    两个 / 基期 0 / 解析失败 → None。@author yys"""
    try:
        d = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None
    units = d.get("units") or {}
    if not units:
        return None
    rows = max(units.values(), key=len) or []
    fy: dict[tuple, tuple] = {}      # (start年,end年) -> (filed, val)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if (r.get("form") not in _FY_FORMS or r.get("fp") != "FY"
                or not r.get("start") or not r.get("end")
                or r.get("val") is None):
            continue
        if r["start"][:4] == r["end"][:4]:
            continue
        key = (r["start"][:4], r["end"][:4])
        cand = (str(r.get("filed", "")), float(r["val"]))
        if key not in fy or cand[0] >= fy[key][0]:
            fy[key] = cand
    if len(fy) < 2:
        return None
    ordered = sorted(fy.items(), key=lambda kv: kv[0][1])
    v1 = ordered[-2][1][1]
    v2 = ordered[-1][1][1]
    if v1 == 0:
        return None
    return round((v2 / v1 - 1) * 100, 4)
```

- [ ] **Step 4: 跑测试通过**：`python3 -m pytest tests/test_valuation.py -q`
- [ ] **Step 5: Commit**：`git add valuation.py tests/test_valuation.py && git commit -m "feat(valuation): static_info归一化与companyconcept增速纯解析（实测锚定fixture，估值因子层Task3）"`

---

### Task 4: valuation.py — run_valuation_round + edgar_daily_loop 挂载

**Files:**
- Modify: `valuation.py`（追加 run_valuation_round）
- Modify: `edgar_fetcher.py`（`edgar_daily_loop` 内 `run_edgar_round()` 调用后挂载，约 :376）
- Test: `tests/test_valuation.py`（追加）

- [ ] **Step 1: 写失败测试**（打桩模式参照 tests/test_edgar_fetcher.py 的 `_round_env`）

```python
# ---------------------------------------------------------------------------
# 轮编排：run_valuation_round（打桩长桥/EDGAR/db）
# ---------------------------------------------------------------------------

def _v_env(monkeypatch, holdings, infos, cc_texts, growth_ts=None):
    cfg = {"VALUATION_GROWTH_TS": growth_ts}
    rec = {"static": [], "growth": [], "set_cfg": []}

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        cfg[key] = value
        rec["set_cfg"].append(key)

    async def fake_holdings(enabled_only=True):
        return holdings

    async def fake_up_static(sym, eps, bps, dy):
        rec["static"].append((sym, eps, bps, dy))

    async def fake_up_growth(sym, g):
        rec["growth"].append((sym, g))

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(db, "get_holdings", fake_holdings)
    monkeypatch.setattr(db, "upsert_holding_valuation_static", fake_up_static)
    monkeypatch.setattr(db, "upsert_holding_valuation_growth", fake_up_growth)

    async def fake_sinfo(symbols):
        return infos

    async def fake_cc(cik, concept="EarningsPerShareDiluted"):
        return cc_texts.get(cik)

    async def fake_tmap():
        return {"NVDA": 1045810, "BABA": 1577552}

    monkeypatch.setattr(longport_fetcher, "fetch_static_info", fake_sinfo)
    monkeypatch.setattr(edgar_fetcher, "fetch_companyconcept", fake_cc)
    monkeypatch.setattr(edgar_fetcher, "get_ticker_map", fake_tmap)
    return rec, cfg


_HOLDINGS = [
    {"symbol": "NVDA", "market": "US", "asset_type": "stock"},
    {"symbol": "BABA", "market": "US", "asset_type": "fpi"},
    {"symbol": "QQQ", "market": "US", "asset_type": "etf"},
    {"symbol": "DRAM", "market": "US", "asset_type": None},
    {"symbol": "F", "market": "US", "asset_type": None},
    {"symbol": "700.HK", "market": "HKFUND", "asset_type": None},
]


def test_run_valuation_round_full(monkeypatch):
    infos = [
        SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0, bps=9.5,
                        dividend_yield=0.52),
        SimpleNamespace(symbol="BABA.US", eps_ttm=4.39, bps=64.37,
                        dividend_yield=1.03),
        SimpleNamespace(symbol="DRAM.US", eps_ttm=0, bps=0,
                        dividend_yield=0),
        # F 不出现（_EDGAR_EXCLUDE，长桥批量请求列表已排除）
    ]
    rec, cfg = _v_env(monkeypatch, _HOLDINGS, infos,
                      {1045810: NVDA_CC, 1577552: BABA_CC})
    stats = asyncio.run(valuation.run_valuation_round())
    assert stats["rows"] == 5                      # 700.HK 不进（非 US）
    # F 被 _EDGAR_EXCLUDE 排除，长桥请求列表都不含 F
    assert ("F", None, None, None) not in rec["static"]
    # ETF fund 门控：QQQ 不写 static 值
    assert ("QQQ", None, None, None) in rec["static"]
    assert not any(r[0] == "QQQ" and r[1] is not None for r in rec["static"])
    # no_data：DRAM eps=0
    assert ("DRAM", 0.0, 0.0, 0.0) in rec["static"]
    # 增速：NVDA/BABA 均写入（首跑无 TS 必跑增速）
    assert ("NVDA", pytest.approx(66.667, abs=0.01)) in [
        (s, g) for s, g in rec["growth"]]
    assert ("BABA", pytest.approx(-17.91, abs=0.01)) in [
        (s, g) for s, g in rec["growth"]]
    assert cfg["VALUATION_GROWTH_TS"] is not None


def test_run_valuation_round_growth_throttle(monkeypatch):
    """非周一且 TS 距今 1 天 → 不拉增速（省请求），static 正常。"""
    from datetime import timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S")
    infos = [SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0, bps=9.5,
                             dividend_yield=0.52)]
    rec, cfg = _v_env(monkeypatch, _HOLDINGS[:1], infos, {},
                      growth_ts=yesterday)
    stats = asyncio.run(valuation.run_valuation_round())
    assert rec["static"] == [("NVDA", 8.0, 9.5, 0.52)]
    assert rec["growth"] == []                     # 节流期内零增速请求
    assert stats["growth_runs"] is False


def test_run_valuation_round_no_cik(monkeypatch):
    """长桥数值好但无 CIK 映射：static 写入、增速跳过。"""
    infos = [SimpleNamespace(symbol="GEV.US", eps_ttm=5.0, bps=30.0,
                             dividend_yield=0.5)]
    holdings = [{"symbol": "GEV", "market": "US", "asset_type": "stock"}]
    rec, cfg = _v_env(monkeypatch, holdings, infos, {})
    asyncio.run(valuation.run_valuation_round())
    assert rec["static"] == [("GEV", 5.0, 30.0, 0.5)]
    assert rec["growth"] == []
```

- [ ] **Step 2: 跑测试确认失败**
- [ ] **Step 3: 实现**（valuation.py 追加）

```python
async def run_valuation_round() -> dict:
    """估值轮：长桥批量静态信息（日更）+ EDGAR companyconcept 增速（周更
    门控：周一或距上次成功 >6 天或首跑）→ 双节奏 upsert。

    stats: rows=写入行数（US 标的，含门控行）/ fund=ETF 门控 /
    no_data=长桥数值缺失 / no_cik=无 CIK 映射 / growth_runs=本轮是否拉增速。
    @author yys"""
    holdings = await db.get_holdings(enabled_only=True)
    us = [h for h in holdings if h.get("market") == "US"
          and h["symbol"].upper() not in edgar_fetcher._EDGAR_EXCLUDE]
    stats = {"rows": 0, "fund": 0, "no_data": 0, "no_cik": 0,
             "growth_runs": False}
    infos = await longport_fetcher.fetch_static_info(
        [h["symbol"] for h in us])
    sinfo = normalize_static_info(infos)

    ts = await db.get_config("VALUATION_GROWTH_TS")
    run_growth = True
    if ts:
        try:
            last = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            age = (datetime.now() - last).total_seconds()
            run_growth = datetime.now().weekday() == 0 \
                or age > GROWTH_INTERVAL_SEC
        except ValueError:
            run_growth = True
    tmap = await edgar_fetcher.get_ticker_map() if run_growth else {}
    growth_written = False
    for h in us:
        sym = h["symbol"]
        try:
            s = sinfo.get(sym) or {}
            eps_ttm, bps = s.get("eps_ttm"), s.get("bps")
            dy = s.get("dividend_yield")
            if h.get("asset_type") == "etf":
                eps_ttm = bps = dy = None       # ETF 字段噪声，整体门控
                stats["fund"] += 1
            if eps_ttm is not None and bps is not None:
                await db.upsert_holding_valuation_static(sym, eps_ttm, bps, dy)
            else:
                stats["no_data"] += 1
            if run_growth and h.get("asset_type") in ("stock", "fpi") \
                    and eps_ttm and bps and eps_ttm > 0 and bps > 0:
                cik = tmap.get(sym.upper()) \
                    or edgar_fetcher._CIK_OVERRIDES.get(sym.upper())
                if cik is None:
                    stats["no_cik"] += 1
                else:
                    text = await edgar_fetcher.fetch_companyconcept(cik)
                    g = parse_eps_growth(text) if text else None
                    if g is not None:
                        await db.upsert_holding_valuation_growth(sym, g)
                        growth_written = True
            stats["rows"] += 1
        except Exception as e:
            logger.warning("估值处理 %s 失败（仅跳过该标的）: %s", sym, e)
            stats["rows"] += 1
    stats["growth_runs"] = run_growth
    if run_growth and growth_written:
        await db.set_config("VALUATION_GROWTH_TS",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "估值增速周更水位")
    return stats
```

edgar_fetcher.py `edgar_daily_loop` 挂载（`await run_edgar_round()` 所在 try 块内、round 完成后；注意该函数现有结构是 `try: ... await run_edgar_round() ... except`，把估值轮放 round 调用后同一 try 内但自包裹 try）：

```python
            # 估值因子轮（长桥日更 + EDGAR 周更增速）：独立兜底，失败不影响主轮
            try:
                import valuation
                vstats = await valuation.run_valuation_round()
                logger.info("估值轮完成: %s", vstats)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("估值轮失败: %s", e)
```

（import 放函数内是刻意的：避免 edgar_fetcher ↔ valuation 模块级循环 import。）

- [ ] **Step 4: 跑测试通过** + `python3 -m pytest tests/test_edgar_fetcher.py -q` 原有用例不回归
- [ ] **Step 5: Commit**：`git add valuation.py edgar_fetcher.py tests/test_valuation.py && git commit -m "feat(valuation): 估值轮编排（长桥日更+EDGAR周更门控）并挂载edgar_daily_loop（估值因子层Task4）"`

---

### Task 5: server.py — factors 注入 valuation

**Files:**
- Modify: `server.py`（`_build_agent_context`：holdings 循环前取 `vals`；循环内 `f` 构造后追加 valuation 块，:2801-2812 区域）
- Test: `tests/test_agent_context.py`（扩展，打桩模式参照该文件现有用例）

- [ ] **Step 1: 写失败测试**

```python
def test_agent_context_valuation_injection(monkeypatch):
    """factors[symbol].valuation：pe/pb/peg 用现价实时算、reason 推导、
    ETF fund 门控、null 安全。"""
    # 打桩方式完全参照本文件现有用例（db.get_daily_bars/get_holdings/
    # get_macro_latest/get_insider_tx/get_config 等 + server.market_cache），
    # 关键新增打桩：db.get_valuations →
    #   {"NVDA": {"eps_ttm": 8.0, "bps": 9.5, "growth_pct": 66.67,
    #             "dividend_yield": 0.52, "updated_at": "2026-10-11 16:00:00"},
    #    "QQQ": {"eps_ttm": None, "bps": None, "growth_pct": None,
    #            "dividend_yield": None, "updated_at": "2026-10-11 16:00:00"}}
    # holdings 含 NVDA(asset_type=stock) 与 QQQ(asset_type=etf)
    # 现价打桩：_pf_ctx["quotes"] = {"NVDA": {"price": 160.0}}
    # 断言：
    #   NVDA valuation: pe=20.0, pb=16.84(160/9.5), peg=3.0(20/0.6667),
    #     eps_ttm=8.0, growth_pct=66.67, reason=None
    #   QQQ valuation: pe/pb/peg 全 None, reason="fund"
    #   growth_pct=-17.91 的标的（可加 BABA fpi 打桩行）peg None reason="no_growth"
```

（具体打桩代码以本文件现有用例的 helper 为准展开成可运行代码，断言值必须如上。）

- [ ] **Step 2: 跑测试确认失败**
- [ ] **Step 3: 实现**（server.py `_build_agent_context`）

holdings 循环前（:2785 附近）：

```python
            vals = await db.get_valuations()
```

循环内 `f` 构造完 insider 块之后、`ctx["factors"][sym] = f` 之前：

```python
                    v = vals.get(sym) or {}
                    v_eps = v.get("eps_ttm")
                    v_bps = v.get("bps")
                    v_g = v.get("growth_pct")
                    v_price = ((_pf_ctx.get("quotes") or {}).get(sym)
                               or {}).get("price")
                    pe = round(v_price / v_eps, 2) \
                        if v_price and v_eps and v_eps > 0 else None
                    pb = round(v_price / v_bps, 2) \
                        if v_price and v_bps and v_bps > 0 else None
                    peg = round(pe / (v_g / 100.0), 2) \
                        if pe is not None and v_g and v_g > 0 else None
                    if at == "etf":
                        v_reason = "fund"
                    elif v_eps is None or v_bps is None:
                        v_reason = "no_data"
                    elif v_g is None or v_g <= 0:
                        v_reason = "no_growth"
                    else:
                        v_reason = None
                    f["valuation"] = {
                        "pe_ttm": pe, "pb": pb, "peg": peg,
                        "eps_ttm": v_eps, "bps": v_bps,
                        "growth_pct": v_g,
                        "dividend_yield": v.get("dividend_yield"),
                        "reason": v_reason,
                        "updated_at": v.get("updated_at")}
```

- [ ] **Step 4: 跑测试通过**：`python3 -m pytest tests/test_agent_context.py tests/test_valuation.py -q`
- [ ] **Step 5: Commit**：`git add server.py tests/test_agent_context.py && git commit -m "feat(agent-context): factors注入valuation块（现价实时算PE/PB/PEG+reason推导，估值因子层Task5）"`

---

### Task 6: 前端 — 撤三列 + 日K弹层决策因子区块

**Files:**
- Modify: `static/portfolio.html`

- [ ] **Step 1: 撤持仓表三列**
  - 删表头 :485-487 三个 `<th>`（RSI14 / vs SPY(20d) / 内部人90d）
  - 删渲染分支（:952-961 附近 `f` 三因子渲染整块，含 `insHtml` 相关局部变量）
  - 全文 `colspan="12"` → `colspan="9"`（grep 确认所有出现处同步：主表空态、已清仓分区等）

- [ ] **Step 2: 日K弹层加决策因子区块**
  - 位置：弹层现有信息行（MA50/MA200/RSI14/吊灯止损，:2371-2377 附近）同区追加一行/网格
  - 数据源：`lastOverview?.context?.factors[symbol]`（SSE 保活合并已含 context，零额外请求；lastOverview 为 null 时整块显示 `--`）
  - 展示七项（缺失 `--`，ETF reason=fund 显示「基金」）：

| 展示项 | 数据路径 | 格式 |
|--------|----------|------|
| RSI14 | `f.rsi14` | 1 位小数 |
| vs SPY(20d) | `f.spy_rel_20d_pct` | ±x.xx% |
| 内部人90d | `f.insider_90d` | `net_value_usd` 百万缩写 + `buys/sells` 笔；fpi 显示「外企无F4」 |
| P/E(TTM) | `f.valuation.pe_ttm` | x.xx |
| P/B | `f.valuation.pb` | x.xx |
| PEG | `f.valuation.peg` | x.xx（reason=no_growth 显示 `--`） |
| 股息率 | `f.valuation.dividend_yield` | x.xx% |

  - 样式沿用弹层信息行现有 class（与 MA50 行同款排版）；区块标题「决策因子」
  - 弹层每次打开/切换标的时重新渲染（挂在现有弹层打开函数内，renderContext 不动）

- [ ] **Step 3: 语法自检**：`node --check` 不适用（内联 script）——用 `python3 -c "re.findall(r'<script>(.*?)</script>', open('static/portfolio.html').read())"` 抽取后肉眼核对括号配对，或直接浏览器冒烟（部署后）
- [ ] **Step 4: Commit**：`git add static/portfolio.html && git commit -m "feat(前端): 撤持仓表三列回归精简，日K弹层新增决策因子区块（RSI/相对SPY/内部人/PE/PB/PEG/股息率，估值因子层Task6）"`

---

### Task 7: 部署 + 线上验证（主 Agent 执行）

- [ ] 本地 15 文件基线：495 + 新增全部 PASS
- [ ] `python3 scripts/check_secrets.py` → 0 处
- [ ] rsync 部署（排除清单照 AGENTS.md）+ `systemctl restart qdii` + `systemctl is-active qdii`
- [ ] journalctl：`估值轮完成` 出现且 stats 合理（rows≈16、fund≥1、no_cik 含 VGT/QQQM/SMH/DRAM 类）；次日 23:30 前可用手动触发验证
- [ ] SQL：`SELECT * FROM holding_valuations`（NVDA eps_ttm≈8.0、BABA≈4.39、QQQ 行 eps NULL；growth_pct NVDA≈66.67、BABA≈-17.91）
- [ ] curl `/api/agent/portfolio`：factors.NVDA.valuation.pe_ttm≈现价/8、QQQ reason=fund
- [ ] 浏览器冒烟（用户）：持仓表三列已撤、点开 NVDA 日K弹层见决策因子七项

---

## Self-Review 记录

- Spec 覆盖：§2 请求预算（T2/T4 周更门控）、§3 口径（T3 fixture 全锚定）、§4 存储（T1，reason 列移除已注明）、§5 模块挂载（T4）、§6.1 注入（T5）、§6.2 前端（T6）、§8 测试（各 Task 内嵌）——全覆盖
- 类型一致性：`upsert_holding_valuation_static(symbol, eps, bps, dy)` / `upsert_holding_valuation_growth(symbol, g)` / `get_valuations() -> dict` / `fetch_static_info(symbols) -> list` / `fetch_companyconcept(cik, concept) -> str|None` / `normalize_static_info(infos) -> dict` / `parse_eps_growth(text) -> float|None` / `run_valuation_round() -> dict(stats)`——各 Task 间签名一致
- 已知交叉点：T4 测试依赖 T1 的 db 函数与 T2 的 fetch 函数（monkeypatch 打桩不依赖实现，但 import 需存在）；执行顺序 T1→T2→T3→T4→T5→T6
