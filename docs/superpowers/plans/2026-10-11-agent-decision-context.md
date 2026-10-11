# 决策上下文层（Agent Decision Context）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 QDII 站点新增 facts-only 决策上下文层：SPY 对标/RSI/10Y/CPI/信用利差/市场广度(RSP-SPY)/EDGAR 内部人交易，通过 `/api/agent/portfolio` 与 `/api/portfolio/overview` 一次下发，持仓页同源渲染面板。

**Architecture:** 四层因子——零成本纯暴露（已有 RSI/US10Y）+ FRED 搭车（macro_fetcher +2 系列）+ 基准搭批（SPY/RSP 进 07:00 日K清单）+ 新模块 edgar_fetcher.py（SEC EDGAR Form 4 日更，自动分类新增持仓，限流以稳为主：串行 ≥2s 间隔 + 429 退避 2/4/8s）。出口层 `_build_agent_context()` 60s 缓存注入两个端点。

**Tech Stack:** Python/FastAPI/httpx/aiomysql/xml.etree；前端单文件 vanilla JS。spec：`docs/superpowers/specs/2026-10-11-agent-decision-context-design.md`

**关键现状锚点（subagent 必读）：**
- `database.py`：DDL 在 `init_tables()`（CREATE TABLE IF NOT EXISTS 模式，`_pool=pool` 传参）；幂等加列参考 `_alter_add_rec_columns`（database.py:110，catch "Duplicate column"）；`execute/execute_dict` 通用助手；`save_daily_bars` 是 executemany+INSERT IGNORE 样板（database.py:1498）
- `daily_bars.py`：`lp_symbol()` 自动加 .US（:311）；`daily_refresh_loop` 固定追加 VIX（:404）；`INDICATORS` 缓存含 rsi14（`get_indicators` :280）
- `server.py`：lifespan 后台任务注册（:133-247，macro_task 样板 :190）；`portfolio_overview`（:2540）；`agent_portfolio`（:2701，agent_router 前缀 `/api/agent` → 实际路径 `GET /api/agent/portfolio`）；`_nav_ctx_cache` 60s 缓存样板（:2632）
- 测试模式：`tests/test_alert_api.py`——monkeypatch `server.db`/`db.execute`/`db.execute_dict` 为 fake async 函数，`TestClient(server.app)` 不用 with 上下文（避免 lifespan），`monkeypatch.setitem(dbar.INDICATORS, ...)` 注入指标缓存
- 全程中文注释；新代码 `@author yys`；禁止 TODO/伪代码；pytest 全量本地跑（禁起服务）
- 基线：当前 441 passed，任何任务完成后不得回归

---

### Task 1: database.py — insider_tx 表 + holdings.asset_type 列 + CRUD

**Files:**
- Modify: `database.py`
- Create: `tests/test_edgar_db.py`

- [ ] **Step 1: 写失败测试 tests/test_edgar_db.py**

```python
"""insider_tx 表 + holdings.asset_type CRUD 测试（fake pool 捕获 SQL，零连库）。@author yys"""
import asyncio
from datetime import date
from pathlib import Path


class _FakeCursor:
    def __init__(self):
        self.sqls = []
        self.many = []
        self.rowcount = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def executemany(self, sql, args):
        self.sqls.append(sql)
        self.many = list(args)
        self.rowcount = len(args)


class _FakeConn:
    def __init__(self):
        self.cur = _FakeCursor()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def cursor(self):
        return self.cur


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


class _FakePool:
    def __init__(self):
        self.conn = _FakeConn()

    def acquire(self):
        return _FakeAcquire(self.conn)


def test_save_insider_tx_insert_ignore_and_null_passthrough(monkeypatch):
    import database as db

    pool = _FakePool()

    async def fake_get_pool():
        return pool

    monkeypatch.setattr(db, "get_pool", fake_get_pool)
    rows = [
        {"symbol": "MU", "cik": "723125", "accession": "0000723125-26-000001",
         "filed_date": "2026-10-08", "trade_date": "2026-10-07",
         "reporter": "SUNNY ERIC", "title": "EVP", "action": "A",
         "tx_code": "P", "shares": 10000, "price": 110.5, "value_usd": 1105000.0},
        {"symbol": "MU", "cik": "723125", "accession": "0000723125-26-000002",
         "filed_date": "2026-10-08", "trade_date": None,
         "reporter": "X", "title": "", "action": "D",
         "tx_code": "S", "shares": 500, "price": None, "value_usd": None},
    ]
    n = asyncio.run(db.save_insider_tx(rows))
    assert n == 2
    assert "INSERT IGNORE INTO insider_tx" in pool.conn.cur.sqls[0]
    assert pool.conn.cur.many[0][10] == 110.5      # price 列
    assert pool.conn.cur.many[1][10] is None       # price NULL 原样透传
    assert pool.conn.cur.many[1][11] is None       # value_usd NULL 原样透传
    assert asyncio.run(db.save_insider_tx([])) == 0


def test_get_insider_tx_window(monkeypatch):
    import database as db

    cap = {}

    async def fake_exec_dict(sql, args=None, _pool=None):
        cap["sql"], cap["args"] = sql, args
        return []

    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    asyncio.run(db.get_insider_tx("MU", days=90))
    assert "FROM insider_tx" in cap["sql"]
    assert "INTERVAL %s DAY" in cap["sql"]
    assert cap["args"] == ("MU", 90)


def test_set_holding_asset_type(monkeypatch):
    import database as db

    cap = {}

    async def fake_execute(sql, args=None, _pool=None):
        cap["sql"], cap["args"] = sql, args
        return 1

    monkeypatch.setattr(db, "execute", fake_execute)
    assert asyncio.run(db.set_holding_asset_type("MU", "stock")) == 1
    assert "UPDATE holdings SET asset_type" in cap["sql"]
    assert cap["args"] == ("stock", "MU")


def test_insider_tx_and_asset_type_ddl_present():
    import database

    src = Path(database.__file__).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS insider_tx" in src
    assert "uk_tx" in src
    assert "ADD COLUMN `asset_type`" in src
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_edgar_db.py -v`
Expected: 4 FAILED（`save_insider_tx`/`set_holding_asset_type`/`get_insider_tx` 不存在 + DDL 无 insider_tx）

- [ ] **Step 3: database.py 实现**

3a. holdings 建表（database.py:308 附近的 `CREATE TABLE IF NOT EXISTS holdings`）在 `enabled` 行后加一列：

```sql
            asset_type  VARCHAR(8)   NULL DEFAULT NULL COMMENT 'NULL未探测/stock个股/etf基金/fpi外国发行人无Form4/none人工排除',
```

3b. 紧接 holdings 建表 execute 之后（约 :323 `_pool=pool` 调用后）追加幂等迁移调用与新函数（放在 `_alter_add_rec_columns` 之后定义，样式照抄）：

```python
async def _alter_add_holdings_asset_type(pool):
    """为 holdings 添加 asset_type 列（幂等；存量表靠此迁移，新表建表已含）。@author yys"""
    try:
        await execute(
            "ALTER TABLE holdings ADD COLUMN `asset_type` VARCHAR(8) NULL "
            "DEFAULT NULL COMMENT 'NULL未探测/stock个股/etf基金/fpi外国发行人无Form4/none人工排除'",
            _pool=pool,
        )
    except Exception as e:
        if "Duplicate column" not in str(e):
            logger.warning("ALTER TABLE holdings ADD asset_type 失败: %s", e)
```

并在 `init_tables()` 中 holdings 建表后加 `await _alter_add_holdings_asset_type(pool)`。

3c. `macro_indicators` 建表（database.py:1484 附近）之后新增建表：

```python
    # ---- insider_tx (SEC Form 4 内部人交易，EDGAR 官方源) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS insider_tx (
            id          BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol      VARCHAR(16)  NOT NULL COMMENT '持仓代码',
            cik         VARCHAR(16)  NOT NULL COMMENT 'SEC CIK',
            accession   VARCHAR(32)  NOT NULL COMMENT '申报编号(带连字符)',
            filed_date  DATE NOT NULL COMMENT '申报日',
            trade_date  DATE NULL COMMENT '成交日',
            reporter    VARCHAR(128) NOT NULL COMMENT '申报人',
            title       VARCHAR(128) NOT NULL DEFAULT '' COMMENT '职务',
            action      CHAR(1) NOT NULL COMMENT 'A=acquire买入 D=dispose卖出',
            tx_code     VARCHAR(8)  NOT NULL DEFAULT '' COMMENT '交易代码 P/S/G/A/F等原样保留',
            shares      BIGINT NOT NULL COMMENT '股数',
            price       DECIMAL(12,4) NULL COMMENT '成交价(缺失为NULL)',
            value_usd   DECIMAL(16,2) NULL COMMENT '金额USD(=shares*price)',
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_tx (accession, reporter, action, shares, price),
            KEY idx_symbol_date (symbol, filed_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SEC Form 4 内部人交易(EDGAR官方源)'
        """,
        _pool=pool,
    )
```

3d. holdings CRUD 区（`delete_holding` 之后）新增：

```python
async def set_holding_asset_type(symbol: str, asset_type: str) -> int:
    """EDGAR 探测回写资产分类（stock/etf/fpi/none）。@author yys"""
    return await execute(
        "UPDATE holdings SET asset_type = %s WHERE symbol = %s",
        (asset_type, symbol))


async def save_insider_tx(rows: list[dict]) -> int:
    """批量写入 Form 4 交易行（INSERT IGNORE 撞 uk_tx 幂等；price/value 允许 NULL）。@author yys"""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO insider_tx (symbol, cik, accession, filed_date, "
                "trade_date, reporter, title, action, tx_code, shares, price, value_usd) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(r["symbol"], r["cik"], r["accession"], r["filed_date"],
                  r.get("trade_date"), r["reporter"], r.get("title") or "",
                  r["action"], r.get("tx_code") or "", int(r["shares"]),
                  None if r.get("price") is None else _safe_decimal(r["price"], 4),
                  None if r.get("value_usd") is None else _safe_decimal(r["value_usd"], 2))
                 for r in rows])
            return cur.rowcount


async def get_insider_tx(symbol: str, days: int = 90) -> list[dict]:
    """近 N 天 Form 4 交易行（filed_date 升序）。@author yys"""
    return await execute_dict(
        "SELECT * FROM insider_tx WHERE symbol = %s AND filed_date >= "
        "DATE_SUB(CURDATE(), INTERVAL %s DAY) ORDER BY filed_date, id",
        (symbol, days))
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_edgar_db.py -v`
Expected: 4 PASSED

- [ ] **Step 5: 回归 + 提交**

Run: `python3 -m pytest tests/ -q`
Expected: 445 passed（441 基线 + 4 新增）

```bash
git add database.py tests/test_edgar_db.py
git commit -m "feat: insider_tx表+holdings.asset_type列及CRUD（决策上下文Task1）"
```

---

### Task 2: edgar_fetcher.py 纯函数层（解析 + 聚合）

**Files:**
- Create: `edgar_fetcher.py`
- Create: `tests/test_edgar_fetcher.py`

- [ ] **Step 1: 写失败测试 tests/test_edgar_fetcher.py**

```python
"""edgar_fetcher 纯函数层测试（fixture 字符串，零网络）。@author yys"""
import json
from datetime import date

import edgar_fetcher as ef


def test_parse_ticker_map():
    text = json.dumps({
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 1045810, "ticker": "nvda", "title": "NVIDIA"},
        "2": {"cik_str": "bad", "ticker": "XX", "title": "坏行跳过"},
    })
    out = ef.parse_ticker_map(text)
    assert out == {"AAPL": 320193, "NVDA": 1045810}


def test_classify_asset_type_priority():
    assert ef.classify_asset_type(["10-K", "4", "8-K"]) == "stock"
    assert ef.classify_asset_type(["10-Q/A", "4"]) == "stock"
    assert ef.classify_asset_type(["20-F", "6-K"]) == "fpi"          # fpi 优先于其它
    assert ef.classify_asset_type(["N-1A", "NPORT-PART"]) == "etf"
    assert ef.classify_asset_type(["N-CSR"]) == "etf"
    assert ef.classify_asset_type(["6-K", "8-K"]) is None            # 无法归类下轮再试
    assert ef.classify_asset_type([]) is None


SUBMISSIONS_JSON = json.dumps({
    "cik": 1045810, "name": "NVIDIA CORP",
    "filings": {"recent": {
        "accessionNumber": ["0001045810-26-000001", "0001045810-26-000002",
                            "0001045810-26-000003"],
        "filingDate": ["2026-10-08", "2026-10-05", "2026-09-30"],
        "formType": ["4", "10-Q", "4"],
        "primaryDocument": ["xslF345X05/primary_doc.xml", "nvda-10q.htm",
                            "primary_doc.xml"],
    }}})


def test_parse_submissions_recent():
    all_rows = ef.parse_submissions_recent(SUBMISSIONS_JSON)
    assert [r["form"] for r in all_rows] == ["4", "10-Q", "4"]
    form4 = ef.parse_submissions_recent(SUBMISSIONS_JSON, form_filter="4")
    assert len(form4) == 2
    assert form4[0]["accession"] == "0001045810-26-000001"
    assert form4[0]["primary_doc"] == "xslF345X05/primary_doc.xml"
    assert ef.parse_submissions_recent('{"filings":{}}') == []


FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-10-08</periodOfReport>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001234567</rptOwnerCik>
      <rptOwnerName>JENSEN HUANG</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>President and CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>141000</value></transactionShares>
        <transactionPricePerShare><value>135.0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>20000</value></transactionShares>
        <transactionPricePerShare><value>136.5</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>G</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>0</value></transactionShares>
        <transactionPricePerShare><value></value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


def test_parse_form4_xml():
    rows = ef.parse_form4_xml(FORM4_XML, "NVDA", "1045810",
                              "0001045810-26-000001", "2026-10-08")
    assert len(rows) == 2                       # 0 股行被跳过
    r0, r1 = rows
    assert r0["reporter"] == "JENSEN HUANG"
    assert r0["title"] == "President and CEO"   # officer 头衔优先于 Director
    assert r0["action"] == "A" and r0["shares"] == 141000
    assert r0["price"] == 135.0 and r0["value_usd"] == 19035000.0
    assert r0["tx_code"] == "P" and r0["trade_date"] == "2026-10-07"
    assert r0["filed_date"] == "2026-10-08" and r0["cik"] == "1045810"
    assert r1["action"] == "D" and r1["value_usd"] == 2730000.0
    assert ef.parse_form4_xml("<not-xml", "X", "1", "a", "d") == []
    # 无非衍生品表 → 空列表
    assert ef.parse_form4_xml(
        "<ownershipDocument><documentType>4</documentType></ownershipDocument>",
        "X", "1", "a", "d") == []


def test_build_insider_summary():
    today = "2026-10-11"
    rows = [
        {"filed_date": date(2026, 10, 1), "action": "A", "shares": 141000,
         "value_usd": 19035000.0},
        {"filed_date": date(2026, 9, 20), "action": "D", "shares": 5000,
         "value_usd": 700000.0},
        {"filed_date": date(2026, 1, 1), "action": "A", "shares": 999999,
         "value_usd": 9999999.0},                # 90 天窗口外剔除
        {"filed_date": date(2026, 8, 1), "action": "A", "shares": 1000,
         "value_usd": None},                     # 金额缺失按 0 计
    ]
    s = ef.build_insider_summary(rows, today)
    assert s["net_shares"] == 141000 - 5000 + 1000
    assert s["net_value_usd"] == 19035000.0 - 700000.0
    assert s["buys"] == 2 and s["sells"] == 1 and s["tx_count"] == 3
    assert s["since"] == "2026-07-13"
    assert ef.build_insider_summary([], today) is None


def test_form4_url_strips_xsl_prefix():
    assert ef.form4_url(1045810, "0001045810-26-000001",
                        "xslF345X05/primary_doc.xml") == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000001/primary_doc.xml"
    assert ef.form4_url(1045810, "0001045810-26-000001",
                        "primary_doc.xml") == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000001/primary_doc.xml"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_edgar_fetcher.py -v`
Expected: FAIL（ModuleNotFoundError: edgar_fetcher）

- [ ] **Step 3: 创建 edgar_fetcher.py（本任务只写纯函数，网络层 Task 3 再加）**

```python
"""EDGAR 内部人交易（Form 4）日频抓取：探测分类 + 限流拉取 + 纯解析聚合。

数据源：SEC EDGAR 官方免费接口（company_tickers 映射 / submissions 索引 /
Form 4 XML）。限流契约（以稳为主）：全程串行、任意两请求间隔 ≥2s（官方上限
10 req/s 的 1/20）；429/503 指数退避 2s→4s→8s 最多重试 3 次；网络异常仅重试
1 次。@author yys
"""
import asyncio
import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import httpx

import database as db

logger = logging.getLogger("qdii.edgar")

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"
EDGAR_MIN_INTERVAL = 2.0          # 请求最小间隔（秒）
EDGAR_BACKOFFS = (2.0, 4.0, 8.0)  # 429/503 退避序列
DEFAULT_UA = "QDII Portfolio Monitor"


def parse_ticker_map(text: str) -> dict[str, int]:
    """company_tickers.json → {ticker大写: cik}；坏行跳过。@author yys"""
    try:
        d = json.loads(text)
    except json.JSONDecodeError:
        return {}
    out = {}
    for v in (d or {}).values():
        try:
            out[str(v["ticker"]).upper()] = int(v["cik_str"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def classify_asset_type(form_types: list[str]) -> str | None:
    """按申报类型集判定资产分类：20-F→fpi / 10-K,10-Q→stock / N系→etf。
    三类互斥（同一发行人不会同时提交），fpi 优先判定；都不含返回 None 下轮再试。@author yys"""
    s = set(form_types or [])
    if any(f.startswith("20-F") for f in s):
        return "fpi"
    if any(f.startswith("10-K") or f.startswith("10-Q") for f in s):
        return "stock"
    if any(f.startswith("N-1A") or f.startswith("N-CSR") or f.startswith("NPORT")
           for f in s):
        return "etf"
    return None


def parse_submissions_recent(text: str, form_filter: str | None = None) -> list[dict]:
    """submissions JSON recent → [{form, filed, accession, primary_doc}]
    （form_filter 指定时仅保留该类型，如 "4"）。解析失败返回 []。@author yys"""
    try:
        d = json.loads(text)
        rec = (d.get("filings") or {}).get("recent") or {}
    except (json.JSONDecodeError, AttributeError):
        return []
    out = []
    for form, filed, acc, doc in zip(rec.get("formType") or [],
                                     rec.get("filingDate") or [],
                                     rec.get("accessionNumber") or [],
                                     rec.get("primaryDocument") or []):
        if form_filter is None or form == form_filter:
            out.append({"form": form, "filed": filed,
                        "accession": acc, "primary_doc": doc})
    return out


def form4_url(cik: int, accession: str, primary_doc: str) -> str:
    """Archives 原始 XML 地址（primary_doc 含 xsl 渲染前缀时取 basename）。@author yys"""
    doc = primary_doc.split("/")[-1] or "primary_doc.xml"
    return ARCHIVES_URL.format(cik=cik, acc=accession.replace("-", ""), doc=doc)


def parse_form4_xml(text: str, symbol: str, cik: str, accession: str,
                    filed_date: str) -> list[dict]:
    """Form 4 XML → 非衍生品交易行列表（一个申报可含多笔）。
    action A=买入 D=卖出；price/value 缺失为 None；shares≤0 或 AD 码异常行跳过；
    解析失败返回 []。@author yys"""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    owner = root.find("reportingOwner")
    rid = owner.find("reportingOwnerId") if owner is not None else None
    reporter = (rid.findtext("rptOwnerName", "").strip()
                if rid is not None else "") or "UNKNOWN"
    rel = owner.find("reportingOwnerRelationship") if owner is not None else None
    title = ""
    if rel is not None:
        if (rel.findtext("isOfficer") or "0") == "1":
            title = (rel.findtext("officerTitle") or "").strip() or "Officer"
        elif (rel.findtext("isDirector") or "0") == "1":
            title = "Director"
        elif (rel.findtext("isTenPercentOwner") or "0") == "1":
            title = "10% Owner"
    rows = []
    table = root.find("nonDerivativeTable")
    for txn in (table.findall("nonDerivativeTransaction")
                if table is not None else []):
        try:
            shares = int(float(txn.findtext(
                "transactionAmounts/transactionShares/value") or 0))
        except ValueError:
            continue
        price_txt = txn.findtext("transactionAmounts/transactionPricePerShare/value")
        try:
            price = float(price_txt) if price_txt not in (None, "") else None
        except ValueError:
            price = None
        action = (txn.findtext(
            "transactionAmounts/transactionAcquiredDisposedCode/value") or "").strip()
        if action not in ("A", "D") or shares <= 0:
            continue
        rows.append({
            "symbol": symbol, "cik": cik, "accession": accession,
            "filed_date": filed_date,
            "trade_date": txn.findtext("transactionDate/value") or None,
            "reporter": reporter, "title": title, "action": action,
            "tx_code": (txn.findtext("transactionCoding/transactionCode") or "").strip(),
            "shares": shares, "price": price,
            "value_usd": round(shares * price, 2) if price is not None else None,
        })
    return rows


def build_insider_summary(rows: list[dict], today: str) -> dict | None:
    """近 90 天净买入聚合（纯函数，rows 为 DB 行）：net_shares/net_value_usd/
    buys/sells/tx_count/since；value 缺失按 0 计；空窗口返回 None。@author yys"""
    cutoff = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=90)) \
        .strftime("%Y-%m-%d")
    sel = [r for r in rows if str(r.get("filed_date"))[:10] >= cutoff]
    if not sel:
        return None
    net_shares = 0
    net_value = 0.0
    buys = sells = 0
    for r in sel:
        sign = 1 if r["action"] == "A" else -1
        net_shares += sign * int(r["shares"])
        v = r.get("value_usd")
        if v is not None:
            net_value += sign * float(v)
        if r["action"] == "A":
            buys += 1
        else:
            sells += 1
    return {"net_shares": net_shares, "net_value_usd": round(net_value, 2),
            "buys": buys, "sells": sells, "tx_count": len(sel), "since": cutoff}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_edgar_fetcher.py -v`
Expected: 7 PASSED

- [ ] **Step 5: 提交**

```bash
git add edgar_fetcher.py tests/test_edgar_fetcher.py
git commit -m "feat: edgar_fetcher纯函数层（ticker映射/分类/Form4解析/90天聚合，决策上下文Task2）"
```

---

### Task 3: edgar_fetcher.py 网络层（节流 + 退避 + 拉取函数）

**Files:**
- Modify: `edgar_fetcher.py`
- Modify: `tests/test_edgar_fetcher.py`

- [ ] **Step 1: 写失败测试（追加到 tests/test_edgar_fetcher.py）**

```python
# ---------------------------------------------------------------------------
# 网络层：节流/退避/UA（monkeypatch httpx + 冻结时钟，零真实网络）
# ---------------------------------------------------------------------------
import asyncio

import httpx


class _FakeResp:
    def __init__(self, code, text="ok"):
        self.status_code = code
        self.text = text


def _patch_client(monkeypatch, responder):
    calls = []

    class FakeClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            calls.append(url)
            return responder(url)

    monkeypatch.setattr(ef.httpx, "AsyncClient", FakeClient)
    return calls


def test_edgar_get_throttle_and_backoff(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(round(s, 1))

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)   # 时钟冻结
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)             # 上次请求恰在当前时刻
    codes = iter([429, 429, 200])
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(next(codes)))
    resp = asyncio.run(ef._edgar_get("https://x", "UA"))
    assert resp.status_code == 200
    assert len(calls) == 3
    # 冻结时钟下每次 gap=2.0；退避序列 2.0/4.0
    assert sleeps == [2.0, 2.0, 2.0, 4.0, 2.0]


def test_edgar_get_503_exhausted(monkeypatch):
    async def fake_sleep(s):
        pass

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(503))
    assert asyncio.run(ef._edgar_get("https://x", "UA")) is None
    assert len(calls) == 4                       # 首试 + 3 次退避重试


def test_edgar_get_network_error_retries_once(monkeypatch):
    async def fake_sleep(s):
        pass

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)
    calls = []

    class FlakyClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            calls.append(url)
            raise httpx.ConnectError("boom")

    monkeypatch.setattr(ef.httpx, "AsyncClient", FlakyClient)
    assert asyncio.run(ef._edgar_get("https://x", "UA")) is None
    assert len(calls) == 2                       # 网络异常仅重试 1 次


def test_resolve_ua_from_config(monkeypatch):
    import database as db

    async def fake_cfg(key):
        return "QDII-Portfolio admin@example.com" if key == "EDGAR_UA" else None

    monkeypatch.setattr(db, "get_config", fake_cfg)
    assert asyncio.run(ef._resolve_ua()) == "QDII-Portfolio admin@example.com"

    async def fake_cfg_empty(key):
        return None

    monkeypatch.setattr(db, "get_config", fake_cfg_empty)
    assert asyncio.run(ef._resolve_ua()) == ef.DEFAULT_UA


def test_get_ticker_map_cache_and_refresh(monkeypatch):
    import database as db

    cfg = {"EDGAR_TICKER_MAP_TS": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    set_calls = []

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        set_calls.append((key, value))

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))

    # 冷启动（内存空）→ 拉一次并落时间戳
    _patch_client(monkeypatch, lambda url: _FakeResp(
        200, json.dumps({"0": {"cik_str": 1, "ticker": "MU", "title": "x"}})))
    m1 = asyncio.run(ef.get_ticker_map())
    assert m1 == {"MU": 1}
    assert set_calls and set_calls[0][0] == "EDGAR_TICKER_MAP_TS"
    # 内存已有且时间戳新鲜 → 不再发请求
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(500))
    assert asyncio.run(ef.get_ticker_map()) == {"MU": 1}
    assert calls == []


def _async_ret(v):
    async def _f(*a, **kw):
        return v
    return _f
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_edgar_fetcher.py -v -k "edgar_get or resolve_ua or ticker_map"`
Expected: 6 FAILED（`_edgar_get`/`_resolve_ua`/`get_ticker_map` 不存在）

- [ ] **Step 3: edgar_fetcher.py 追加网络层实现**

```python
# ---------------------------------------------------------------------------
# 网络层：全局节流 + 退避 + 拉取函数（全程串行，以稳为主）
# ---------------------------------------------------------------------------

_last_req_ts = 0.0                # 模块级节流状态（monotonic）
_TICKER_MAP: dict[str, int] | None = None   # symbol(大写) -> cik


async def _resolve_ua() -> str:
    """UA 从 sys_config.EDGAR_UA 热取（缺省无域名占位；真实联系方式库配置，
    域名属敏感值禁入代码）。@author yys"""
    try:
        ua = await db.get_config("EDGAR_UA")
        if ua:
            return ua
    except Exception:
        pass
    return DEFAULT_UA


async def _edgar_get(url: str, ua: str) -> httpx.Response | None:
    """限流 GET：距上次请求 ≥EDGAR_MIN_INTERVAL；429/503 按 EDGAR_BACKOFFS
    退避重试；网络异常仅重试 1 次；穷尽返回 None（调用方按数据缺处理）。@author yys"""
    global _last_req_ts
    last_err = ""
    net_retried = False
    for attempt in range(len(EDGAR_BACKOFFS) + 1):
        gap = _last_req_ts + EDGAR_MIN_INTERVAL - time.monotonic()
        if gap > 0:
            await asyncio.sleep(gap)
        _last_req_ts = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True,
                                         headers={"User-Agent": ua}) as client:
                resp = await client.get(url)
            if resp.status_code not in (429, 503):
                return resp
            last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = repr(e)
            if net_retried:
                break
            net_retried = True
        if attempt < len(EDGAR_BACKOFFS):
            await asyncio.sleep(EDGAR_BACKOFFS[attempt])
    logger.warning("EDGAR GET 失败 %s: %s", url, last_err)
    return None


async def _map_fresh() -> bool:
    """sys_config.EDGAR_TICKER_MAP_TS 是否 7 天内。@author yys"""
    ts = await db.get_config("EDGAR_TICKER_MAP_TS")
    if not ts:
        return False
    try:
        return (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).days < 7
    except ValueError:
        return False


async def get_ticker_map() -> dict[str, int] | None:
    """ticker→CIK 全量映射：内存缓存 + 时间戳 7 天过期（重启重拉一次可接受）。
    拉取失败保留旧缓存兜底。@author yys"""
    global _TICKER_MAP
    if _TICKER_MAP is not None and await _map_fresh():
        return _TICKER_MAP
    resp = await _edgar_get(TICKER_MAP_URL, await _resolve_ua())
    if resp is not None and resp.status_code == 200:
        _TICKER_MAP = parse_ticker_map(resp.text)
        await db.set_config("EDGAR_TICKER_MAP_TS",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "EDGAR ticker映射刷新时间")
    return _TICKER_MAP


async def fetch_submissions(cik: int, ua: str) -> str | None:
    """公司申报索引 JSON 文本（非 200/网络穷尽 → None）。@author yys"""
    resp = await _edgar_get(SUBMISSIONS_URL.format(cik=cik), ua)
    if resp is None or resp.status_code != 200:
        return None
    return resp.text


async def fetch_form4_text(cik: int, accession: str, primary_doc: str,
                           ua: str) -> str | None:
    """Form 4 原始 XML 文本（404/失败 → None，数据缺不重试轰炸）。@author yys"""
    resp = await _edgar_get(form4_url(cik, accession, primary_doc), ua)
    if resp is None or resp.status_code != 200:
        return None
    return resp.text
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_edgar_fetcher.py -v`
Expected: 13 PASSED

- [ ] **Step 5: 提交**

```bash
git add edgar_fetcher.py tests/test_edgar_fetcher.py
git commit -m "feat: edgar_fetcher网络层（≥2s串行节流+429退避2/4/8s+UA热取，决策上下文Task3）"
```

---

### Task 4: edgar 每日轮 run_edgar_round + edgar_daily_loop

**Files:**
- Modify: `edgar_fetcher.py`
- Modify: `database.py`
- Modify: `tests/test_edgar_fetcher.py`

- [ ] **Step 1: 写失败测试（追加）**

```python
# ---------------------------------------------------------------------------
# 每日轮：探测分类 + Form 4 增量 + 水位（全部打桩零网络）
# ---------------------------------------------------------------------------
from datetime import datetime


def _round_env(monkeypatch, holdings, forms_by_cik, form4_texts, watermark=None):
    """打桩 db 与网络层，返回记录器。forms_by_cik: {cik: submissions_json文本}。"""
    import database as db

    cfg = {"EDGAR_WATERMARK": watermark,
           "EDGAR_TICKER_MAP_TS": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    cfg = {k: v for k, v in cfg.items() if v is not None}
    rec = {"classified": [], "saved": [], "set_cfg": []}

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        cfg[key] = value
        rec["set_cfg"].append((key, value))

    async def fake_holdings(enabled_only=True):
        return holdings

    async def fake_classify(symbol, at):
        rec["classified"].append((symbol, at))

    async def fake_save(rows):
        rec["saved"].extend(rows)
        return len(rows)

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(db, "get_holdings", fake_holdings)
    monkeypatch.setattr(db, "set_holding_asset_type", fake_classify)
    monkeypatch.setattr(db, "save_insider_tx", fake_save)
    monkeypatch.setattr(db, "get_insider_accessions", _async_ret(set()))
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))
    monkeypatch.setattr(ef, "_TICKER_MAP", {"MU": 723125, "BABA": 1577552,
                                            "SMH": 1234567})
    fetched = []

    async def fake_sub(cik, ua):
        return forms_by_cik.get(cik)

    async def fake_form4(cik, accession, doc, ua):
        fetched.append((cik, accession))
        return form4_texts.get(accession)

    monkeypatch.setattr(ef, "fetch_submissions", fake_sub)
    monkeypatch.setattr(ef, "fetch_form4_text", fake_form4)
    return rec, cfg, fetched


def _subs_json(cik, forms):
    return json.dumps({"cik": cik, "filings": {"recent": {
        "accessionNumber": [f[2] for f in forms],
        "filingDate": [f[1] for f in forms],
        "formType": [f[0] for f in forms],
        "primaryDocument": ["primary_doc.xml"] * len(forms),
    }}})


def test_run_edgar_round_classify_and_poll(monkeypatch):
    forms_mu = _subs_json(723125, [
        ("10-Q", "2026-10-05", "0000723125-26-000002"),
        ("4", "2026-10-08", "0000723125-26-000001"),
    ])
    forms_baba = _subs_json(1577552, [("20-F", "2026-06-30", "x-baba-1")])
    forms_smh = _subs_json(1234567, [("N-CSR", "2026-09-30", "x-smh-1")])
    holdings = [
        {"symbol": "MU", "market": "US", "asset_type": None},      # 待探测→stock
        {"symbol": "BABA", "market": "US", "asset_type": None},    # 待探测→fpi
        {"symbol": "SMH", "market": "US", "asset_type": None},     # 待探测→etf
        {"symbol": "HK0000740297", "market": "HKFUND", "asset_type": None},
    ]
    form4 = {"0000723125-26-000001": FORM4_XML}
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings,
        {723125: forms_mu, 1577552: forms_baba, 1234567: forms_smh},
        form4, watermark="2026-10-01")
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    assert rec["classified"] == [("MU", "stock"), ("BABA", "fpi"), ("SMH", "etf")]
    # 只有 stock 拉 Form 4；fpi/etf 零 Form 4 请求
    assert fetched == [(723125, "0000723125-26-000001")]
    assert len(rec["saved"]) == 2                  # FORM4_XML 两笔有效交易
    # 水位推进到已处理的最大 filingDate
    assert dict(rec["set_cfg"])["EDGAR_WATERMARK"] == "2026-10-08"
    assert any(k == "EDGAR_LAST_OK" for k, _ in rec["set_cfg"])


def test_run_edgar_round_form4_fetch_fail_keeps_watermark(monkeypatch):
    forms_mu = _subs_json(723125, [("10-Q", "2026-10-05", "0000723125-26-000002"),
                                   ("4", "2026-10-08", "0000723125-26-000001")])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {},
        watermark="2026-10-01")                    # form4_texts 空 → XML 拉取失败
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True and r["new_tx"] == 0
    # 拉取失败的申报不推水位 → 次日重试
    assert not any(k == "EDGAR_WATERMARK" for k, _ in rec["set_cfg"])


def test_run_edgar_round_first_round_backfill_90d(monkeypatch):
    forms_mu = _subs_json(723125, [
        ("4", "2026-10-08", "0000723125-26-000001"),
        ("4", "2026-06-01", "0000723125-26-000000"),   # 90 天外：since 之前不拉
    ])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {"0000723125-26-000001": FORM4_XML},
        watermark=None)                            # 无水位 → 首轮回看 90 天
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    # 只有 2026-10-08（≥ today-90）被处理；2026-06-01 在窗口外不拉
    assert fetched == [(723125, "0000723125-26-000001")]


def test_run_edgar_round_skips_processed_accession(monkeypatch):
    """已入库 accession（uk_tx 对 NULL price 不去重的补偿）不重复拉取/插入。"""
    import database as db

    forms_mu = _subs_json(723125, [("10-Q", "2026-10-05", "0000723125-26-000002"),
                                   ("4", "2026-10-08", "0000723125-26-000001")])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {"0000723125-26-000001": FORM4_XML},
        watermark="2026-10-01")

    async def fake_accessions(sym, days=120):
        return {"0000723125-26-000001"}            # 该申报已入库

    monkeypatch.setattr(db, "get_insider_accessions", fake_accessions)
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True and r["new_tx"] == 0
    assert fetched == []                           # 不再拉 XML
    assert rec["saved"] == []                      # 不再插入
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_edgar_fetcher.py -v -k run_edgar_round`
Expected: 4 FAILED（`run_edgar_round` 不存在）

- [ ] **Step 3a: database.py 新增 get_insider_accessions（uk_tx 对 NULL price 不去重的补偿）**

在 `get_insider_tx` 之后新增：

```python
async def get_insider_accessions(symbol: str, days: int = 120) -> set[str]:
    """近 N 天已入库的申报编号集合（accession 级去重：uk_tx 唯一键含可 NULL
    的 price 列，MySQL 唯一索引不对 NULL 去重，水位 >= 重放靠此防重复插入）。@author yys"""
    rows = await execute_dict(
        "SELECT DISTINCT accession FROM insider_tx WHERE symbol = %s AND "
        "filed_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)",
        (symbol, days))
    return {r["accession"] for r in rows}
```

- [ ] **Step 3b: edgar_fetcher.py 追加每日轮实现**

```python
# ---------------------------------------------------------------------------
# 每日轮：刷映射 → 未探测分类 → stock 轮询 Form 4 → 推进水位
# ---------------------------------------------------------------------------

async def run_edgar_round() -> dict:
    """一轮完整处理。单标的失败跳过不中断；水位只在申报 XML 成功拉取后推进；
    XML 拉取失败不推水位（次日自然重试，重复靠 uk_tx INSERT IGNORE 幂等）。@author yys"""
    ua = await _resolve_ua()
    tmap = await get_ticker_map()
    if tmap is None:
        logger.warning("EDGAR ticker 映射不可用，本轮顺延")
        return {"ok": False, "reason": "ticker_map"}

    watermark = await db.get_config("EDGAR_WATERMARK")
    if watermark:
        since = watermark
    else:
        since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    holdings = await db.get_holdings(enabled_only=True)
    stats = {"classified": 0, "polled": 0, "new_tx": 0, "skipped": 0}
    max_filed = since
    for h in holdings:
        if h.get("market") != "US":
            continue
        symbol = h["symbol"]
        at = h.get("asset_type")
        sub = None
        try:
            cik = tmap.get(symbol.upper())
            if not at:
                if cik is None:
                    logger.warning("EDGAR 探测: %s 无 CIK 映射（未分类，下轮再试）",
                                   symbol)
                    stats["skipped"] += 1
                    continue
                sub = await fetch_submissions(cik, ua)
                if sub is None:
                    stats["skipped"] += 1
                    continue
                atype = classify_asset_type(
                    [f["form"] for f in parse_submissions_recent(sub)])
                if atype is None:
                    logger.warning("EDGAR 探测: %s 申报类型无法归类，下轮再试", symbol)
                    stats["skipped"] += 1
                    continue
                await db.set_holding_asset_type(symbol, atype)
                logger.info("EDGAR 分类: %s → %s", symbol, atype)
                stats["classified"] += 1
                at = atype
            if at == "stock":
                if cik is None:
                    stats["skipped"] += 1
                    continue
                if sub is None:
                    sub = await fetch_submissions(cik, ua)
                if sub is None:
                    stats["skipped"] += 1
                    continue
                # accession 级去重：uk_tx 唯一键含可 NULL 的 price 列（MySQL 唯一
                # 索引不对 NULL 去重），水位 >= 重放会重复插入 NULL 价格行——
                # 按已入库 accession 跳过，同时省掉重复 XML 请求
                processed = await db.get_insider_accessions(symbol, days=120)
                for f in parse_submissions_recent(sub, form_filter="4"):
                    if f["filed"] >= since and f["accession"] not in processed:
                        text = await fetch_form4_text(cik, f["accession"],
                                                      f["primary_doc"], ua)
                        if text is None:
                            continue              # 拉取失败不推水位，次日重试
                        rows = parse_form4_xml(text, symbol, str(cik),
                                               f["accession"], f["filed"])
                        if rows:
                            await db.save_insider_tx(rows)
                            stats["new_tx"] += len(rows)
                        max_filed = max(max_filed, f["filed"])
                stats["polled"] += 1
        except Exception as e:
            logger.warning("EDGAR 处理 %s 失败（跳过）: %s", symbol, e)
            stats["skipped"] += 1
    if max_filed > since:
        await db.set_config("EDGAR_WATERMARK", max_filed, "EDGAR Form4 已处理水位")
    await db.set_config("EDGAR_LAST_OK",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "EDGAR 上轮成功时间")
    logger.info("EDGAR 轮完成: %s", stats)
    return {"ok": True, **stats}


async def edgar_daily_loop():
    """后台任务：启动即跑一次，此后每日北京时间 23:30（错峰 22:35 ledger-fx /
    22:40 macro / 07:00 日K）。@author yys"""
    while True:
        try:
            await run_edgar_round()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("EDGAR 循环异常: %s", e)
        now = datetime.now()
        target = now.replace(hour=23, minute=30, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_edgar_fetcher.py tests/test_edgar_db.py -v`
Expected: 17 PASSED（edgar_fetcher）+ 4 PASSED（edgar_db，含本任务新增 helper 后不回归）

- [ ] **Step 5: 提交**

```bash
git add edgar_fetcher.py database.py tests/test_edgar_fetcher.py
git commit -m "feat: edgar每日轮（启动即跑+23:30，自动分类/增量Form4/accession去重/水位安全推进，决策上下文Task4）"
```

---

### Task 5: macro_fetcher — CPI YoY + 信用利差两系列

**Files:**
- Modify: `macro_fetcher.py`
- Modify: `tests/test_macro_fetcher.py`

- [ ] **Step 1: 写失败测试（追加到 tests/test_macro_fetcher.py，并改注册表断言）**

```python
CPI_TEXT = "\n".join([
    "DATE,CPIAUCSL",
    "2025-09-01,320.5",
    "2025-10-01,.",                    # 无效行跳过
    "2025-11-01,321.0",
    "2025-12-01,322.1",
    "2026-01-01,323.0",
    "2026-02-01,324.0",
    "2026-03-01,325.0",
    "2026-04-01,326.0",
    "2026-05-01,327.0",
    "2026-06-01,328.0",
    "2026-07-01,329.0",
    "2026-08-01,330.0",
    "2026-09-01,331.0",
])


def test_parse_fred_csv_tail():
    tail = mf.parse_fred_csv_tail(CPI_TEXT, 14)
    assert len(tail) == 13                 # '.' 行被跳过
    assert tail[-1] == ("2026-09-01", 331.0)
    assert tail[0] == ("2025-09-01", 320.5)
    assert mf.parse_fred_csv_tail("DATE,X\n") == []


def test_fetch_cpi_yoy(monkeypatch):
    async def fake_get(url, headers=None, allow_proxy=True):
        assert "CPIAUCSL" in url
        return CPI_TEXT

    monkeypatch.setattr(mf, "_get_text", fake_get)
    r = asyncio.run(mf.fetch_cpi_yoy())
    assert r["name"] == "cpi_yoy"
    assert r["asof"] == "2026-09-01"
    assert r["value"] == round((331.0 / 320.5 - 1) * 100, 2)
    assert r["extra"]["year_ago"] == 320.5


def test_fetch_cpi_yoy_insufficient_history(monkeypatch):
    short = "DATE,CPIAUCSL\n" + "\n".join(f"2026-0{i}-01,{320 + i}" for i in range(1, 4))

    async def fake_get(url, headers=None, allow_proxy=True):
        return short

    monkeypatch.setattr(mf, "_get_text", fake_get)
    assert asyncio.run(mf.fetch_cpi_yoy()) is None


def test_fetch_credit_spread(monkeypatch):
    async def fake_get(url, headers=None, allow_proxy=True):
        assert "BAMLH0A0HYM2" in url
        return "DATE,BAMLH0A0HYM2\n2026-10-10,3.18\n"

    monkeypatch.setattr(mf, "_get_text", fake_get)
    r = asyncio.run(mf.fetch_credit_spread())
    assert r == {"name": "credit_spread_hy", "asof": "2026-10-10",
                 "value": 3.18, "extra": {}}
```

同时把 `test_fetchers_registry` 改为：

```python
def test_fetchers_registry():
    assert {f.__name__ for f in mf.FETCHERS} == {
        "fetch_buffett", "fetch_cape", "fetch_yield_spread", "fetch_fear_greed",
        "fetch_cpi_yoy", "fetch_credit_spread"}
```

并在文件头部 import 区补 `import asyncio`（若无）。

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_macro_fetcher.py -v`
Expected: 4 FAILED（parse_fred_csv_tail/fetch_cpi_yoy/fetch_credit_spread 不存在）+ 1 FAILED（registry 缺新条目）

- [ ] **Step 3: macro_fetcher.py 实现**

3a. 在 `parse_fred_csv` 函数后新增：

```python
def parse_fred_csv_tail(text: str, n: int = 14) -> list[tuple[str, float]]:
    """FRED CSV → 尾部 n 行 [(date, value)]（升序，'.'/空值行跳过）。
    CPIAUCSL 月度，14 行覆盖 12 个月前即可算同比。@author yys"""
    out = []
    for line in text.strip().splitlines()[1:]:
        parts = line.strip().split(",")
        if len(parts) != 2:
            continue
        d, v = parts
        if v in (".", ""):
            continue
        try:
            out.append((d, float(v)))
        except ValueError:
            continue
    return out[-n:]
```

3b. 在 `fetch_fear_greed` 后新增两个 fetcher：

```python
async def fetch_cpi_yoy() -> dict | None:
    """CPI 同比 = CPIAUCSL 最新值 / 12 个月前值 − 1（FRED 月度，尾部 14 行）。@author yys"""
    rows = parse_fred_csv_tail(await _get_text(FRED_CSV_URL.format(sid="CPIAUCSL")) or "")
    if len(rows) < 13:               # 最新 + 12 个月前
        return None
    latest_d, latest_v = rows[-1]
    year_ago_d, year_ago_v = rows[-13]
    if year_ago_v <= 0:
        return None
    return {"name": "cpi_yoy", "asof": latest_d,
            "value": round((latest_v / year_ago_v - 1.0) * 100.0, 2),
            "extra": {"latest": latest_v, "year_ago": year_ago_v,
                      "year_ago_asof": year_ago_d}}


async def fetch_credit_spread() -> dict | None:
    """高收益债信用利差 = BAMLH0A0HYM2（ICE BofA US HY OAS，pct）。@author yys"""
    got = parse_fred_csv(await _get_text(FRED_CSV_URL.format(sid="BAMLH0A0HYM2")) or "")
    if not got:
        return None
    return {"name": "credit_spread_hy", "asof": got[0],
            "value": round(got[1], 3), "extra": {}}
```

3c. `FETCHERS` 元组改为：

```python
FETCHERS = (fetch_buffett, fetch_cape, fetch_yield_spread, fetch_fear_greed,
            fetch_cpi_yoy, fetch_credit_spread)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_macro_fetcher.py -v`
Expected: 全部 PASSED（含改后的 registry）

- [ ] **Step 5: 提交**

```bash
git add macro_fetcher.py tests/test_macro_fetcher.py
git commit -m "feat: 宏观循环新增CPI同比+高收益信用利差两系列（FRED搭车，决策上下文Task5）"
```

---

### Task 6: daily_bars — SPY/RSP 基准追加进日K清单

**Files:**
- Modify: `daily_bars.py`
- Modify: `tests/test_daily_bars.py`

- [ ] **Step 1: 写失败测试（追加到 tests/test_daily_bars.py）**

```python
def test_us_symbols_with_benchmarks():
    holdings = [
        {"symbol": "QQQ", "market": "US"},
        {"symbol": "SPY", "market": "US"},        # 已持仓不重复追加
        {"symbol": "VGT", "market": "US"},
        {"symbol": "HK0000740297", "market": "HKFUND"},   # 港股基金排除
    ]
    out = dbar._us_symbols_with_benchmarks(holdings)
    assert out == ["QQQ", "SPY", "VGT", "VIX", "RSP"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_daily_bars.py -v -k benchmarks`
Expected: FAIL（`_us_symbols_with_benchmarks` 不存在）

- [ ] **Step 3: daily_bars.py 实现**

3a. `lp_symbol`（:311）之前新增纯函数：

```python
def _us_symbols_with_benchmarks(holdings: list[dict]) -> list[str]:
    """美股持仓 + VIX/SPY/RSP 固定基准（决策上下文数据源：VIX 分位 +
    SPY 对标 + RSP 等权广度代理；已持仓的基准不重复追加）。@author yys"""
    symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
    for fixed in ("VIX", "SPY", "RSP"):
        if fixed not in symbols:
            symbols.append(fixed)
    return symbols
```

3b. `daily_refresh_loop`（:403-405）中：

```python
            symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if "VIX" not in symbols:      # VIX 分位数据源（V2 止盈过滤），固定追加不依赖持仓
                symbols.append("VIX")
```

替换为：

```python
            symbols = _us_symbols_with_benchmarks(holdings)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_daily_bars.py -v`
Expected: 全部 PASSED

- [ ] **Step 5: 提交**

```bash
git add daily_bars.py tests/test_daily_bars.py
git commit -m "feat: 日K清单固定追加SPY/RSP基准（对标+等权广度代理，长桥3s节流照旧，决策上下文Task6）"
```

---

### Task 7: server — _build_agent_context 聚合 + 双端点注入 + 后台任务注册

**Files:**
- Modify: `server.py`
- Create: `tests/test_agent_context.py`

- [ ] **Step 1: 写失败测试 tests/test_agent_context.py**

```python
"""决策上下文聚合 + 双端点注入测试（db 全打桩零连库）。@author yys"""
import asyncio
import json
import time
from datetime import date

from fastapi.testclient import TestClient


def _bars(n, base=100.0):
    """n 根日K，close 递增 1%。"""
    return [{"date": date(2026, 9, 1 + i % 28), "open": base, "high": base,
             "low": base, "close": round(base * (1.01 ** i), 4), "volume": 1}
            for i in range(n)]


def _env(monkeypatch):
    import server
    import daily_bars as dbar

    server._agent_ctx_cache.update(ts=0.0, ctx=None)

    async def fake_bars(sym, limit=None):
        return {"SPY": _bars(30, 500.0), "RSP": _bars(30, 100.0),
                "MU": _bars(30, 900.0), "BABA": _bars(30, 100.0),
                "SMH": _bars(30, 200.0)}.get(sym, [])

    async def fake_holdings(enabled_only=True):
        return [
            {"symbol": "MU", "market": "US", "asset_type": "stock"},
            {"symbol": "BABA", "market": "US", "asset_type": "fpi"},
            {"symbol": "SMH", "market": "US", "asset_type": "etf"},
            {"symbol": "HK0000740297", "market": "HKFUND", "asset_type": None},
        ]

    async def fake_insider(sym, days=90):
        if sym != "MU":
            return []
        return [
            {"filed_date": date(2026, 10, 1), "action": "A", "shares": 141000,
             "value_usd": 19035000.0},
            {"filed_date": date(2026, 9, 20), "action": "D", "shares": 5000,
             "value_usd": 700000.0},
        ]

    async def fake_macro_latest():
        return [
            {"name": "credit_spread_hy", "value": 3.18, "asof": date(2026, 10, 10),
             "extra": "{}"},
            {"name": "cpi_yoy", "value": 2.9, "asof": date(2026, 9, 30),
             "extra": "{}"},
            {"name": "yield_2s10s", "value": 0.15, "asof": date(2026, 10, 10),
             "extra": json.dumps({"dgs10": 4.05, "dgs2": 3.9})},
        ]

    async def fake_cfg(key):
        return "2026-10-11 23:31:00" if key == "EDGAR_LAST_OK" else None

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_insider_tx", fake_insider)
    monkeypatch.setattr(server.db, "get_macro_latest", fake_macro_latest)
    monkeypatch.setattr(server.db, "get_config", fake_cfg)
    monkeypatch.setattr(server, "market_cache",
                        [{"key": "US10Y", "price": 4.04}])
    monkeypatch.setitem(dbar.INDICATORS, "MU",
                        {"ma50": 1.0, "ma200": 1.0, "rsi14": 62.3,
                         "stop_level": None, "hhv22": 2.0, "atr22": 0.1,
                         "enough_history": True, "asof": "2026-10-10"})
    return server


def test_build_agent_context(monkeypatch):
    server = _env(monkeypatch)
    ctx = asyncio.run(server._build_agent_context())
    # SPY 块
    assert ctx["spy"]["close"] == round(500.0 * 1.01 ** 29, 4)
    assert ctx["spy"]["chg_pct_1d"] == 1.0
    # 广度：RSP/SPY 首日对齐比值
    assert ctx["breadth"]["rsp_spy_ratio"] == round(100.0 / 500.0, 6)
    assert ctx["breadth"]["chg_pct_5d"] is not None
    # US10Y 双源
    assert ctx["us10y"]["price"] == 4.04
    assert ctx["us10y"]["dgs10_fred"] == 4.05
    assert ctx["credit_spread_hy"] == {"value": 3.18, "asof": "2026-10-10"}
    assert ctx["cpi_yoy"] == {"value": 2.9, "asof": "2026-09-30"}
    # factors：个股三分类
    f_mu = ctx["factors"]["MU"]
    assert f_mu["rsi14"] == 62.3 and f_mu["asset_type"] == "stock"
    assert f_mu["spy_rel_20d_pct"] == 0.0          # 同涨幅 → 相对差 0
    assert f_mu["insider_90d"]["net_shares"] == 136000
    assert f_mu["insider_90d"]["net_value_usd"] == 18335000.0
    assert ctx["factors"]["BABA"]["insider_note"] == "fpi_no_form4"
    assert ctx["factors"]["SMH"]["insider_90d"] is None
    assert "HK0000740297" not in ctx["factors"]    # 港股基金不进 factors
    assert ctx["meta"]["edgar_ok"] is True


def test_build_agent_context_cache_60s(monkeypatch):
    server = _env(monkeypatch)
    calls = []

    async def fake_bars(sym, limit=None):
        calls.append(sym)
        return []

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    asyncio.run(server._build_agent_context())
    n1 = len(calls)
    asyncio.run(server._build_agent_context())     # 60s 内第二次 → 缓存命中
    assert len(calls) == n1


def test_agent_portfolio_endpoint_injects_context(monkeypatch):
    server = _env(monkeypatch)

    async def fake_ctx():
        return {"spy": {"close": 600.0}}

    monkeypatch.setattr(server, "_build_agent_context", fake_ctx)

    async def fake_cfg(key):
        return "test-key" if key == "AGENT_API_KEY" else None

    monkeypatch.setattr(server.db, "get_config", fake_cfg)
    client = TestClient(server.app)
    r = client.get("/api/agent/portfolio", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json()["context"] == {"spy": {"close": 600.0}}


def test_overview_endpoint_injects_context(monkeypatch):
    server = _env(monkeypatch)

    async def fake_ctx():
        return {"spy": {"close": 600.0}}

    monkeypatch.setattr(server, "_build_agent_context", fake_ctx)
    monkeypatch.setattr(server.db, "get_recent_trades", _async_ret([]))
    monkeypatch.setattr(server, "_get_closed_ctx", _async_ret(([], [])))

    async def fake_session(t):
        return {"username": "u", "expires": time.time() + 3600}

    monkeypatch.setattr(server.db, "get_session", fake_session)
    client = TestClient(server.app)
    client.cookies.set("session_token", "tok")
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    assert r.json()["context"] == {"spy": {"close": 600.0}}


def _async_ret(v):
    async def _f(*a, **kw):
        return v
    return _f
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_agent_context.py -v`
Expected: 4 FAILED（`_build_agent_context`/`_agent_ctx_cache` 不存在）

- [ ] **Step 3: server.py 实现**

3a. 顶部 import 区（`import macro_fetcher` 附近）加 `import edgar_fetcher`。

3b. 在 `agent_portfolio` 定义（:2701）之前插入：

```python
_agent_ctx_cache = {"ts": 0.0, "ctx": None}


def _pct_change(series: list[float], n: int) -> float | None:
    """近 n 期涨跌幅%（数据不足/基数为 0 返回 None）。@author yys"""
    if len(series) < n + 1 or not series[-n - 1]:
        return None
    return round((series[-1] / series[-n - 1] - 1.0) * 100.0, 2)


def _ratio_series(bars_a: list[dict], bars_b: list[dict]) -> list[float]:
    """按日期对齐的 a/b 收盘比值序列（升序，仅取两序列共有日期）。@author yys"""
    mb = {str(b["date"])[:10]: float(b["close"]) for b in bars_b}
    return [float(a["close"]) / mb[str(a["date"])[:10]]
            for a in bars_a
            if str(a["date"])[:10] in mb and mb[str(a["date"])[:10]]]


async def _build_agent_context() -> dict:
    """决策上下文聚合（60s 缓存）：facts only，缺数逐项 null 降级不阻塞主体。@author yys"""
    now = time.time()
    if _agent_ctx_cache["ctx"] is not None and now - _agent_ctx_cache["ts"] < 60:
        return _agent_ctx_cache["ctx"]
    ctx: dict = {"spy": None, "us10y": None, "credit_spread_hy": None,
                 "cpi_yoy": None, "breadth": None, "factors": {}, "meta": {}}
    try:
        spy_bars = await db.get_daily_bars("SPY")
        rsp_bars = await db.get_daily_bars("RSP")
        spy_closes = [float(b["close"]) for b in spy_bars] if spy_bars else []
        if spy_bars:
            ctx["spy"] = {"close": spy_closes[-1],
                          "asof": str(spy_bars[-1]["date"])[:10],
                          "chg_pct_1d": _pct_change(spy_closes, 1)}
        if spy_bars and rsp_bars:
            ratios = _ratio_series(rsp_bars, spy_bars)
            if ratios:
                ctx["breadth"] = {
                    "rsp_spy_ratio": round(ratios[-1], 6),
                    "chg_pct_5d": _pct_change(ratios, 5),
                    "chg_pct_20d": _pct_change(ratios, 20),
                }
        latest = {r["name"]: r for r in await db.get_macro_latest()}

        def _macro_val(name):
            r = latest.get(name)
            return None if not r else {"value": float(r["value"]),
                                       "asof": str(r["asof"])[:10]}

        ctx["credit_spread_hy"] = _macro_val("credit_spread_hy")
        ctx["cpi_yoy"] = _macro_val("cpi_yoy")
        us10y_price = next((m.get("price") for m in (market_cache or [])
                            if m.get("key") == "US10Y" and m.get("price")), None)
        dgs10 = None
        row = latest.get("yield_2s10s")
        if row:
            try:
                extra = json.loads(row["extra"]) if row.get("extra") else {}
                dgs10 = extra.get("dgs10")
            except Exception:
                dgs10 = None
        ctx["us10y"] = {"price": us10y_price, "dgs10_fred": dgs10,
                        "asof": str(row["asof"])[:10] if row else None}
        today = datetime.now().strftime("%Y-%m-%d")
        holdings = await db.get_holdings(enabled_only=True)
        for h in holdings:
            if h.get("market") != "US":
                continue
            sym = h["symbol"]
            at = h.get("asset_type")
            ind = daily_bars.get_indicators(sym) or {}
            bars = await db.get_daily_bars(sym)
            closes = [float(b["close"]) for b in bars]
            rel = None
            if len(closes) >= 21 and len(spy_closes) >= 21:
                rel = round(_pct_change(closes, 20) - _pct_change(spy_closes, 20), 2)
            f = {"rsi14": ind.get("rsi14"), "spy_rel_20d_pct": rel,
                 "asset_type": at}
            if at == "fpi":
                f["insider_90d"] = None
                f["insider_note"] = "fpi_no_form4"
            elif at == "stock":
                rows = await db.get_insider_tx(sym)
                f["insider_90d"] = edgar_fetcher.build_insider_summary(rows, today)
            else:
                f["insider_90d"] = None
            ctx["factors"][sym] = f
        last_ok = await db.get_config("EDGAR_LAST_OK")
        edgar_ok = False
        if last_ok:
            try:
                edgar_ok = (datetime.now() - datetime.strptime(
                    last_ok, "%Y-%m-%d %H:%M:%S")).total_seconds() < 36 * 3600
            except ValueError:
                pass
        ctx["meta"] = {"edgar_ok": edgar_ok, "edgar_last_ok": last_ok,
                       "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    except Exception as e:
        logger.warning("决策上下文构建失败（本轮降级）: %s", e)
    _agent_ctx_cache.update(ts=now, ctx=ctx)
    return ctx
```

3c. `agent_portfolio` 返回 dict 中 `"fx": {...}` 行之后追加一行：

```python
        "context": await _build_agent_context(),
```

3d. `portfolio_overview` 的 `return data` 之前（closed 块之后）追加：

```python
    # 决策上下文（facts only，agent 接口同源；60s 缓存，缺数逐项 null）
    data["context"] = await _build_agent_context()
```

3e. lifespan 注册（macro_task 块 :188-190 之后）：

```python
    # EDGAR 内部人交易日更（23:30，决策上下文 IR 因子源；限流以稳为主）
    global edgar_task
    edgar_task = asyncio.create_task(edgar_fetcher.edgar_daily_loop())
```

并在 shutdown 取消区（`if macro_task:` 块之后）加：

```python
    if edgar_task:
        edgar_task.cancel()
```

- [ ] **Step 4: 跑测试确认通过**

Run: `python3 -m pytest tests/test_agent_context.py tests/test_server_auth.py tests/test_alert_api.py -v`
Expected: 新增 4 PASSED，既有零回归

- [ ] **Step 5: 全量回归 + 提交**

Run: `python3 -m pytest tests/ -q`
Expected: 全部通过（约 466 passed）

```bash
git add server.py tests/test_agent_context.py
git commit -m "feat: 决策上下文聚合出口（60s缓存注入agent/overview双端点+EDGAR后台任务注册，Task7）"
```

---

### Task 8: 前端 portfolio.html — 决策上下文面板 + 持仓表三列

**Files:**
- Modify: `static/portfolio.html`

（前端无自动化测试；结构用 grep 验证，线上浏览器冒烟在 Task 9）

- [ ] **Step 1: HTML——市场估值 section（:396-400 附近）之后新增面板卡片**

```html
  <!-- 决策上下文：facts-only 因子面板（SPY基准/利率/CPI/信用利差/广度，日更粒度，
       与外部 Agent 接口同源 60s 缓存） -->
  <section class="card pad">
    <div class="card-head">
      <div class="card-title">决策上下文</div>
      <div class="card-note">日更粒度 · facts only · 供外部 Agent 决策</div>
    </div>
    <div class="macro-grid" id="ctxGrid"></div>
  </section>
```

- [ ] **Step 2: 表头——持仓明细表（:467-476）「盈亏比例」th 之后追加三列**

```html
          <th>RSI14</th>
          <th>vs SPY(20d)</th>
          <th>内部人90d</th>
```

并把两处 `colspan="9"` 改为 `colspan="12"`（:480 骨架行、renderPositions 空态行 :886）。

- [ ] **Step 3: JS——renderContext 函数（renderMacro 函数之后新增）**

```javascript
// ===== 决策上下文（facts only，overview.context 同源 60s 缓存）=====
// @author yys
function ctxVal(v, suffix) {
  return v == null ? '--' : (v > 0 ? '+' : '') + fmt(v) + (suffix || '');
}
function renderContext(d) {
  const box = document.getElementById('ctxGrid');
  if (!box) return;
  const c = d && d.context;
  if (!c) {
    box.innerHTML = '<div class="empty" style="grid-column:1/-1">暂无决策上下文数据</div>';
    return;
  }
  const spy = c.spy, br = c.breadth, cs = c.credit_spread_hy,
        cpi = c.cpi_yoy, u10 = c.us10y;
  const cells = [
    ['SPY 基准', spy ? `${fmt(spy.close)}<span class="macro-asof"> · ${spy.asof}</span>` : '--',
     spy && spy.chg_pct_1d != null ? `<span class="${cls(spy.chg_pct_1d)}">${ctxVal(spy.chg_pct_1d, '%')}</span>` : ''],
    ['US 10Y', u10 && u10.price != null ? fmt(u10.price, 2) + '%'
      : (u10 && u10.dgs10_fred != null ? fmt(u10.dgs10_fred, 2) + '%' : '--'),
     u10 && u10.asof ? `<span class="macro-asof">${u10.asof}</span>` : ''],
    ['CPI 同比', cpi ? ctxVal(cpi.value, '%') : '--',
     cpi ? `<span class="macro-asof">${cpi.asof}</span>` : ''],
    ['信用利差 HY OAS', cs ? fmt(cs.value, 2) + '%' : '--',
     cs ? `<span class="macro-asof">${cs.asof}</span>` : ''],
    ['市场广度 RSP/SPY', br ? fmt(br.rsp_spy_ratio, 4) : '--',
     br && br.chg_pct_5d != null ? `5日 <span class="${cls(br.chg_pct_5d)}">${ctxVal(br.chg_pct_5d, '%')}</span>` : ''],
  ];
  box.innerHTML = cells.map(([t, v, s]) =>
    `<div class="macro-cell"><div class="macro-head">${t}</div>` +
    `<div class="macro-val">${v}</div><div class="macro-asof">${s || ''}</div></div>`
  ).join('');
}
```

- [ ] **Step 4: JS——renderPositions 行模板（:890 forEach 内）**

在 `const nb = nightBadge(...)` 行之后追加：

```javascript
    // 决策上下文因子列（仅美股有值；基金/港股显示 —）
    const f = (d.context && d.context.factors && d.context.factors[p.symbol]) || null;
    const rsiHtml = f && f.rsi14 != null ? fmt(f.rsi14, 1) : '—';
    const relHtml = f && f.spy_rel_20d_pct != null
      ? `<span class="${cls(f.spy_rel_20d_pct)}">${ctxVal(f.spy_rel_20d_pct, '%')}</span>` : '—';
    let insHtml = '—';
    if (f && f.insider_note === 'fpi_no_form4') insHtml = 'FPI';
    else if (f && f.insider_90d) {
      const v = f.insider_90d.net_value_usd;
      insHtml = v == null ? `${f.insider_90d.buys}/${f.insider_90d.sells}笔`
        : `<span class="${cls(v)}">${sign(v)}$${fmt(Math.abs(v) / 1000, 1)}k</span>`;
    }
```

并在 `tr.innerHTML`（:905-912）的盈亏比例 td 之后、成本价 td 之前插入：

```javascript
      <td>${rsiHtml}</td><td>${relHtml}</td><td>${insHtml}</td>
```

- [ ] **Step 5: JS——调用接线**

两处调用 `renderPositions(d)` 的地方（初始化 :822 `renderPositions(lastOverview)` 与 overview 轮询 :1085 `renderPositions(d)`）之后各追加一行 `renderContext(d);`（:822 处为 `renderContext(lastOverview);`）。

- [ ] **Step 6: 结构验证 + 提交**

Run: `grep -c "renderContext\|ctxGrid\|colspan=\"12\"" static/portfolio.html`
Expected: ≥ 6 处命中

Run: `node --check <(sed -n '/<script>/,/<\/script>/p' static/portfolio.html | sed '1d;$d')`（若 node 不可用则跳过，靠 Task 9 浏览器冒烟）

```bash
git add static/portfolio.html
git commit -m "feat: 持仓页决策上下文面板+RSI/vsSPY/内部人三列（同源overview.context，Task8）"
```

---

### Task 9: 全量回归 + 部署 + 线上验证

**Files:** 无新文件（运维任务）

- [ ] **Step 1: 全量测试 + 密钥门禁**

Run: `python3 -m pytest tests/ -q`
Expected: 全部通过（约 466 passed，零回归）

Run: `python3 scripts/check_secrets.py`
Expected: `检出 0 处`

- [ ] **Step 2: 部署（命令见 AGENTS.md 部署节；目标 IP/域名见 AGENTS.local.md）**

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  --exclude='qdii.env' --exclude='*.local.md' --exclude='.mcp.json' \
  --exclude='static/download' \
  ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"
```

- [ ] **Step 3: 服务器库配置 EDGAR_UA（合规联系方式，域名不入代码库）**

```bash
ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -udubai -p\"\$(grep QDII_DB_PASS /opt/qdii/qdii.env | cut -d= -f2)\" qdii -e \"INSERT INTO sys_config (\`key\`, value, remark) VALUES ('EDGAR_UA', 'QDII-Portfolio admin@<contact-domain>', 'EDGAR合规UA联系方式') ON DUPLICATE KEY UPDATE value=VALUES(value);\""
```

（sys_config 列名为 `` `key`/`value`/`remark` ``，database.py:182；`<contact-domain>` 真实联系邮箱域名见 AGENTS.local.md，敏感值禁入代码库与文档）

- [ ] **Step 4: 线上验证**

```bash
ssh root@<server-ip> "systemctl is-active qdii && journalctl -u qdii --since '5 min ago' | grep -E 'EDGAR|决策上下文|日K刷新' | tail -30"
```

Expected 依次确认：
- `日K刷新开始: N 只`（N = 美股持仓数 + 3：VIX/SPY/RSP）
- `EDGAR 分类: MU → stock`、`EDGAR 分类: BABA → fpi`、`EDGAR 分类: SMH → etf` 等分类日志
- `EDGAR 轮完成: {...}` stats 行
- 次日 22:40 后 macro 日志含 `macro cpi_yoy = ...` 与 `macro credit_spread_hy = ...`

```bash
ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -udubai -p\"\$(grep QDII_DB_PASS /opt/qdii/qdii.env | cut -d= -f2)\" qdii -e \"SELECT symbol, asset_type FROM holdings WHERE market='US' AND enabled=1; SELECT symbol, COUNT(*) AS tx FROM insider_tx GROUP BY symbol;\""
```

Expected：asset_type 全部收敛（stock 11 只个股/fpi 1 只 BABA/etf 6 只）；insider_tx 有 8 只个股的 90 天回溯数据。

```bash
ssh root@<server-ip> "curl -s -H \"X-API-Key: \$(mysql -h127.0.0.1 -P3914 -udubai -p\"\$(grep QDII_DB_PASS /opt/qdii/qdii.env | cut -d= -f2)\" qdii -N -e \"SELECT value FROM sys_config WHERE \\\`key\\\`='AGENT_API_KEY'\")\" http://127.0.0.1:8888/api/agent/portfolio" | python3 -c "import json,sys; d=json.load(sys.stdin); c=d['context']; print('spy:', c['spy']); print('breadth:', c['breadth']); print('MU:', c['factors']['MU']); print('meta:', c['meta'])"
```

Expected：context 块完整（spy/breadth/us10y/cpi_yoy/credit_spread_hy/factors 逐项有值或 null 降级，MU 含 rsi14 + insider_90d）。

- [ ] **Step 5: 浏览器冒烟（人工，可留待用户）**

打开持仓页确认「决策上下文」面板 5 张卡 + 持仓表 RSI14 / vs SPY(20d) / 内部人90d 三列渲染正常（美股行有值，基金行 —，BABA 行 FPI）。

- [ ] **Step 6: 提交部署记录（若有遗留改动）**

```bash
git status   # 确认工作区干净，所有任务已分步提交
```

---

## Self-Review 结论（写计划后自查）

1. **Spec 覆盖**：层1（RSI/US10Y 暴露→Task7）、层2（FRED→Task5）、层3（SPY/RSP→Task6+Task7）、层4（EDGAR→Task1-4）、出口（→Task7）、前端（→Task8）、测试/部署（→Task9）；spec「明确不做」清单未引入。
2. **占位符**：无 TBD/TODO；每步带完整代码与命令。
3. **类型一致性**：`save_insider_tx` 行 dict 键 = `parse_form4_xml` 输出键；`build_insider_summary` 消费 DB 行键（action/shares/value_usd/filed_date）；`_build_agent_context` 的 factors 键与前端 `f.rsi14/f.spy_rel_20d_pct/f.insider_90d/f.insider_note` 对齐；`EDGAR_WATERMARK/EDGAR_LAST_OK/EDGAR_TICKER_MAP_TS/EDGAR_UA` 四个 sys_config 键全文一致。
