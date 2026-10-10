# A 股跨境 ETF 集合竞价套利监控系统 - 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一个 FastAPI + SSE 的实时监控看板，抓取跨境 QDII ETF 竞价盘口数据，计算套利得分并推送排序结果到浏览器。

**Architecture:** FastAPI 单进程后端，asyncio 后台任务循环抓取 akshare 数据，SSE 推送到前端。前端单页 HTML/JS，三 Tab 布局（监控看板/代理管理/标的池）。

**Tech Stack:** Python 3.12, FastAPI, akshare, httpx[socks], aiomysql, uvicorn

---

## 数据源策略

| 数据 | akshare 接口 | 调用方式 | 频率 |
|------|-------------|---------|------|
| ETF 批量行情+IOPV | `fund_etf_spot_em()` | 1次批量 | 每轮1次 |
| ETF 五档买卖量 | `stock_bid_ask_em(symbol)` | 逐个并发 | 每轮M次 |
| 全球期货行情 | `futures_global_spot_em()` | 1次批量 | 每轮1次 |

`fund_etf_spot_em()` 返回全部 ETF 数据，包含 **IOPV实时估值**、**基金折价率**、买一/卖一价格、委比等，是最关键的批量接口。

---

## File Structure

```
qdii/
├── .mcp.json
├── config.py           # 全局配置
├── database.py         # 异步 MySQL CRUD
├── fetcher.py          # akshare 数据抓取 + 代理轮询
├── scorer.py           # 得分计算引擎
├── server.py           # FastAPI 主服务
├── static/
│   └── index.html      # 单页看板
├── tests/
│   └── test_scorer.py  # scorer 单元测试
├── requirements.txt
└── docs/
    └── superpowers/
        ├── specs/
        │   └── 2026-04-16-etf-call-auction-monitor-design.md
        └── plans/
            └── 2026-04-17-etf-monitor-plan.md
```

---

### Task 1: 项目脚手架

**Files:**
- Create: `requirements.txt`
- Create: `config.py`

- [ ] **Step 1: 创建 requirements.txt**

```txt
fastapi
uvicorn[standard]
aiomysql
akshare
httpx[socks]
pytest
pytest-asyncio
```

- [ ] **Step 2: 创建 config.py**

```python
"""全局配置"""
import os

# MySQL 连接
DB_HOST = os.getenv("DB_HOST", "<server-ip>")
DB_PORT = int(os.getenv("DB_PORT", "3914"))
DB_USER = os.getenv("DB_USER", "<db-user>")
DB_PASS = os.getenv("DB_PASS", "n7VO5yOzTDavUocK")
DB_NAME = os.getenv("DB_NAME", "qdii")
DB_POOL_SIZE = 5

# 得分模型默认权重
DEFAULT_W_RATIO = 0.7
DEFAULT_W_PREMIUM = 0.3
DEFAULT_FUTURES_KILL_THRESHOLD = -1.5
DEFAULT_SAFE_REQ_PER_MIN = 20
MIN_INTERVAL_SEC = 3.0

# 服务端口
SERVER_PORT = 8000

# 期指符号映射 (ETF futures_index -> akshare futures 名称关键字)
FUTURES_NAME_MAP = {
    "NQ": "纳斯达克",
    "ES": "标普",
    "YM": "道琼斯",
    "DAX": "DAX",
    "NK": "日经",
}
```

- [ ] **Step 3: 安装依赖**

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

- [ ] **Step 4: 初始化 git**

```bash
git init
```

---

### Task 2: 数据库模块

**Files:**
- Create: `database.py`

- [ ] **Step 1: 创建 database.py**

```python
"""异步 MySQL 数据访问层"""
import aiomysql
import logging
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_POOL_SIZE

logger = logging.getLogger(__name__)
_pool: aiomysql.Pool | None = None


async def get_pool() -> aiomysql.Pool:
    global _pool
    if _pool is None:
        _pool = await aiomysql.create_pool(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASS, db=DB_NAME,
            minsize=2, maxsize=DB_POOL_SIZE,
            autocommit=True, charset="utf8mb4",
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        _pool.close()
        await _pool.wait_closed()
        _pool = None


async def execute(sql: str, args=None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            if sql.strip().upper().startswith("SELECT"):
                return await cur.fetchall()
            elif sql.strip().upper().startswith("INSERT"):
                return cur.lastrowid


async def execute_dict(sql: str, args=None) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args)
            return await cur.fetchall()


# ---- 建表 ----
async def init_tables():
    await execute("""
        CREATE TABLE IF NOT EXISTS etf_targets (
            code VARCHAR(6) PRIMARY KEY,
            name VARCHAR(20) NOT NULL DEFAULT '',
            futures_index VARCHAR(20) NOT NULL DEFAULT '',
            prev_nav DECIMAL(8,4) DEFAULT 0,
            enabled TINYINT(1) DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS proxy_pool (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(100) NOT NULL UNIQUE,
            latency_ms INT DEFAULT 0,
            alive TINYINT(1) DEFAULT 1,
            last_check DATETIME DEFAULT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    await execute("""
        CREATE TABLE IF NOT EXISTS sys_config (
            `key` VARCHAR(50) PRIMARY KEY,
            `value` VARCHAR(200) NOT NULL,
            remark VARCHAR(100) DEFAULT ''
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    # 种子配置
    seeds = [
        ("w_ratio", "0.7", "买/卖比权重"),
        ("w_premium", "0.3", "溢价率权重"),
        ("futures_kill_threshold", "-1.5", "期指跌幅杀分阈值(%)"),
        ("safe_req_per_min", "20", "单IP安全请求频率/分钟"),
    ]
    for k, v, r in seeds:
        await execute(
            "INSERT IGNORE INTO sys_config (`key`, `value`, remark) VALUES (%s, %s, %s)",
            (k, v, r),
        )
    logger.info("数据库表初始化完成")


# ---- ETF CRUD ----
async def get_etf_list(enabled_only=True) -> list[dict]:
    sql = "SELECT * FROM etf_targets"
    if enabled_only:
        sql += " WHERE enabled=1"
    return await execute_dict(sql)


async def add_etf(code: str, name: str, futures_index: str, prev_nav: float = 0):
    await execute(
        "INSERT INTO etf_targets (code, name, futures_index, prev_nav) VALUES (%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE name=%s, futures_index=%s, prev_nav=%s",
        (code, name, futures_index, prev_nav, name, futures_index, prev_nav),
    )


async def delete_etf(code: str):
    await execute("DELETE FROM etf_targets WHERE code=%s", (code,))


async def toggle_etf(code: str, enabled: bool):
    await execute("UPDATE etf_targets SET enabled=%s WHERE code=%s", (int(enabled), code))


# ---- Proxy CRUD ----
async def get_proxy_list() -> list[dict]:
    return await execute_dict("SELECT * FROM proxy_pool ORDER BY id")


async def get_alive_proxies() -> list[dict]:
    return await execute_dict("SELECT * FROM proxy_pool WHERE alive=1 ORDER BY id")


async def add_proxy(url: str):
    await execute("INSERT IGNORE INTO proxy_pool (url) VALUES (%s)", (url,))


async def delete_proxy(proxy_id: int):
    await execute("DELETE FROM proxy_pool WHERE id=%s", (proxy_id,))


async def update_proxy_status(proxy_id: int, alive: bool, latency_ms: int):
    await execute(
        "UPDATE proxy_pool SET alive=%s, latency_ms=%s, last_check=NOW() WHERE id=%s",
        (int(alive), latency_ms, proxy_id),
    )


# ---- Config CRUD ----
async def get_configs() -> dict:
    rows = await execute_dict("SELECT `key`, `value` FROM sys_config")
    return {r["key"]: r["value"] for r in rows}


async def get_config(key: str, default: str = "") -> str:
    rows = await execute_dict("SELECT `value` FROM sys_config WHERE `key`=%s", (key,))
    return rows[0]["value"] if rows else default


async def set_config(key: str, value: str):
    await execute(
        "INSERT INTO sys_config (`key`, `value`) VALUES (%s,%s) "
        "ON DUPLICATE KEY UPDATE `value`=%s",
        (key, value, value),
    )
```

- [ ] **Step 2: 初始化数据库并插入种子 ETF 数据**

创建临时脚本 `init_db.py`（仅运行一次）：

```python
"""一次性初始化脚本：建表 + 插入种子 ETF 数据"""
import asyncio
from database import init_tables, add_etf

SEED_ETFS = [
    ("513100", "纳指ETF",     "NQ", 0),
    ("159941", "纳指ETF",     "NQ", 0),
    ("513030", "德国ETF",     "DAX", 0),
    ("513500", "标普500ETF",  "ES", 0),
    ("513400", "道琼斯ETF",   "YM", 0),
    ("513520", "日经ETF",     "NK", 0),
    ("513300", "纳斯达克ETF", "NQ", 0),
    ("513050", "中概互联网ETF", "NQ", 0),
]

async def main():
    await init_tables()
    for code, name, fi, nav in SEED_ETFS:
        await add_etf(code, name, fi, nav)
        print(f"  已添加: {code} {name}")
    print("初始化完成")

asyncio.run(main())
```

- [ ] **Step 3: 运行初始化脚本并验证**

```bash
python init_db.py
```

预期输出：
```
  已添加: 513100 纳指ETF
  已添加: 159941 纳指ETF
  ...
初始化完成
```

- [ ] **Step 4: 提交**

```bash
git add config.py database.py requirements.txt init_db.py .mcp.json
git commit -m "feat: 项目脚手架 + 数据库模块 + 种子数据"
```

---

### Task 3: 得分计算模块 (TDD)

**Files:**
- Create: `tests/test_scorer.py`
- Create: `scorer.py`

- [ ] **Step 1: 创建 tests/test_scorer.py**

```python
"""scorer 模块单元测试"""
import pytest
from scorer import normalize, calc_score, apply_futures_kill, rank_etfs


class TestNormalize:
    def test_basic(self):
        assert normalize([1, 2, 3, 4, 5]) == [0.0, 0.25, 0.5, 0.75, 1.0]

    def test_single_value(self):
        assert normalize([42]) == [1.0]

    def test_all_same(self):
        assert normalize([3, 3, 3]) == [1.0, 1.0, 1.0]

    def test_empty(self):
        assert normalize([]) == []

    def test_with_zeros(self):
        result = normalize([0, 5, 10])
        assert result[0] == 0.0
        assert result[2] == 1.0


class TestCalcScore:
    def test_basic_weighted(self):
        # ratio_norm=1.0, premium_norm=0.0 -> 0.7*1 - 0.3*0 = 0.7
        assert calc_score(1.0, 0.0) == pytest.approx(0.7)

    def test_zero_inputs(self):
        assert calc_score(0.0, 0.0) == 0.0

    def test_max_ratio_min_premium(self):
        score = calc_score(1.0, 1.0)
        # W1*1 - W2*1 = 0.7 - 0.3 = 0.4
        assert score == pytest.approx(0.4)

    def test_custom_weights(self):
        score = calc_score(1.0, 0.0, w_ratio=0.5, w_premium=0.5)
        assert score == pytest.approx(0.5)


class TestFuturesKill:
    def test_kill_when_divergence(self):
        # 期指跌2%, ETF涨1.5% -> 杀分
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=True)
        assert score == 0.0

    def test_no_kill_when_not_diverging(self):
        # 期指跌0.5%, ETF涨1.5% -> 不杀
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-0.5,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0

    def test_no_kill_when_etf_down(self):
        # 期指跌2%, ETF也跌 -> 不杀(同向)
        score = apply_futures_kill(85.0, etf_change_pct=-1.0, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0

    def test_no_kill_when_disabled(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=-2.0,
                                   threshold=-1.5, enabled=False)
        assert score == 85.0

    def test_no_kill_when_futures_none(self):
        score = apply_futures_kill(85.0, etf_change_pct=1.5, futures_change_pct=None,
                                   threshold=-1.5, enabled=True)
        assert score == 85.0


class TestRankEtfs:
    def test_ranking_order(self):
        etfs = [
            {"code": "A", "buy1_vol": 100, "sell1_vol": 50,
             "price": 1.5, "iopv": 1.5, "change_pct": 1.0, "futures_change": None},
            {"code": "B", "buy1_vol": 500, "sell1_vol": 10,
             "price": 1.2, "iopv": 1.25, "change_pct": 0.5, "futures_change": None},
        ]
        result = rank_etfs(etfs, w_ratio=0.7, w_premium=0.3, futures_kill_enabled=False,
                           futures_kill_threshold=-1.5)
        # B should rank higher (stronger buy/sell ratio)
        assert result[0]["code"] == "B"

    def test_buy1_zero_gets_zero_score(self):
        etfs = [
            {"code": "A", "buy1_vol": 0, "sell1_vol": 100,
             "price": 1.0, "iopv": 1.0, "change_pct": 0, "futures_change": None},
        ]
        result = rank_etfs(etfs)
        assert result[0]["score"] == 0.0
```

- [ ] **Step 2: 运行测试确认失败**

```bash
python -m pytest tests/test_scorer.py -v
```

预期：全部 FAIL（ModuleNotFoundError: scorer）

- [ ] **Step 3: 创建 scorer.py**

```python
"""得分计算引擎"""
from __future__ import annotations


def normalize(values: list[float]) -> list[float]:
    """Min-Max 归一化到 [0, 1]"""
    if not values:
        return []
    mn, mx = min(values), max(values)
    if mn == mx:
        return [1.0] * len(values)
    return [(v - mn) / (mx - mn) for v in values]


def calc_score(ratio_norm: float, premium_norm: float,
               w_ratio: float = 0.7, w_premium: float = 0.3) -> float:
    """
    Score = W1 * ratio_norm - W2 * premium_norm
    ratio_norm 越大越好, premium_norm 越小越好(归一化时已取反)
    """
    return w_ratio * ratio_norm - w_premium * premium_norm


def apply_futures_kill(score: float, etf_change_pct: float,
                       futures_change_pct: float | None,
                       threshold: float, enabled: bool) -> float:
    """期指风控：期指大跌但 ETF 上涨 → 杀分"""
    if not enabled or futures_change_pct is None:
        return score
    if futures_change_pct < threshold and etf_change_pct > 0:
        return 0.0
    return score


def rank_etfs(etf_data: list[dict], w_ratio: float = 0.7,
              w_premium: float = 0.3, futures_kill_enabled: bool = False,
              futures_kill_threshold: float = -1.5) -> list[dict]:
    """
    对 ETF 列表计算得分并排序。

    每个 etf_data dict 需包含:
      code, buy1_vol, sell1_vol, price, iopv, change_pct, futures_change
    返回按 score 降序排列的列表, 每项增加 score, ratio, premium 字段。
    """
    if not etf_data:
        return []

    # 计算 ratio 和 premium
    ratios = []
    premiums = []
    for e in etf_data:
        buy = e.get("buy1_vol", 0) or 0
        sell = e.get("sell1_vol", 0) or 0
        if buy == 0:
            ratios.append(0.0)
        else:
            ratios.append(buy / max(sell, 1))

        iopv = e.get("iopv") or 0
        price = e.get("price") or 0
        if iopv > 0 and price > 0:
            premiums.append((price - iopv) / iopv * 100)
        else:
            premiums.append(0.0)

    # 归一化
    ratio_norms = normalize(ratios)
    # premium 取反归一化: 折价(负premium)得分高, 溢价(正premium)得分低
    inv_premiums = [-p for p in premiums]
    premium_norms = normalize(inv_premiums)

    # 计算得分
    results = []
    for i, e in enumerate(etf_data):
        score = calc_score(ratio_norms[i], premium_norms[i], w_ratio, w_premium)

        # 买一量为0 -> 得分置0
        if (e.get("buy1_vol", 0) or 0) == 0:
            score = 0.0

        # 期指风控
        score = apply_futures_kill(
            score,
            e.get("change_pct", 0) or 0,
            e.get("futures_change"),
            futures_kill_threshold,
            futures_kill_enabled,
        )

        result = {
            **e,
            "score": round(score, 4),
            "ratio": round(ratios[i], 2),
            "premium": round(premiums[i], 2),
        }
        results.append(result)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results
```

- [ ] **Step 4: 运行测试确认通过**

```bash
python -m pytest tests/test_scorer.py -v
```

预期：全部 PASS

- [ ] **Step 5: 提交**

```bash
git add scorer.py tests/test_scorer.py
git commit -m "feat: 得分计算模块 + TDD 单元测试"
```

---

### Task 4: 数据抓取模块

**Files:**
- Create: `fetcher.py`

核心逻辑：
1. `fund_etf_spot_em()` 批量获取所有 ETF 行情（含 IOPV）→ 1次请求
2. `stock_bid_ask_em(symbol)` 获取每只 ETF 的五档买卖量 → M次并发请求
3. `futures_global_spot_em()` 获取全球期货行情 → 1次请求

对于步骤2，akshare 是同步库，使用 `asyncio.to_thread()` 包装并发。代理轮询通过在调用前设置环境变量 `http_proxy`/`https_proxy` 实现（因 akshare 内部使用 requests）。

- [ ] **Step 1: 创建 fetcher.py**

```python
"""数据抓取模块 - 基于 akshare"""
from __future__ import annotations
import asyncio
import logging
import os
import time
from typing import Any

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

# 代理轮询状态
_proxy_list: list[str] = []
_proxy_index = 0
_last_proxy_use: dict[str, float] = {}  # proxy_url -> last_use_timestamp


def set_proxies(proxies: list[str]):
    """设置可用代理列表"""
    global _proxy_list, _proxy_index, _last_proxy_use
    _proxy_list = proxies
    _proxy_index = 0
    _last_proxy_use = {p: 0.0 for p in proxies}


def _get_next_proxy() -> str | None:
    """轮询获取下一个代理, 确保每个代理至少间隔 MIN_INTERVAL_SEC"""
    from config import MIN_INTERVAL_SEC

    if not _proxy_list:
        return None

    global _proxy_index
    now = time.monotonic()
    start_idx = _proxy_index

    for _ in range(len(_proxy_list)):
        proxy = _proxy_list[_proxy_index]
        _proxy_index = (_proxy_index + 1) % len(_proxy_list)
        elapsed = now - _last_proxy_use.get(proxy, 0.0)
        if elapsed >= MIN_INTERVAL_SEC:
            _last_proxy_use[proxy] = now
            return proxy

    # 所有代理都在冷却期, 返回冷却最久的那个
    oldest = min(_last_proxy_use, key=_last_proxy_use.get)
    _last_proxy_use[oldest] = now
    return oldest


def _apply_proxy(proxy: str | None):
    """设置 akshare 内部 requests 使用的代理环境变量"""
    if proxy:
        os.environ["http_proxy"] = proxy
        os.environ["https_proxy"] = proxy
    else:
        os.environ.pop("http_proxy", None)
        os.environ.pop("https_proxy", None)


# ---- ETF 批量行情 (含 IOPV) ----

def _fetch_etf_batch_sync() -> pd.DataFrame:
    """同步调用 fund_etf_spot_em"""
    return ak.fund_etf_spot_em()


async def fetch_etf_batch() -> pd.DataFrame:
    """异步批量获取 ETF 行情, 返回含 IOPV 的 DataFrame"""
    try:
        df = await asyncio.to_thread(_fetch_etf_batch_sync)
        logger.info(f"ETF批量行情: 获取 {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"ETF批量行情获取失败: {e}")
        return pd.DataFrame()


# ---- ETF 五档买卖量 (逐个) ----

def _fetch_bid_ask_sync(symbol: str, proxy: str | None) -> dict | None:
    """同步获取单只 ETF 五档数据"""
    try:
        _apply_proxy(proxy)
        df = ak.stock_bid_ask_em(symbol=symbol)
        # df 有 item/value 两列, 转为 dict
        data = dict(zip(df["item"], df["value"]))
        return {
            "buy1_vol": data.get("buy_1_vol", 0),
            "sell1_vol": data.get("sell_1_vol", 0),
            "buy1_price": data.get("buy_1", 0),
            "sell1_price": data.get("sell_1", 0),
            "latest": data.get("最新", 0),
            "change_pct": data.get("涨幅", 0),
        }
    except Exception as e:
        logger.warning(f"盘口数据获取失败 {symbol}: {e}")
        return None
    finally:
        _apply_proxy(None)


async def fetch_bid_ask(symbol: str) -> dict | None:
    """异步获取单只 ETF 五档, 自动代理轮询"""
    proxy = _get_next_proxy()
    return await asyncio.to_thread(_fetch_bid_ask_sync, symbol, proxy)


async def fetch_all_bid_asks(codes: list[str]) -> dict[str, dict]:
    """并发获取多只 ETF 的五档数据"""
    tasks = [fetch_bid_ask(code) for code in codes]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output = {}
    for code, result in zip(codes, results):
        if isinstance(result, dict):
            output[code] = result
        elif isinstance(result, Exception):
            logger.warning(f"盘口并发获取异常 {code}: {result}")
    return output


# ---- 全球期货行情 ----

def _fetch_futures_sync() -> pd.DataFrame:
    """同步获取全球期货行情"""
    return ak.futures_global_spot_em()


async def fetch_futures() -> pd.DataFrame:
    """异步获取全球期货行情"""
    try:
        df = await asyncio.to_thread(_fetch_futures_sync)
        logger.info(f"期货行情: 获取 {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"期货行情获取失败: {e}")
        return pd.DataFrame()


# ---- 数据聚合 ----

async def fetch_round(etf_codes: list[str], proxy_urls: list[str]) -> dict[str, Any]:
    """
    执行一轮完整的数据抓取, 返回聚合结果:
    {
        "etf_data": [ {code, name, price, iopv, premium_pct, change_pct,
                       buy1_vol, sell1_vol, ratio, futures_index, ...} ],
        "futures_data": { "NQ": change_pct, "DAX": change_pct, ... },
        "errors": [ ... ],
        "fetch_time": "HH:MM:SS"
    }
    """
    from config import FUTURES_NAME_MAP

    set_proxies(proxy_urls)
    errors: list[str] = []

    # 1. ETF 批量行情
    etf_df = await fetch_etf_batch()

    # 2. 五档买卖量 (并发)
    bid_ask_map = await fetch_all_bid_asks(etf_codes)

    # 3. 期货行情
    futures_df = await fetch_futures()

    # 4. 解析期指涨跌幅
    futures_changes: dict[str, float | None] = {}
    for fi_key, name_kw in FUTURES_NAME_MAP.items():
        change = _extract_futures_change(futures_df, name_kw)
        futures_changes[fi_key] = change

    # 5. 合并 ETF 数据
    etf_list = []
    if not etf_df.empty:
        filtered = etf_df[etf_df["代码"].isin(etf_codes)]
        for _, row in filtered.iterrows():
            code = str(row.get("代码", ""))
            ba = bid_ask_map.get(code, {})
            price = _safe_float(row.get("最新价"))
            iopv = _safe_float(row.get("IOPV实时估值"))
            premium_pct = _safe_float(row.get("基金折价率"))  # 注意: 这是折价率, 正=折价
            change_pct = _safe_float(row.get("涨跌幅"))

            etf_list.append({
                "code": code,
                "name": str(row.get("名称", "")),
                "price": price,
                "iopv": iopv,
                "premium_pct": -premium_pct if premium_pct else 0,  # 转为溢价率(正=溢价)
                "change_pct": change_pct,
                "buy1_vol": int(ba.get("buy1_vol", 0) or 0),
                "sell1_vol": int(ba.get("sell1_vol", 0) or 0),
                "buy1_price": _safe_float(ba.get("buy1_price")),
                "sell1_price": _safe_float(ba.get("sell1_price")),
                "iopv_source": "IOPV" if iopv else "N/A",
            })

    # 6. 对没有 batch 数据但需要抓取的 ETF, 尝试从 bid_ask 补充
    codes_in_batch = {str(r) for r in etf_df["代码"].values} if not etf_df.empty else set()
    for code in etf_codes:
        if code not in codes_in_batch and code in bid_ask_map:
            ba = bid_ask_map[code]
            etf_list.append({
                "code": code,
                "name": "",
                "price": _safe_float(ba.get("latest")),
                "iopv": 0,
                "premium_pct": 0,
                "change_pct": _safe_float(ba.get("change_pct")),
                "buy1_vol": int(ba.get("buy1_vol", 0) or 0),
                "sell1_vol": int(ba.get("sell1_vol", 0) or 0),
                "buy1_price": _safe_float(ba.get("buy1_price")),
                "sell1_price": _safe_float(ba.get("sell1_price")),
                "iopv_source": "N/A",
            })

    now_str = asyncio.get_event_loop().time()
    from datetime import datetime
    fetch_time = datetime.now().strftime("%H:%M:%S")

    return {
        "etf_data": etf_list,
        "futures_data": futures_changes,
        "errors": errors,
        "fetch_time": fetch_time,
    }


def _extract_futures_change(df: pd.DataFrame, name_keyword: str) -> float | None:
    """从期货行情 DataFrame 中按名称关键字提取涨跌幅"""
    if df.empty:
        return None
    try:
        matched = df[df["名称"].str.contains(name_keyword, na=False)]
        if matched.empty:
            return None
        return _safe_float(matched.iloc[0].get("涨跌幅"))
    except Exception:
        return None


def _safe_float(val) -> float:
    if val is None or (isinstance(val, float) and __import__("math").isnan(val)):
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
```

- [ ] **Step 2: 验证 akshare 接口可用**

```bash
python -c "
import akshare as ak
df = ak.fund_etf_spot_em()
print('fund_etf_spot_em columns:', list(df.columns))
print('rows:', len(df))
print(df[df['代码']=='513100'][['代码','名称','最新价','IOPV实时估值','基金折价率','涨跌幅']].to_string())
"
```

预期：输出 513100 纳指ETF 的实时数据行。

- [ ] **Step 3: 验证期货行情接口**

```bash
python -c "
import akshare as ak
df = ak.futures_global_spot_em()
print('futures_global_spot_em columns:', list(df.columns))
print(df[df['名称'].str.contains('纳斯达克', na=False)][['名称','最新价','涨跌幅']].to_string())
"
```

预期：输出纳斯达克期指数据行。

- [ ] **Step 4: 提交**

```bash
git add fetcher.py
git commit -m "feat: 数据抓取模块 (akshare + 代理轮询 + 并发)"
```

---

### Task 5: FastAPI 服务端

**Files:**
- Create: `server.py`

- [ ] **Step 1: 创建 server.py**

```python
"""FastAPI 主服务: SSE 推送 + REST 管理 API"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
import fetcher
from config import (
    DEFAULT_W_RATIO, DEFAULT_W_PREMIUM,
    DEFAULT_FUTURES_KILL_THRESHOLD, MIN_INTERVAL_SEC, SERVER_PORT,
)
from scorer import rank_etfs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# ---- 全局状态 ----
clients: list[asyncio.Queue] = []
monitor_task: asyncio.Task | None = None
monitor_running = False
futures_kill_enabled = False


def _config_float(rows: dict, key: str, default: float) -> float:
    return float(rows.get(key, default))


async def _broadcast(event_type: str, payload: dict):
    """向所有 SSE 客户端广播消息"""
    msg = json.dumps(payload, ensure_ascii=False, default=str)
    dead = []
    for q in clients:
        try:
            q.put_nowait({"type": event_type, "payload": msg})
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        clients.remove(q)


async def _monitor_loop():
    """后台监控主循环"""
    global monitor_running
    monitor_running = True
    logger.info("监控循环启动")

    try:
        while monitor_running:
            t0 = time.monotonic()

            # 1. 加载配置和 ETF 列表
            configs = await db.get_configs()
            w_ratio = _config_float(configs, "w_ratio", DEFAULT_W_RATIO)
            w_premium = _config_float(configs, "w_premium", DEFAULT_W_PREMIUM)
            threshold = _config_float(configs, "futures_kill_threshold", DEFAULT_FUTURES_KILL_THRESHOLD)
            safe_freq = _config_float(configs, "safe_req_per_min", 20)

            etfs = await db.get_etf_list(enabled_only=True)
            proxies_rows = await db.get_alive_proxies()
            proxy_urls = [r["url"] for r in proxies_rows]

            etf_codes = [e["code"] for e in etfs]
            # 建立 futures_index 映射
            fi_map = {e["code"]: e.get("futures_index", "") for e in etfs}

            if not etf_codes:
                await _broadcast("status", {
                    "running": True, "proxy_count": len(proxy_urls),
                    "interval_sec": MIN_INTERVAL_SEC, "timestamp": datetime.now().isoformat(),
                    "message": "无启用的ETF标的",
                })
                await asyncio.sleep(MIN_INTERVAL_SEC)
                continue

            # 2. 执行一轮抓取
            try:
                raw = await fetcher.fetch_round(etf_codes, proxy_urls)
            except Exception as e:
                logger.error(f"抓取异常: {e}")
                await _broadcast("error", {"message": str(e)})
                await asyncio.sleep(MIN_INTERVAL_SEC)
                continue

            # 3. 补充 futures_change 到每只 ETF
            for item in raw["etf_data"]:
                fi = fi_map.get(item["code"], "")
                item["futures_change"] = raw["futures_data"].get(fi)

            # 4. 计算得分并排序
            ranked = rank_etfs(
                raw["etf_data"],
                w_ratio=w_ratio, w_premium=w_premium,
                futures_kill_enabled=futures_kill_enabled,
                futures_kill_threshold=threshold,
            )

            # 5. 推送快照
            await _broadcast("snapshot", {
                "items": ranked,
                "futures": raw["futures_data"],
                "fetch_time": raw["fetch_time"],
            })

            # 6. 计算间隔
            elapsed = time.monotonic() - t0
            # 间隔 = max(MIN_INTERVAL_SEC - elapsed, 0.5)
            interval = max(MIN_INTERVAL_SEC - elapsed, 0.5)

            await _broadcast("status", {
                "running": True,
                "proxy_count": len(proxy_urls),
                "interval_sec": round(interval, 1),
                "etf_count": len(etf_codes),
                "timestamp": datetime.now().isoformat(),
            })

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        pass
    finally:
        monitor_running = False
        logger.info("监控循环停止")
        await _broadcast("status", {
            "running": False, "proxy_count": 0,
            "interval_sec": 0, "timestamp": datetime.now().isoformat(),
        })


# ---- SSE ----

async def _sse_generator(q: asyncio.Queue):
    try:
        while True:
            msg = await q.get()
            yield f"event: {msg['type']}\ndata: {msg['payload']}\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        if q in clients:
            clients.remove(q)


# ---- App ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_tables()
    yield
    await db.close_pool()


app = FastAPI(title="QDII ETF Monitor", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static", html=True), name="static")


# ---- SSE Endpoint ----

@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    clients.append(q)
    return StreamingResponse(_sse_generator(q), media_type="text/event-stream")


# ---- Monitor Control ----

@app.post("/api/monitor/start")
async def start_monitor():
    global monitor_task, monitor_running
    if monitor_running:
        return {"status": "already_running"}
    monitor_running = True
    monitor_task = asyncio.create_task(_monitor_loop())
    return {"status": "started"}


@app.post("/api/monitor/stop")
async def stop_monitor():
    global monitor_task, monitor_running
    monitor_running = False
    if monitor_task and not monitor_task.done():
        monitor_task.cancel()
    monitor_task = None
    return {"status": "stopped"}


@app.get("/api/monitor/status")
async def monitor_status():
    return {"running": monitor_running}


@app.post("/api/monitor/futures_kill")
async def toggle_futures_kill(enabled: bool = True):
    global futures_kill_enabled
    futures_kill_enabled = enabled
    return {"futures_kill_enabled": futures_kill_enabled}


# ---- ETF CRUD ----

@app.get("/api/etf")
async def list_etf():
    return await db.get_etf_list(enabled_only=False)


class EtfInput(BaseModel):
    code: str
    name: str = ""
    futures_index: str = ""


@app.post("/api/etf")
async def create_etf(data: EtfInput):
    await db.add_etf(data.code, data.name, data.futures_index)
    return {"ok": True}


@app.delete("/api/etf/{code}")
async def remove_etf(code: str):
    await db.delete_etf(code)
    return {"ok": True}


@app.post("/api/etf/{code}/toggle")
async def toggle_etf_api(code: str, enabled: bool = True):
    await db.toggle_etf(code, enabled)
    return {"ok": True}


# ---- Proxy CRUD ----

@app.get("/api/proxy")
async def list_proxy():
    return await db.get_proxy_list()


class ProxyInput(BaseModel):
    url: str


@app.post("/api/proxy")
async def create_proxy(data: ProxyInput):
    await db.add_proxy(data.url)
    return {"ok": True}


@app.delete("/api/proxy/{proxy_id}")
async def remove_proxy(proxy_id: int):
    await db.delete_proxy(proxy_id)
    return {"ok": True}


@app.post("/api/proxy/{proxy_id}/check")
async def check_proxy(proxy_id: int):
    proxies = await db.get_proxy_list()
    proxy = next((p for p in proxies if p["id"] == proxy_id), None)
    if not proxy:
        return {"error": "not found"}
    import httpx
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(proxy=proxy["url"], timeout=5) as client:
            await client.get("http://www.baidu.com")
        latency = int((time.monotonic() - t0) * 1000)
        await db.update_proxy_status(proxy_id, True, latency)
        return {"alive": True, "latency_ms": latency}
    except Exception as e:
        await db.update_proxy_status(proxy_id, False, 0)
        return {"alive": False, "error": str(e)}


# ---- Config ----

@app.get("/api/config")
async def list_config():
    return await db.get_configs()


class ConfigInput(BaseModel):
    value: str


@app.put("/api/config/{key}")
async def update_config(key: str, data: ConfigInput):
    await db.set_config(key, data.value)
    return {"ok": True}


# ---- Root redirect ----

@app.get("/")
async def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/static/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
```

- [ ] **Step 2: 验证服务启动**

```bash
cd /root/workspace/project/qdii && python server.py &
sleep 3
curl -s http://localhost:8000/api/monitor/status
curl -s http://localhost:8000/api/etf
kill %1
```

预期：返回 JSON `{"running": false}` 和 ETF 列表。

- [ ] **Step 3: 提交**

```bash
git add server.py
git commit -m "feat: FastAPI 服务端 (SSE + REST API + 监控循环)"
```

---

### Task 6: 前端看板

**Files:**
- Create: `static/index.html`

- [ ] **Step 1: 创建 static 目录**

```bash
mkdir -p /root/workspace/project/qdii/static
```

- [ ] **Step 2: 创建 static/index.html**

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QDII ETF 竞价监控</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, "Microsoft YaHei", sans-serif; background: #1a1a2e; color: #e0e0e0; }
.header { display: flex; align-items: center; gap: 12px; padding: 12px 20px; background: #16213e; border-bottom: 1px solid #0f3460; }
.header h1 { font-size: 18px; color: #e94560; margin-right: auto; }
.btn { padding: 6px 16px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; font-weight: 600; }
.btn-start { background: #00b894; color: #fff; }
.btn-start:hover { background: #00a383; }
.btn-stop { background: #e17055; color: #fff; }
.btn-stop:hover { background: #d35400; }
.btn-sm { padding: 3px 10px; font-size: 12px; }
.btn-danger { background: #636e72; color: #fff; }
.btn-danger:hover { background: #d63031; }
.toggle-label { font-size: 13px; display: flex; align-items: center; gap: 6px; }
.toggle-label input { width: 16px; height: 16px; }
.status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
.status-dot.green { background: #00b894; box-shadow: 0 0 6px #00b894; }
.status-dot.red { background: #e17055; }
.status-info { font-size: 12px; color: #b2bec3; }
.tabs { display: flex; background: #16213e; border-bottom: 2px solid #0f3460; }
.tab { padding: 10px 24px; cursor: pointer; font-size: 14px; color: #b2bec3; border-bottom: 2px solid transparent; margin-bottom: -2px; }
.tab.active { color: #e94560; border-bottom-color: #e94560; }
.tab-content { display: none; padding: 16px 20px; }
.tab-content.active { display: block; }

/* 监控表格 */
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 8px 10px; background: #16213e; color: #b2bec3; position: sticky; top: 0; z-index: 1; }
td { padding: 6px 10px; border-bottom: 1px solid #2d2d44; }
tr:hover td { background: #1e2a4a; }
/* 高亮行 */
.row-top { background: #006644 !important; color: #fff; font-weight: 700; }
.row-top td { font-size: 15px; border-bottom: 2px solid #00b894; }
.row-good { background: #1a3a2a !important; }
.row-killed { background: #2d2d2d !important; color: #636e72; text-decoration: line-through; }
.row-danger { background: #3a1a1a !important; }
.score-cell { font-weight: 700; min-width: 60px; }
.pct-up { color: #00b894; }
.pct-down { color: #e17055; }
.iopv-est { color: #fdcb6e; font-size: 11px; }

/* 代理/ETF管理 */
.manage-card { background: #16213e; border-radius: 8px; padding: 16px; margin-bottom: 12px; }
.manage-card h3 { margin-bottom: 10px; color: #e94560; font-size: 15px; }
.add-row { display: flex; gap: 8px; margin-bottom: 12px; }
.add-row input { flex: 1; padding: 6px 10px; border: 1px solid #0f3460; border-radius: 4px; background: #1a1a2e; color: #e0e0e0; font-size: 13px; }
.proxy-alive { color: #00b894; }
.proxy-dead { color: #e17055; }
.footer-info { text-align: center; padding: 8px; font-size: 12px; color: #636e72; }

/* 模态框 */
.modal-overlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6); z-index: 100; }
.modal-overlay.show { display: flex; align-items: center; justify-content: center; }
.modal { background: #16213e; border-radius: 8px; padding: 20px; width: 600px; max-height: 80vh; overflow-y: auto; }
.modal h3 { color: #e94560; margin-bottom: 12px; }
</style>
</head>
<body>

<div class="header">
    <h1>QDII ETF 竞价监控</h1>
    <label class="toggle-label">
        <input type="checkbox" id="futuresKillToggle" onchange="toggleFuturesKill(this.checked)">
        竞价风控
    </label>
    <button class="btn btn-start" id="btnStart" onclick="startMonitor()">开始监控</button>
    <button class="btn btn-stop" id="btnStop" onclick="stopMonitor()" style="display:none">停止监控</button>
    <span class="status-dot" id="statusDot"></span>
    <span class="status-info" id="statusText">未连接</span>
    <button class="btn btn-sm" onclick="showEtfModal()">管理标的</button>
</div>

<div class="tabs">
    <div class="tab active" onclick="switchTab('monitor')">监控看板</div>
    <div class="tab" onclick="switchTab('proxy')">代理管理</div>
</div>

<!-- Tab 1: 监控看板 -->
<div class="tab-content active" id="tab-monitor">
    <table>
        <thead>
            <tr>
                <th>代码</th><th>名称</th><th>得分</th><th>当前价</th><th>涨幅%</th>
                <th>溢价%</th><th>买一量(手)</th><th>卖一量(手)</th><th>买/卖比</th><th>期指走势</th>
            </tr>
        </thead>
        <tbody id="monitorBody">
            <tr><td colspan="10" style="text-align:center;padding:40px;color:#636e72">点击"开始监控"启动数据抓取</td></tr>
        </tbody>
    </table>
</div>

<!-- Tab 2: 代理管理 -->
<div class="tab-content" id="tab-proxy">
    <div class="manage-card">
        <h3>代理池</h3>
        <div class="add-row">
            <input id="proxyUrl" placeholder="socks5://ip:port 或 http://ip:port">
            <button class="btn btn-start btn-sm" onclick="addProxy()">添加</button>
        </div>
        <table>
            <thead><tr><th>ID</th><th>地址</th><th>延迟</th><th>状态</th><th>操作</th></tr></thead>
            <tbody id="proxyBody"></tbody>
        </table>
        <div class="footer-info" id="proxySummary"></div>
    </div>
</div>

<!-- ETF 管理模态框 -->
<div class="modal-overlay" id="etfModal">
    <div class="modal">
        <h3>ETF 标的池管理 <button class="btn btn-sm" style="float:right" onclick="closeEtfModal()">关闭</button></h3>
        <div class="add-row">
            <input id="etfCode" placeholder="ETF代码 如 513100">
            <input id="etfName" placeholder="名称">
            <input id="etfFutures" placeholder="期指标识 NQ/DAX/ES/YM/NK">
            <button class="btn btn-start btn-sm" onclick="addEtf()">添加</button>
        </div>
        <table>
            <thead><tr><th>代码</th><th>名称</th><th>期指</th><th>状态</th><th>操作</th></tr></thead>
            <tbody id="etfBody"></tbody>
        </table>
    </div>
</div>

<script>
let es = null;
let monitorRunning = false;

// ---- SSE 连接 ----
function connectSSE() {
    if (es) es.close();
    es = new EventSource('/api/stream');
    es.addEventListener('snapshot', e => renderTable(JSON.parse(e.data)));
    es.addEventListener('status', e => {
        const s = JSON.parse(e.data);
        updateStatus(s);
    });
    es.addEventListener('error', e => {
        if (e.data) console.warn('Error:', e.data);
    });
    es.onerror = () => {
        document.getElementById('statusDot').className = 'status-dot red';
        document.getElementById('statusText').textContent = '连接断开,重连中...';
    };
}

// ---- 监控控制 ----
async function startMonitor() {
    await fetch('/api/monitor/start', {method:'POST'});
    document.getElementById('btnStart').style.display = 'none';
    document.getElementById('btnStop').style.display = '';
    monitorRunning = true;
    connectSSE();
}
async function stopMonitor() {
    await fetch('/api/monitor/stop', {method:'POST'});
    document.getElementById('btnStart').style.display = '';
    document.getElementById('btnStop').style.display = 'none';
    monitorRunning = false;
    if (es) { es.close(); es = null; }
    document.getElementById('statusDot').className = 'status-dot red';
    document.getElementById('statusText').textContent = '已停止';
}
async function toggleFuturesKill(enabled) {
    await fetch(`/api/monitor/futures_kill?enabled=${enabled}`, {method:'POST'});
}

function updateStatus(s) {
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (s.running) {
        dot.className = 'status-dot green';
        txt.textContent = `${s.proxy_count||0}代理 ${s.interval_sec||'-'}s/轮 ${s.etf_count||0}标的`;
    } else {
        dot.className = 'status-dot red';
        txt.textContent = '已停止';
    }
}

// ---- 表格渲染 ----
function renderTable(data) {
    const items = data.items || [];
    const futures = data.futures || {};
    if (!items.length) return;

    let html = '';
    items.forEach((item, idx) => {
        const score = item.score || 0;
        let rowClass = '';
        if (score === 0 && item.buy1_vol === 0) rowClass = 'row-danger';
        else if (score === 0 && item.premium > 0) rowClass = 'row-killed';
        else if (idx < 3 && score > 70) rowClass = 'row-top';
        else if (score > 50) rowClass = 'row-good';

        const changeCls = (item.change_pct||0) >= 0 ? 'pct-up' : 'pct-down';
        const premiumVal = item.premium_pct != null ? item.premium_pct : item.premium;
        const premiumStr = premiumVal != null ? premiumVal.toFixed(2) + '%' : '-';
        const iopvTag = item.iopv_source === 'EST' ? ' <span class="iopv-est">(估)</span>' : '';
        const fiChange = item.futures_change;
        const fiStr = fiChange != null ? (fiChange >= 0 ? '+' : '') + fiChange.toFixed(2) + '%' : '-';

        html += `<tr class="${rowClass}">
            <td>${item.code}</td>
            <td>${item.name}</td>
            <td class="score-cell">${score.toFixed(1)}</td>
            <td>${(item.price||0).toFixed(3)}</td>
            <td class="${changeCls}">${(item.change_pct||0).toFixed(2)}%</td>
            <td>${premiumStr}${iopvTag}</td>
            <td>${(item.buy1_vol/100).toFixed(0)}</td>
            <td>${(item.sell1_vol/100).toFixed(0)}</td>
            <td>${(item.ratio||0).toFixed(2)}</td>
            <td class="${fiChange>=0?'pct-up':'pct-down'}">${fiStr}</td>
        </tr>`;
    });
    document.getElementById('monitorBody').innerHTML = html;
    document.title = `QDII监控 ${data.fetch_time||''}`;
}

// ---- Tab 切换 ----
function switchTab(name) {
    document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', t.textContent.includes(name==='monitor'?'看板':'代理')));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById('tab-'+name).classList.add('active');
    if (name === 'proxy') loadProxies();
}

// ---- 代理管理 ----
async function loadProxies() {
    const resp = await fetch('/api/proxy');
    const list = await resp.json();
    let html = '';
    let aliveCount = 0;
    list.forEach(p => {
        const alive = p.alive ? '<span class="proxy-alive">存活</span>' : '<span class="proxy-dead">失效</span>';
        const latency = p.alive ? p.latency_ms + 'ms' : '--';
        if (p.alive) aliveCount++;
        html += `<tr>
            <td>${p.id}</td><td>${p.url}</td><td>${latency}</td><td>${alive}</td>
            <td><button class="btn btn-sm" onclick="checkProxy(${p.id})">检测</button>
                <button class="btn btn-sm btn-danger" onclick="delProxy(${p.id})">删除</button></td>
        </tr>`;
    });
    document.getElementById('proxyBody').innerHTML = html || '<tr><td colspan="5" style="text-align:center;color:#636e72">无代理, 将使用本机直连</td></tr>';
    document.getElementById('proxySummary').textContent = `当前可用: ${aliveCount}   理论刷新间隔: ${aliveCount > 0 ? (3/aliveCount).toFixed(1) : '3'}s`;
}
async function addProxy() {
    const url = document.getElementById('proxyUrl').value.trim();
    if (!url) return;
    await fetch('/api/proxy', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url})});
    document.getElementById('proxyUrl').value = '';
    loadProxies();
}
async function delProxy(id) {
    await fetch('/api/proxy/'+id, {method:'DELETE'});
    loadProxies();
}
async function checkProxy(id) {
    await fetch('/api/proxy/'+id+'/check', {method:'POST'});
    loadProxies();
}

// ---- ETF 管理 ----
function showEtfModal() { document.getElementById('etfModal').classList.add('show'); loadEtfs(); }
function closeEtfModal() { document.getElementById('etfModal').classList.remove('show'); }
async function loadEtfs() {
    const resp = await fetch('/api/etf');
    const list = await resp.json();
    let html = '';
    list.forEach(e => {
        const status = e.enabled ? '<span class="proxy-alive">启用</span>' : '<span class="proxy-dead">禁用</span>';
        html += `<tr>
            <td>${e.code}</td><td>${e.name}</td><td>${e.futures_index||'-'}</td><td>${status}</td>
            <td><button class="btn btn-sm" onclick="toggleEtf('${e.code}',${!e.enabled})">${e.enabled?'禁用':'启用'}</button>
                <button class="btn btn-sm btn-danger" onclick="delEtf('${e.code}')">删除</button></td>
        </tr>`;
    });
    document.getElementById('etfBody').innerHTML = html;
}
async function addEtf() {
    const code = document.getElementById('etfCode').value.trim();
    const name = document.getElementById('etfName').value.trim();
    const fi = document.getElementById('etfFutures').value.trim();
    if (!code) return;
    await fetch('/api/etf', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({code,name,futures_index:fi})});
    document.getElementById('etfCode').value = '';
    document.getElementById('etfName').value = '';
    document.getElementById('etfFutures').value = '';
    loadEtfs();
}
async function delEtf(code) {
    await fetch('/api/etf/'+code, {method:'DELETE'});
    loadEtfs();
}
async function toggleEtf(code, enabled) {
    await fetch(`/api/etf/${code}/toggle?enabled=${enabled}`, {method:'POST'});
    loadEtfs();
}

// ---- 初始化 ----
loadProxies();
</script>
</body>
</html>
```

- [ ] **Step 3: 提交**

```bash
git add static/index.html
git commit -m "feat: 前端看板 (监控/代理/标的管理三Tab)"
```

---

### Task 7: 冒烟测试

- [ ] **Step 1: 启动服务**

```bash
cd /root/workspace/project/qdii && python server.py &
sleep 3
```

- [ ] **Step 2: 验证 API 端点**

```bash
# ETF 列表
curl -s http://localhost:8000/api/etf | python -m json.tool

# 代理列表
curl -s http://localhost:8000/api/proxy | python -m json.tool

# 配置
curl -s http://localhost:8000/api/config | python -m json.tool

# 根路径重定向
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/
# 预期: 307
```

- [ ] **Step 3: 验证 SSE 数据流**

```bash
# 启动监控
curl -s -X POST http://localhost:8000/api/monitor/start

# 监听 SSE (等待10秒看是否有数据)
timeout 15 curl -sN http://localhost:8000/api/stream 2>&1 | head -20

# 停止监控
curl -s -X POST http://localhost:8000/api/monitor/stop
```

预期：看到 `event: snapshot` 和 `event: status` 事件。

- [ ] **Step 4: 验证前端页面**

用浏览器打开 `http://localhost:8000`，检查：
- 页面正常加载
- "管理标的"弹窗显示种子 ETF 数据
- 代理管理 Tab 显示空列表
- 点击"开始监控"后状态灯变绿，表格出现数据

- [ ] **Step 5: 清理后台进程**

```bash
kill %1 2>/dev/null || true
```

- [ ] **Step 6: 最终提交**

```bash
git add -A
git commit -m "feat: 完成初始版本 - ETF竞价监控看板"
```

---

## Spec 自查

**1. Spec 覆盖率检查：**
- [x] 多标的配置 → database.py ETF CRUD + UI 管理
- [x] 动态代理池 → database.py Proxy CRUD + fetcher.py 代理轮询
- [x] 实时并发计算 → fetcher.py asyncio.to_thread 并发 + scorer.py
- [x] 实时得分排序 → scorer.rank_etfs + SSE snapshot 推送
- [x] Web 看板 → static/index.html 三 Tab
- [x] IOPV 混合方案 → fund_etf_spot_em IOPV 列 + UI (估) 标注
- [x] 手动启停 → monitor start/stop API
- [x] 竞价风控开关 → futures_kill toggle API + UI checkbox
- [x] 视觉突出 → CSS row-top/row-good/row-killed/row-danger
- [x] 期指风控杀分 → scorer.apply_futures_kill
- [x] MCP 配置 → .mcp.json
- [x] 数据库配置 → config.py + database.py

**2. Placeholder 扫描：** 无 TBD/TODO。

**3. 类型一致性检查：**
- `fetch_round` 返回 dict 含 `etf_data` (list[dict])、`futures_data` (dict)、`errors`、`fetch_time`
- `rank_etfs` 接收 list[dict] 返回 list[dict]，字段一致
- `_monitor_loop` 正确传递 `futures_change` 到每只 ETF dict
- API 请求/响应模型 (Pydantic) 与 fetcher/database 接口匹配
