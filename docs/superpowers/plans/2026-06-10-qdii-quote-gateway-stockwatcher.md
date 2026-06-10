# QDII 数据网关 + StockWatcher 子项目改造 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 qdii 中新增候选池数据抓取与缓存机制，暴露统一的 `/api/quote` JSON 接口供外部客户端（如 StockWatcher）获取股票、ETF、期指的实时行情数据；同时将 StockWatcher 作为子项目引入并改造为对接 qdii API。

**Architecture:** qdii 作为数据网关，新增 `candidate_pool` 数据库表存储候选代码列表（股票/ETF/基金），`fetcher.py` 新增通用行情抓取方法（复用腾讯 API + 代理池），`server.py` 新增缓存层和 `/api/quote` 端点。StockWatcher 从直连腾讯 API 改为调用 qdii 的 `/api/quote`，扩展数据模型和 UI 支持多种数据类型。

**Tech Stack:** Python 3 + FastAPI + aiomysql + httpx (qdii)；C# .NET Framework 4.5.2 + WinForms (StockWatcher)

---

## 文件变更地图

| 操作 | 文件路径 | 职责 |
|------|---------|------|
| 修改 | `qdii/database.py` | 新增 candidate_pool 表 CRUD |
| 修改 | `qdii/fetcher.py` | 新增 `fetch_candidates()` 通用行情抓取 |
| 修改 | `qdii/server.py` | 新增缓存层 + `/api/quote` 端点 + 候选池 CRUD 端点 |
| 修改 | `qdii/config.py` | 新增候选池刷新间隔配置 |
| 修改 | `qdii/init_db.py` | 新增候选池种子数据 |
| 新建 | `StockWatcher/` 整个目录 | 从 GitHub 复制并改造 |

---

## 第一阶段：qdii 侧改造

### Task 1: database.py — 新增 candidate_pool 表与 CRUD

**Files:**
- Modify: `database.py:115-192` (init_tables 函数内追加建表)
- Modify: `database.py:339-509` (文件末尾追加 CRUD 方法)

- [ ] **Step 1: 在 init_tables 中追加 candidate_pool 建表语句**

在 `init_tables()` 函数中，`sys_config` 建表之后、种子配置之前，插入：

```python
    # ---- candidate_pool (候选池：股票/ETF/基金/期指) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_pool (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            code        VARCHAR(20)  NOT NULL UNIQUE COMMENT '代码(如 600519, NQ, 513100)',
            name        VARCHAR(30)  NOT NULL DEFAULT '' COMMENT '名称',
            type        VARCHAR(10)  NOT NULL DEFAULT 'stock' COMMENT '类型: stock/etf/fund/futures/index',
            source      VARCHAR(20)  NOT NULL DEFAULT 'tencent' COMMENT '数据源: tencent/sina_futures/eastmoney_futures',
            enabled     TINYINT(1)   DEFAULT 1 COMMENT '1=启用 0=停用',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='候选池(股票/ETF/基金/期指)'
        """,
        _pool=pool,
    )
```

- [ ] **Step 2: 在 database.py 文件末尾追加候选池 CRUD 方法**

```python
# ---------------------------------------------------------------------------
# Candidate Pool CRUD (候选池)
# ---------------------------------------------------------------------------

async def get_candidate_list(enabled_only: bool = False) -> list[dict]:
    """返回候选池列表。enabled_only=True 时只返回启用的。"""
    sql = "SELECT * FROM candidate_pool"
    if enabled_only:
        sql += " WHERE enabled = 1"
    sql += " ORDER BY type, code"
    return await execute_dict(sql)


async def add_candidate(code: str, name: str = "", type: str = "stock", source: str = "tencent") -> int:
    """新增候选，返回 affected rows。"""
    return await execute(
        "INSERT INTO candidate_pool (code, name, source, type) VALUES (%s, %s, %s, %s)",
        (code, name, source, type),
    )


async def delete_candidate(candidate_id: int) -> int:
    """按 id 删除候选，返回 affected rows。"""
    return await execute("DELETE FROM candidate_pool WHERE id = %s", (candidate_id,))


async def toggle_candidate(candidate_id: int) -> int:
    """切换候选启用/停用状态，返回 affected rows。"""
    return await execute(
        "UPDATE candidate_pool SET enabled = IF(enabled=1, 0, 1) WHERE id = %s",
        (candidate_id,),
    )


async def update_candidate_names(naming: dict[str, str]) -> int:
    """批量更新候选名称。naming: {code: name}，仅更新有变化的行。"""
    total = 0
    for code, name in naming.items():
        n = await execute(
            "UPDATE candidate_pool SET name = %s WHERE code = %s AND name != %s",
            (name, code, name),
        )
        total += n
    return total
```

- [ ] **Step 3: Commit**

```bash
git add database.py
git commit -m "feat: 新增 candidate_pool 表及 CRUD 方法"
```

---

### Task 2: fetcher.py — 新增 fetch_candidates() 通用行情抓取

**Files:**
- Modify: `fetcher.py:268-390` (在 ETF 解析区块之后追加通用解析方法)
- Modify: `fetcher.py:650-772` (在文件末尾追加 fetch_candidates 函数)

- [ ] **Step 1: 在 fetcher.py 的 ETF 解析区块后追加通用腾讯行情解析方法**

在 `_parse_tencent_etf` 函数之后（约 L390 后），追加：

```python
# ---------------------------------------------------------------------------
# 通用腾讯行情解析 (股票/ETF/基金/指数 — 非期指的 A 股品种)
# ---------------------------------------------------------------------------

def _parse_tencent_quote(raw: str, codes: list[str]) -> tuple[list[dict], dict[str, str]]:
    """解析腾讯批量响应，返回 (quote_data, naming)。

    与 _parse_tencent_etf 类似但输出字段更通用，
    适用于股票、基金、指数等品种。
    """
    quote_data: list[dict] = []
    naming: dict[str, str] = {}

    for code in codes:
        prefixed = _add_prefix(code)
        match = re.search(rf'v_{prefixed}="([^"]*)"', raw)
        if not match:
            continue

        value = match.group(1)
        if not value:
            continue

        fields = value.split("~")
        if len(fields) < 30:
            continue

        try:
            price = _safe_float(fields[_T_PRICE])
            name = fields[_T_NAME]
            change_pct = _safe_float(fields[_T_CHANGE_PCT])
            pre_close = _safe_float(fields[_T_PRE_CLOSE])
            total_volume = int(_safe_float(fields[_T_TOTAL_VOL])) if len(fields) > _T_TOTAL_VOL else 0
            amount = _safe_float(fields[_T_AMOUNT]) if len(fields) > _T_AMOUNT else 0.0
            iopv = _safe_float(fields[_T_IOPV]) if len(fields) > _T_IOPV else 0.0

            premium_pct = (price - iopv) / iopv * 100 if iopv > 0 else 0.0

            naming[code] = name
            item = {
                "code": code,
                "name": name,
                "price": price,
                "pre_close": pre_close,
                "change_pct": change_pct,
                "iopv": iopv,
                "premium_pct": premium_pct,
                "total_volume": total_volume,
                "amount": amount,
            }

            if len(fields) > _T_BUY1_VOL:
                item["buy1_price"] = _safe_float(fields[_T_BUY1_PRICE])
                item["buy1_vol"] = int(_safe_float(fields[_T_BUY1_VOL]))
                item["sell1_price"] = _safe_float(fields[_T_SELL1_PRICE])
                item["sell1_vol"] = int(_safe_float(fields[_T_SELL1_VOL]))

                total_buy = sum(int(_safe_float(fields[i])) for i in _T_BUY_VOLS if i < len(fields))
                total_sell = sum(int(_safe_float(fields[i])) for i in _T_SELL_VOLS if i < len(fields))
                item["buy_total_vol"] = total_buy
                item["sell_total_vol"] = total_sell

            quote_data.append(item)
        except Exception as e:
            logger.exception("解析行情 %s 异常", code)

    return quote_data, naming
```

- [ ] **Step 2: 在 fetcher.py 文件末尾追加 fetch_candidates 函数**

```python
# ---------------------------------------------------------------------------
# 候选池行情抓取 (股票/ETF/基金/指数 + 期指)
# ---------------------------------------------------------------------------

async def fetch_candidates(tencent_codes: list[str], futures_codes: list[str] | None = None) -> dict:
    """抓取候选池行情数据。

    按数据源分组批量请求：
    1. 腾讯 API 批量获取 A 股品种（股票/ETF/基金/指数）
    2. 期指路由获取期货数据

    Args:
        tencent_codes: 腾讯数据源的代码列表（纯数字，如 ['600519', '513100']）
        futures_codes: 期指代码列表（如 ['NQ', 'ES', 'YM']），为 None 则不获取

    Returns:
        {
            "quotes": [{"code": "600519", "name": "...", "price": ..., ...}, ...],
            "futures": {"NQ": float|None, ...},
            "errors": [...],
            "fetch_time": "HH:MM:SS"
        }
    """
    errors: list[str] = []
    fetch_start = time.monotonic()

    quote_data: list[dict] = []

    # 1. 腾讯批量行情
    if tencent_codes:
        prefixed = [_add_prefix(c) for c in tencent_codes]
        tencent_url = f"{TENCENT_BASE}{','.join(prefixed)}"

        raw = await _http_get(tencent_url, source="tencent_candidate")
        if raw:
            quote_data, naming = _parse_tencent_quote(raw, tencent_codes)
            logger.info("候选池腾讯行情: %d/%d 成功", len(quote_data), len(tencent_codes))

            if naming:
                try:
                    import database as db
                    await db.update_candidate_names(naming)
                except Exception as e:
                    logger.warning("候选池名称回写失败: %s", e)
        else:
            errors.append("候选池腾讯接口请求失败")

    # 2. 期指行情
    futures_data: dict[str, float | None] = {}
    if futures_codes:
        futures_data = await fetch_futures_with_routing(futures_codes)
        for key, val in futures_data.items():
            futures_data[key] = val

    elapsed = time.monotonic() - fetch_start
    fetch_time = datetime.now().strftime("%H:%M:%S")

    logger.info(
        "候选池抓取完成: %d 条行情, %d 期指, 耗时 %.1fs",
        len(quote_data), len(futures_data), elapsed,
    )

    return {
        "quotes": quote_data,
        "futures": futures_data,
        "errors": errors,
        "fetch_time": fetch_time,
    }
```

- [ ] **Step 3: Commit**

```bash
git add fetcher.py
git commit -m "feat: 新增 fetch_candidates 通用候选池行情抓取"
```

---

### Task 3: config.py — 新增候选池刷新间隔配置

**Files:**
- Modify: `config.py:17-34`

- [ ] **Step 1: 在 config.py 末尾追加候选池配置**

在 `FUTURES_NAME_MAP` 之后追加：

```python
CANDIDATE_REFRESH_SEC = 5.0  # 候选池缓存刷新间隔(秒)
```

- [ ] **Step 2: Commit**

```bash
git add config.py
git commit -m "feat: 新增 CANDIDATE_REFRESH_SEC 配置"
```

---

### Task 4: server.py — 新增缓存层 + /api/quote 端点 + 候选池 CRUD

**Files:**
- Modify: `server.py:20-24` (import 追加)
- Modify: `server.py:95-99` (全局变量区追加)
- Modify: `server.py:345-428` (在 Config 区块后追加候选池 CRUD 端点)
- Modify: `server.py:428-499` (在 _daily_init 中追加候选池初始化)
- Modify: `server.py:55-80` (lifespan 中追加候选池刷新任务)

- [ ] **Step 1: 修改 import 行，追加 config 引用**

将 `server.py` L20-24 改为：

```python
from config import (
    MIN_INTERVAL_SEC,
    SERVER_PORT,
    TRADING_SESSIONS,
    CANDIDATE_REFRESH_SEC,
)
```

- [ ] **Step 2: 在全局变量区追加候选池缓存**

在 `server.py` 的 `prev_day_changes_date` 变量之后（L99后），追加：

```python
candidate_cache: dict = {}          # 候选池缓存数据
candidate_cache_time: float = 0     # 缓存时间戳 (monotonic)
candidate_refresh_task: Optional[asyncio.Task] = None  # 后台刷新任务
```

- [ ] **Step 3: 在 server.py 中追加候选池刷新协程**

在 `_daily_init` 函数之前（约 L434前），追加：

```python
# ---------------------------------------------------------------------------
# 候选池缓存刷新 (后台定时任务)
# ---------------------------------------------------------------------------

async def _candidate_refresh_loop():
    """后台定时刷新候选池缓存。"""
    global candidate_cache, candidate_cache_time
    logger.info("候选池刷新循环启动 (间隔 %.0fs)", CANDIDATE_REFRESH_SEC)
    while True:
        try:
            candidates = await db.get_candidate_list(enabled_only=True)
            if not candidates:
                candidate_cache = {}
                await asyncio.sleep(CANDIDATE_REFRESH_SEC)
                continue

            tencent_codes = [c["code"] for c in candidates if c["source"] == "tencent"]
            futures_codes = [c["code"] for c in candidates if c["source"] in ("sina_futures", "eastmoney_futures")]

            proxy_rows = await db.get_alive_proxies()
            proxy_urls = [row["url"] for row in proxy_rows]
            fetcher.set_proxies(proxy_urls)

            result = await fetcher.fetch_candidates(tencent_codes, futures_codes or None)

            # 组装统一缓存：code -> quote dict
            cache: dict = {}
            for q in result.get("quotes", []):
                cache[q["code"]] = q

            for code, change_pct in result.get("futures", {}).items():
                name = next((c["name"] for c in candidates if c["code"] == code), code)
                cache[code] = {
                    "code": code,
                    "name": name,
                    "type": "futures",
                    "change_pct": change_pct,
                    "price": 0,
                    "pre_close": 0,
                }

            for c in candidates:
                if c["code"] not in cache:
                    cache[c["code"]] = {
                        "code": c["code"],
                        "name": c["name"],
                        "type": c["type"],
                        "price": 0,
                        "pre_close": 0,
                        "change_pct": 0,
                    }

            candidate_cache = cache
            candidate_cache_time = time.monotonic()

            await asyncio.sleep(CANDIDATE_REFRESH_SEC)
        except asyncio.CancelledError:
            logger.info("候选池刷新循环已停止")
            break
        except Exception as e:
            logger.exception("候选池刷新错误: %s", e)
            await asyncio.sleep(CANDIDATE_REFRESH_SEC)
```

- [ ] **Step 4: 在 lifespan 中启动候选池刷新任务**

在 `server.py` lifespan 函数中（L68 `scheduler_task = ...` 之后），追加：

```python
    # 启动候选池缓存刷新
    global candidate_refresh_task
    candidate_refresh_task = asyncio.create_task(_candidate_refresh_loop())
```

在 lifespan 的 yield 之后（L74 `if daily_task:` 之前），追加：

```python
    if candidate_refresh_task:
        candidate_refresh_task.cancel()
```

- [ ] **Step 5: 追加 /api/quote 端点**

在 `server.py` 的 Config 端点区块之后（约 L428后），追加：

```python
# ---------------------------------------------------------------------------
# Quote API (候选池行情 — 供外部客户端消费)
# ---------------------------------------------------------------------------

@app.get("/api/quote")
async def quote_list(codes: str = Query(default="", description="逗号分隔的代码列表，为空则返回全部")):
    """返回候选池缓存的行情数据。

    示例:
      GET /api/quote                  → 返回全部候选池行情
      GET /api/quote?codes=600519,NQ  → 返回指定代码行情
    """
    if not candidate_cache:
        return {"items": [], "total": 0, "fetch_time": ""}

    if codes:
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        items = [candidate_cache[c] for c in code_list if c in candidate_cache]
    else:
        items = list(candidate_cache.values())

    return {
        "items": items,
        "total": len(items),
        "fetch_time": datetime.now().strftime("%H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Candidate Pool CRUD (候选池管理)
# ---------------------------------------------------------------------------

class CandidateInput(BaseModel):
    code: str
    name: str = ""
    type: str = "stock"
    source: str = "tencent"


@app.get("/api/candidate")
async def candidate_list():
    rows = await db.get_candidate_list()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/candidate")
async def candidate_add(data: CandidateInput):
    try:
        affected = await db.add_candidate(data.code, data.name, data.type, data.source)
        if affected:
            return {"ok": True}
        return JSONResponse(status_code=409, content={"ok": False, "message": "Candidate already exists"})
    except Exception as e:
        return JSONResponse(status_code=409, content={"ok": False, "message": str(e)})


@app.delete("/api/candidate/{candidate_id}")
async def candidate_delete(candidate_id: int):
    affected = await db.delete_candidate(candidate_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "Candidate not found"})


@app.post("/api/candidate/{candidate_id}/toggle")
async def candidate_toggle(candidate_id: int, enabled: Optional[bool] = Query(None)):
    if enabled is not None:
        affected = await db.execute(
            "UPDATE candidate_pool SET enabled = %s WHERE id = %s",
            (1 if enabled else 0, candidate_id),
        )
    else:
        affected = await db.toggle_candidate(candidate_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "Candidate not found"})
```

- [ ] **Step 6: 在 _daily_init 中追加候选池首次加载**

在 `_daily_init` 函数中，"隔夜美股"获取之后（约 L457后），追加：

```python
        # 3.5. 首次填充候选池缓存
        if not candidate_cache:
            candidates = await db.get_candidate_list(enabled_only=True)
            tencent_codes = [c["code"] for c in candidates if c["source"] == "tencent"]
            futures_codes = [c["code"] for c in candidates if c["source"] in ("sina_futures", "eastmoney_futures")]
            if tencent_codes or futures_codes:
                cresult = await fetcher.fetch_candidates(tencent_codes, futures_codes or None)
                cache: dict = {}
                for q in cresult.get("quotes", []):
                    cache[q["code"]] = q
                for code, change_pct in cresult.get("futures", {}).items():
                    name = next((c["name"] for c in candidates if c["code"] == code), code)
                    cache[code] = {"code": code, "name": name, "type": "futures", "change_pct": change_pct, "price": 0, "pre_close": 0}
                for c in candidates:
                    if c["code"] not in cache:
                        cache[c["code"]] = {"code": c["code"], "name": c["name"], "type": c["type"], "price": 0, "pre_close": 0, "change_pct": 0}
                global candidate_cache_time
                candidate_cache = cache
                candidate_cache_time = time.monotonic()
                logger.info("候选池首次加载: %d 条", len(cache))
```

注意：需要在 `_daily_init` 的 `global` 声明行追加 `candidate_cache, candidate_cache_time`。

- [ ] **Step 7: Commit**

```bash
git add server.py
git commit -m "feat: 新增 /api/quote 行情接口及候选池管理 API"
```

---

### Task 5: init_db.py — 新增候选池种子数据

**Files:**
- Modify: `init_db.py:13-22` (追加种子数据)

- [ ] **Step 1: 在 SEED_ETFS 之后追加候选池种子**

```python
SEED_CANDIDATES = [
    ("600519", "贵州茅台", "stock", "tencent"),
    ("000858", "五粮液",   "stock", "tencent"),
    ("300750", "宁德时代", "stock", "tencent"),
    ("601318", "中国平安", "stock", "tencent"),
    ("NQ",     "纳斯达克期指", "futures", "sina_futures"),
    ("ES",     "标普期指",     "futures", "sina_futures"),
    ("YM",     "道琼斯期指",   "futures", "sina_futures"),
    ("513100", "纳指ETF",   "etf",   "tencent"),
    ("513500", "标普500ETF", "etf", "tencent"),
    ("513050", "中概互联网ETF", "etf", "tencent"),
]
```

- [ ] **Step 2: 在 init() 函数中追加种子写入逻辑**

在 `init()` 函数中，ETF 种子写入之后（约 L44后），追加：

```python
    # 4. 种子候选池
    from database import get_candidate_list, add_candidate
    existing_candidates = await get_candidate_list()
    existing_candidate_codes = {row["code"] for row in existing_candidates}
    added_candidates = 0
    for code, name, ctype, source in SEED_CANDIDATES:
        if code not in existing_candidate_codes:
            await add_candidate(code, name, ctype, source)
            added_candidates += 1
            logger.info("  + Candidate %s %s %s %s", code, name, ctype, source)
        else:
            logger.info("  . Candidate %s 已存在，跳过", code)
    logger.info("种子候选池写入完成，新增 %d 条", added_candidates)
```

- [ ] **Step 3: Commit**

```bash
git add init_db.py
git commit -m "feat: 新增候选池种子数据(股票+期指+ETF)"
```

---

## 第二阶段：StockWatcher 子项目引入与改造

### Task 6: 复制 StockWatcher 到 qdii 项目根目录

**Files:**
- Create: `StockWatcher/` 目录（从 /tmp/StockWatcher/src/StockWatcher/ 复制核心文件）

- [ ] **Step 1: 复制 StockWatcher 核心源码到 qdii 项目**

```bash
mkdir -p /root/workspace/project/qdii/StockWatcher/src
cp -r /tmp/StockWatcher/src/StockWatcher/* /root/workspace/project/qdii/StockWatcher/src/
cp /tmp/StockWatcher/README.md /root/workspace/project/qdii/StockWatcher/
```

- [ ] **Step 2: 验证文件结构**

```bash
ls -la /root/workspace/project/qdii/StockWatcher/
ls -la /root/workspace/project/qdii/StockWatcher/src/
```

预期输出应包含 `StockPreviewControl.cs`, `StockConfig.cs`, `Util.cs`, `frmSetting.cs` 等。

- [ ] **Step 3: Commit**

```bash
git add StockWatcher/
git commit -m "chore: 引入 StockWatcher 子项目"
```

---

### Task 7: StockWatcher — StockModel 扩展为通用 QuoteModel

**Files:**
- Modify: `StockWatcher/src/StockPreviewControl.cs:216-243` (替换 StockModel)

- [ ] **Step 1: 替换 StockModel 为 QuoteModel**

将 `StockPreviewControl.cs` 中的 `StockModel` 类（L216-243）替换为：

```csharp
class QuoteModel
{
    public string Code { get; set; }
    public string Name { get; set; }
    public string Type { get; set; }
    public float CurrentPrice { get; set; }
    public float LastClose { get; set; }
    public float ChangePct { get; set; }
    public float Iopv { get; set; }
    public float PremiumPct { get; set; }
    public float BuyPrice1 { get; set; }
    public float BuyVolume1 { get; set; }
    public float SellPrice1 { get; set; }
    public float SellVolume1 { get; set; }

    public bool IsUp
    {
        get { return LastClose > 0 && CurrentPrice >= LastClose; }
    }

    public string DisplayText
    {
        get
        {
            if (Type == "futures")
            {
                return $"{Name} {(ChangePct >= 0 ? "+" : "")}{(ChangePct).ToString("F2")}%";
            }
            else if (Type == "etf" && Iopv > 0)
            {
                return $"{Name} {CurrentPrice.ToString("F3")}\r\n{(PremiumPct >= 0 ? "+" : "")}{(PremiumPct).ToString("F2")}%";
            }
            else
            {
                return $"{Name} {CurrentPrice.ToString("F2")}\r\n{(ChangePct >= 0 ? "+" : "")}{(ChangePct).ToString("F2")}%";
            }
        }
    }

    public string DetailText
    {
        get
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"代码：{Code}");
            sb.AppendLine($"名称：{Name}");
            sb.AppendLine($"类型：{Type}");
            sb.AppendLine($"现价：{CurrentPrice}");
            if (Type == "futures")
            {
                sb.AppendLine($"涨跌幅：{(ChangePct >= 0 ? "+" : "")}{ChangePct.ToString("F2")}%");
            }
            else
            {
                sb.AppendLine($"昨收：{LastClose}");
                sb.AppendLine($"涨跌幅：{(ChangePct >= 0 ? "+" : "")}{ChangePct.ToString("F2")}%");
                if (Iopv > 0)
                {
                    sb.AppendLine($"IOPV：{Iopv.ToString("F3")}");
                    sb.AppendLine($"溢价率：{(PremiumPct >= 0 ? "+" : "")}{PremiumPct.ToString("F2")}%");
                }
            }
            return sb.ToString();
        }
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add StockWatcher/
git commit -m "feat: StockModel 扩展为 QuoteModel，支持多类型显示"
```

---

### Task 8: StockWatcher — 数据源切换为 qdii API

**Files:**
- Modify: `StockWatcher/src/StockPreviewControl.cs:129-199` (替换 GetStockInfo + InvokeSinaApi)
- Modify: `StockWatcher/src/StockPreviewControl.cs:60-88` (修改 Timer_Tick 使用新模型)

- [ ] **Step 1: 替换 GetStockInfo 和 InvokeSinaApi 方法**

将 `StockPreviewControl.cs` 中的 `GetStockInfo` (L129-176) 和 `InvokeSinaApi` (L178-199) 替换为：

```csharp
private async Task<QuoteModel> GetQuoteInfo(string code)
{
    try
    {
        var res = await InvokeQdiiApi(code);
        if (string.IsNullOrEmpty(res))
        {
            throw new Exception("qdii API 返回空响应");
        }
        var json = Newtonsoft.Json.Linq.JObject.Parse(res);
        var items = json["items"] as Newtonsoft.Json.Linq.JArray;
        if (items == null || items.Count == 0)
        {
            throw new Exception($"代码 {code} 未在候选池中找到");
        }
        var item = items[0];
        return new QuoteModel()
        {
            Code = item["code"]?.ToString() ?? code,
            Name = item["name"]?.ToString() ?? "",
            Type = item["type"]?.ToString() ?? "stock",
            CurrentPrice = float.TryParse(item["price"]?.ToString(), out var p) ? p : 0,
            LastClose = float.TryParse(item["pre_close"]?.ToString(), out var pc) ? pc : 0,
            ChangePct = float.TryParse(item["change_pct"]?.ToString(), out var cp) ? cp : 0,
            Iopv = float.TryParse(item["iopv"]?.ToString(), out var iv) ? iv : 0,
            PremiumPct = float.TryParse(item["premium_pct"]?.ToString(), out var pp) ? pp : 0,
            BuyPrice1 = float.TryParse(item["buy1_price"]?.ToString(), out var bp) ? bp : 0,
            BuyVolume1 = float.TryParse(item["buy1_vol"]?.ToString(), out var bv) ? bv : 0,
            SellPrice1 = float.TryParse(item["sell1_price"]?.ToString(), out var sp) ? sp : 0,
            SellVolume1 = float.TryParse(item["sell1_vol"]?.ToString(), out var sv) ? sv : 0,
        };
    }
    catch (Exception ex)
    {
        errorCount++;
        if (errorCount < 10)
        {
            Util.Log($"查询行情【{code}】出错！", ex);
        }
        return null;
    }
}

private async Task<string> InvokeQdiiApi(string code)
{
    var api = $"{StockConfig.QdiiApiUrl}?codes={code}";
    try
    {
        using (var client = new HttpClient()
        {
            Timeout = TimeSpan.FromSeconds(5),
        })
        {
            return await client.GetStringAsync(api);
        }
    }
    catch (Exception ex)
    {
        Util.Log($"请求qdii API【{api}】出错！", ex);
        return "";
    }
}
```

- [ ] **Step 2: 修改 Timer_Tick 中的调用和显示逻辑**

将 `StockPreviewControl.cs` 中 `Timer_Tick` 方法内的 `Task.Run` 块（L69-86）替换为：

```csharp
                Task.Run(async () =>
                {
                    var currentCode = StockConfig.StockList[currentIndex];
                    var quoteModel = await GetQuoteInfo(currentCode);
                    if (quoteModel == null)
                    {
                        UpdateStatus($"查询{currentCode}失败");
                    }
                    else
                    {
                        labelForStatus.Tag = quoteModel;
                        UpdateStatus(quoteModel.DisplayText
                            , quoteModel.IsUp ? StockColor.Red : StockColor.Green);
                    }
                });
```

- [ ] **Step 3: 修改双击详情展示**

将 `labelForStatus_DoubleClick` 方法（L201-213）替换为：

```csharp
        private void labelForStatus_DoubleClick(object sender, EventArgs e)
        {
            if (labelForStatus.Tag != null && labelForStatus.Tag is QuoteModel model)
            {
                Util.Info(model.DetailText, "查看详情");
            }
        }
```

- [ ] **Step 4: Commit**

```bash
git add StockWatcher/
git commit -m "feat: 数据源切换为 qdii API，支持多类型行情展示"
```

---

### Task 9: StockWatcher — StockConfig 扩展

**Files:**
- Modify: `StockWatcher/src/StockConfig.cs:19-26` (追加配置项)
- Modify: `StockWatcher/src/StockConfig.cs:52-72` (LoadSetting 追加)

- [ ] **Step 1: 在 StockConfig 中追加 QdiiApiUrl 配置**

在 `StockConfig.cs` 的 `INI_PROPERTY_REFRESH_INTERVAL` 之后（L25后），追加：

```csharp
        private const string INI_PROPERTY_QDII_API_URL = "QDII_API_URL";
```

在 `DEFAULT_REFRESH_INTERVAL` 之后（L20后），追加：

```csharp
        private const string DEFAULT_QDII_API_URL = "http://localhost:8000/api/quote";
```

修改 `DEFAULT_SETTING`（L21-23）为：

```csharp
        private const string DEFAULT_SETTING =
            INI_PROPERTY_REFRESH_INTERVAL + "=" + DEFAULT_REFRESH_INTERVAL + "\r\n" +
            INI_PROPERTY_QDII_API_URL + "=" + DEFAULT_QDII_API_URL + "\r\n" +
            INI_PROPERTY_STOCK_LIST + "=" + CODE_SH_STOCK + "," + CODE_SZ_STOCK;
```

- [ ] **Step 2: 在 LoadSetting 中追加 QdiiApiUrl 读取**

在 `LoadSetting()` 方法的末尾（`_RefreshInterval = _refreshInterval;` 之后），追加：

```csharp
            var qdiiApiUrlStr = INIHelper.Read(CONFIG_PATH, INI_PROPERTY_QDII_API_URL, DEFAULT_QDII_API_URL);
            if (string.IsNullOrEmpty(qdiiApiUrlStr))
            {
                INIHelper.Write(CONFIG_PATH, INI_PROPERTY_QDII_API_URL, DEFAULT_QDII_API_URL);
                qdiiApiUrlStr = DEFAULT_QDII_API_URL;
            }
            _QdiiApiUrl = qdiiApiUrlStr;
```

在 `RefreshInterval` 属性之后（L131后），追加：

```csharp
        private static string _QdiiApiUrl = DEFAULT_QDII_API_URL;
        public static string QdiiApiUrl
        {
            get { return _QdiiApiUrl; }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    _QdiiApiUrl = DEFAULT_QDII_API_URL;
                }
                else
                {
                    _QdiiApiUrl = value;
                }
                INIHelper.Write(CONFIG_PATH, INI_PROPERTY_QDII_API_URL, _QdiiApiUrl);
            }
        }
```

- [ ] **Step 3: Commit**

```bash
git add StockWatcher/
git commit -m "feat: StockConfig 新增 QdiiApiUrl 配置"
```

---

### Task 10: StockWatcher — Util.cs 代码修正逻辑扩展

**Files:**
- Modify: `StockWatcher/src/Util.cs:63-100` (ReturnFixStockCode 扩展)

- [ ] **Step 1: 扩展 ReturnFixStockCode 支持期指和 ETF**

将 `Util.cs` 中的 `ReturnFixStockCode` 方法（L63-100）替换为：

```csharp
        public static string ReturnFixStockCode(string code)
        {
            code = code.Trim();
            if (string.IsNullOrEmpty(code))
            {
                return null;
            }

            // 期指代码：纯字母（NQ, ES, YM 等），直接透传
            var pureLetters = new System.Text.StringBuilder();
            foreach (var item in code)
            {
                if ((item >= 'A' && item <= 'Z') || (item >= 'a' && item <= 'z'))
                {
                    pureLetters.Append(item);
                }
            }
            if (pureLetters.Length > 0 && pureLetters.Length == code.Length)
            {
                return code.ToUpper();
            }

            // 数字代码：提取纯数字
            var codeStringBuilder = new System.Text.StringBuilder();
            foreach (var item in code)
            {
                if (item >= '0' && item <= '9')
                {
                    codeStringBuilder.Append(item);
                }
            }
            code = codeStringBuilder.ToString();
            if (code.Length != 6)
            {
                return null;
            }
            if (code == StockConfig.CODE_SH_STOCK_NUMBER)
            {
                code = StockConfig.CODE_SH_STOCK;
            }
            else if (code == StockConfig.CODE_SZ_STOCK_NUMBER)
            {
                code = StockConfig.CODE_SZ_STOCK;
            }
            else if (code.StartsWith("60") || code.StartsWith("900") || code.StartsWith("51"))
            {
                code = $"sh{code}";
            }
            else if (code.StartsWith("000") || code.StartsWith("002") || code.StartsWith("300")
                     || code.StartsWith("200") || code.StartsWith("15"))
            {
                code = $"sz{code}";
            }
            return code;
        }
```

关键改动：期指字母代码直接透传；`51` 开头（沪市 ETF）→ `sh`；`15` 开头（深市 ETF）→ `sz`。

- [ ] **Step 2: Commit**

```bash
git add StockWatcher/
git commit -m "feat: ReturnFixStockCode 支持期指和 ETF 代码"
```

---

### Task 11: StockWatcher — frmSetting.cs 设置界面适配

**Files:**
- Modify: `StockWatcher/src/frmSetting.cs:25-38` (添加按钮提示语)
- Modify: `StockWatcher/src/frmSetting.cs:48-74` (保存逻辑放行期指代码)

- [ ] **Step 1: 修改 buttonForAddStock_Click 提示语**

将 `frmSetting.cs` L27 的提示语改为：

```csharp
            var input = Util.Input("请输入代码（股票/ETF/期指）：");
```

- [ ] **Step 2: 修改 buttonSave_Click 中的验证逻辑**

将 `frmSetting.cs` L57-68 的保存验证替换为：

```csharp
            var stockList = this.textBoxStockList.Text.Trim();
            var arr = stockList.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            var list = new List<string>();
            foreach (var item in arr)
            {
                var _code = Util.ReturnFixStockCode(item);
                if (string.IsNullOrEmpty(_code))
                {
                    Util.Alert($"您输入的代码【{item}】不是正确的代码，请检查！");
                    return;
                }
                list.Add(_code);
            }
```

（逻辑不变，但 `ReturnFixStockCode` 已扩展支持期指，此处无需其他改动。）

- [ ] **Step 3: Commit**

```bash
git add StockWatcher/
git commit -m "feat: 设置界面支持股票/ETF/期指代码输入"
```

---

### Task 12: StockWatcher — 添加 Newtonsoft.Json 引用

**Files:**
- Modify: `StockWatcher/src/StockWatcher.csproj` (追加 NuGet 包引用)

- [ ] **Step 1: 在 csproj 中追加 Newtonsoft.Json 包引用**

先查看当前 csproj 内容，找到 `<ItemGroup>` 中的 `<Reference>` 节点，在其后或新的 `<ItemGroup>` 中追加：

```xml
  <ItemGroup>
    <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
  </ItemGroup>
```

如果 csproj 使用旧式格式（无 `PackageReference` 支持），则改为在代码中使用 `System.Web.Script.Serialization.JavaScriptSerializer`（.NET Framework 内置），并相应修改 `GetQuoteInfo` 中的 JSON 解析逻辑。

- [ ] **Step 2: Commit**

```bash
git add StockWatcher/
git commit -m "feat: 添加 Newtonsoft.Json 引用"
```

---

### Task 13: 整体验证 — qdii 服务启动测试

**Files:**
- 无文件变更，纯验证步骤

- [ ] **Step 1: 初始化数据库**

```bash
cd /root/workspace/project/qdii && python init_db.py
```

预期：新增 candidate_pool 表 + 种子数据写入成功。

- [ ] **Step 2: 启动 qdii 服务（短暂运行验证）**

```bash
cd /root/workspace/project/qdii && timeout 15 python server.py 2>&1 || true
```

预期：服务启动，`/api/quote` 端点可访问，候选池缓存加载成功。

- [ ] **Step 3: 测试 /api/quote 接口**

```bash
curl -s http://localhost:8000/api/quote | python -m json.tool
curl -s "http://localhost:8000/api/quote?codes=600519,NQ" | python -m json.tool
curl -s http://localhost:8000/api/candidate | python -m json.tool
```

预期：返回 JSON 格式的行情数据和候选池列表。

---

## 自检清单

### 1. 需求覆盖

| 需求 | 对应 Task |
|------|----------|
| qdii 新增候选池表 | Task 1 |
| qdii 通用行情抓取 | Task 2 |
| qdii 缓存刷新间隔配置 | Task 3 |
| qdii /api/quote 端点 | Task 4 |
| qdii 候选池管理 CRUD | Task 4 |
| qdii 种子数据 | Task 5 |
| StockWatcher 引入为子项目 | Task 6 |
| StockWatcher 数据模型扩展 | Task 7 |
| StockWatcher 数据源切换 | Task 8 |
| StockWatcher 配置扩展 | Task 9 |
| StockWatcher 代码修正扩展 | Task 10 |
| StockWatcher 设置界面适配 | Task 11 |
| StockWatcher JSON 解析依赖 | Task 12 |
| 端到端验证 | Task 13 |

### 2. 占位符扫描

无 TBD/TODO/placeholder。

### 3. 类型一致性

- `QuoteModel.Type` 在 Task 7 定义，在 Task 8 的 `DisplayText`/`DetailText` 中使用 — 一致
- `StockConfig.QdiiApiUrl` 在 Task 9 定义，在 Task 8 的 `InvokeQdiiApi` 中使用 — 一致
- `candidate_pool.source` 字段在 Task 1 定义（tencent/sina_futures/eastmoney_futures），在 Task 4 缓存组装和 Task 5 种子数据中引用 — 一致
- `fetch_candidates` 参数签名（tencent_codes, futures_codes）在 Task 2 定义，在 Task 4 和 Task 6 中调用 — 一致
