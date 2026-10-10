# 持仓收益监控模块 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 qdii 项目上叠加"券商风格持仓收益页"：美股+港股基金持仓、USD 统一口径、当日分时+历史日线双图、算术验证码登录、全系统取消手动启停，并部署到 <server-ip>:8888。

**Architecture:** 同进程模块化扩展（spec 方案A）：新增 `portfolio_fetcher.py`（行情/净值/汇率采集与纯计算函数）+ `static/portfolio.html`；`database.py` 加 3 表与 CRUD；`server.py` 挂 API 路由、验证码、两个后台任务并删除手动启停；现有 A股监控/候选池/SSE/代理池不动。

**Tech Stack:** FastAPI + httpx + aiomysql + Pillow(验证码) + zoneinfo(美东时区) + 原生 canvas 前端。设计依据：`docs/superpowers/specs/2026-10-01-portfolio-module-design.md` 与 `docs/us-hk-quote-api-research.md`。

**工作目录:** /root/workspace/project/qdii（下文相对路径均基于此）

**约定:** 所有新代码中文注释、禁止 TODO/伪代码；红涨绿跌（#e94560 红 / #00b894 绿）；测试一律离线（不发真实网络请求）。

---

### Task 1: 依赖配置 + 数据层（database.py）

**Files:**
- Modify: `requirements.txt`（追加 pillow）
- Modify: `config.py:19-20`（SERVER_PORT 8880→见下）
- Modify: `database.py`（文件末尾追加持仓 CRUD；`init_tables` 中追加建表与种子）
- Test: 手动 `python3 -c "import database"`（连接 DB 幂等建表）

- [ ] **Step 1: requirements.txt 追加一行 `pillow`；config.py 把 `SERVER_PORT = 8000` 改为 `SERVER_PORT = 8888`**

- [ ] **Step 2: database.py 的 `init_tables()` 末尾（`_alter_add_rec_columns(pool)` 调用之后、`logger.info("数据库表初始化完成")` 之前）插入：**

```python
    # ---- holdings (持仓列表) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS holdings (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            symbol      VARCHAR(20)  NOT NULL UNIQUE COMMENT '代码 MU/SKHY/HK0000740297',
            name        VARCHAR(40)  NOT NULL DEFAULT '' COMMENT '名称',
            market      VARCHAR(10)  NOT NULL DEFAULT 'US' COMMENT 'US=美股 HKFUND=港股场外基金',
            quantity    DECIMAL(14,4) NOT NULL DEFAULT 0 COMMENT '数量/份额',
            cost_price  DECIMAL(14,4) NOT NULL DEFAULT 0 COMMENT '成本价(原币种)',
            currency    VARCHAR(5)   NOT NULL DEFAULT 'USD' COMMENT 'USD/HKD',
            enabled     TINYINT(1)   DEFAULT 1 COMMENT '1=启用',
            sort_order  INT          DEFAULT 0 COMMENT '排序',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
            updated_at  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='持仓列表'
        """,
        _pool=pool,
    )

    # ---- portfolio_ticks (组合市值采样) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_ticks (
            id             BIGINT AUTO_INCREMENT PRIMARY KEY,
            ts             DATETIME(3) NOT NULL COMMENT '北京时间采样点',
            trade_date     DATE NOT NULL COMMENT '美东交易日',
            total_value_usd DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '总资产USD',
            day_pnl_usd    DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '今日盈亏USD',
            total_pnl_usd  DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '持仓盈亏USD',
            day_pnl_pct    DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '今日收益率%',
            total_pnl_pct DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '累计收益率%',
            INDEX idx_trade_ts (trade_date, ts)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组合市值采样(盘中10s/扩展时段30s)'
        """,
        _pool=pool,
    )

    # ---- fund_nav_history (基金官方日频净值) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS fund_nav_history (
            nav_date    DATE PRIMARY KEY COMMENT '净值日期',
            nav         DECIMAL(12,4) NOT NULL DEFAULT 0 COMMENT '净值(HKD)',
            nav_change_pct DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '日涨跌%'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金官方日频净值(FT)'
        """,
        _pool=pool,
    )

    # ---- 种子持仓 (示例数据，幂等) ----
    seed_holdings = [
        ("MU", "美光科技", "US", 100, 900.0, "USD", 1),
        ("SKHY", "SK海力士 ADR", "US", 100, 150.0, "USD", 2),
        ("QQQM", "纳斯达克100ETF-Invesco", "US", 200, 250.0, "USD", 3),
        ("SPYM", "标普500ETF-SPDR", "US", 300, 75.0, "USD", 4),
        ("DRAM", "存储芯片ETF-Roundhill", "US", 200, 45.0, "USD", 5),
        ("DISK", "存储芯片ETF-Tema", "US", 200, 30.0, "USD", 6),
        ("HK0000740297", "邓普顿智选亚太股票入息基金 A(Mdis) HKD", "HKFUND", 5000, 13.0, "HKD", 7),
    ]
    for symbol, name, market, qty, cost, curr, sort in seed_holdings:
        await execute(
            "INSERT IGNORE INTO holdings (symbol, name, market, quantity, cost_price, currency, sort_order) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (symbol, name, market, qty, cost, curr, sort),
            _pool=pool,
        )
```

- [ ] **Step 3: database.py 文件末尾追加 CRUD：**

```python
# ---------------------------------------------------------------------------
# Holdings CRUD (持仓)
# ---------------------------------------------------------------------------

async def get_holdings(enabled_only: bool = False) -> list[dict]:
    """返回持仓列表，按 sort_order 排序。"""
    sql = "SELECT * FROM holdings"
    if enabled_only:
        sql += " WHERE enabled = 1"
    sql += " ORDER BY sort_order, id"
    rows = await execute_dict(sql)
    for r in rows:
        r["quantity"] = float(r["quantity"])
        r["cost_price"] = float(r["cost_price"])
    return rows


async def upsert_holding(symbol: str, name: str, market: str, quantity: float,
                         cost_price: float, currency: str) -> int:
    """按 symbol UPSERT 持仓。"""
    return await execute(
        "INSERT INTO holdings (symbol, name, market, quantity, cost_price, currency) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE name=VALUES(name), market=VALUES(market), "
        "quantity=VALUES(quantity), cost_price=VALUES(cost_price), currency=VALUES(currency)",
        (symbol, name, market, quantity, cost_price, currency),
    )


async def update_holding(holding_id: int, name: str, market: str, quantity: float,
                         cost_price: float, currency: str, enabled: int, sort_order: int) -> int:
    """按 id 修改持仓。"""
    return await execute(
        "UPDATE holdings SET name=%s, market=%s, quantity=%s, cost_price=%s, "
        "currency=%s, enabled=%s, sort_order=%s WHERE id=%s",
        (name, market, quantity, cost_price, currency, enabled, sort_order, holding_id),
    )


async def delete_holding(holding_id: int) -> int:
    """按 id 删除持仓。"""
    return await execute("DELETE FROM holdings WHERE id = %s", (holding_id,))


async def update_holding_names(naming: dict[str, str]) -> int:
    """回写行情接口返回的名称。"""
    total = 0
    for symbol, name in naming.items():
        if not name:
            continue
        n = await execute(
            "UPDATE holdings SET name = %s WHERE symbol = %s AND name != %s",
            (name, symbol, name),
        )
        total += n
    return total


# ---------------------------------------------------------------------------
# Portfolio ticks (组合采样)
# ---------------------------------------------------------------------------

async def save_portfolio_tick(ts, trade_date, total_value: float, day_pnl: float,
                              total_pnl: float, day_pnl_pct: float, total_pnl_pct: float) -> int:
    """保存一条组合市值采样。"""
    return await execute(
        "INSERT INTO portfolio_ticks (ts, trade_date, total_value_usd, day_pnl_usd, "
        "total_pnl_usd, day_pnl_pct, total_pnl_pct) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (ts, trade_date, _safe_decimal(total_value, 2), _safe_decimal(day_pnl, 2),
         _safe_decimal(total_pnl, 2), _safe_decimal(day_pnl_pct), _safe_decimal(total_pnl_pct)),
    )


async def get_portfolio_intraday(trade_date) -> list[dict]:
    """返回指定美东交易日的全部采样点（当日分时）。"""
    return await execute_dict(
        "SELECT ts, total_value_usd, day_pnl_usd FROM portfolio_ticks "
        "WHERE trade_date = %s ORDER BY ts",
        (trade_date,),
    )


async def get_portfolio_daily(days: int) -> list[dict]:
    """返回近 N 天的日线（每美东交易日取最后一条采样）。"""
    return await execute_dict(
        """
        SELECT t.trade_date AS date, t.total_value_usd AS total_value,
               t.day_pnl_usd AS day_pnl, t.total_pnl_pct AS total_pnl_pct
        FROM portfolio_ticks t
        JOIN (SELECT trade_date, MAX(ts) AS mx FROM portfolio_ticks GROUP BY trade_date) m
          ON t.trade_date = m.trade_date AND t.ts = m.mx
        WHERE t.trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY t.trade_date
        """,
        (days,),
    )


# ---------------------------------------------------------------------------
# Fund NAV (基金净值)
# ---------------------------------------------------------------------------

async def upsert_fund_nav(nav_date, nav: float, nav_change_pct: float) -> int:
    """UPSERT 基金官方净值。"""
    return await execute(
        "INSERT INTO fund_nav_history (nav_date, nav, nav_change_pct) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE nav=VALUES(nav), nav_change_pct=VALUES(nav_change_pct)",
        (nav_date, _safe_decimal(nav), _safe_decimal(nav_change_pct)),
    )


async def get_latest_fund_nav() -> dict | None:
    """返回最新净值与前一日净值。"""
    rows = await execute_dict(
        "SELECT nav_date, nav, nav_change_pct FROM fund_nav_history ORDER BY nav_date DESC LIMIT 2"
    )
    if not rows:
        return None
    latest = rows[0]
    prev = float(rows[1]["nav"]) if len(rows) > 1 else 0.0
    return {
        "nav_date": latest["nav_date"],
        "nav": float(latest["nav"]),
        "nav_change_pct": float(latest["nav_change_pct"]),
        "prev_nav": prev,
    }


async def get_fund_nav_history(days: int) -> list[dict]:
    """返回近 N 天基金净值历史。"""
    return await execute_dict(
        "SELECT nav_date, nav, nav_change_pct FROM fund_nav_history "
        "WHERE nav_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) ORDER BY nav_date",
        (days,),
    )
```

- [ ] **Step 4: 验证**：`python3 -c "import asyncio, database; asyncio.run(database.init_tables()); print('ok')"`（连接现有 MySQL 幂等建表），再 `mysql` 不通时改为仅 `python3 -m py_compile database.py`。
- [ ] **Step 5: Commit** `git add requirements.txt config.py database.py docs/ && git commit -m "feat(portfolio): 持仓数据层与配置（3表+CRUD+种子）"`

---

### Task 2: portfolio_fetcher.py 纯函数（TDD）

**Files:**
- Create: `portfolio_fetcher.py`
- Test: `tests/test_portfolio.py`

- [ ] **Step 1: 先写失败测试 `tests/test_portfolio.py`：**

```python
"""持仓模块离线单元测试：时段/解析/盈亏计算（不发真实网络请求）"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import portfolio_fetcher as pf

ET = ZoneInfo("America/New_York")


class TestUsMarketSession:
    def test_pre(self):
        s, d, _ = pf.us_market_session(datetime(2026, 10, 1, 9, 0, tzinfo=ET))
        assert s == "pre" and d == date(2026, 10, 1)

    def test_regular(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 12, 0, tzinfo=ET))[0] == "regular"

    def test_post(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 17, 0, tzinfo=ET))[0] == "post"

    def test_closed_night(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 21, 0, tzinfo=ET))[0] == "closed"

    def test_closed_weekend(self):
        # 2026-10-03 周六
        assert pf.us_market_session(datetime(2026, 10, 3, 12, 0, tzinfo=ET))[0] == "closed"


TENCENT_RAW = 'v_usMU="200~美光科技~MU.OQ~1065.11~1065.11~0.00~2125925~0~0~1068.98~80~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~~2026-10-01 09:30:00~0.00~0.00~1083.50~1062.36~USD~1035004~193308777~0.01~12.55~~44.05~";\nv_usSKHY="200~SK海力士~SKHY.OQ~184.15~184.15~0.00~1035004~0~0~185.2~80~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~~2026-10-01 09:30:00~0.00~0.00~189.14~183.00~USD~1035004~193308777~0.01~12.55~~44.05~";\n'


class TestParseTencentUs:
    def test_basic(self):
        quotes = pf.parse_tencent_us(TENCENT_RAW, ["MU", "SKHY"])
        assert set(quotes) == {"MU", "SKHY"}
        mu = quotes["MU"]
        assert mu["name"] == "美光科技"
        assert mu["price"] == 1065.11
        assert mu["pre_close"] == 1065.11
        assert mu["currency"] == "USD"
        assert mu["quote_time"] == "2026-10-01 09:30:00"

    def test_missing_symbol(self):
        quotes = pf.parse_tencent_us(TENCENT_RAW, ["NOPE"])
        assert "NOPE" not in quotes


SINA_RAW = 'var hq_str_gb_mu="美光,1065.1100,0.00,2026-10-01 21:27:06,0.0000,0.0000,0.0000,0.0000,1254.8500,179.1950,0,25290278,1202386340212,44.80,23.770000,0.00,0.00,0.00,0.00,1128884660,85,1051.9950,-1.23,-13.12,Oct 01 09:27AM EDT,Oct 01 09:10AM EDT,1065.1100,2127019,1,2026,0.0000,1086.4100,1050.2500,2258052759.8623,1072.6400,1065.1100";\nvar hq_str_gb_skhy="SK海力士,184.1500,0.00,2026-10-01 21:27:04,0.0000,0.0000,0.0000,0.0000,199.8650,124.8000,0,20495622,1342205818250,0.00,--,0.00,-0.01,0.00,0.00,7288655000,0,0,0.00,0.00,,,184.1500,1035173,1,2026,0.0000,189.1400,183.0000,193308509.9160,183.0000,184.1500";\n'


class TestParseSinaUs:
    def test_with_ext(self):
        ext = pf.parse_sina_us(SINA_RAW, ["MU"])
        assert ext["MU"]["ext_price"] == 1051.9950
        assert ext["MU"]["pre_close"] == 1065.1100
        assert ext["MU"]["ext_time"] == "Oct 01 09:27AM EDT"

    def test_without_ext(self):
        ext = pf.parse_sina_us(SINA_RAW, ["SKHY"])
        assert ext["SKHY"]["ext_price"] == 0.0


class TestParseFx:
    def test_ok(self):
        raw = 'var hq_str_fx_susdhkd="21:55:24,7.846600,7.847600,7.846400,19,7.846700,7.847300,7.845400,7.846600,美元兑港元即期汇率,0.010000,0.000200,0.0002421,,7.847600,7.781500,,2026-10-01";'
        assert pf.parse_fx_usdhkd(raw) == 7.8466

    def test_out_of_range(self):
        raw = 'var hq_str_fx_susdhkd="21:55:24,70.8466,7.847,7.846,19,7.8467,7.847,7.845,7.8466,x,0.01,0.0002,0.0002,,7.847,7.781,,2026-10-01";'
        assert pf.parse_fx_usdhkd(raw) == 0.0

    def test_garbage(self):
        assert pf.parse_fx_usdhkd("") == 0.0


FT_DATA = {"data": {"Overview": {"fundname": "Templeton Select APAC Equity Income Fund",
    "shareclass": [{"shclname": "A (Mdis) HKD", "nav": {
        "navvalue": "$15.50", "navdate": "30/09/2026",
        "navchngvalue": "$-0.12", "navchngpct": "-0.77%", "ytdtotretatnav": "30.91%"}}]}}}


class TestParseFtNav:
    def test_ok(self):
        r = pf.parse_ft_nav(FT_DATA)
        assert r["nav"] == 15.50
        assert r["nav_date"] == date(2026, 9, 30)
        assert r["change_pct"] == -0.77
        assert r["ytd_pct"] == 30.91

    def test_bad(self):
        assert pf.parse_ft_nav({"data": {"Overview": {}}}) is None


HOLDINGS = [
    {"symbol": "MU", "name": "美光科技", "market": "US", "quantity": 100, "cost_price": 900.0, "currency": "USD"},
    {"symbol": "HK0000740297", "name": "邓普顿基金", "market": "HKFUND", "quantity": 5000, "cost_price": 13.0, "currency": "HKD"},
]
QUOTES = {"MU": {"name": "美光科技", "price": 1065.11, "pre_close": 1065.11,
                 "ext_price": 1051.995, "ext_time": "Oct 01 09:27AM EDT",
                 "change_pct": 0.0, "currency": "USD", "quote_time": "t"}}
FUND = {"nav": 15.50, "prev_nav": 15.62, "nav_date": date(2026, 9, 30), "nav_change_pct": -0.77}


class TestBuildPositions:
    def test_summary(self):
        positions, summary = pf.build_positions(HOLDINGS, QUOTES, FUND, fx_usdhkd=7.8)
        mu = next(p for p in positions if p["symbol"] == "MU")
        assert mu["price"] == 1051.995          # 扩展时段价优先
        assert mu["today_pnl"] == (1051.995 - 1065.11) * 100
        assert mu["value"] == 1051.995 * 100
        assert mu["position_pnl"] == (1051.995 - 900.0) * 100
        fund = next(p for p in positions if p["symbol"] == "HK0000740297")
        assert fund["value"] == 15.50 * 5000 / 7.8
        assert fund["today_pnl"] == (15.50 - 15.62) * 5000 / 7.8
        assert fund["position_pnl"] == (15.50 - 13.0) * 5000 / 7.8
        assert summary["total_value"] == mu["value"] + fund["value"]
        assert summary["base_value"] == 1065.11 * 100 + 15.62 * 5000 / 7.8

    def test_missing_quote(self):
        positions, summary = pf.build_positions(HOLDINGS, {}, FUND, 7.8)
        mu = next(p for p in positions if p["symbol"] == "MU")
        assert mu["price"] == 0 and mu["value"] == 0 and mu["stale"] is True
```

- [ ] **Step 2: 运行确认失败**：`cd /root/workspace/project/qdii && python3 -m pytest tests/test_portfolio.py -x -q` → 预期 `ModuleNotFoundError: No module named 'portfolio_fetcher'`
- [ ] **Step 3: 创建 `portfolio_fetcher.py`（本任务先只写纯函数部分）：**

```python
"""持仓数据抓取模块：美股行情(腾讯主源/新浪扩展/东财兜底) + 基金净值(FT GraphQL) + 汇率。

数据源结论见 docs/us-hk-quote-api-research.md：
- 腾讯 us 前缀批量：常规时段行情（无盘前盘后）
- 新浪 gb_ 前缀批量：字段[21]-[24] 为盘前/盘后实时价
- 东财 ulist 批量：兜底（仅常规价，105=NASDAQ/107=AMEX，易封 IP，≤1次/分钟）
- FT 官网 GraphQL：基金官方日频净值（fundid=31402/shclcode=2B）
- 新浪 fx_susdhkd：USD/HKD 汇率（失败兜底 7.80）
"""

import logging
import re
import time
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:  # 兜底：无 tzdata 时退化为固定 UTC-4/5 不可行，直接报错更安全
    raise

# ---------------------------------------------------------------------------
# 接口常量
# ---------------------------------------------------------------------------

TENCENT_US_URL = "http://qt.gtimg.cn/q={symbols}"
SINA_US_URL = "https://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}
SINA_FX_URL = "https://hq.sinajs.cn/list=fx_susdhkd"
EM_ULIST_URL = "http://push2.eastmoney.com/api/qt/ulist.np/get"
EM_FIELDS = "f2,f3,f12,f13,f14,f18,f124"
FT_PDS_URL = "https://www.franklintempleton.com.hk/api/pds/price-and-performance"
FT_HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://www.franklintempleton.com.hk",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"),
}
FT_FUND_QUERY = (
    "query O($f:String!,$s:String!,$c:String!,$l:String!){Overview(fundid:$f,"
    "shareclasscode:$s,countrycode:$c,languagecode:$l){fundname shareclass{shclname "
    "nav{navvalue navdate navchngvalue navchngpct ytdtotretatnav}}}}"
)
FT_FUND_VARS = {"f": "31402", "s": "2B", "c": "HK", "l": "en_GB"}
FX_FALLBACK = 7.80

# 东财兜底市场号（105=NASDAQ 107=AMEX），未知符号默认 105
EM_MARKET_MAP = {"MU": "105", "SKHY": "105", "QQQM": "105", "SPYM": "107", "DRAM": "107", "DISK": "107"}
EM_DEFAULT_MARKET = "105"

# 东财兜底节流：主源连续失败>=3轮才启用，且间隔>=60s
_EM_FAIL_THRESHOLD = 3
_EM_MIN_INTERVAL_SEC = 60.0
_em_fail_streak = 0
_last_em_used = 0.0


# ---------------------------------------------------------------------------
# 美股时段判断（美东时钟，DST 自动正确）
# ---------------------------------------------------------------------------

def us_market_session(now_et: datetime | None = None) -> tuple[str, date, str]:
    """返回 (session, 美东日期, 美东时间串)。session ∈ pre/regular/post/closed。"""
    now = now_et or datetime.now(ET_TZ)
    session = "closed"
    if now.weekday() < 5:
        minutes = now.hour * 60 + now.minute
        if 4 * 60 <= minutes < 9 * 60 + 30:
            session = "pre"
        elif 9 * 60 + 30 <= minutes < 16 * 60:
            session = "regular"
        elif 16 * 60 <= minutes < 20 * 60:
            session = "post"
    return session, now.date(), now.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 解析纯函数
# ---------------------------------------------------------------------------

def _f(s: str | None, default: float = 0.0) -> float:
    """安全转 float。"""
    try:
        return float(s) if s not in (None, "") else default
    except (ValueError, TypeError):
        return default


def parse_tencent_us(raw: str, symbols: list[str]) -> dict[str, dict]:
    """解析腾讯美股批量响应（GBK 已解码文本），返回 {symbol: quote}。"""
    quotes: dict[str, dict] = {}
    for sym in symbols:
        m = re.search(rf'v_us{re.escape(sym)}="([^"]*)"', raw)
        if not m or not m.group(1):
            continue
        f = m.group(1).split("~")
        if len(f) < 36:
            continue
        price, pre_close = _f(f[3]), _f(f[4])
        change_pct = (price - pre_close) / pre_close * 100 if pre_close > 0 else _f(f[32])
        quotes[sym] = {
            "symbol": sym,
            "name": f[1],
            "price": price,
            "pre_close": pre_close,
            "change_pct": round(change_pct, 4),
            "high": _f(f[33]),
            "low": _f(f[34]),
            "currency": f[35] or "USD",
            "quote_time": f[30] if len(f) > 30 else "",
            "ext_price": 0.0,
            "ext_time": "",
        }
    return quotes


def parse_sina_us(raw: str, symbols: list[str]) -> dict[str, dict]:
    """解析新浪美股批量响应，返回 {symbol: {ext_price, ext_time, pre_close, price}}。"""
    result: dict[str, dict] = {}
    for sym in symbols:
        m = re.search(rf'var hq_str_gb_{re.escape(sym.lower())}="([^"]*)"', raw)
        if not m or not m.group(1):
            continue
        f = m.group(1).split(",")
        if len(f) < 27:
            continue
        result[sym] = {
            "price": _f(f[1]),
            "pre_close": _f(f[26]),
            "ext_price": _f(f[21]) if len(f) > 21 else 0.0,
            "ext_pct": _f(f[22]) if len(f) > 22 else 0.0,
            "ext_time": f[24].strip() if len(f) > 24 else "",
        }
    return result


def parse_fx_usdhkd(raw: str) -> float:
    """解析新浪 fx_susdhkd，返回 USD/HKD 汇率；失败/越界返回 0.0。"""
    m = re.search(r'var hq_str_fx_susdhkd="([^"]*)"', raw or "")
    if not m:
        return 0.0
    f = m.group(1).split(",")
    if len(f) < 2:
        return 0.0
    v = _f(f[1])
    return v if 7.5 <= v <= 8.5 else 0.0


def parse_ft_nav(data: dict) -> dict | None:
    """解析 FT GraphQL Overview 响应，返回 {nav, nav_date, change_pct, ytd_pct}。"""
    try:
        nav = data["data"]["Overview"]["shareclass"][0]["nav"]
        val = float(str(nav["navvalue"]).replace("$", "").replace(",", ""))
        dd, mm, yy = str(nav["navdate"]).split("/")
        return {
            "nav": val,
            "nav_date": date(int(yy), int(mm), int(dd)),
            "change_pct": float(str(nav["navchngpct"]).replace("%", "")),
            "ytd_pct": float(str(nav["ytdtotretatnav"]).replace("%", "")),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 盈亏计算（纯函数，所有金额统一 USD）
# ---------------------------------------------------------------------------

def build_positions(holdings: list[dict], us_quotes: dict[str, dict],
                    fund: dict | None, fx_usdhkd: float) -> tuple[list[dict], dict]:
    """计算每持仓盈亏与组合汇总。

    us_quotes: parse_tencent_us 结果（ext_price 已合并）
    fund: {"nav": float, "prev_nav": float, "nav_date": date, "nav_change_pct": float} 或 None
    返回 (positions, summary)
    """
    fx = fx_usdhkd if fx_usdhkd and 7.5 <= fx_usdhkd <= 8.5 else FX_FALLBACK
    positions: list[dict] = []
    total_value = total_cost = base_value = 0.0

    for h in holdings:
        sym, qty, cost = h["symbol"], float(h["quantity"]), float(h["cost_price"])
        if h["market"] == "HKFUND":
            if not fund or not fund.get("nav"):
                positions.append(_position(h, 0.0, None, None, None, "fund", True))
                continue
            nav, prev = fund["nav"], fund.get("prev_nav") or 0.0
            value = nav * qty / fx
            today = (nav - prev) * qty / fx if prev > 0 else 0.0
            pnl = (nav - cost) * qty / fx
            cost_usd = cost * qty / fx
            base_value += prev * qty / fx if prev > 0 else value
            positions.append(_position(h, nav, value, today, pnl, "fund", False,
                                       cost_usd=cost_usd, nav_date=str(fund.get("nav_date", "")),
                                       nav_change_pct=fund.get("nav_change_pct")))
        else:
            q = us_quotes.get(sym)
            if not q or not q.get("price"):
                positions.append(_position(h, 0.0, None, None, None, "none", True))
                continue
            price_now = q["ext_price"] if q.get("ext_price") else q["price"]
            ext = "pre/post" if q.get("ext_price") else "regular"
            value = price_now * qty
            today = (price_now - q["pre_close"]) * qty if q["pre_close"] > 0 else 0.0
            pnl = (price_now - cost) * qty
            base_value += q["pre_close"] * qty if q["pre_close"] > 0 else value
            positions.append(_position(h, price_now, value, today, pnl, ext, False,
                                       pre_close=q["pre_close"], quote_time=q.get("ext_time") or q.get("quote_time", "")))
        p = positions[-1]
        total_value += p["value"]
        total_cost += p["cost_usd"]

    day_pnl = sum(p["today_pnl"] for p in positions if p["today_pnl"])
    total_pnl = total_value - total_cost
    summary = {
        "total_value": round(total_value, 2),
        "cost_basis": round(total_cost, 2),
        "day_pnl": round(day_pnl, 2),
        "day_pnl_pct": round(day_pnl / base_value * 100, 4) if base_value > 0 else 0.0,
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / total_cost * 100, 4) if total_cost > 0 else 0.0,
        "base_value": round(base_value, 2),
    }
    return positions, summary


def _position(h: dict, price, value, today, pnl, ext, stale,
              cost_usd=None, pre_close=None, quote_time="", nav_date="", nav_change_pct=None):
    """构造单持仓输出行（内部辅助）。"""
    qty, cost = float(h["quantity"]), float(h["cost_price"])
    if cost_usd is None:
        cost_usd = cost * qty
    return {
        "id": h.get("id"),
        "symbol": h["symbol"], "name": h.get("name", ""), "market": h.get("market", "US"),
        "currency": h.get("currency", "USD"), "quantity": qty, "cost_price": cost,
        "price": price, "pre_close": pre_close,
        "change_pct": round((price - pre_close) / pre_close * 100, 4)
                      if (pre_close and pre_close > 0 and price) else None,
        "value": round(value, 2) if value is not None else 0.0,
        "today_pnl": round(today, 2) if today is not None else None,
        "position_pnl": round(pnl, 2) if pnl is not None else None,
        "position_pnl_pct": round(pnl / cost_usd * 100, 4) if (cost_usd > 0 and pnl is not None) else None,
        "cost_usd": round(cost_usd, 2),
        "ext": ext, "stale": stale,
        "quote_time": quote_time, "nav_date": nav_date, "nav_change_pct": nav_change_pct,
    }
```

- [ ] **Step 4: 运行测试至全绿**：`python3 -m pytest tests/test_portfolio.py -q` → 预期全部 PASS
- [ ] **Step 5: Commit** `git add portfolio_fetcher.py tests/test_portfolio.py && git commit -m "feat(portfolio): 采集纯函数(时段/解析/盈亏)与离线单测"`

---

### Task 3: portfolio_fetcher.py 网络函数

**Files:**
- Modify: `portfolio_fetcher.py`（文件末尾追加）

- [ ] **Step 1: 追加网络函数（httpx 直连 + GBK 解码 + 东财兜底节流 + FT 代理优先）：**

```python
# ---------------------------------------------------------------------------
# 网络函数（境内源直连；FT 优先走代理池）
# ---------------------------------------------------------------------------

async def _get_text(url: str, headers: dict | None = None, timeout: float = 10.0) -> str | None:
    """GET 并按 GBK 解码（腾讯/新浪），失败返回 None。"""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers=headers or {})
            if resp.status_code == 200:
                return resp.content.decode("gbk", errors="replace")
            logger.warning("GET %s → HTTP %d", url[:70], resp.status_code)
    except Exception as e:
        logger.warning("GET %s 失败: %s", url[:70], e)
    return None


async def fetch_us_quotes(symbols: list[str], session: str | None = None) -> dict[str, dict]:
    """抓取美股行情：腾讯批量主源 + 扩展时段新浪批量 + 东财兜底。

    返回 {symbol: quote}，quote 含 price/pre_close/ext_price/ext_time 等。
    """
    global _em_fail_streak, _last_em_used
    if not symbols:
        return {}
    if session is None:
        session, _, _ = us_market_session()

    raw = await _get_text(TENCENT_US_URL.format(symbols=",".join("us" + s for s in symbols)))
    quotes: dict[str, dict] = {}
    if raw:
        quotes = parse_tencent_us(raw, symbols)
        _em_fail_streak = 0
    else:
        _em_fail_streak += 1

    # 扩展时段合并新浪盘前盘后价
    if session in ("pre", "post") and quotes:
        raw2 = await _get_text(
            SINA_US_URL.format(symbols=",".join("gb_" + s.lower() for s in symbols)),
            headers=SINA_HEADERS,
        )
        if raw2:
            ext = parse_sina_us(raw2, symbols)
            for sym, q in quotes.items():
                e = ext.get(sym)
                if e and e.get("ext_price", 0) > 0:
                    q["ext_price"] = e["ext_price"]
                    q["ext_time"] = e["ext_time"]

    # 东财兜底：主源整体失败且连续>=3轮、距上次>=60s
    if not quotes and _em_fail_streak >= _EM_FAIL_THRESHOLD and time.monotonic() - _last_em_used > _EM_MIN_INTERVAL_SEC:
        _last_em_used = time.monotonic()
        secids = ",".join(f"{EM_MARKET_MAP.get(s, EM_DEFAULT_MARKET)}.{s}" for s in symbols)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(EM_ULIST_URL, params={
                    "secids": secids, "fields": EM_FIELDS, "fltt": "2", "invt": "2"})
                data = resp.json()
                for item in (data.get("data") or {}).get("diff") or []:
                    sym = item.get("f12")
                    if sym in symbols:
                        quotes[sym] = {
                            "symbol": sym, "name": item.get("f14", ""),
                            "price": _f(item.get("f2")), "pre_close": _f(item.get("f18")),
                            "change_pct": _f(item.get("f3")), "high": 0.0, "low": 0.0,
                            "currency": "USD", "quote_time": "",
                            "ext_price": 0.0, "ext_time": "",
                        }
                logger.info("东财兜底: %d/%d", len(quotes), len(symbols))
        except Exception as e:
            logger.warning("东财兜底失败: %s", e)

    return quotes


async def fetch_fx_usdhkd() -> tuple[float, bool]:
    """获取 USD/HKD 汇率。返回 (汇率, 是否兜底)。"""
    raw = await _get_text(SINA_FX_URL, headers=SINA_HEADERS)
    v = parse_fx_usdhkd(raw or "")
    if v > 0:
        return v, False
    return FX_FALLBACK, True


async def fetch_fund_nav(proxy_urls: list[str] | None = None) -> dict | None:
    """FT 官网 GraphQL 获取基金净值。优先代理（前2个），最后直连重试。"""
    payload = {"query": FT_FUND_QUERY, "variables": FT_FUND_VARS}
    candidates = list(proxy_urls or [])[:2] + [None]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.post(FT_PDS_URL, json=payload, headers=FT_HEADERS)
                if resp.status_code == 200:
                    parsed = parse_ft_nav(resp.json())
                    if parsed:
                        return parsed
                    last_err = "nav 解析失败"
                else:
                    last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("FT 基金净值获取失败: %s", last_err)
    return None
```

- [ ] **Step 2: 语法+回归**：`python3 -m py_compile portfolio_fetcher.py && python3 -m pytest tests/test_portfolio.py -q` → PASS
- [ ] **Step 3: Commit** `git add portfolio_fetcher.py && git commit -m "feat(portfolio): 行情/净值/汇率网络函数与东财兜底节流"`

---

### Task 4: server.py 验证码 + 登录改造

**Files:**
- Modify: `server.py`

- [ ] **Step 1: 顶部 import 区追加**（`import secrets` 已有，追加 `base64, io, random`）：

```python
import base64
import io
import random
```

- [ ] **Step 2: `SESSION_TTL` 改为 30 天**：`SESSION_TTL = 7 * 24 * 3600` → `SESSION_TTL = 30 * 24 * 3600  # 30 天有效期`

- [ ] **Step 3: 在 `_sessions` 定义附近追加验证码存储与登录限速：**

```python
# 验证码: captcha_id -> {"answer": int, "expires": float}
_captchas: dict[str, dict] = {}
CAPTCHA_TTL = 300  # 验证码 5 分钟有效
# 登录限速: ip -> [时间戳...]
_login_attempts: dict[str, list[float]] = {}
```

- [ ] **Step 4: `LoginInput` 增加字段：**

```python
class LoginInput(BaseModel):
    username: str
    password: str
    captcha_id: str = ""
    captcha_answer: str = ""
```

- [ ] **Step 5: 新增验证码生成与接口**（放在登录接口之前）：

```python
def _generate_captcha() -> tuple[int, str]:
    """生成算术验证码，返回 (答案, PNG base64)。"""
    from PIL import Image, ImageDraw, ImageFont
    a, b = random.randint(2, 19), random.randint(2, 19)
    if random.random() < 0.5:
        text, answer = f"{a}+{b}=?", a + b
    else:
        hi, lo = max(a, b), min(a, b)
        text, answer = f"{hi}-{lo}=?", hi - lo
    img = Image.new("RGB", (150, 46), (26, 26, 46))
    draw = ImageDraw.Draw(img)
    # 干扰线
    for _ in range(5):
        x1, y1 = random.randint(0, 150), random.randint(0, 46)
        x2, y2 = random.randint(0, 150), random.randint(0, 46)
        draw.line([(x1, y1), (x2, y2)], fill=(70, 70, 110), width=1)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 26)
    except Exception:
        font = ImageFont.load_default()
    # 逐字符随机偏移上色
    colors = [(233, 69, 96), (0, 184, 148), (253, 203, 110), (116, 185, 255)]
    x = 14
    for ch in text:
        draw.text((x, random.randint(4, 12)), ch, fill=random.choice(colors), font=font)
        x += 24
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return answer, base64.b64encode(buf.getvalue()).decode()


@app.get("/api/captcha")
async def captcha():
    """下发算术验证码（免鉴权，5 分钟有效，一次性）。"""
    captcha_id = secrets.token_hex(8)
    answer, b64 = _generate_captcha()
    _captchas[captcha_id] = {"answer": answer, "expires": time.time() + CAPTCHA_TTL}
    # 清理过期验证码
    for k in [k for k, v in _captchas.items() if time.time() > v["expires"]]:
        _captchas.pop(k, None)
    return {"ok": True, "captcha_id": captcha_id, "image": f"data:image/png;base64,{b64}"}
```

- [ ] **Step 6: `login()` 开头插入校验**（在 `user = await db.get_user(...)` 之前）：

```python
    # 验证码校验（一次性）
    cap = _captchas.pop(data.captcha_id, None) if data.captcha_id else None
    if not cap or time.time() > cap["expires"] or data.captcha_answer.strip() != str(cap["answer"]):
        return JSONResponse(status_code=401, content={"ok": False, "message": "验证码错误或已过期"})
    # 登录限速：每 IP 每分钟最多 10 次
    client_ip = request.client.host if request.client else "?"
    now_ts = time.time()
    attempts = [t for t in _login_attempts.get(client_ip, []) if now_ts - t < 60]
    if len(attempts) >= 10:
        return JSONResponse(status_code=429, content={"ok": False, "message": "尝试过于频繁，请稍后再试"})
    attempts.append(now_ts)
    _login_attempts[client_ip] = attempts
```

并把签名改为 `async def login(data: LoginInput, request: Request):`

- [ ] **Step 7: `_is_auth_path` 增加 `or path.startswith("/api/captcha")`**
- [ ] **Step 8: 验证**：`python3 -m py_compile server.py` → 无输出；`pip install pillow -q`（若未装）
- [ ] **Step 9: Commit** `git add server.py && git commit -m "feat(auth): 算术图形验证码+登录限速+30天会话"`

---

### Task 5: server.py 持仓 API + 后台任务 + 移除手动启停

**Files:**
- Modify: `server.py`、`config.py`（无，Task1 已改端口）

- [ ] **Step 1: import 区追加 `import portfolio_fetcher as pf` 与 `from datetime import date as _date`（按需）**

- [ ] **Step 2: 全局变量区（`global_index_cache` 附近）追加：**

```python
# 持仓模块状态
portfolio_cache: dict = {"ok": False, "positions": [], "summary": {}, "errors": []}
fund_nav_cache: dict | None = None   # {"nav","prev_nav","nav_date","nav_change_pct"}
fx_state: dict = {"rate": 7.80, "fallback": True, "time": ""}
portfolio_task: Optional[asyncio.Task] = None
fund_nav_task: Optional[asyncio.Task] = None
```

- [ ] **Step 3: lifespan 中启动/停止两个后台任务**——在 `candidate_refresh_task = asyncio.create_task(_candidate_refresh_loop())` 之后加：

```python
    global portfolio_task, fund_nav_task
    fund_nav_cache = await db.get_latest_fund_nav()
    fund_nav_task = asyncio.create_task(_fund_nav_loop())
    portfolio_task = asyncio.create_task(_portfolio_loop())
```

yield 之后（现有 cancel 清理区）追加：

```python
    if portfolio_task:
        portfolio_task.cancel()
    if fund_nav_task:
        fund_nav_task.cancel()
```

- [ ] **Step 4: 删除手动启停**：删除 `monitor_start`/`monitor_stop`/`monitor_set_mode` 三个路由函数（server.py 约 383-408 行）与全局 `monitor_mode` 变量（约 174 行）；全文件 grep `monitor_mode` 把 `"mode": monitor_mode` 字段与相关赋值删干净（`_do_monitor_stop`/`_broadcast`/`monitor_status` 中的引用）。

- [ ] **Step 5: 在监控循环之后追加两个后台任务与持仓路由：**

```python
# ---------------------------------------------------------------------------
# 持仓模块：行情采集循环 + 基金净值/汇率循环
# ---------------------------------------------------------------------------

async def _portfolio_loop():
    """持仓行情采集：盘中10s/扩展时段30s/休市600s，成功才写 tick。"""
    global portfolio_cache
    logger.info("持仓行情采集循环启动")
    while True:
        interval = 600.0
        try:
            session, et_date, et_now = pf.us_market_session()
            interval = {"regular": 10.0, "pre": 30.0, "post": 30.0}.get(session, 600.0)
            holdings = await db.get_holdings(enabled_only=True)
            us_symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            if not holdings:
                await asyncio.sleep(interval)
                continue
            quotes = await pf.fetch_us_quotes(us_symbols, session) if us_symbols else {}
            if quotes:
                try:
                    await db.update_holding_names({s: q["name"] for s, q in quotes.items() if q.get("name")})
                except Exception:
                    pass
            positions, summary = pf.build_positions(holdings, quotes, fund_nav_cache, fx_state["rate"])
            portfolio_cache = {
                "ok": bool(quotes),
                "fetch_time": datetime.now().strftime("%H:%M:%S"),
                "session": session,
                "et_now": et_now,
                "trade_date": str(et_date),
                "fx_usdhkd": fx_state["rate"],
                "fx_fallback": fx_state["fallback"],
                "fx_time": fx_state["time"],
                "positions": positions,
                "summary": summary,
                "errors": [] if quotes else ["行情源暂不可用"],
            }
            # 休市不写 tick（避免日线聚合混入休市平线）
            if quotes and session != "closed":
                await db.save_portfolio_tick(
                    datetime.now(), et_date, summary["total_value"], summary["day_pnl"],
                    summary["total_pnl"], summary["day_pnl_pct"], summary["total_pnl_pct"])
        except asyncio.CancelledError:
            logger.info("持仓行情采集循环已停止")
            break
        except Exception as e:
            logger.exception("持仓采集循环错误: %s", e)
        await asyncio.sleep(interval)


async def _fund_nav_loop():
    """基金净值+汇率：启动立即执行，此后每天北京时间 14:00/20:00；失败 30 分钟后重试。"""
    global fund_nav_cache, fx_state
    logger.info("基金净值/汇率任务启动")
    while True:
        ok = False
        try:
            rate, fallback = await pf.fetch_fx_usdhkd()
            fx_state = {"rate": rate, "fallback": fallback,
                        "time": datetime.now().strftime("%m-%d %H:%M")}
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            nav = await pf.fetch_fund_nav(proxies)
            if nav:
                await db.upsert_fund_nav(nav["nav_date"], nav["nav"], nav["change_pct"])
                logger.info("基金净值已更新: %s = %s", nav["nav_date"], nav["nav"])
            latest = await db.get_latest_fund_nav()
            if latest:
                fund_nav_cache = latest
            ok = nav is not None
        except asyncio.CancelledError:
            logger.info("基金净值任务已停止")
            break
        except Exception as e:
            logger.exception("基金净值任务错误: %s", e)
        if not ok:
            await asyncio.sleep(1800)
            continue
        now = datetime.now()
        targets = [now.replace(hour=14, minute=0, second=0, microsecond=0),
                   now.replace(hour=20, minute=0, second=0, microsecond=0)]
        nxt = next((t for t in targets if t > now), targets[0] + timedelta(days=1))
        await asyncio.sleep((nxt - now).total_seconds())


# ---------------------------------------------------------------------------
# Portfolio API (持仓)
# ---------------------------------------------------------------------------

class HoldingInput(BaseModel):
    symbol: str
    name: str = ""
    market: str = "US"
    quantity: float = 0
    cost_price: float = 0
    currency: str = "USD"


class HoldingUpdateInput(BaseModel):
    name: str = ""
    market: str = "US"
    quantity: float = 0
    cost_price: float = 0
    currency: str = "USD"
    enabled: int = 1
    sort_order: int = 0


@app.get("/api/portfolio/overview")
async def portfolio_overview():
    """持仓总览（读缓存，含汇总/持仓明细/时段/汇率）。"""
    data = dict(portfolio_cache)
    data["fund"] = fund_nav_cache
    return data


@app.get("/api/portfolio/intraday")
async def portfolio_intraday(trade_date: str = Query(default="")):
    """当日（或指定美东交易日）分时点。"""
    if not trade_date:
        trade_date = portfolio_cache.get("trade_date") or datetime.now().strftime("%Y-%m-%d")
    rows = await db.get_portfolio_intraday(trade_date)
    return {"ok": True, "trade_date": trade_date, "points": [
        {"ts": r["ts"].strftime("%H:%M:%S") if isinstance(r["ts"], datetime) else str(r["ts"]),
         "v": float(r["total_value_usd"]), "p": float(r["day_pnl_usd"])} for r in rows]}


@app.get("/api/portfolio/daily")
async def portfolio_daily(range: str = Query(default="30")):
    """历史日线（每美东交易日最后一条采样）。range: 30/90/all。"""
    days = 3650 if range == "all" else (90 if range == "90" else 30)
    rows = await db.get_portfolio_daily(days)
    return {"ok": True, "days": [
        {"date": str(r["date"]), "v": float(r["total_value"]),
         "p": float(r["day_pnl"]), "pct": float(r["total_pnl_pct"])} for r in rows]}


@app.get("/api/portfolio/holding")
async def holding_list():
    """持仓列表（编辑用，含停用）。"""
    rows = await db.get_holdings(enabled_only=False)
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/portfolio/holding")
async def holding_add(data: HoldingInput):
    symbol = data.symbol.strip().upper()
    if not symbol:
        return JSONResponse(status_code=400, content={"ok": False, "message": "symbol 不能为空"})
    await db.upsert_holding(symbol, data.name.strip(), data.market.strip() or "US",
                            data.quantity, data.cost_price, data.currency.strip() or "USD")
    return {"ok": True}


@app.put("/api/portfolio/holding/{holding_id}")
async def holding_update(holding_id: int, data: HoldingUpdateInput):
    affected = await db.update_holding(holding_id, data.name.strip(), data.market.strip() or "US",
                                       data.quantity, data.cost_price, data.currency.strip() or "USD",
                                       data.enabled, data.sort_order)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "holding not found"})


@app.delete("/api/portfolio/holding/{holding_id}")
async def holding_delete(holding_id: int):
    affected = await db.delete_holding(holding_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "holding not found"})
```

- [ ] **Step 6: 验证**：`python3 -m py_compile server.py && grep -n "monitor_mode" server.py` → 无残留；`python3 -m pytest tests/ -q` → 现有测试全绿
- [ ] **Step 7: Commit** `git add server.py && git commit -m "feat(portfolio): 持仓API+双后台任务，移除手动启停改为全自动"`

---

### Task 6: 前端三文件

**Files:**
- Create: `static/portfolio.html`
- Modify: `static/index.html`（header 加持仓入口 + 删启停按钮/JS）
- Modify: `static/login.html`（加验证码）

- [ ] **Step 1: 创建 `static/portfolio.html`（完整文件）：**

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>我的持仓</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  background: #1a1a2e; color: #e0e0e0; font-size: 14px; min-height: 100vh; }
.header { background: #16213e; padding: 12px 20px; display: flex; align-items: center; gap: 16px;
  border-bottom: 2px solid #e94560; flex-wrap: wrap; }
.header-title { font-size: 18px; font-weight: bold; color: #e94560; }
.btn { padding: 6px 14px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; }
.btn:hover { opacity: .85; }
.btn-ghost { background: transparent; border: 1px solid #555; color: #e0e0e0; }
.btn-primary { background: #e94560; color: #fff; }
.status-area { margin-left: auto; display: flex; align-items: center; gap: 10px; font-size: 13px; color: #aaa; }
.badge { padding: 2px 10px; border-radius: 10px; font-size: 12px; }
.badge-pre { background: #3d3d7a; color: #a6a6ff; }
.badge-regular { background: #1a4a2e; color: #00b894; }
.badge-post { background: #4a3a1a; color: #fdcb6e; }
.badge-closed { background: #333; color: #888; }
.content { padding: 16px 20px; max-width: 1200px; margin: 0 auto; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 16px; }
.card { background: #16213e; border: 1px solid #2a2a4a; border-radius: 8px; padding: 16px 18px; }
.card-label { font-size: 12px; color: #888; margin-bottom: 6px; }
.card-value { font-size: 26px; font-weight: bold; }
.card-sub { font-size: 12px; margin-top: 4px; color: #aaa; }
.text-red { color: #e94560; } .text-green { color: #00b894; }
.chart-box { background: #16213e; border: 1px solid #2a2a4a; border-radius: 8px; padding: 14px 16px; margin-bottom: 16px; }
.chart-head { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }
.chart-title { font-size: 14px; font-weight: bold; }
.range-btn { padding: 2px 10px; font-size: 12px; background: transparent; border: 1px solid #444;
  color: #888; border-radius: 4px; cursor: pointer; }
.range-btn.active { border-color: #e94560; color: #e94560; }
canvas { width: 100%; height: 220px; display: block; }
table { width: 100%; border-collapse: collapse; background: #16213e; border-radius: 8px; overflow: hidden; }
th, td { padding: 10px 12px; text-align: right; border-bottom: 1px solid #232345; font-size: 13px; white-space: nowrap; }
th { color: #888; font-weight: 500; background: #1b1b38; }
th:first-child, td:first-child { text-align: left; }
.sym { color: #888; font-size: 12px; }
.tag { font-size: 10px; padding: 1px 6px; border-radius: 3px; margin-left: 6px; }
.tag-pre { background: #3d3d7a; } .tag-fund { background: #145a32; color: #7bed9f; }
.tag-stale { background: #555; color: #ddd; }
.modal-mask { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.6); z-index: 10;
  align-items: center; justify-content: center; }
.modal-mask.show { display: flex; }
.modal { background: #16213e; border: 1px solid #2a2a4a; border-radius: 8px; padding: 20px;
  width: 640px; max-height: 80vh; overflow: auto; }
.modal h3 { color: #e94560; margin-bottom: 12px; }
.modal table td input { width: 100%; }
.form-row { display: flex; gap: 10px; margin: 8px 0; flex-wrap: wrap; }
.form-row input, .form-row select { flex: 1; min-width: 90px; padding: 7px 9px; background: #1a1a2e;
  border: 1px solid #3a3a5a; border-radius: 4px; color: #e0e0e0; }
.editable td:hover { background: #1e1e40; cursor: pointer; }
.link { color: #e94560; cursor: pointer; text-decoration: none; }
</style>
</head>
<body>
<div class="header">
  <a class="link" href="/static/index.html">‹ 监控</a>
  <div class="header-title">我的持仓</div>
  <span class="badge badge-closed" id="sessionBadge">--</span>
  <button class="btn btn-primary" onclick="openEditor()">编辑持仓</button>
  <div class="status-area">
    <span id="fetchTime">--:--:--</span>
    <span id="fxInfo"></span>
    <a class="link" href="#" onclick="logout();return false;">登出</a>
  </div>
</div>

<div class="content">
  <div class="cards">
    <div class="card"><div class="card-label">总资产 (USD)</div>
      <div class="card-value" id="totalValue">$0.00</div>
      <div class="card-sub" id="costInfo"></div></div>
    <div class="card"><div class="card-label">今日盈亏</div>
      <div class="card-value" id="dayPnl">$0.00</div>
      <div class="card-sub" id="dayPnlPct"></div></div>
    <div class="card"><div class="card-label">持仓盈亏</div>
      <div class="card-value" id="totalPnl">$0.00</div>
      <div class="card-sub" id="totalPnlPct"></div></div>
  </div>

  <div class="chart-box">
    <div class="chart-head">
      <div class="chart-title">总资产走势</div>
      <div style="margin-left:auto">
        <button class="range-btn active" data-r="intraday" onclick="switchRange('intraday',this)">当日分时</button>
        <button class="range-btn" data-r="30" onclick="switchRange('30',this)">1月</button>
        <button class="range-btn" data-r="90" onclick="switchRange('90',this)">3月</button>
        <button class="range-btn" data-r="all" onclick="switchRange('all',this)">全部</button>
      </div>
    </div>
    <canvas id="chart" width="1100" height="220"></canvas>
  </div>

  <table id="posTable">
    <thead><tr>
      <th>名称/代码</th><th>现价</th><th>涨跌幅</th><th>市值(USD)</th>
      <th>今日盈亏</th><th>持仓盈亏</th><th>盈亏比例</th><th>成本价</th><th>数量</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>

<div class="modal-mask" id="editorMask">
  <div class="modal">
    <h3>编辑持仓</h3>
    <table id="editTable"><thead><tr>
      <th>代码</th><th>名称</th><th>市场</th><th>数量</th><th>成本价</th><th>币种</th><th>启用</th><th></th>
    </tr></thead><tbody></tbody></table>
    <div class="form-row" id="newRow">
      <input id="n_symbol" placeholder="代码 如 MU">
      <input id="n_name" placeholder="名称">
      <select id="n_market"><option value="US">US</option><option value="HKFUND">HKFUND</option></select>
      <input id="n_quantity" placeholder="数量" type="number" step="any">
      <input id="n_cost" placeholder="成本价" type="number" step="any">
      <select id="n_currency"><option>USD</option><option>HKD</option></select>
      <button class="btn btn-primary" onclick="addHolding()">新增</button>
    </div>
    <div style="text-align:right;margin-top:10px">
      <button class="btn btn-ghost" onclick="closeEditor()">关闭</button>
    </div>
  </div>
</div>

<script>
const SESSION_LABEL = {pre:'盘前', regular:'盘中', post:'盘后', closed:'休市'};
const fmt = (v, d=2) => (v===null||v===undefined) ? '--' : Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});
const usd = v => (v===null||v===undefined) ? '--' : (v<0?'-$':'$') + fmt(Math.abs(v));
const cls = v => v>0 ? 'text-red' : (v<0 ? 'text-green' : '');
const sign = v => v>0?'+':'';

async function logout() {
  await fetch('/api/logout', {method:'POST'});
  location.href = '/static/login.html';
}

function renderOverview(d) {
  if (!d || !d.ok && !d.positions) return;
  const s = d.summary || {};
  document.getElementById('totalValue').textContent = usd(s.total_value);
  document.getElementById('costInfo').textContent = '成本 $' + fmt(s.cost_basis);
  const dp = document.getElementById('dayPnl');
  dp.textContent = sign(s.day_pnl) + usd(s.day_pnl); dp.className = 'card-value ' + cls(s.day_pnl);
  document.getElementById('dayPnlPct').textContent = sign(s.day_pnl_pct) + fmt(s.day_pnl_pct) + '%';
  document.getElementById('dayPnlPct').className = 'card-sub ' + cls(s.day_pnl);
  const tp = document.getElementById('totalPnl');
  tp.textContent = sign(s.total_pnl) + usd(s.total_pnl); tp.className = 'card-value ' + cls(s.total_pnl);
  document.getElementById('totalPnlPct').textContent = sign(s.total_pnl_pct) + fmt(s.total_pnl_pct) + '%';
  document.getElementById('totalPnlPct').className = 'card-sub ' + cls(s.total_pnl);
  const badge = document.getElementById('sessionBadge');
  badge.textContent = SESSION_LABEL[d.session] || d.session;
  badge.className = 'badge badge-' + (d.session || 'closed');
  document.getElementById('fetchTime').textContent = d.fetch_time || '--';
  document.getElementById('fxInfo').textContent = 'USD/HKD ' + fmt(d.fx_usdhkd, 4) + (d.fx_fallback ? '(默认)' : '');

  const tbody = document.querySelector('#posTable tbody');
  tbody.innerHTML = '';
  (d.positions || []).forEach(p => {
    const tr = document.createElement('tr');
    let tags = '';
    if (p.market === 'HKFUND') tags += '<span class="tag tag-fund">官方日频</span>';
    else if (p.ext === 'pre/post') tags += '<span class="tag tag-pre">扩展时段</span>';
    if (p.stale) tags += '<span class="tag tag-stale">无行情</span>';
    const sub = p.market === 'HKFUND' ? (p.nav_date || '') : (p.quote_time || '');
    tr.innerHTML = `<td><div>${p.name||p.symbol}${tags}</div><div class="sym">${p.symbol} ${sub}</div></td>
      <td>${p.price ? fmt(p.price, p.market==='HKFUND'?4:2) : '--'}</td>
      <td class="${cls(p.change_pct)}">${p.change_pct===null?'--':sign(p.change_pct)+fmt(p.change_pct)+'%'}</td>
      <td>$${fmt(p.value)}</td>
      <td class="${cls(p.today_pnl)}">${p.today_pnl===null?'--':sign(p.today_pnl)+'$'+fmt(Math.abs(p.today_pnl))}</td>
      <td class="${cls(p.position_pnl)}">${p.position_pnl===null?'--':sign(p.position_pnl)+'$'+fmt(Math.abs(p.position_pnl))}</td>
      <td class="${cls(p.position_pnl_pct)}">${p.position_pnl_pct===null?'--':sign(p.position_pnl_pct)+fmt(p.position_pnl_pct)+'%'}</td>
      <td>${fmt(p.cost_price)}</td><td>${fmt(p.quantity,0)}</td>`;
    tbody.appendChild(tr);
  });
}

let chartData = null, chartMode = 'intraday';
function switchRange(mode, btn) {
  chartMode = mode;
  document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadChart();
}
async function loadChart() {
  let url = chartMode === 'intraday' ? '/api/portfolio/intraday' : '/api/portfolio/daily?range=' + chartMode;
  try {
    const d = await (await fetch(url)).json();
    chartData = chartMode === 'intraday'
      ? (d.points || []).map(p => ({x: p.ts, y: p.v}))
      : (d.days || []).map(p => ({x: p.date, y: p.v}));
    drawChart();
  } catch (e) { console.warn(e); }
}
function drawChart() {
  const cv = document.getElementById('chart'), ctx = cv.getContext('2d');
  const W = cv.width = cv.clientWidth * 2, H = cv.height = 440;
  ctx.clearRect(0, 0, W, H);
  const pts = chartData || [];
  if (pts.length < 2) {
    ctx.fillStyle = '#666'; ctx.font = '28px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(pts.length ? '数据积累中…' : '暂无历史数据', W/2, H/2);
    return;
  }
  const xs = pts.map(p => p.x), ys = pts.map(p => p.y);
  let ymin = Math.min(...ys), ymax = Math.max(...ys);
  if (ymax - ymin < 1e-9) { ymax += 1; ymin -= 1; }
  const pad = (ymax - ymin) * 0.1; ymin -= pad; ymax += pad;
  const L = 130, R = 30, T = 20, B = 40;
  const px = i => L + (W - L - R) * i / (pts.length - 1);
  const py = v => T + (H - T - B) * (1 - (v - ymin) / (ymax - ymin));
  // 网格与Y轴刻度
  ctx.strokeStyle = '#232345'; ctx.fillStyle = '#888'; ctx.font = '20px sans-serif'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const v = ymin + (ymax - ymin) * i / 4, y = py(v);
    ctx.beginPath(); ctx.moveTo(L, y); ctx.lineTo(W - R, y); ctx.stroke();
    ctx.fillText('$' + fmt(v, 0), L - 10, y + 7);
  }
  // 首值虚线基准
  ctx.setLineDash([8, 6]); ctx.strokeStyle = '#444';
  ctx.beginPath(); ctx.moveTo(L, py(ys[0])); ctx.lineTo(W - R, py(ys[0])); ctx.stroke();
  ctx.setLineDash([]);
  // 折线（高于首值红、低于绿）
  const up = ys[ys.length-1] >= ys[0];
  ctx.strokeStyle = up ? '#e94560' : '#00b894'; ctx.lineWidth = 3; ctx.beginPath();
  pts.forEach((p, i) => i ? ctx.lineTo(px(i), py(p.y)) : ctx.moveTo(px(i), py(p.y)));
  ctx.stroke();
  // X轴标签（等间隔取5个）
  ctx.textAlign = 'center'; ctx.fillStyle = '#888';
  for (let i = 0; i < 5; i++) {
    const idx = Math.round((pts.length - 1) * i / 4);
    ctx.fillText(String(xs[idx]), px(idx), H - 12);
  }
}

async function refresh() {
  try { renderOverview(await (await fetch('/api/portfolio/overview')).json()); }
  catch (e) { console.warn(e); }
}

// ---- 编辑器 ----
let editRows = [];
async function openEditor() {
  editRows = await (await fetch('/api/portfolio/holding')).json();
  renderEditTable();
  document.getElementById('editorMask').classList.add('show');
}
function closeEditor() { document.getElementById('editorMask').classList.remove('show'); refresh(); }
function renderEditTable() {
  const tb = document.querySelector('#editTable tbody'); tb.innerHTML = '';
  editRows.forEach((h, i) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${h.symbol}</td>
      <td><input value="${h.name||''}" data-k="name"></td>
      <td><select data-k="market"><option ${h.market==='US'?'selected':''}>US</option><option ${h.market==='HKFUND'?'selected':''}>HKFUND</option></select></td>
      <td><input type="number" step="any" value="${h.quantity}" data-k="quantity"></td>
      <td><input type="number" step="any" value="${h.cost_price}" data-k="cost_price"></td>
      <td><select data-k="currency"><option ${h.currency==='USD'?'selected':''}>USD</option><option ${h.currency==='HKD'?'selected':''}>HKD</option></select></td>
      <td><input type="checkbox" ${h.enabled?'checked':''} data-k="enabled"></td>
      <td><button class="btn btn-ghost" onclick="saveRow(${i})">存</button>
          <button class="btn btn-ghost" onclick="delRow(${h.id})">删</button></td>`;
    tr.querySelectorAll('[data-k]').forEach(el => el.addEventListener('change', () => {
      editRows[i][el.dataset.k] = el.type === 'checkbox' ? (el.checked ? 1 : 0) : el.value;
    }));
    tb.appendChild(tr);
  });
}
async function saveRow(i) {
  const h = editRows[i];
  await fetch('/api/portfolio/holding/' + h.id, {method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({name:h.name, market:h.market, quantity:Number(h.quantity),
      cost_price:Number(h.cost_price), currency:h.currency, enabled:Number(h.enabled?1:0), sort_order:h.sort_order||0})});
  openEditor();
}
async function delRow(id) {
  await fetch('/api/portfolio/holding/' + id, {method:'DELETE'});
  openEditor();
}
async function addHolding() {
  await fetch('/api/portfolio/holding', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol:v('n_symbol'), name:v('n_name'), market:v('n_market'),
      quantity:Number(v('n_quantity')||0), cost_price:Number(v('n_cost')||0), currency:v('n_currency')})});
  openEditor();
}
const v = id => document.getElementById(id).value.trim();

refresh(); loadChart();
setInterval(refresh, 10000);
setInterval(() => { if (chartMode === 'intraday') loadChart(); }, 30000);
</script>
</body>
</html>
```

- [ ] **Step 2: `index.html` 修改**：
  1. 删除 344 行启停按钮：`<button class="btn btn-start" id="monitorBtn" onclick="toggleMonitor()">开始监控</button>`
  2. 同位置加：`<a class="btn btn-ghost" href="/static/portfolio.html" style="text-decoration:none;display:inline-block;line-height:28px">我的持仓</a>`
  3. 删除 JS 函数 `toggleMonitor()`（约 748-785 行）与 `updateMonitorButton()`（约 780 行起，grep `monitorBtn` 清干净所有引用，引用处改为空操作或删除所在行）
  4. grep `monitor/mode`，若存在模式切换按钮/JS 一并删除

- [ ] **Step 3: `login.html` 修改**：密码 form-group 之后追加：

```html
      <div class="form-group">
        <label class="form-label" for="captcha">验证码</label>
        <div style="display:flex;gap:10px;align-items:center">
          <input type="text" id="captcha" class="form-input" placeholder="计算结果" required
                 style="flex:1" autocomplete="off">
          <img id="captchaImg" alt="验证码" title="点击刷新"
               style="height:40px;border-radius:4px;cursor:pointer;background:#1a1a2e">
        </div>
      </div>
```

JS 部分：`doLogin` 之前加载验证码并绑定刷新；`doLogin` 的 body 改为携带 `captcha_id`/`captcha_answer`：

```javascript
let captchaId = '';
async function loadCaptcha() {
  try {
    const d = await (await fetch('/api/captcha')).json();
    captchaId = d.captcha_id;
    document.getElementById('captchaImg').src = d.image;
  } catch (e) { console.warn(e); }
}
document.getElementById('captchaImg').addEventListener('click', loadCaptcha);
loadCaptcha();
```

body 修改为 `JSON.stringify({ username, password, captcha_id: captchaId, captcha_answer: document.getElementById('captcha').value.trim() })`，登录失败分支追加 `loadCaptcha();`（刷新验证码）。

- [ ] **Step 4: Commit** `git add static/ && git commit -m "feat(portfolio): 券商风格持仓页+登录验证码前端，移除启停按钮"`

---

### Task 7: 本地集成验证

- [ ] **Step 1: 安装依赖**：`pip install pillow -q`（若未装）
- [ ] **Step 2: 全量测试**：`python3 -m pytest tests/ -q` → 全绿（现有 6 个测试文件 + 新增 test_portfolio.py）
- [ ] **Step 3: 启动本地服务**（本地 8000 若被旧进程占用先 `kill`）：`nohup python3 -m uvicorn server:app --host 0.0.0.0 --port 8888 > /tmp/qdii_test.log 2>&1 &`，sleep 5
- [ ] **Step 4: 接口链路验证**（curl 断言）：
  - `curl -s http://127.0.0.1:8888/api/captcha | head -c 120` → 含 `captcha_id`
  - `curl -s -c /tmp/ck.txt -X POST http://127.0.0.1:8888/api/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"hello9987","captcha_id":"x","captcha_answer":"x"}'` → 401 验证码错误（负例）
  - 用 python 脚本算出验证码答案后正例登录 → `{"ok":true}`（写个小脚本：requests 取 captcha，人工无法介入——改为直接调用 `server._generate_captcha` 单元验证 + 浏览器人工正例，或脚本解析已知答案：`_generate_captcha` 返回答案，可通过 pytest 直接测）
  - `curl -s -b /tmp/ck.txt http://127.0.0.1:8888/api/portfolio/overview | python3 -m json.tool | head -40` → 含 positions（7 条）
  - `curl -s http://127.0.0.1:8888/api/quote` → 现有候选池接口正常（回归）
- [ ] **Step 5: 停测试服务**：`pkill -f "uvicorn server:app --host 0.0.0.0 --port 8888"`
- [ ] **Step 6: Commit（如有修补）** `git add -A && git commit -m "test(portfolio): 本地集成验证修补"`

---

### Task 8: 部署上线（<server-ip>:8888）【主Agent执行，不委派】

- [ ] **Step 1: rsync 代码**：
```bash
ssh root@<server-ip> "mkdir -p /opt/qdii"
rsync -az --delete --exclude '.git' --exclude 'akshare-code' --exclude 'akshare-docs' \
  --exclude 'juejin' --exclude 'StockWatcher' --exclude '__pycache__' --exclude '.idea' \
  --exclude '.pytest_cache' --exclude 'uvicorn.log' --exclude '.codegraph' --exclude '.claude' \
  --exclude '.mcp.json' --exclude 'qdii.db' --exclude 'docs' \
  /root/workspace/project/qdii/ root@<server-ip>:/opt/qdii/
```
（docs 体积小可留；--delete 前先确认目标目录无他物）
- [ ] **Step 2: 服务器环境**：
```bash
ssh root@<server-ip> "cd /opt/qdii && python3 -m venv venv && venv/bin/pip install -q -r requirements.txt && venv/bin/python -c 'import fastapi, httpx, aiomysql, PIL; print(\"deps ok\")'"
```
- [ ] **Step 3: 验证 DB 本机可达**：`ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e 'SELECT COUNT(*) FROM holdings'"`（预期 7；不通则 systemd unit 不设 DB_HOST=127.0.0.1，沿用默认公网地址）
- [ ] **Step 4: systemd unit** `/etc/systemd/system/qdii.service`：
```ini
[Unit]
Description=QDII Portfolio Monitor
After=network.target

[Service]
WorkingDirectory=/opt/qdii
Environment=DB_HOST=127.0.0.1
ExecStart=/opt/qdii/venv/bin/python -m uvicorn server:app --host 0.0.0.0 --port 8888
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
`systemctl daemon-reload && systemctl enable --now qdii`
- [ ] **Step 5: 代理池种子**（幂等）：
```bash
ssh root@<server-ip> "mysql -h127.0.0.1 -P3914 -u<db-user> -p"$QDII_DB_PASS" qdii -e \"
INSERT IGNORE INTO proxy_pool (url, latency_ms, alive) VALUES
('http://proxyuser:proxypass@proxy.test:7779', 800, 1),
('http://proxyuser:proxypass@proxy.test:7779', 900, 1),
('http://proxyuser:proxypass@proxy.test:7779', 950, 1);\""
```
- [ ] **Step 6: 连通性检测目标改造**（本任务前置小改，主Agent完成）：server.py `proxy_check` 的 `https://www.baidu.com` 改为 `https://ipinfo.io/ip`（海外代理用 baidu 会误判）
- [ ] **Step 7: 上线验证**：
```bash
curl -s http://<server-ip>:8888/api/captcha | head -c 100
curl -s http://<server-ip>:8888/api/quote | head -c 100
ssh root@<server-ip> "journalctl -u qdii -n 50 --no-pager"
```
浏览器人工验证：登录页验证码、admin/hello9987 登录、持仓页 7 条有行情、指数监控页正常
- [ ] **Step 8: 停本地服务**：`kill 2365780`（本地 8000 旧 uvicorn）；确认 `ss -tlnp | grep 8000` 为空
- [ ] **Step 9: 最终 Commit** `git add -A && git commit -m "chore(deploy): 上线<server-ip>:8888并停本地服务"`

---

## Self-Review 记录

- **Spec 覆盖**：3表(Task1)、纯函数+解析(Task2)、网络+兜底+FT+汇率(Task3)、验证码/30天/限速(Task4)、API+后台任务+启停移除(Task5)、前端三件(Task6)、集成验证(Task7)、部署+代理种子+停本地(Task8) ✅
- **占位符扫描**：无 TBD/TODO；所有代码完整 ✅
- **类型一致性**：`build_positions(holdings, us_quotes, fund, fx_usdhkd)` 签名 Task2 定义、Task5 调用一致；`fetch_us_quotes(symbols, session)`、`fetch_fx_usdhkd()->(float,bool)`、`fetch_fund_nav(proxies)` 一致；DB 函数名与 CRUD 调用一致 ✅
- **已知取舍**：EM 兜底对未知符号默认 105 市场（spec 允许）；验证码正例登录走浏览器人工+单元测试覆盖
