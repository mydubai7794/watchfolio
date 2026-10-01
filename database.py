"""异步 MySQL CRUD 模块（aiomysql）"""
import logging
import aiomysql
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_POOL_SIZE

logger = logging.getLogger(__name__)

_pool: aiomysql.Pool = None

# ---------------------------------------------------------------------------
# 连接池管理
# ---------------------------------------------------------------------------

async def get_pool() -> aiomysql.Pool:
    """获取连接池，若未创建则自动创建。"""
    global _pool
    if _pool is None or _pool._closed:
        _pool = await aiomysql.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            maxsize=DB_POOL_SIZE,
            autocommit=True,
            charset="utf8mb4",
        )
        logger.info("MySQL 连接池已创建 (host=%s, db=%s)", DB_HOST, DB_NAME)
    return _pool


async def close_pool():
    """关闭连接池。"""
    global _pool
    if _pool is not None and not _pool._closed:
        _pool.close()
        await _pool.wait_closed()
        logger.info("MySQL 连接池已关闭")
        _pool = None

# ---------------------------------------------------------------------------
# 通用执行辅助
# ---------------------------------------------------------------------------

async def execute(sql: str, args=None, _pool=None):
    """执行写操作 (INSERT / UPDATE / DELETE)，返回 affected rows。"""
    pool = _pool or await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            return cur.rowcount


async def execute_dict(sql: str, args=None, _pool=None):
    """执行查询，返回 list[dict]。"""
    pool = _pool or await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, args)
            return await cur.fetchall()

# ---------------------------------------------------------------------------
# 建表 & 种子配置
# ---------------------------------------------------------------------------

def _safe_decimal(value, decimal_places: int = 4) -> float:
    """确保值安全写入 MySQL DECIMAL 列：处理 NaN/Inf，四舍五入到指定精度。"""
    if value is None:
        return 0
    try:
        v = float(value)
    except (ValueError, TypeError):
        return 0
    # NaN / Inf → 0
    if v != v or abs(v) == float('inf'):
        return 0
    return round(v, decimal_places)


async def _alter_widen_columns(pool):
    """将 history_ticks 中容易溢出的 DECIMAL 列加宽（幂等）。"""
    alterations = [
        ("change_pct",  "DECIMAL(10,4)"),
        ("premium_pct", "DECIMAL(10,4)"),
        ("ratio",       "DECIMAL(14,4)"),
        ("premium",     "DECIMAL(10,4)"),
        ("weibi",       "DECIMAL(10,4)"),
    ]
    for col, new_type in alterations:
        try:
            await execute(
                f"ALTER TABLE history_ticks MODIFY COLUMN `{col}` {new_type} DEFAULT 0",
                _pool=pool,
            )
        except Exception as e:
            logger.warning("ALTER TABLE history_ticks MODIFY COLUMN %s 失败(可能已是最新): %s", col, e)

async def _alter_add_rec_columns(pool):
    """为 history_ticks 添加 rec_action / rec_price 列（幂等）。"""
    new_cols = [
        ("rec_action", "VARCHAR(4) DEFAULT '' COMMENT '推荐操作(Buy/Sell)'"),
        ("rec_price",  "DECIMAL(10,3) DEFAULT 0 COMMENT '推荐操作价位'"),
    ]
    for col, col_type in new_cols:
        try:
            await execute(
                f"ALTER TABLE history_ticks ADD COLUMN `{col}` {col_type}",
                _pool=pool,
            )
        except Exception as e:
            if "Duplicate column" not in str(e):
                logger.warning("ALTER TABLE history_ticks ADD COLUMN %s 失败: %s", col, e)


async def init_tables():
    """创建所有业务表并插入种子配置（幂等）。"""
    pool = await get_pool()

    # ---- history_ticks (回测数据) ----
    await init_history_ticks_table()

    # ---- etf_targets ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS etf_targets (
            code        VARCHAR(6)   PRIMARY KEY COMMENT 'ETF代码',
            name        VARCHAR(20)  NOT NULL COMMENT 'ETF名称',
            futures_index VARCHAR(20) NOT NULL COMMENT '关联期指标识 NQ/ES/YM/DAX/NK',
            prev_nav    DECIMAL(8,4) DEFAULT 0 COMMENT '上一交易日净值估算',
            prev_close  DECIMAL(10,4) DEFAULT 0 COMMENT '前一个交易日收盘价(自动维护)',
            enabled     TINYINT(1)   DEFAULT 1 COMMENT '1=启用 0=停用',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='监控的ETF列表'
        """,
        _pool=pool,
    )

    # ---- proxy_pool ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS proxy_pool (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            url         VARCHAR(100) NOT NULL UNIQUE COMMENT '代理地址',
            latency_ms  INT          DEFAULT 0 COMMENT '延迟(毫秒)',
            alive       TINYINT(1)   DEFAULT 1 COMMENT '1=可用 0=不可用',
            last_check  DATETIME     NULL COMMENT '最后检测时间',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='代理IP池'
        """,
        _pool=pool,
    )

    # ---- sys_config ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS sys_config (
            `key`   VARCHAR(50)  PRIMARY KEY COMMENT '配置键',
            value   VARCHAR(200) NOT NULL COMMENT '配置值',
            remark  VARCHAR(100) DEFAULT '' COMMENT '备注'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统配置'
        """,
        _pool=pool,
    )

    # ---- users (登录用户表，密码明文存储方便修改) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            username    VARCHAR(50)  NOT NULL UNIQUE COMMENT '用户名',
            password    VARCHAR(100) NOT NULL COMMENT '密码(明文)',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录用户'
        """,
        _pool=pool,
    )

    # ---- 种子用户 admin / hello9987 (幂等) ----
    await execute(
        "INSERT IGNORE INTO users (username, password) VALUES ('admin', 'hello9987')",
        _pool=pool,
    )

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

    # ---- 种子配置 (INSERT IGNORE 保证幂等) ----
    seed_configs = [
        # 旧模型参数（保留，但新模型不再使用）
        ("w_ratio",                "0.7",   "溢价得分权重(旧模型)"),
        ("w_premium",              "0.3",   "偏离率得分权重(旧模型)"),
        ("futures_kill_threshold", "-1.5",  "期指跌幅熔断阈值(旧模型)"),
        ("safe_req_per_min",       "20",    "安全请求频率(次/分钟)"),
        # 新模型：4步条件分支决策
        ("futures_beta",           "1.15",  "期指基差修正系数"),
        ("scenario_a_fut_drop",    "-0.6",  "剧本A触发:修正后期指跌幅阈值(%)"),
        ("scenario_b_ndx_drop",    "-0.5",  "剧本B条件1:昨夜指数跌幅(%)"),
        ("scenario_b_fut_rise",    "0.3",   "剧本B条件2:修正后期指涨幅(%)"),
        ("scenario_a_overshot",    "1.0",   "剧本A错杀阈值:跳空需低于期指+此值(%)"),
        ("scenario_b_lag",         "0.3",   "剧本B落后阈值:跳空落后超此值(%)"),
        ("buysell_ratio_threshold","2.0",   "盘口否决:买卖比阈值"),
    ]
    for key, value, remark in seed_configs:
        await execute(
            "INSERT IGNORE INTO sys_config (`key`, value, remark) VALUES (%s, %s, %s)",
            (key, value, remark),
            _pool=pool,
        )

    # ---- 历史表字段升级 (兼容已有表) ----
    await _alter_widen_columns(pool)
    await _alter_add_rec_columns(pool)

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

    # ---- cash_accounts (现金账户) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS cash_accounts (
            currency   VARCHAR(5) PRIMARY KEY COMMENT '币种 USD/HKD',
            amount     DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '金额',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='现金账户'
        """,
        _pool=pool,
    )
    # 种子：用户当前剩余资产（幂等）
    for curr, amt in (("USD", 476.79), ("HKD", 70.48)):
        await execute(
            "INSERT IGNORE INTO cash_accounts (currency, amount) VALUES (%s, %s)",
            (curr, amt),
            _pool=pool,
        )

    logger.info("数据库表初始化完成")

# ---------------------------------------------------------------------------
# ETF CRUD
# ---------------------------------------------------------------------------

async def get_etf_list(enabled_only: bool = False):
    """返回所有 ETF 列表。enabled_only=True 时只返回启用的。"""
    sql = "SELECT * FROM etf_targets"
    if enabled_only:
        sql += " WHERE enabled = 1"
    sql += " ORDER BY code"
    return await execute_dict(sql)


async def add_etf(code: str, name: str, futures_index: str) -> int:
    """新增 ETF，返回 affected rows。"""
    return await execute(
        "INSERT INTO etf_targets (code, name, futures_index) VALUES (%s, %s, %s)",
        (code, name, futures_index),
    )


async def delete_etf(code: str) -> int:
    """按 code 删除 ETF，返回 affected rows。"""
    return await execute("DELETE FROM etf_targets WHERE code = %s", (code,))


async def toggle_etf(code: str) -> int:
    """切换 ETF 启用/停用状态，返回 affected rows。"""
    return await execute(
        "UPDATE etf_targets SET enabled = IF(enabled=1, 0, 1) WHERE code = %s",
        (code,),
    )


async def update_etf_names(naming: dict[str, str]) -> int:
    """批量更新 ETF 名称。naming: {code: name}，仅更新有变化的行。"""
    total = 0
    for code, name in naming.items():
        n = await execute(
            "UPDATE etf_targets SET name = %s WHERE code = %s AND name != %s",
            (name, code, name),
        )
        total += n
    return total


async def update_prev_close(etf_data: list[dict]) -> int:
    """将每只ETF的 pre_close（昨收）写入 prev_close 字段。

    在每个交易日第一次监控循环时调用：
    写入的 pre_close 实际是 T-1 日收盘价，
    下一个交易日读出时就是 T-2 日收盘价，
    配合当时的 pre_close（T-1）即可本地计算昨日涨跌幅。

    Returns:
        更新的行数
    """
    total = 0
    for etf in etf_data:
        pre_close = etf.get("pre_close", 0)
        if pre_close and pre_close > 0:
            n = await execute(
                "UPDATE etf_targets SET prev_close = %s WHERE code = %s AND (prev_close = 0 OR prev_close != %s)",
                (pre_close, etf["code"], pre_close),
            )
            total += n
    return total


async def get_prev_close_map() -> dict[str, float]:
    """获取所有ETF的 prev_close（前日收盘价）。

    Returns:
        {code: prev_close}
    """
    rows = await execute_dict(
        "SELECT code, prev_close FROM etf_targets WHERE prev_close > 0"
    )
    return {r["code"]: float(r["prev_close"]) for r in rows}

# ---------------------------------------------------------------------------
# Proxy CRUD
# ---------------------------------------------------------------------------

async def get_proxy_list() -> list[dict]:
    """返回所有代理。"""
    return await execute_dict("SELECT * FROM proxy_pool ORDER BY id")


async def get_alive_proxies() -> list[dict]:
    """返回所有可用代理。"""
    return await execute_dict(
        "SELECT * FROM proxy_pool WHERE alive = 1 ORDER BY latency_ms ASC"
    )


async def add_proxy(url: str, latency_ms: int = 0) -> int:
    """新增代理，返回 affected rows。"""
    return await execute(
        "INSERT INTO proxy_pool (url, latency_ms) VALUES (%s, %s)",
        (url, latency_ms),
    )


async def delete_proxy(proxy_id: int) -> int:
    """按 id 删除代理，返回 affected rows。"""
    return await execute("DELETE FROM proxy_pool WHERE id = %s", (proxy_id,))


async def update_proxy_status(
    proxy_id: int, alive: int, latency_ms: int = 0
) -> int:
    """更新代理状态。"""
    return await execute(
        "UPDATE proxy_pool SET alive = %s, latency_ms = %s, last_check = NOW() "
        "WHERE id = %s",
        (alive, latency_ms, proxy_id),
    )

# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------

async def get_configs() -> list[dict]:
    """返回所有系统配置。"""
    return await execute_dict("SELECT * FROM sys_config ORDER BY `key`")


async def get_config(key: str) -> str | None:
    """获取单个配置值，不存在返回 None。"""
    rows = await execute_dict(
        "SELECT value FROM sys_config WHERE `key` = %s", (key,)
    )
    if rows:
        return rows[0]["value"]
    return None


async def set_config(key: str, value: str, remark: str = "") -> int:
    """UPSERT 配置项。"""
    return await execute(
        "INSERT INTO sys_config (`key`, value, remark) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE value = VALUES(value), remark = VALUES(remark)",
        (key, value, remark),
    )

# ---------------------------------------------------------------------------
# History Ticks (回测数据)
# ---------------------------------------------------------------------------

async def init_history_ticks_table():
    """创建 history_ticks 表（幂等）。"""
    await execute(
        """
        CREATE TABLE IF NOT EXISTS history_ticks (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
            timestamp       DATETIME(3)  NOT NULL COMMENT '快照时间(毫秒精度)',
            code            VARCHAR(6)   NOT NULL COMMENT 'ETF代码',
            name            VARCHAR(20)  NOT NULL COMMENT 'ETF名称',
            price           DECIMAL(10,4) NOT NULL COMMENT '最新价',
            pre_close       DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '昨日收盘价',
            iopv            DECIMAL(10,4) DEFAULT 0 COMMENT '实时参考净值(IOPV)',
            change_pct      DECIMAL(10,4) DEFAULT 0 COMMENT '涨跌幅(%)',
            premium_pct     DECIMAL(10,4) DEFAULT 0 COMMENT '溢价率(%)',
            buy1_vol        INT          DEFAULT 0 COMMENT '买一量(手)',
            sell1_vol       INT          DEFAULT 0 COMMENT '卖一量(手)',
            buy1_price      DECIMAL(10,4) DEFAULT 0 COMMENT '买一价',
            sell1_price     DECIMAL(10,4) DEFAULT 0 COMMENT '卖一价',
            buy_total_vol   INT          DEFAULT 0 COMMENT '5档总买量(手)',
            sell_total_vol  INT          DEFAULT 0 COMMENT '5档总卖量(手)',
            total_volume    BIGINT       DEFAULT 0 COMMENT '累计成交量(手)',
            amount          DECIMAL(18,2) DEFAULT 0 COMMENT '累计成交额(元)',
            ratio           DECIMAL(14,4) DEFAULT 0 COMMENT '买卖比(buy1_vol/sell1_vol)',
            premium         DECIMAL(10,4)  DEFAULT 0 COMMENT '溢价((price-iopv)/iopv*100)',
            score           DECIMAL(8,4)  DEFAULT 0 COMMENT '综合得分',
            futures_index   VARCHAR(20)  DEFAULT '' COMMENT '关联期指标识',
            futures_change  DECIMAL(8,4)  DEFAULT NULL COMMENT '期指涨跌幅(%)',
            iopv_source     VARCHAR(10)  DEFAULT 'N/A' COMMENT 'IOPV来源',
            weibi           DECIMAL(10,4)  DEFAULT 0 COMMENT '委比(%)',
            outer_vol       BIGINT       DEFAULT 0 COMMENT '外盘量',
            inner_vol       BIGINT       DEFAULT 0 COMMENT '内盘量',
            rec_action      VARCHAR(4)   DEFAULT '' COMMENT '推荐操作(Buy/Sell)',
            rec_price       DECIMAL(10,3) DEFAULT 0 COMMENT '推荐操作价位',
            INDEX idx_timestamp_code (timestamp, code),
            INDEX idx_code_timestamp (code, timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF历史快照数据(回测用)'
        """
    )


async def save_history_tick(etf: dict, fetch_time: str) -> int:
    """保存单条 ETF 快照到 history_ticks 表。"""
    from datetime import datetime
    now = datetime.now()
    time_parts = fetch_time.split(":")
    if len(time_parts) == 3:
        timestamp = now.replace(
            hour=int(time_parts[0]),
            minute=int(time_parts[1]),
            second=int(time_parts[2]),
            microsecond=0
        )
    else:
        timestamp = now

    return await execute(
        """
        INSERT INTO history_ticks (
            timestamp, code, name, price, pre_close, iopv, change_pct, premium_pct,
            buy1_vol, sell1_vol, buy1_price, sell1_price,
            buy_total_vol, sell_total_vol, total_volume, amount,
            ratio, premium, score, futures_index, futures_change,
            iopv_source, weibi, outer_vol, inner_vol,
            rec_action, rec_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            timestamp,
            etf.get("code"),
            etf.get("name", ""),
            _safe_decimal(etf.get("price", 0)),
            _safe_decimal(etf.get("pre_close", 0)),
            _safe_decimal(etf.get("iopv", 0)),
            _safe_decimal(etf.get("change_pct", 0)),
            _safe_decimal(etf.get("premium_pct", 0)),
            etf.get("buy1_vol", 0),
            etf.get("sell1_vol", 0),
            _safe_decimal(etf.get("buy1_price", 0)),
            _safe_decimal(etf.get("sell1_price", 0)),
            etf.get("buy_total_vol", 0),
            etf.get("sell_total_vol", 0),
            etf.get("total_volume", 0),
            _safe_decimal(etf.get("amount", 0), 2),
            _safe_decimal(etf.get("ratio", 0)),
            _safe_decimal(etf.get("premium", 0)),
            _safe_decimal(etf.get("score", 0)),
            etf.get("futures_index", ""),
            _safe_decimal(etf.get("futures_change")),
            etf.get("iopv_source", "N/A"),
            _safe_decimal(etf.get("weibi", 0)),
            etf.get("outer_vol", 0),
            etf.get("inner_vol", 0),
            etf.get("rec_action", ""),
            _safe_decimal(etf.get("rec_price", 0), 3),
        ),
    )


async def save_history_ticks_batch(etf_list: list[dict], fetch_time: str) -> int:
    """批量保存 ETF 快照到 history_ticks 表。"""
    from datetime import datetime

    if not etf_list:
        return 0

    now = datetime.now()
    time_parts = fetch_time.split(":")
    if len(time_parts) == 3:
        timestamp = now.replace(
            hour=int(time_parts[0]),
            minute=int(time_parts[1]),
            second=int(time_parts[2]),
            microsecond=0
        )
    else:
        timestamp = now

    values = []
    for etf in etf_list:
        values.append((
            timestamp,
            etf.get("code"),
            etf.get("name", ""),
            _safe_decimal(etf.get("price", 0)),
            _safe_decimal(etf.get("pre_close", 0)),
            _safe_decimal(etf.get("iopv", 0)),
            _safe_decimal(etf.get("change_pct", 0)),
            _safe_decimal(etf.get("premium_pct", 0)),
            etf.get("buy1_vol", 0),
            etf.get("sell1_vol", 0),
            _safe_decimal(etf.get("buy1_price", 0)),
            _safe_decimal(etf.get("sell1_price", 0)),
            etf.get("buy_total_vol", 0),
            etf.get("sell_total_vol", 0),
            etf.get("total_volume", 0),
            _safe_decimal(etf.get("amount", 0), 2),
            _safe_decimal(etf.get("ratio", 0)),
            _safe_decimal(etf.get("premium", 0)),
            _safe_decimal(etf.get("score", 0)),
            etf.get("futures_index", ""),
            _safe_decimal(etf.get("futures_change")),
            etf.get("iopv_source", "N/A"),
            _safe_decimal(etf.get("weibi", 0)),
            etf.get("outer_vol", 0),
            etf.get("inner_vol", 0),
            etf.get("rec_action", ""),
            _safe_decimal(etf.get("rec_price", 0), 3),
        ))

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO history_ticks (
                    timestamp, code, name, price, pre_close, iopv, change_pct, premium_pct,
                    buy1_vol, sell1_vol, buy1_price, sell1_price,
                    buy_total_vol, sell_total_vol, total_volume, amount,
                    ratio, premium, score, futures_index, futures_change,
                    iopv_source, weibi, outer_vol, inner_vol,
                    rec_action, rec_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                values,
            )
            return cur.rowcount

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


# ---------------------------------------------------------------------------
# Users CRUD (登录鉴权)
# ---------------------------------------------------------------------------

async def get_user(username: str) -> dict | None:
    """按用户名查询用户，不存在返回 None。"""
    rows = await execute_dict(
        "SELECT id, username, password FROM users WHERE username = %s",
        (username,),
    )
    return rows[0] if rows else None


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


# ---------------------------------------------------------------------------
# Cash accounts (现金账户)
# ---------------------------------------------------------------------------

async def get_cash_accounts() -> list[dict]:
    """返回现金账户列表。"""
    rows = await execute_dict("SELECT currency, amount FROM cash_accounts ORDER BY currency")
    for r in rows:
        r["amount"] = float(r["amount"])
    return rows


async def set_cash(currency: str, amount: float) -> int:
    """设置某币种现金余额。"""
    return await execute(
        "INSERT INTO cash_accounts (currency, amount) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE amount=VALUES(amount)",
        (currency, amount),
    )


async def deduct_cash(currency: str, amount: float) -> float | None:
    """扣减现金（可为负），返回扣减后余额。"""
    await execute(
        "UPDATE cash_accounts SET amount = amount - %s WHERE currency = %s",
        (amount, currency),
    )
    rows = await execute_dict("SELECT amount FROM cash_accounts WHERE currency = %s", (currency,))
    return float(rows[0]["amount"]) if rows else None
