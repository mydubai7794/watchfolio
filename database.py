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

async def init_tables():
    """创建所有业务表并插入种子配置（幂等）。"""
    pool = await get_pool()

    # ---- etf_targets ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS etf_targets (
            code        VARCHAR(6)   PRIMARY KEY COMMENT 'ETF代码',
            name        VARCHAR(20)  NOT NULL COMMENT 'ETF名称',
            futures_index VARCHAR(20) NOT NULL COMMENT '关联期指标识 NQ/ES/YM/DAX/NK',
            prev_nav    DECIMAL(8,4) DEFAULT 0 COMMENT '上一交易日净值估算',
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

    # ---- 种子配置 (INSERT IGNORE 保证幂等) ----
    seed_configs = [
        ("w_ratio",                "0.7",   "溢价得分权重"),
        ("w_premium",              "0.3",   "偏离率得分权重"),
        ("futures_kill_threshold", "-1.5",  "期指跌幅熔断阈值(%)"),
        ("safe_req_per_min",       "20",    "安全请求频率(次/分钟)"),
    ]
    for key, value, remark in seed_configs:
        await execute(
            "INSERT IGNORE INTO sys_config (`key`, value, remark) VALUES (%s, %s, %s)",
            (key, value, remark),
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
