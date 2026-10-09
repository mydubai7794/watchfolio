"""异步 MySQL CRUD 模块（aiomysql）"""
import asyncio
import hashlib
import hmac
import json
import logging
import secrets
from datetime import date, datetime, timedelta

import aiomysql
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_POOL_SIZE

logger = logging.getLogger(__name__)

_pool: aiomysql.Pool = None
# 连接池创建锁：防御并发首调双建池（锁内双检 _pool）
_pool_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# 连接池管理
# ---------------------------------------------------------------------------

async def get_pool() -> aiomysql.Pool:
    """获取连接池，若未创建则自动创建（加锁双检，防并发首调双建池）。"""
    global _pool
    if _pool is not None and not _pool._closed:
        return _pool
    async with _pool_lock:
        if _pool is None or _pool._closed:  # 双检：拿锁后再查，避免重复建池
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
            region      VARCHAR(16)  DEFAULT 'unknown' COMMENT '代理分类: overseas=境外(24h数据源可用) unknown=未分类',
            last_check  DATETIME     NULL COMMENT '最后检测时间',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='代理IP池'
        """,
        _pool=pool,
    )
    # 代理分类列迁移（存量库补 region 列；MySQL 无 ADD COLUMN IF NOT EXISTS，先查再补）
    cols = await execute_dict(
        "SELECT COUNT(*) AS n FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'proxy_pool' "
        "AND COLUMN_NAME = 'region'",
        _pool=pool,
    )
    if not cols or not cols[0].get("n"):
        await execute(
            "ALTER TABLE proxy_pool ADD COLUMN region VARCHAR(16) "
            "DEFAULT 'unknown' COMMENT '代理分类: overseas=境外(24h数据源可用) unknown=未分类'",
            _pool=pool,
        )

    # ---- sys_config ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS sys_config (
            `key`   VARCHAR(50)  PRIMARY KEY COMMENT '配置键',
            value   VARCHAR(2000) NOT NULL COMMENT '配置值',
            remark  VARCHAR(100) DEFAULT '' COMMENT '备注'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统配置'
        """,
        _pool=pool,
    )
    # value 列加宽迁移（存量库 VARCHAR(200) 装不下长桥 access_token ~1300 字符；
    # 仿 region 列迁移先查再改，幂等）
    vcols = await execute_dict(
        "SELECT CHARACTER_MAXIMUM_LENGTH AS len FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sys_config' "
        "AND COLUMN_NAME = 'value'",
        _pool=pool,
    )
    if vcols and int(vcols[0].get("len") or 0) < 2000:
        await execute(
            "ALTER TABLE sys_config MODIFY COLUMN value VARCHAR(2000) NOT NULL "
            "COMMENT '配置值'",
            _pool=pool,
        )
    # ---- 长桥 OpenAPI 凭证 seed（幂等，INSERT IGNORE 不覆盖已更新的值）----
    # 换 token 流程（约 90 天一次）：长桥后台「开放平台 → 访问令牌」重新生成
    # （务必选正式账户身份，模拟盘 token 会被行情网关 401004 拒绝）后执行
    #   UPDATE sys_config SET value='<新token>' WHERE `key`='LONGPORT_ACCESS_TOKEN';
    # 服务 60s 内热加载生效，无需重启。行情套餐：美股 LV1（QBBO 含夜盘）永久
    # 免费，换 token 不影响套餐；港股 LV2 / A股 LV1 为体验套餐有独立到期时间。
    import longport_fetcher as _lp
    await execute(
        "INSERT IGNORE INTO sys_config (`key`, value, remark) VALUES "
        "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
        args=(_lp.CFG_KEY_APP_KEY, _lp.DEFAULT_APP_KEY, "长桥开放平台 App Key",
              _lp.CFG_KEY_APP_SECRET, _lp.DEFAULT_APP_SECRET, "长桥开放平台 App Secret",
              _lp.CFG_KEY_ACCESS_TOKEN, _lp.DEFAULT_ACCESS_TOKEN,
              "长桥访问令牌(约90天到期,后台重新生成后UPDATE本行,60s热加载)"),
        _pool=pool,
    )

    # ---- users (登录用户表，存量明文密码登录命中后自动升级 pbkdf2 哈希) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            username    VARCHAR(50)  NOT NULL UNIQUE COMMENT '用户名',
            password    VARCHAR(200) NOT NULL COMMENT '密码(pbkdf2哈希或存量明文)',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录用户'
        """,
        _pool=pool,
    )

    # 种子用户不在此种入：init_tables 在每次服务启动时执行，此处种默认账户
    # 会在账户被删除后随重启自动重建（弱口令复活）。新库初始化的引导账户
    # 仅由 init_db.py 一次性 CLI 写入，且以 pbkdf2 哈希落库。

    # ---- sessions (登录会话，支持服务重启后会话不丢) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token      VARCHAR(64)  PRIMARY KEY COMMENT '会话token',
            username   VARCHAR(64)  NOT NULL COMMENT '登录用户名',
            expires    DATETIME     NOT NULL COMMENT '过期时间',
            created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录会话'
        """,
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
        ("cny_invested",            "84560", "人民币累计投入本金(29000+30000+25000+850换汇-290电费)"),
        ("expense_cny",             "0",     "个人消费折CNY累计(从cny_invested扣除得净投入,与ledger expense事件同步)"),
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
            INDEX idx_trade_ts (trade_date, ts),
            INDEX idx_ts (ts)
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

    # ---- ledger_events (记账事件流) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_events (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            event_date     DATE NOT NULL COMMENT '事件日期',
            event_type     VARCHAR(20) NOT NULL COMMENT 'inflow入金/fx换汇/fee费用/reward奖励/expense消耗/transfer转移/realized已实现/balance余额快照',
            account        VARCHAR(20) NOT NULL COMMENT 'za_bank/za_sec/hsbc/zf/ccb/cmb/cib/external',
            channel        VARCHAR(40) DEFAULT NULL COMMENT '通道: 建行购汇汇出/汇丰柜台/众安App换U/兴业SWIFT/招行原币/致富换汇/支付宝+Visa等',
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

    # ---- market_ticks (市场看板当日分时，累积落库) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS market_ticks (
            symbol VARCHAR(16)   NOT NULL COMMENT '看板标的 key(QQQ/BZ/USDCNH 等)',
            ts     DATETIME      NOT NULL COMMENT '数据源原时区 naive 时间(不做时区换算)',
            price  DECIMAL(18,6) NOT NULL COMMENT '价格',
            UNIQUE KEY uk_sym_ts (symbol, ts)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='市场看板当日分时(累积, spark 数据源)'
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

    # ---- trade_records (券商成交邮件交易记录，message_id 唯一作幂等锁) ----
    await execute(
        """
        CREATE TABLE IF NOT EXISTS trade_records (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            message_id  VARCHAR(255) UNIQUE NOT NULL COMMENT '邮件 Message-ID(幂等键)',
            trade_no    VARCHAR(32)  DEFAULT '' COMMENT '券商交易编号（汇丰 P741002 / 致富 98438804）',
            symbol      VARCHAR(16)  DEFAULT '' COMMENT '代码 NVDA/00700',
            name        VARCHAR(64)  DEFAULT '' COMMENT '股票名称',
            market      VARCHAR(8)   DEFAULT '' COMMENT 'US/HK',
            side        VARCHAR(4)   DEFAULT '' COMMENT 'BUY/SELL',
            quantity    DECIMAL(18,6) DEFAULT 0 COMMENT '成交数量',
            price       DECIMAL(18,6) DEFAULT 0 COMMENT '成交价',
            currency    VARCHAR(8)   DEFAULT '' COMMENT 'USD/HKD',
            email_date  DATETIME     NULL COMMENT '邮件 Date 头',
            created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='券商成交邮件交易记录'
        """,
        _pool=pool,
    )

    # ---- 提醒模块三表（长桥日K / 规则 / 提醒历史，幂等）----
    await init_alert_tables()

    # ---- 宏观估值指标（巴菲特/CAPE/利差/恐慌贪婪，日频幂等）----
    await init_macro_tables()

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
    """批量更新 ETF 名称。naming: {code: name}，仅更新有变化的行。

    先一次性取回现名做应用层比对，名称无变化时 0 次 UPDATE 往返；
    UPDATE 仍保留 name != %s 守卫，防比对与写入之间被并发修改。
    """
    if not naming:
        return 0
    codes = list(naming.keys())
    placeholders = ", ".join(["%s"] * len(codes))
    rows = await execute_dict(
        f"SELECT code, name FROM etf_targets WHERE code IN ({placeholders})",
        tuple(codes),
    )
    current = {r["code"]: r["name"] for r in rows}
    total = 0
    for code, name in naming.items():
        if current.get(code) == name:
            continue  # 名称未变化，跳过不发 UPDATE
        total += await execute(
            "UPDATE etf_targets SET name = %s WHERE code = %s AND name != %s",
            (name, code, name),
        )
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


async def get_overseas_proxies() -> list[dict]:
    """返回境外分类的可用代理（24h数据源专用）。

    region='overseas' 由境外可达性探测循环维护（H24_PING 探测成功标记）。
    """
    return await execute_dict(
        "SELECT * FROM proxy_pool WHERE alive = 1 AND region = 'overseas' "
        "ORDER BY latency_ms ASC"
    )


async def mark_proxy_region(proxy_id: int, region: str) -> int:
    """标记代理分类（overseas=境外/unknown=未分类）。"""
    return await execute(
        "UPDATE proxy_pool SET region = %s WHERE id = %s", (region, proxy_id)
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


def hash_password(plain: str) -> str:
    """明文密码 → pbkdf2 哈希串（pbkdf2_sha256$迭代$盐hex$哈希hex，随机盐）。"""
    salt = secrets.token_hex(16)
    dig = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), 200_000)
    return f"pbkdf2_sha256$200000${salt}${dig.hex()}"


def verify_password(plain: str, stored: str) -> bool:
    """密码校验：pbkdf2 哈希串走恒时比较；存量明文直接相等（登录命中后由
    server 侧自动升级为哈希，一次性迁移）；损坏哈希串按不匹配处理不抛异常。"""
    if stored.startswith("pbkdf2_sha256$"):
        try:
            _, iters, salt, expect = stored.split("$")
            dig = hashlib.pbkdf2_hmac("sha256", plain.encode(),
                                      salt.encode(), int(iters))
            return hmac.compare_digest(dig.hex(), expect)
        except (ValueError, TypeError):
            return False
    return stored == plain


async def update_user_password(user_id: int, password_hash: str) -> int:
    """更新用户密码（登录自动升级哈希/改密用）。"""
    return await execute(
        "UPDATE users SET password = %s WHERE id = %s", (password_hash, user_id))


# ---------------------------------------------------------------------------
# Sessions CRUD (登录会话)
# ---------------------------------------------------------------------------

async def create_session(token: str, username: str, ttl: int) -> int:
    """写入会话（expires = 当前时间 + ttl 秒）。"""
    expires = datetime.now() + timedelta(seconds=ttl)
    return await execute(
        "INSERT INTO sessions (token, username, expires) VALUES (%s, %s, %s)",
        (token, username, expires),
    )


async def get_session(token: str) -> dict | None:
    """按 token 查会话，过期则删除并返回 None（过期即失效）。"""
    rows = await execute_dict(
        "SELECT username, expires FROM sessions WHERE token = %s",
        (token,),
    )
    if not rows:
        return None
    if rows[0]["expires"] < datetime.now():
        # 已过期：顺手清理，视为无会话
        await execute("DELETE FROM sessions WHERE token = %s", (token,))
        return None
    return {"username": rows[0]["username"], "expires": rows[0]["expires"]}


async def delete_session(token: str) -> int:
    """按 token 删除会话（登出）。"""
    return await execute("DELETE FROM sessions WHERE token = %s", (token,))


async def purge_expired_sessions() -> int:
    """批量清理所有已过期会话行（每日定时任务调用，防止 sessions 表无限膨胀）。"""
    return await execute("DELETE FROM sessions WHERE expires < %s", (datetime.now(),))


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
    """回写行情接口返回的名称。

    先一次性取回现名做应用层比对，仅对有变化的行发 UPDATE（无变化 0 往返）；
    UPDATE 仍保留 name != %s 守卫，防比对与写入之间被并发修改。
    """
    symbols = [s for s in naming if naming[s]]
    if not symbols:
        return 0
    placeholders = ", ".join(["%s"] * len(symbols))
    rows = await execute_dict(
        f"SELECT symbol, name FROM holdings WHERE symbol IN ({placeholders})",
        tuple(symbols),
    )
    current = {r["symbol"]: r["name"] for r in rows}
    total = 0
    for symbol in symbols:
        name = naming[symbol]
        if current.get(symbol) == name:
            continue  # 名称未变化，跳过不发 UPDATE
        total += await execute(
            "UPDATE holdings SET name = %s WHERE symbol = %s AND name != %s",
            (name, symbol, name),
        )
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


async def get_portfolio_recent(hours: int) -> list[dict]:
    """返回最近 N 小时的采样点（跨日分时窗口，字段与 get_portfolio_intraday 一致）。"""
    return await execute_dict(
        "SELECT ts, total_value_usd, day_pnl_usd FROM portfolio_ticks "
        "WHERE ts >= NOW() - INTERVAL %s HOUR ORDER BY ts",
        (hours,),
    )


async def get_portfolio_daily(days: int) -> list[dict]:
    """返回近 N 天的日线（北京自然日「当日定格」快照，ts 列为北京时间）。

    每日取 21:30（美东开盘）前最后一条 tick：交易日点=前一美东场收盘定格
    （day_pnl 即该场累计盈亏），周末点=24h跟价现值；今天自然出现当日点，
    日期与用户直觉（北京日）一致。白天定格段若无新 tick（价变即写），
    自动回退该日更早的最后采样（凌晨收盘前后），仍为定格语义。
    """
    return await execute_dict(
        """
        SELECT DATE(t.ts) AS date, t.total_value_usd AS total_value,
               t.day_pnl_usd AS day_pnl, t.total_pnl_pct AS total_pnl_pct
        FROM portfolio_ticks t
        JOIN (SELECT DATE(ts) AS d, MAX(ts) AS mx
              FROM portfolio_ticks
              WHERE ts < TIMESTAMP(CONCAT(DATE(ts), ' 21:30:00'))
              GROUP BY DATE(ts)) m
          ON t.ts = m.mx AND DATE(t.ts) = m.d
        WHERE t.ts >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY t.ts
        """,
        (days,),
    )


# ---------------------------------------------------------------------------
# Market ticks (市场看板当日分时)
# ---------------------------------------------------------------------------

async def save_market_ticks(symbol: str, points: list[tuple]) -> int:
    """批量写入看板分时点（INSERT IGNORE 幂等：同 symbol+ts 重复拉取自动去重）。

    points: [(naive datetime, price)]，price 经 _safe_decimal 防 NaN/Inf。
    """
    if not points:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO market_ticks (symbol, ts, price) VALUES (%s, %s, %s)",
                [(symbol, ts, _safe_decimal(p, 6)) for ts, p in points],
            )
            return cur.rowcount


async def save_market_ticks_batch(rows: list) -> int:
    """多 symbol 批量写 market_ticks（quotes_hub 降采样攒批用）。

    rows: [(symbol, naive_ts, price)]，INSERT IGNORE 幂等（uk_sym_ts）。
    调用方须保证同 symbol 的 ts 互不相同，否则重复行会被静默去重。@author yys
    """
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT IGNORE INTO market_ticks (symbol, ts, price) VALUES (%s, %s, %s)",
                [(s, ts, _safe_decimal(p, 6)) for s, ts, p in rows],
            )
            return cur.rowcount


async def get_market_last_tick(symbol: str) -> tuple | None:
    """取该标的库内最新一条分时 (ts, price)；无数据返回 None。

    用途：①主价快照写入去重（价与最新 tick 相同则跳过，闭市不堆重复点）
    ②分时 backfill 判定（最新 tick 距今超阈值才补拉一次全量分时）。
    """
    rows = await execute_dict(
        "SELECT ts, price FROM market_ticks WHERE symbol = %s ORDER BY ts DESC LIMIT 1",
        (symbol,),
    )
    if not rows:
        return None
    return rows[0]["ts"], float(rows[0]["price"])


async def get_market_day_spark(symbol: str, max_points: int = 60) -> list[tuple[str, float]]:
    """取该标的库内最近一个「有效分时日」的全程分时（升序），等距降采样 ≤max_points。

    「有效分时日」判定（全表统一 UTC naive 口径）：当日 tick 数 ≥5，且满足
    密度条件之一——点数 ≥30（完整时段）或首尾跨 ≤2h（刚开盘的实时累积段）。
    排除两类杂点日（实测线上）：美股类标的跨 UTC 日后腾讯夜间刷新落下的
    单点（QQQ Oct-2 仅 1 点）、Yahoo backfill 写入的稀疏隔夜段（Oct-2 仅
    5 点横跨数小时）——二者都会让「最大 ts 日」只剩残段、分时图消失，
    自动回退上一完整时段。全部日期都不满足时退回最新日（新标的冷启动
    有数可显）。
    """
    day_rows = await execute_dict(
        "SELECT DATE(ts) d, COUNT(*) n, MIN(ts) t_min, MAX(ts) t_max "
        "FROM market_ticks WHERE symbol = %s "
        "GROUP BY DATE(ts) ORDER BY d DESC LIMIT 7",
        (symbol,),
    )
    day = None
    for r in day_rows:
        span_h = ((r["t_max"] - r["t_min"]).total_seconds() / 3600.0
                  if r["t_max"] and r["t_min"] else 0.0)
        if r["n"] >= 5 and (r["n"] >= 30 or span_h <= 2.0):
            day = r["d"]
            break
    if day is None and day_rows:
        day = day_rows[0]["d"]
    if day is None:
        return []
    rows = await execute_dict(
        "SELECT ts, price FROM market_ticks "
        "WHERE symbol = %s AND DATE(ts) = %s ORDER BY ts",
        (symbol, day),
    )
    pts = [(str(r["ts"]), float(r["price"])) for r in rows]
    if len(pts) <= max_points:
        return pts
    step = (len(pts) - 1) / (max_points - 1)
    idxs = sorted({round(i * step) for i in range(max_points)})
    return [pts[i] for i in idxs]


async def get_market_ticks_range(symbol: str, limit: int = 5000) -> list[dict]:
    """按时间升序取该标的最近 limit 条分时 tick（分时K聚合用）。@author yys"""
    rows = await execute_dict(
        "SELECT ts, price FROM (SELECT ts, price FROM market_ticks "
        "WHERE symbol = %s ORDER BY ts DESC LIMIT %s) t ORDER BY ts ASC",
        (symbol, int(limit)),
    )
    return [{"ts": r["ts"], "price": float(r["price"])} for r in rows]


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


# ---------------------------------------------------------------------------
# Trade records (券商成交邮件交易记录：汇丰/致富)
# ---------------------------------------------------------------------------

async def trade_message_exists(message_id: str) -> bool:
    """按邮件 Message-ID 判重（幂等主键）。"""
    rows = await execute_dict(
        "SELECT id FROM trade_records WHERE message_id = %s", (message_id,)
    )
    return bool(rows)


async def trade_no_exists(trade_no: str) -> bool:
    """按券商交易编号判重（防同邮件多收/重复投递）。"""
    rows = await execute_dict(
        "SELECT id FROM trade_records WHERE trade_no = %s", (trade_no,)
    )
    return bool(rows)


async def insert_trade_record(message_id: str, trade_no: str, symbol: str,
                              name: str, market: str, side: str,
                              quantity: float, price: float, currency: str,
                              email_date=None) -> int:
    """插入一条交易记录（message_id 冲突会抛异常，由调用方幂等检查前置避免）。"""
    return await execute(
        "INSERT INTO trade_records (message_id, trade_no, symbol, name, market, "
        "side, quantity, price, currency, email_date) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (message_id, trade_no, symbol, name, market, side,
         _safe_decimal(quantity, 6), _safe_decimal(price, 6), currency, email_date),
    )


async def get_recent_trades(limit: int = 20) -> list[dict]:
    """返回最近 N 条交易记录（新→旧）。"""
    rows = await execute_dict(
        "SELECT * FROM trade_records ORDER BY id DESC LIMIT %s", (limit,)
    )
    for r in rows:
        r["quantity"] = float(r["quantity"])
        r["price"] = float(r["price"])
    return rows


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


# ---------------------------------------------------------------------------
# 提醒模块 CRUD（daily_bars 日K / alert_rules 规则 / alerts 历史）
# ---------------------------------------------------------------------------

def _json_field(value):
    """JSON 列读取兼容：驱动返回 str 则解析，已解析的 dict 原样透传。@author yys"""
    return json.loads(value) if isinstance(value, str) else value


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


async def init_macro_tables():
    """宏观估值指标表（幂等）。@author yys"""
    pool = await get_pool()
    await execute(
        """
        CREATE TABLE IF NOT EXISTS macro_indicators (
            id          BIGINT AUTO_INCREMENT PRIMARY KEY,
            name        VARCHAR(32) NOT NULL COMMENT 'buffett/cape/yield_2s10s/fear_greed',
            asof        DATE NOT NULL,
            value       DOUBLE NOT NULL,
            extra       TEXT COMMENT 'JSON 扩展（rating/source/分子分母等）',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_name_asof (name, asof)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='宏观估值指标日频快照'
        """,
        _pool=pool,
    )


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
    """整段重写（除权事件后覆盖旧复权数据；显式事务保证 DELETE+INSERT 原子）。@author yys"""
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await conn.begin()
            try:
                await cur.execute("DELETE FROM daily_bars WHERE symbol = %s", (symbol,))
                await cur.executemany(
                    "INSERT IGNORE INTO daily_bars (symbol, date, open, high, low, close, volume) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    [(symbol, d, _safe_decimal(o, 6), _safe_decimal(h, 6),
                      _safe_decimal(l, 6), _safe_decimal(c, 6), int(v or 0))
                     for d, o, h, l, c, v in rows])
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            return cur.rowcount


async def get_daily_bars(symbol: str, limit: int | None = None) -> list[dict]:
    """日K升序（旧→新）。@author yys"""
    sql = ("SELECT date, open, high, low, close, volume FROM daily_bars "
           "WHERE symbol = %s ORDER BY date")
    if limit is not None and limit > 0:
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


async def update_alert_rule_params(symbol: str, params: dict, enabled: int = 1) -> int:
    """仅更新策略参数与开关；首存自动建行（fired_state 列不在 UPDATE 列，保留旧值，
    新行初始化 '{}'，消除与求值循环的丢更新竞态）。@author yys"""
    return await execute(
        "INSERT INTO alert_rules (symbol, params, fired_state, enabled) VALUES (%s, %s, '{}', %s) "
        "ON DUPLICATE KEY UPDATE params = VALUES(params), enabled = VALUES(enabled)",
        (symbol, json.dumps(params, ensure_ascii=False), enabled))


async def get_alert_rule(symbol: str) -> dict | None:
    """读单票规则（None=未配置，调用方用默认值）。@author yys"""
    rows = await execute_dict(
        "SELECT symbol, params, fired_state, enabled FROM alert_rules WHERE symbol = %s",
        (symbol,))
    if not rows:
        return None
    r = rows[0]
    return {"symbol": r["symbol"], "params": _json_field(r["params"]),
            "fired_state": _json_field(r["fired_state"]), "enabled": r["enabled"]}


async def get_alert_rules() -> list[dict]:
    """全部已配置规则。@author yys"""
    rows = await execute_dict(
        "SELECT symbol, params, fired_state, enabled FROM alert_rules ORDER BY symbol")
    return [{"symbol": r["symbol"], "params": _json_field(r["params"]),
             "fired_state": _json_field(r["fired_state"]), "enabled": r["enabled"]}
            for r in rows]


async def save_alert(event: dict) -> int:
    """提醒落库，返回自增 id（INSERT 与 LAST_INSERT_ID 同连接，避免池跨连接取错会话值）。@author yys"""
    now = event.get("triggered_at") or datetime.now()
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 同连接同 cursor：INSERT 与 LAST_INSERT_ID 会话一致
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO alerts (symbol, rule, level, title, body, triggered_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (event["symbol"], event["rule"], event.get("level", "info"),
                 event["title"][:120], json.dumps(event.get("body", {}), ensure_ascii=False), now))
            await cur.execute("SELECT LAST_INSERT_ID() AS id")
            row = await cur.fetchone()
            return int(row[0]) if row else 0


async def get_today_alert_keys() -> set[tuple[str, str]]:
    """当日已触发提醒 (symbol, rule) 键集（自然日去重预载）。@author yys

    triggered_at 由 save_alert 写服务器本地时间，CURDATE() 同口径。"""
    rows = await execute_dict(
        "SELECT DISTINCT symbol, rule FROM alerts WHERE triggered_at >= CURDATE()")
    return {(r["symbol"], r["rule"]) for r in rows}


async def get_alerts(unread_only: bool = False, limit: int = 50) -> list[dict]:
    """提醒历史（新→旧）。@author yys"""
    where = "WHERE is_read = 0 " if unread_only else ""
    rows = await execute_dict(
        f"SELECT id, symbol, rule, level, title, body, triggered_at, is_read "
        f"FROM alerts {where}ORDER BY triggered_at DESC LIMIT {int(limit)}")
    for r in rows:
        r["body"] = _json_field(r["body"])
    return rows


async def mark_alert_read(alert_id: int) -> int:
    """标记单条提醒已读。@author yys"""
    return await execute("UPDATE alerts SET is_read = 1 WHERE id = %s", (alert_id,))


async def mark_all_alerts_read() -> int:
    """全部提醒标记已读。@author yys"""
    return await execute("UPDATE alerts SET is_read = 1 WHERE is_read = 0")


# ---------------------------------------------------------------------------
# 宏观估值指标 CRUD（巴菲特/CAPE/美债2s10s利差/恐慌贪婪，日频底座）
# ---------------------------------------------------------------------------

async def upsert_macro_indicator(name: str, asof: str, value: float,
                                 extra: dict | None = None) -> int:
    """写入单条宏观指标（name+asof 幂等）。@author yys"""
    return await execute(
        "INSERT INTO macro_indicators (name, asof, value, extra) VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE value = VALUES(value), extra = VALUES(extra)",
        (name, asof, value,
         json.dumps(extra, ensure_ascii=False) if extra else None))


async def get_macro_latest() -> list[dict]:
    """四项宏观指标各自最新一行。@author yys"""
    return await execute_dict(
        "SELECT m.name, m.asof, m.value, m.extra FROM macro_indicators m "
        "JOIN (SELECT name, MAX(asof) mx FROM macro_indicators GROUP BY name) t "
        "ON m.name = t.name AND m.asof = t.mx")


async def get_macro_history(name: str, days: int = 365) -> list[dict]:
    """单指标历史（升序，供看板 spark）。@author yys"""
    return await execute_dict(
        "SELECT asof, value FROM macro_indicators WHERE name = %s "
        "AND asof >= DATE_SUB(CURDATE(), INTERVAL %s DAY) ORDER BY asof",
        (name, days))
