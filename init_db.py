#!/usr/bin/env python3
"""一次性初始化脚本：建表 + 种子数据。"""
import asyncio
import logging
import os
import secrets

import config  # noqa: F401  导入即完成项目根 qdii.env → os.environ 注入（幂等）

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# 种子 ETF 数据
SEED_ETFS = [
    ("513100", "纳指ETF",       "NQ"),
    ("159941", "纳指ETF",       "NQ"),
    ("513030", "德国ETF",       "DAX"),
    ("513500", "标普500ETF",    "ES"),
    ("513400", "道琼斯ETF",     "YM"),
    ("513520", "日经ETF",       "NK"),
    ("513300", "纳斯达克ETF",   "NQ"),
    ("513050", "中概互联网ETF", "NQ"),
]


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


async def init():
    from database import init_tables, close_pool, add_etf, get_etf_list, get_candidate_list, add_candidate

    # 1. 建表 + 种子配置
    await init_tables()

    # 1.5 初始账号（仅 users 表为空时创建；pbkdf2 哈希落库避免明文。
    #     口令优先取环境变量 QDII_ADMIN_PASSWORD，未设置时 secrets 随机
    #     生成并打印到启动日志——stdout 即日志，docker logs 亦可见，
    #     上线后请立即登录修改初始口令）
    from database import execute as db_execute, execute_dict as db_query, hash_password
    rows = await db_query("SELECT COUNT(*) AS n FROM users")
    if rows and rows[0].get("n", 0) == 0:
        admin_password = os.environ.get("QDII_ADMIN_PASSWORD") or secrets.token_urlsafe(12)
        await db_execute(
            "INSERT IGNORE INTO users (username, password) VALUES (%s, %s)",
            args=("admin", hash_password(admin_password)),
        )
        logger.info("=" * 60)
        logger.info("[init] 初始账号 admin / 初始口令 %s —— 请立即登录并修改", admin_password)
        logger.info("=" * 60)
    else:
        logger.info("users 表已有账号，跳过初始账号创建")

    # 2. 种子 ETF（INSERT IGNORE 等效逻辑 —— add_etf 使用 INSERT，
    #    如果已存在会因 PRIMARY KEY 冲突跳过，这里先查再插入）
    rows_existing = await get_etf_list()
    existing = {row["code"] for row in rows_existing}
    added = 0
    for code, name, futures_index in SEED_ETFS:
        if code not in existing:
            await add_etf(code, name, futures_index)
            added += 1
            logger.info("  + ETF %s %s %s", code, name, futures_index)
        else:
            logger.info("  . ETF %s 已存在，跳过", code)

    logger.info("种子 ETF 写入完成，新增 %d 条", added)

    # 候选池种子
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

    # 3. 验证
    rows = await get_etf_list()
    logger.info("当前 ETF 列表（共 %d 条）:", len(rows))
    for r in rows:
        logger.info("  %s %s %s enabled=%s", r["code"], r["name"], r["futures_index"], r["enabled"])

    await close_pool()


if __name__ == "__main__":
    asyncio.run(init())
