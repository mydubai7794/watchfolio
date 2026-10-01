#!/usr/bin/env python3
"""造长效测试 token（写入 MySQL sessions 表）。

用法:
    python3 scripts/make_token.py [有效期天数，默认 36500]

输出仅 token 一行，便于脚本捕获。
"""
import asyncio
import secrets
import sys

import database


async def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 36500
    token = secrets.token_hex(24)
    await database.init_tables()
    await database.create_session(token, "admin", days * 86400)
    print(token)


if __name__ == "__main__":
    asyncio.run(main())
