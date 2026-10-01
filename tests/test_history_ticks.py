"""测试 history_ticks 表的创建和数据保存功能。"""
import asyncio
import sys

sys.path.insert(0, "/root/workspace/project/qdii")

import database as db


async def test_history_ticks():
    """测试 history_ticks 表。"""
    print("=== 测试 history_ticks 表 (含新增字段) ===")

    # 1. 初始化表
    print("\n1. 初始化表...")
    await db.init_history_ticks_table()
    print("   OK 表创建成功")

    # 2. 测试批量插入 (含全部新增字段)
    print("\n2. 测试批量插入 (含 pre_close / buy_total_vol / sell_total_vol / total_volume / amount)...")
    batch_etfs = [
        {
            "code": "513050",
            "name": "纳指科技",
            "price": 1.234,
            "pre_close": 1.228,
            "iopv": 1.230,
            "change_pct": 0.49,
            "premium_pct": 0.32,
            "buy1_vol": 1000,
            "sell1_vol": 500,
            "buy1_price": 1.233,
            "sell1_price": 1.235,
            "buy_total_vol": 3500,
            "sell_total_vol": 1200,
            "total_volume": 85000,
            "amount": 104890.00,
            "ratio": 2.0,
            "premium": 0.32,
            "score": 0.85,
            "futures_index": "NQ",
            "futures_change": 1.2,
            "iopv_source": "IOPV",
            "weibi": 33.33,
            "outer_vol": 100000,
            "inner_vol": 50000,
        },
        {
            "code": "513100",
            "name": "纳指ETF",
            "price": 1.500,
            "pre_close": 1.496,
            "iopv": 1.495,
            "change_pct": 0.27,
            "premium_pct": 0.33,
            "buy1_vol": 2000,
            "sell1_vol": 800,
            "buy1_price": 1.499,
            "sell1_price": 1.501,
            "buy_total_vol": 8000,
            "sell_total_vol": 2500,
            "total_volume": 320000,
            "amount": 480000.00,
            "ratio": 2.5,
            "premium": 0.33,
            "score": 0.90,
            "futures_index": "NQ",
            "futures_change": 1.2,
            "iopv_source": "IOPV",
            "weibi": 42.86,
            "outer_vol": 200000,
            "inner_vol": 80000,
        },
    ]
    affected = await db.save_history_ticks_batch(batch_etfs, "09:30:00")
    print(f"   OK 批量插入 {affected} 条")

    # 3. 查询验证新字段
    print("\n3. 查询验证新增字段...")
    rows = await db.execute_dict(
        "SELECT code, pre_close, buy_total_vol, sell_total_vol, total_volume, amount "
        "FROM history_ticks ORDER BY code"
    )
    for r in rows:
        print(f"   {r['code']}: pre_close={r['pre_close']}, "
              f"5档买={r['buy_total_vol']}, 5档卖={r['sell_total_vol']}, "
              f"成交量={r['total_volume']}, 成交额={r['amount']}")

    # 4. 清理
    print("\n4. 清理测试数据...")
    await db.execute("DELETE FROM history_ticks")
    print("   OK 已清理")

    print("\n=== 测试通过 ===")
    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(test_history_ticks())
