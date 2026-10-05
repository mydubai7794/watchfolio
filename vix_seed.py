"""VIX 日K种子导入：CSV(date,close) → daily_bars(symbol='VIX')，幂等可重跑。

数据来源：QQQ×VIX 回测项目的 Yahoo 10 年日K导出（2016-10 起 2516 根），
历史底座一次导入；此后日增量由长桥日K刷新循环承担（lp_symbol→VIX.US）。
用法（本地/服务器均可，database.py 即线上远程库）：
    python3 vix_seed.py [csv路径]
@author yys
"""
import asyncio
import csv
import os
import sys

import database as db

SYMBOL = "VIX"
CANDIDATES = ["/root/kline_csv/VIX_US_day_10y.csv",
              "VIX_US_day_10y.csv",
              "/tmp/opencode/backtest/kline_csv/VIX_US_day_10y.csv"]


def parse_csv(path: str) -> list[tuple]:
    """解析 date,close 两列 → daily_bars 行元组（open/high/low=close，volume=0）。

    分位只看 close；OHLC 填充 close 是 daily_bars 表 NOT NULL 约束的最小满足。"""
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d, c = r["date"].strip()[:10], float(r["close"])
            rows.append((d, c, c, c, c, 0))
    rows.sort(key=lambda x: x[0])
    return rows


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else next(
        (p for p in CANDIDATES if os.path.exists(p)), None)
    if not path:
        print("未找到 VIX CSV，用法: python3 vix_seed.py <csv路径>")
        sys.exit(1)
    rows = parse_csv(path)
    if not rows:
        print(f"CSV {path} 无有效数据行（须含 date/close 两列表头）")
        sys.exit(1)
    try:
        n = await db.save_daily_bars(SYMBOL, rows)
        total = await db.get_daily_bars(SYMBOL)
        print(f"CSV {len(rows)} 根，本次新增 {n} 根（INSERT IGNORE 幂等）；"
              f"{SYMBOL} 库内共 {len(total)} 根，"
              f"区间 {total[0]['date']} ~ {total[-1]['date']}")
    finally:
        await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
