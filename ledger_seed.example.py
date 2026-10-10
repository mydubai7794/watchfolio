"""ledger 种子事件示例：记账事件录入格式参考（数据完全虚构）。

字段含义（与 ledger_events 表一一对应）：
- event_date:     事件日期 YYYY-MM-DD；
- event_type:     inflow 入金 / fx 换汇 / fee 显性费用 / reward 奖励与被动收入 /
                  expense 生活消耗 / transfer 转出 / realized 已实现盈亏 / balance 余额快照；
- account:        账户标识（za_bank/hsbc/zf/za_sec/external 等，可自定义）；
- direction:      "+" 资金流入 / "-" 资金流出；
- amount_ccy/amount: 付出币种与金额；
- ref_no:         幂等键，同 ref_no 重跑覆盖不重复插入；
- channel:        通道描述（如 银行购汇汇出/券商换汇）；
- recv_ccy/recv_amount: 换汇收到币种与金额（fx/inflow 用）；
- usd_equivalent: 折 USD 金额（报表统一口径）；
- cny_amount:     折 CNY 金额（inflow 必填，合计即总投入）；
- mid_rate:       换汇当日理论汇率（点差/损失由报表实时推导，不落库）；
- note:           备注。

用法（幂等可重跑，按 ref_no 幂等）：
    python3 ledger_seed.py

正式使用：复制本文件为 ledger_seed.py，填入真实流水后运行
（ledger_seed.py 已 gitignore，不入库）。
"""
import asyncio

import database as db


def E(date, etype, account, direction, ccy, amount, ref, channel=None,
      recv_ccy=None, recv_amount=None, usd=None, cny=None, mid=None, note=""):
    return dict(event_date=date, event_type=etype, account=account, channel=channel,
                direction=direction, amount_ccy=ccy, amount=amount, ref_no=ref,
                recv_ccy=recv_ccy, recv_amount=recv_amount, usd_equivalent=usd,
                cny_amount=cny, mid_rate=mid, note=note)


SEED_EVENTS = [
    # ---- inflow 入金（常与 fx 换汇成对录入）----
    E("2026-01-05", "inflow", "za_bank", "+", "CNY", 200, "EXAMPLE-01", "银行购汇汇出",
      "HKD", 230.50, 29.40, 200, None, "示例:人民币购汇汇出"),
    # ---- fx 换汇（mid=当日理论汇率，点差/损失由报表实时推导）----
    E("2026-01-05", "fx", "za_bank", "-", "CNY", 200, "EXAMPLE-02", "银行购汇汇出",
      "HKD", 230.50, None, None, 1.152500),
    # ---- fee 显性费用 ----
    E("2026-01-10", "fee", "hsbc", "-", "USD", 1.00, "EXAMPLE-03", "交易佣金",
      None, None, 1.00, None, None, "示例:卖出佣金"),
    # ---- expense 生活消耗 ----
    E("2026-01-20", "expense", "hsbc", "-", "HKD", 100, "EXAMPLE-04", "生活缴费",
      None, None, 12.75, None, None, "示例:公用事业缴费"),
]


async def seed():
    """幂等入库全部种子事件。"""
    n = 0
    for ev in SEED_EVENTS:
        await db.upsert_ledger_event(**ev)
        n += 1
    return n


if __name__ == "__main__":
    total = asyncio.run(seed())
    print(f"ledger seed 完成: {total} 笔事件(幂等upsert)")
