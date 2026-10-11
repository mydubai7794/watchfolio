"""已清仓周期推导测试：build_closed_positions 回放口径（spec
2026-10-11 §3）+ trade_records 查询 SQL 形状 + API closed/kline.trades。

纯计算部分无 DB 依赖；API 部分打桩 db 层（同 test_alert_api.py 模式），
不起服务不连 MySQL。
"""
import asyncio
import time
from datetime import date, datetime

import database as db
import ledger
import server
from fastapi.testclient import TestClient


def run(coro):
    return asyncio.run(coro)


def _tr(sym, side, qty, price, dt, **kw):
    """构造 trade_records 形状行；email_date/created_at 传 datetime。"""
    row = {"symbol": sym, "side": side, "quantity": qty, "price": price,
           "email_date": dt, "created_at": dt, "name": "", "market": "US"}
    row.update(kw)
    return row


def _h(sym, qty, cost=0.0, name="", market="US"):
    """构造 holdings 形状行。"""
    return {"symbol": sym, "quantity": qty, "cost_price": cost,
            "name": name, "market": market}


# ---------------------------------------------------------------------------
# 纯计算：build_closed_positions
# ---------------------------------------------------------------------------

def test_single_cycle_exact():
    """单周期：买10@100 → 卖10@120，realized=200/exit_avg=120/36天。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1 and rows[0].get("no_records") is not True
    r = rows[0]
    assert r["symbol"] == "MU"
    assert r["exit_date"] == "2026-02-10"
    assert r["hold_days"] == 36
    assert r["exit_qty"] == 10
    assert r["exit_avg"] == 120.0
    assert r["realized_pnl"] == 200.0
    assert r["realized_pct"] == 20.0
    assert r["approx"] is False


def test_partial_sells_then_close():
    """部分减仓后清仓：买10@100 → 卖4@110 → 卖6@120，realized=160/均价116。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 4, 110, datetime(2026, 1, 20)),
              _tr("MU", "SELL", 6, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1
    assert rows[0]["exit_qty"] == 10
    assert rows[0]["exit_avg"] == 116.0
    assert rows[0]["realized_pnl"] == 160.0
    assert rows[0]["realized_pct"] == 16.0


def test_reentry_two_cycles():
    """再入场：两轮归零 → 两个周期，输出按 exit_date 倒序。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 110, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 10, 130, datetime(2026, 3, 1)),
              _tr("MU", "SELL", 5, 140, datetime(2026, 3, 15)),
              _tr("MU", "SELL", 5, 150, datetime(2026, 4, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 2
    assert rows[0]["exit_date"] == "2026-04-01"      # 倒序：新周期在前
    assert rows[0]["realized_pnl"] == 150.0          # (700+750)-1300
    assert rows[0]["exit_avg"] == 145.0
    assert rows[1]["exit_date"] == "2026-01-20"
    assert rows[1]["realized_pnl"] == 100.0
    assert rows[1]["exit_avg"] == 110.0


def test_anchor_initial_position():
    """期初锚点：无买入记录直接卖5@100，cost=80 → q0=5，realized=100，approx。"""
    trades = [_tr("AMD", "SELL", 5, 100, datetime(2026, 1, 10))]
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)])
    assert len(rows) == 1
    assert rows[0]["realized_pnl"] == 100.0          # 500 - 5×80
    assert rows[0]["realized_pct"] == 25.0
    assert rows[0]["approx"] is True


def test_anchor_negative_clamped():
    """锚点负值钳制：记录净量5 但 holdings=0 → q0 钳 0、周期打 approx 不抛异常。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 5, 50, datetime(2026, 3, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1                            # 首周期闭合；尾部未闭合丢弃
    assert rows[0]["realized_pnl"] == 200.0
    assert rows[0]["approx"] is True


def test_open_position_no_cycle():
    """未闭合：qty>0（部分减仓后仍持有）→ 不产生周期。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 5, 110, datetime(2026, 1, 20))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 5)])
    assert rows == []


def test_placeholder_no_records():
    """占位行：qty==0 且无任何成交记录 → 单条 no_records 行，字段全 None。"""
    rows = ledger.build_closed_positions([], [_h("F", 0, name="福特")])
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "F" and r["no_records"] is True
    assert r["exit_date"] is None and r["exit_avg"] is None
    assert r["realized_pnl"] is None and r["hold_days"] is None


def test_email_date_none_falls_back_created_at():
    """email_date 为 NULL → 回退 created_at 参与回放（日期与排序口径）。"""
    trades = [_tr("MU", "BUY", 10, 100, None, created_at=datetime(2026, 1, 5, 9, 0)),
              _tr("MU", "SELL", 10, 120, None, created_at=datetime(2026, 1, 20, 9, 0))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1
    assert rows[0]["exit_date"] == "2026-01-20"
    assert rows[0]["realized_pnl"] == 200.0


def test_orphan_symbol_holdings_row_deleted():
    """孤儿 symbol：holdings 行已删但有完整成交 → 周期照常推导，名称取末条记录。"""
    trades = [_tr("PCG", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("PCG", "SELL", 10, 120, datetime(2026, 2, 10),
                  name="PG&E", market="US")]
    rows = ledger.build_closed_positions(trades, [])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "PCG"
    assert rows[0]["name"] == "PG&E"
    assert rows[0]["realized_pnl"] == 200.0


def test_holding_no_records_qty_positive():
    """qty>0 且无成交记录（手动持仓/基金行）：不抛异常、不产生任何行。"""
    rows = ledger.build_closed_positions([], [_h("HK0000740297", 500,
                                                 cost=7.5, name="基金",
                                                 market="HKFUND")])
    assert rows == []


def test_mixed_closed_and_manual_holding():
    """混合形态：已清仓 symbol 与 qty>0 无记录 symbol 共存互不干扰。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(
        trades, [_h("MU", 0), _h("HK0000740297", 500, market="HKFUND")])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "MU"
    assert rows[0]["realized_pnl"] == 200.0
