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


# ---------------------------------------------------------------------------
# database 查询 SQL 形状（打桩 execute_dict，零连库）
# ---------------------------------------------------------------------------

class TestTradeQueries:
    def test_get_all_trades_sql(self, monkeypatch):
        cap = {}

        async def fake_ed(sql, args=None, _pool=None):
            cap["sql"], cap["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_ed)
        run(db.get_all_trades())
        assert "FROM trade_records" in cap["sql"]
        assert "COALESCE(email_date, created_at)" in cap["sql"]   # NULL 日期回退
        assert "ORDER BY COALESCE(email_date, created_at), id" in cap["sql"]  # id 二级排序
        assert cap["args"] is None

    def test_get_trades_by_symbol_sql(self, monkeypatch):
        cap = {}

        async def fake_ed(sql, args=None, _pool=None):
            cap["sql"], cap["args"] = sql, args
            return []

        monkeypatch.setattr(db, "execute_dict", fake_ed)
        run(db.get_trades_by_symbol("MU"))
        assert "FROM trade_records" in cap["sql"]
        assert "WHERE symbol = %s" in cap["sql"]
        assert "COALESCE(email_date, created_at)" in cap["sql"]
        assert "ORDER BY COALESCE(email_date, created_at), id" in cap["sql"]  # id 二级排序
        assert cap["args"] == ("MU",)


# ---------------------------------------------------------------------------
# API：overview closed 块（TestClient 打桩，同 test_alert_api._client 模式）
# ---------------------------------------------------------------------------

def _client(monkeypatch):
    async def fake_get_session(t):
        return {"username": "u", "expires": time.time() + 3600}
    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    c = TestClient(server.app)
    c.cookies.set("session_token", "tok")
    return c


def _reset_closed_cache(monkeypatch):
    monkeypatch.setattr(server, "_closed_ctx_cache",
                        {"ts": 0.0, "trades": None, "holdings": None})


def test_overview_closed_block(monkeypatch):
    """overview 注入 closed 块：周期行结构/数值正确（回放口径）。"""
    async def fake_trades():
        return [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
                _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]

    async def fake_holdings(enabled_only=False):
        return [_h("MU", 0)]

    monkeypatch.setattr(server.db, "get_all_trades", fake_trades)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    closed = r.json().get("closed")
    assert closed and closed[0]["symbol"] == "MU"
    assert closed[0]["realized_pnl"] == 200.0
    assert closed[0]["exit_avg"] == 120.0
    assert closed[0]["exit_date"] == "2026-02-10"


def test_overview_closed_db_error_degrades(monkeypatch):
    """closed 上下文 DB 异常 → 降级空数组，不 500（同 recent_trades 模式）。"""
    async def boom():
        raise RuntimeError("db down")

    async def fake_holdings(enabled_only=False):
        return []

    monkeypatch.setattr(server.db, "get_all_trades", boom)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    assert r.json().get("closed") == []


def test_kline_trades_payload(monkeypatch):
    """kline 响应含 trades 数组（date=YYYY-MM-DD/side/price/quantity）。"""
    async def fake_bars(sym, limit=None):
        return [{"date": date(2026, 10, 1), "open": 1, "high": 2, "low": 0.5,
                 "close": 1.5, "volume": 10}]

    async def fake_rule(sym):
        return None

    async def fake_holdings(enabled_only=True):
        return []

    async def fake_trades(sym):
        return [{"symbol": "QQQ", "side": "BUY", "quantity": 10, "price": 100.0,
                 "email_date": datetime(2026, 9, 30, 10, 0),
                 "created_at": datetime(2026, 9, 30, 11, 0)}]

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_trades_by_symbol", fake_trades)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    d = client.get("/api/kline/QQQ?tf=day&range=1000").json()
    assert d["trades"] == [{"date": "2026-09-30", "side": "BUY",
                            "price": 100.0, "quantity": 10.0}]


def test_kline_trades_email_date_fallback(monkeypatch):
    """email_date 缺失时回退 created_at 归一日期（防只读 email_date 的回归）。"""
    async def fake_bars(sym, limit=None):
        return [{"date": date(2026, 10, 1), "open": 1, "high": 2, "low": 0.5,
                 "close": 1.5, "volume": 10}]

    async def fake_rule(sym):
        return None

    async def fake_holdings(enabled_only=True):
        return []

    async def fake_trades(sym):
        return [{"symbol": "MU", "side": "SELL", "quantity": 5, "price": 90.0,
                 "email_date": None,
                 "created_at": datetime(2026, 10, 2, 15, 30)}]

    monkeypatch.setattr(server.db, "get_daily_bars", fake_bars)
    monkeypatch.setattr(server.db, "get_alert_rule", fake_rule)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    monkeypatch.setattr(server.db, "get_trades_by_symbol", fake_trades)
    server._KLINE_CACHE.clear()
    client = _client(monkeypatch)
    d = client.get("/api/kline/MU?tf=day&range=1000").json()
    assert d["trades"][0]["date"] == "2026-10-02"
