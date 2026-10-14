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
import portfolio_fetcher as pf
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
    """期初锚点：无买入记录直接卖5@100，cost=80 → q0=5，realized=100，approx。

    期初买入日无成交记录 → hold_days 无从计算，输出 None（前端显示 --），
    不再取首条记录日（=卖出日，恒 0 天，误导）。
    """
    trades = [_tr("AMD", "SELL", 5, 100, datetime(2026, 1, 10))]
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)])
    assert len(rows) == 1
    assert rows[0]["realized_pnl"] == 100.0          # 500 - 5×80
    assert rows[0]["realized_pct"] == 25.0
    assert rows[0]["approx"] is True
    assert rows[0]["hold_days"] is None


def test_anchor_cycle_addon_not_reset():
    """期初周期内 BUY 加仓不重置周期：q0=4，SELL2 → BUY3 → SELL5 清仓。

    realized = (200+600) − (4×80 + 3×50) = 330（期初成本与加仓成本都计入）；
    hold_days=None（期初买入日未知）；exit_avg=800/7。
    """
    trades = [_tr("AMD", "SELL", 2, 100, datetime(2026, 1, 10)),
              _tr("AMD", "BUY", 3, 50, datetime(2026, 1, 12)),
              _tr("AMD", "SELL", 5, 120, datetime(2026, 1, 15))]
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)])
    assert len(rows) == 1
    r = rows[0]
    assert r["exit_date"] == "2026-01-15"
    assert r["hold_days"] is None
    assert r["exit_qty"] == 7.0
    assert r["exit_avg"] == round(800 / 7, 4)
    assert r["realized_pnl"] == 330.0
    assert r["approx"] is True


def test_anchor_negative_clamped():
    """锚点负值钳制：记录净量5 但 holdings=0 → q0 钳 0、周期打 approx 不抛异常。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 5, 50, datetime(2026, 3, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1                            # 首周期闭合；尾部未闭合丢弃
    assert rows[0]["realized_pnl"] == 200.0
    assert rows[0]["approx"] is True


def test_oversell_clamped_mid_cycle():
    """周期中段超卖钳制：SELL 量超当时持有 → 钳 0 闭合 + approx=True。

    注意口径：仅 BUY 10/SELL 15 两笔不会触发钳制——锚点反推
    q0 = 0 − (10−15) = +5 > 0，超卖缺口被解释为 email 记录前的期初持仓
    （cost=0 期初成本 0，approx=False）。尾部补 BUY 5 使锚点净额归 0，
    SELL 15 才真正落入轨迹钳制分支（qty<0 → 钳 0、周期打 approx）。
    realized = 15×120 − 10×100 = 800（期初 q0=0 无期初成本）。
    """
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 15, 120, datetime(2026, 1, 20)),
              _tr("MU", "BUY", 5, 50, datetime(2026, 3, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)])
    assert len(rows) == 1                            # 首周期闭合；尾部 5 股未闭合丢弃
    r = rows[0]
    assert r["exit_date"] == "2026-01-20"
    assert r["hold_days"] == 15
    assert r["exit_qty"] == 15.0
    assert r["exit_avg"] == 120.0
    assert r["realized_pnl"] == 800.0
    assert r["realized_pct"] == 80.0
    assert r["approx"] is True


def test_anchor_start_dates_hold_days():
    """anchor_start：期初锚定周期 hold_days 从人工配置建仓日（sys_config）
    起算；不传时保持 None。买入 7-14 → 清仓 10-12 = 90 天。"""
    trades = [_tr("AMD", "SELL", 5, 100, datetime(2026, 10, 12))]
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)])
    assert rows[0]["hold_days"] is None               # 不传 → None
    rows = ledger.build_closed_positions(trades, [_h("AMD", 0, cost=80)],
                                         anchor_start=date(2026, 7, 14))
    assert rows[0]["hold_days"] == 90


def test_anchor_start_deleted_holdings_row():
    """anchor_start 覆盖 holdings 行已删分支（03152 场景）：无 BUY 记录
    直接 SELL 钳制闭合，hold_days 同样从配置建仓日起算。"""
    trades = [_tr("03152", "SELL", 1, 1135.10, datetime(2026, 10, 7))]
    rows = ledger.build_closed_positions(trades, [], anchor_start=date(2026, 7, 14))
    assert len(rows) == 1
    assert rows[0]["hold_days"] == 85                 # 10-07 − 07-14


def test_anchor_start_not_override_real_buy():
    """anchor_start 不覆盖真实买入日：完整周期（有 BUY 记录）hold_days
    仍从首笔 BUY 起算。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 9, 1)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 10, 1))]
    rows = ledger.build_closed_positions(trades, [_h("MU", 0)],
                                         anchor_start=date(2026, 7, 14))
    assert rows[0]["hold_days"] == 30                 # 首笔 BUY 9-1 起算


def test_dirty_quantity_rows_filtered():
    """脏量行过滤：quantity=0 / −3 行被 quantity>0 门挡掉（锚点 net 同步按
    过滤后口径），周期推导与无脏行基线完全一致。"""
    clean = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
             _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]
    dirty = clean + [_tr("MU", "BUY", 0, 100, datetime(2026, 1, 6)),
                     _tr("MU", "SELL", -3, 120, datetime(2026, 2, 11))]
    base = ledger.build_closed_positions(clean, [_h("MU", 0)])
    with_dirty = ledger.build_closed_positions(dirty, [_h("MU", 0)])
    assert with_dirty == base
    assert len(with_dirty) == 1
    assert with_dirty[0]["realized_pnl"] == 200.0
    assert with_dirty[0]["exit_qty"] == 10.0
    assert with_dirty[0]["exit_avg"] == 120.0
    assert with_dirty[0]["approx"] is False


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
# 人工钉值覆盖（pnl_overrides）：期初成本缺失回放失真的人工修正
# ---------------------------------------------------------------------------

def test_pnl_override_applies():
    """钉值生效：孤儿 symbol（holdings 行已删、期初成本缺失回放虚高）→
    realized_pnl 覆盖为人工值、approx 清除、realized_pct 失效置 None。"""
    trades = [_tr("03152", "SELL", 1, 1135.10, datetime(2026, 10, 7),
                  name="A博时港元", market="HK")]
    rows = ledger.build_closed_positions(trades, [],
                                         pnl_overrides={"03152": -1.0})
    assert len(rows) == 1
    assert rows[0]["realized_pnl"] == -1.0
    assert rows[0]["approx"] is False
    assert rows[0]["realized_pct"] is None


def test_pnl_override_missing_symbol_untouched():
    """钉值字典不含该 symbol → 回放推导值原样保留。"""
    trades = [_tr("MU", "BUY", 10, 100, datetime(2026, 1, 5)),
              _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]
    rows = ledger.build_closed_positions(
        trades, [_h("MU", 0)], pnl_overrides={"03152": -1.0})
    assert rows[0]["realized_pnl"] == 200.0
    assert rows[0]["approx"] is False
    assert rows[0]["realized_pct"] == 20.0


def test_pnl_override_placeholder_not_applied():
    """占位行（no_records 无周期）不参与钉值覆盖。"""
    rows = ledger.build_closed_positions(
        [], [_h("F", 0, name="福特")], pnl_overrides={"F": -1.0})
    assert len(rows) == 1
    assert rows[0]["no_records"] is True
    assert rows[0]["realized_pnl"] is None


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

    async def fake_get_config(key):
        return None

    monkeypatch.setattr(server.db, "get_session", fake_get_session)
    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    c = TestClient(server.app)
    c.cookies.set("session_token", "tok")
    return c


def _reset_closed_cache(monkeypatch):
    monkeypatch.setattr(server, "_closed_ctx_cache",
                        {"ts": 0.0, "trades": None, "holdings": None})
    monkeypatch.setattr(server, "_closed_exit_ctx",
                        {"ts": 0.0, "mapping": {}})
    monkeypatch.setattr(server, "_closed_pnl_overrides_cache",
                        {"ts": 0.0, "value": None})
    monkeypatch.setattr(server, "_anchor_start_cache",
                        {"ts": 0.0, "value": None})


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


def test_overview_closed_pnl_override(monkeypatch):
    """sys_config closed_pnl_overrides JSON → closed 块人工钉值生效。"""
    async def fake_trades():
        return [_tr("03152", "SELL", 1, 1135.10, datetime(2026, 10, 7),
                    name="A博时港元", market="HK")]

    async def fake_holdings(enabled_only=False):
        return []

    async def fake_get_config(key):
        return '{"03152": -1.0}' if key == "closed_pnl_overrides" else None

    monkeypatch.setattr(server.db, "get_all_trades", fake_trades)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    closed = r.json().get("closed")
    assert closed and closed[0]["symbol"] == "03152"
    assert closed[0]["realized_pnl"] == -1.0
    assert closed[0]["approx"] is False


def test_overview_closed_anchor_start(monkeypatch):
    """sys_config anchor_start_date → closed 块期初周期 hold_days 生效。"""
    async def fake_trades():
        return [_tr("AMD", "SELL", 5, 100, datetime(2026, 10, 12))]

    async def fake_holdings(enabled_only=False):
        return [_h("AMD", 0, cost=80)]

    async def fake_get_config(key):
        return "2026-07-14" if key == "anchor_start_date" else None

    monkeypatch.setattr(server.db, "get_all_trades", fake_trades)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    client = _client(monkeypatch)
    monkeypatch.setattr(server.db, "get_config", fake_get_config)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    closed = r.json().get("closed")
    assert closed and closed[0]["symbol"] == "AMD"
    assert closed[0]["hold_days"] == 90


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


# ---------------------------------------------------------------------------
# 当日贡献出图口径：清仓超过 1 个交易日的 today_pnl 置 null
# （_mask_stale_closed_pnl 纯函数 + overview 端点接线）
# ---------------------------------------------------------------------------

def _pos(sym, qty, today=0.0):
    """构造 positions 形状行（mask 只消费 symbol/quantity/today_pnl）。"""
    return {"symbol": sym, "quantity": qty, "today_pnl": today}


def test_mask_stale_closed_pnl_weekday(monkeypatch):
    """工作日周三盘中（cutoff=周二）：在仓行/清仓当日/次交易日保留，
    更早清仓与无成交占位行 today_pnl 置 None；stale 行原样；入参不被修改。"""
    wed = datetime(2026, 10, 14, 12, 0, tzinfo=pf.ET_TZ)   # 周三 regular
    rows = [_pos("QQQ", 10, 30.0),     # 在仓：不动
            _pos("MU", 0),             # 清仓当日（exit=周三）
            _pos("AMD", 0),            # 次交易日（exit=周二）
            _pos("LITE", 0),           # 陈旧（exit=上周五）
            _pos("F", 0),              # qty=0 无成交占位：出图
            {"symbol": "SKHY", "quantity": 0, "today_pnl": None}]  # stale 原样
    out = server._mask_stale_closed_pnl(
        rows, {"MU": "2026-10-14", "AMD": "2026-10-13", "LITE": "2026-10-09"},
        now_et=wed)
    assert [p["today_pnl"] for p in out] == [30.0, 0.0, 0.0, None, None, None]
    assert rows[3]["today_pnl"] == 0.0   # 浅拷贝：入参行不被置 null


def test_mask_stale_closed_pnl_weekend(monkeypatch):
    """周六（场=周五，cutoff=周四）：周五/周四清仓保留，周三及更早出图
    （周四清仓到周六恰经 1 个完整交易日，不大于 1 仍保留）。"""
    sat = datetime(2026, 10, 17, 12, 0, tzinfo=pf.ET_TZ)
    out = server._mask_stale_closed_pnl(
        [_pos("A", 0), _pos("B", 0), _pos("C", 0)],
        {"A": "2026-10-16", "B": "2026-10-15", "C": "2026-10-14"}, now_et=sat)
    assert [p["today_pnl"] for p in out] == [0.0, 0.0, None]


def test_mask_stale_closed_pnl_monday_pre(monkeypatch):
    """周一盘前 08:00（场=周五，cutoff=周四）：周五清仓保留（仍属周五场），
    上周三出图；空映射时 qty=0 行全出图（冷启动/映射失败降级不误杀在仓）。"""
    mon_pre = datetime(2026, 10, 12, 8, 0, tzinfo=pf.ET_TZ)
    out = server._mask_stale_closed_pnl(
        [_pos("A", 0), _pos("B", 0)],
        {"A": "2026-10-09", "B": "2026-10-07"}, now_et=mon_pre)
    assert [p["today_pnl"] for p in out] == [0.0, None]
    empty = server._mask_stale_closed_pnl([_pos("A", 0)], {}, now_et=mon_pre)
    assert empty[0]["today_pnl"] is None   # qty=0 无映射：保守出图


def test_overview_masks_stale_closed_today_pnl(monkeypatch):
    """overview 端点接线：陈旧清仓行（exit=2026-02-10）today_pnl 置 null。"""
    async def fake_trades():
        return [_tr("MU", "BUY", 10, 100, datetime(2026, 2, 1)),
                _tr("MU", "SELL", 10, 120, datetime(2026, 2, 10))]

    async def fake_holdings(enabled_only=False):
        return [_h("MU", 0)]

    monkeypatch.setattr(server.db, "get_all_trades", fake_trades)
    monkeypatch.setattr(server.db, "get_holdings", fake_holdings)
    _reset_closed_cache(monkeypatch)
    monkeypatch.setattr(server, "portfolio_cache", {
        "ok": True, "session": "regular", "positions": [_pos("MU", 0)],
        "summary": {"total_value": 0.0}, "cash": [], "errors": []})
    client = _client(monkeypatch)
    r = client.get("/api/portfolio/overview")
    assert r.status_code == 200
    assert r.json()["positions"][0]["today_pnl"] is None
