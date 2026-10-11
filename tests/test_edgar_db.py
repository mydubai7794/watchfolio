"""insider_tx 表 + holdings.asset_type CRUD 测试（fake pool 捕获 SQL，零连库）。@author yys"""
import asyncio
from datetime import date
from pathlib import Path


class _FakeCursor:
    def __init__(self):
        self.sqls = []
        self.many = []
        self.rowcount = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def executemany(self, sql, args):
        self.sqls.append(sql)
        self.many = list(args)
        self.rowcount = len(args)


class _FakeConn:
    def __init__(self):
        self.cur = _FakeCursor()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def cursor(self):
        return self.cur


class _FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


class _FakePool:
    def __init__(self):
        self.conn = _FakeConn()

    def acquire(self):
        return _FakeAcquire(self.conn)


def test_save_insider_tx_insert_ignore_and_null_passthrough(monkeypatch):
    import database as db

    pool = _FakePool()

    async def fake_get_pool():
        return pool

    monkeypatch.setattr(db, "get_pool", fake_get_pool)
    rows = [
        {"symbol": "MU", "cik": "723125", "accession": "0000723125-26-000001",
         "filed_date": "2026-10-08", "trade_date": "2026-10-07",
         "reporter": "SUNNY ERIC", "title": "EVP", "action": "A",
         "tx_code": "P", "shares": 10000, "price": 110.5, "value_usd": 1105000.0},
        {"symbol": "MU", "cik": "723125", "accession": "0000723125-26-000002",
         "filed_date": "2026-10-08", "trade_date": None,
         "reporter": "X", "title": "", "action": "D",
         "tx_code": "S", "shares": 500, "price": None, "value_usd": None},
    ]
    n = asyncio.run(db.save_insider_tx(rows))
    assert n == 2
    assert "INSERT IGNORE INTO insider_tx" in pool.conn.cur.sqls[0]
    assert pool.conn.cur.many[0][10] == 110.5      # price 列
    assert pool.conn.cur.many[1][10] is None       # price NULL 原样透传
    assert pool.conn.cur.many[1][11] is None       # value_usd NULL 原样透传
    assert asyncio.run(db.save_insider_tx([])) == 0


def test_get_insider_tx_window(monkeypatch):
    import database as db

    cap = {}

    async def fake_exec_dict(sql, args=None, _pool=None):
        cap["sql"], cap["args"] = sql, args
        return []

    monkeypatch.setattr(db, "execute_dict", fake_exec_dict)
    asyncio.run(db.get_insider_tx("MU", days=90))
    assert "FROM insider_tx" in cap["sql"]
    assert "INTERVAL %s DAY" in cap["sql"]
    assert cap["args"] == ("MU", 90)


def test_set_holding_asset_type(monkeypatch):
    import database as db

    cap = {}

    async def fake_execute(sql, args=None, _pool=None):
        cap["sql"], cap["args"] = sql, args
        return 1

    monkeypatch.setattr(db, "execute", fake_execute)
    assert asyncio.run(db.set_holding_asset_type("MU", "stock")) == 1
    assert "UPDATE holdings SET asset_type" in cap["sql"]
    assert cap["args"] == ("stock", "MU")


def test_insider_tx_and_asset_type_ddl_present():
    import database

    src = Path(database.__file__).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS insider_tx" in src
    assert "uk_tx" in src
    assert "ADD COLUMN `asset_type`" in src
