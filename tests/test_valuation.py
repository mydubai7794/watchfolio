"""估值因子层测试：纯函数 + DB CRUD + 轮编排（全部打桩零网络零 DB 连接）。@author yys"""
import asyncio
import json
import pytest
from datetime import datetime
from types import SimpleNamespace

import database as db
import valuation
import longport_fetcher
import edgar_fetcher


# ---------------------------------------------------------------------------
# DB CRUD（fake pool 模式参照 tests/test_edgar_db.py）
# ---------------------------------------------------------------------------

def test_upsert_valuation_static_and_get(monkeypatch):
    """日更字段 upsert 不触碰 growth 列；周更 upsert 仅更新 growth。"""
    calls = []

    async def fake_exec(sql, params=None):
        calls.append((sql, params))

    async def fake_dict(sql, params=None):
        calls.append((sql, params))
        return [{"symbol": "NVDA", "eps_ttm": 8.0, "bps": 9.5,
                 "growth_pct": 66.67, "dividend_yield": 0.52,
                 "updated_at": "2026-10-11 16:00:00"}]

    monkeypatch.setattr(db, "execute", fake_exec)
    monkeypatch.setattr(db, "execute_dict", fake_dict)
    asyncio.run(db.upsert_holding_valuation_static(
        "NVDA", 8.0, 9.5, 0.52))
    asyncio.run(db.upsert_holding_valuation_growth("NVDA", 66.67))
    # static upsert 只更新 eps/bps/dy 三列
    assert "eps_ttm=VALUES(eps_ttm)" in calls[0][0]
    assert "growth_pct" not in calls[0][0].split("ON DUPLICATE")[1]
    # growth upsert 只更新 growth 列
    assert "growth_pct=VALUES(growth_pct)" in calls[1][0]
    assert "eps_ttm" not in calls[1][0].split("ON DUPLICATE")[1]
    rows = asyncio.run(db.get_valuations())
    assert rows["NVDA"]["eps_ttm"] == 8.0
    assert rows["NVDA"]["growth_pct"] == 66.67
    # 回归：execute_dict 无参数化不做 % 插值，DATE_FORMAT 必须 MySQL 单 %
    #（%% 会输出字面量字符串，2026-10-11 批1审查 Important-1）
    get_sql = calls[2][0]
    assert "%%" not in get_sql
    assert "DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s')" in get_sql


# ---------------------------------------------------------------------------
# 纯函数：normalize_static_info / parse_eps_growth（实测锚定 fixture）
# ---------------------------------------------------------------------------

def test_normalize_static_info():
    infos = [
        SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0033, bps=9.5014,
                        dividend_yield=0.52),
        SimpleNamespace(symbol="BABA.US", eps_ttm=4.3942, bps=64.3653,
                        dividend_yield=1.03),
        SimpleNamespace(symbol="SMH.US", eps_ttm=0, bps=0, dividend_yield=0),
    ]
    out = valuation.normalize_static_info(infos)
    assert out["NVDA"] == {"eps_ttm": 8.0033, "bps": 9.5014,
                           "dividend_yield": 0.52}
    assert out["BABA"]["eps_ttm"] == 4.3942
    assert out["SMH"]["eps_ttm"] == 0.0          # 保留原值，门控在上层
    assert "QQQ" not in out                      # 静默空（未返回标的）自然缺
    assert valuation.normalize_static_info(None) == {}
    # Decimal 与 None 防御
    from decimal import Decimal
    out2 = valuation.normalize_static_info([
        SimpleNamespace(symbol="X.US", eps_ttm=Decimal("1.5"), bps=None,
                        dividend_yield=None)])
    assert out2["X"] == {"eps_ttm": 1.5, "bps": None, "dividend_yield": None}


NVDA_CC = json.dumps({"units": {"USD/shares": [
    {"start": "2023-01-30", "end": "2024-01-28", "val": 1.19,
     "form": "10-K", "fp": "FY", "filed": "2024-02-21"},
    {"start": "2024-01-29", "end": "2025-01-26", "val": 2.94,
     "form": "10-K", "fp": "FY", "filed": "2025-02-26"},
    {"start": "2024-01-29", "end": "2025-01-26", "val": 2.94,
     "form": "10-K", "fp": "FY", "filed": "2025-02-26"},   # 重复申报行
    {"start": "2025-01-27", "end": "2026-01-25", "val": 4.90,
     "form": "10-K", "fp": "FY", "filed": "2026-02-25"},
    {"start": "2025-01-27", "end": "2026-04-26", "val": 3.5,
     "form": "10-Q", "fp": "Q1", "filed": "2026-05-28"},   # 季度值须被过滤
]}})

BABA_CC = json.dumps({"units": {
    "CNY/shares": [
        {"start": "2023-04-01", "end": "2024-03-31", "val": 6.94,
         "form": "20-F", "fp": "FY", "filed": "2024-06-01"},
        {"start": "2024-04-01", "end": "2025-03-31", "val": 6.7,
         "form": "20-F", "fp": "FY", "filed": "2025-06-01"},
        {"start": "2025-04-01", "end": "2026-03-31", "val": 5.5,
         "form": "20-F", "fp": "FY", "filed": "2026-06-01"},
    ],
    "USD/shares": [
        {"start": "2024-04-01", "end": "2025-03-31", "val": 0.92,
         "form": "20-F", "fp": "FY", "filed": "2025-06-01"},
        {"start": "2025-04-01", "end": "2026-03-31", "val": 0.80,
         "form": "20-F", "fp": "FY", "filed": "2026-06-01"},
    ]}})

CCJ_CC = json.dumps({"units": {"USD/shares": [
    {"start": "2024-01-01", "end": "2024-12-31", "val": 0.34,
     "form": "40-F", "fp": "FY", "filed": "2025-03-01"},
    {"start": "2025-01-01", "end": "2025-12-31", "val": 0.42,
     "form": "40-F", "fp": "FY", "filed": "2026-03-01"},
]}})


def test_parse_eps_growth():
    # NVDA：重复行去重 + 季度混入过滤 → 4.90/2.94-1
    assert valuation.parse_eps_growth(NVDA_CC) == pytest.approx(66.667, abs=0.01)
    # BABA：主币种 = 行数最多的 unit（CNY 3 行 > USD 2 行）→ 原币同比
    assert valuation.parse_eps_growth(BABA_CC) == pytest.approx(-17.91, abs=0.01)
    # 40-F 申报人有效
    assert valuation.parse_eps_growth(CCJ_CC) == pytest.approx(23.53, abs=0.01)
    # 财年不足两个 / v1=0 / 坏 JSON / None
    one_fy = json.dumps({"units": {"USD/shares": [
        {"start": "2025-01-01", "end": "2025-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(one_fy) is None
    zero_base = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 0,
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(zero_base) is None
    assert valuation.parse_eps_growth("not json") is None
    assert valuation.parse_eps_growth(None) is None
    # 合法 JSON 但结构异常（代理拦截页等）：顶层非 dict / val 非数值 → None
    assert valuation.parse_eps_growth("[1,2]") is None
    bad_val = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": {"x": 1},
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 2.0,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(bad_val) is None
    # 同财年重复行取 filed 最大（后申报修正值优先；修正申报行取白名单内
    # 同财年后申报的 10-K 行——保守口径不含 10-K/A）
    corrected = json.dumps({"units": {"USD/shares": [
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.0,
         "form": "10-K", "fp": "FY", "filed": "2025-02-01"},
        {"start": "2024-01-01", "end": "2024-12-31", "val": 1.2,
         "form": "10-K", "fp": "FY", "filed": "2025-04-01"},
        {"start": "2025-01-01", "end": "2025-12-31", "val": 2.4,
         "form": "10-K", "fp": "FY", "filed": "2026-02-01"}]}})
    assert valuation.parse_eps_growth(corrected) == pytest.approx(100.0, abs=0.01)


# ---------------------------------------------------------------------------
# 轮编排：run_valuation_round（打桩长桥/EDGAR/db）
# ---------------------------------------------------------------------------

def _v_env(monkeypatch, holdings, infos, cc_texts, growth_ts=None):
    """估值轮打桩环境：db 配置/持仓/upsert 与长桥/EDGAR 拉取全 fake，
    rec 记录调用序列供断言。@author yys"""
    cfg = {"VALUATION_GROWTH_TS": growth_ts}
    rec = {"static": [], "growth": [], "set_cfg": []}

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        cfg[key] = value
        rec["set_cfg"].append(key)

    async def fake_holdings(enabled_only=True):
        return holdings

    async def fake_up_static(sym, eps, bps, dy):
        rec["static"].append((sym, eps, bps, dy))

    async def fake_up_growth(sym, g):
        rec["growth"].append((sym, g))

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(db, "get_holdings", fake_holdings)
    monkeypatch.setattr(db, "upsert_holding_valuation_static", fake_up_static)
    monkeypatch.setattr(db, "upsert_holding_valuation_growth", fake_up_growth)

    async def fake_sinfo(symbols):
        return infos

    async def fake_cc(cik, concept="EarningsPerShareDiluted"):
        return cc_texts.get(cik)

    async def fake_tmap():
        return {"NVDA": 1045810, "BABA": 1577552}

    monkeypatch.setattr(longport_fetcher, "fetch_static_info", fake_sinfo)
    monkeypatch.setattr(edgar_fetcher, "fetch_companyconcept", fake_cc)
    monkeypatch.setattr(edgar_fetcher, "get_ticker_map", fake_tmap)
    return rec, cfg


_HOLDINGS = [
    {"symbol": "NVDA", "market": "US", "asset_type": "stock"},
    {"symbol": "BABA", "market": "US", "asset_type": "fpi"},
    {"symbol": "QQQ", "market": "US", "asset_type": "etf"},
    {"symbol": "DRAM", "market": "US", "asset_type": None},
    {"symbol": "F", "market": "US", "asset_type": None},
    {"symbol": "700.HK", "market": "HKFUND", "asset_type": None},
]


def test_run_valuation_round_full(monkeypatch):
    infos = [
        SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0, bps=9.5,
                        dividend_yield=0.52),
        SimpleNamespace(symbol="BABA.US", eps_ttm=4.39, bps=64.37,
                        dividend_yield=1.03),
        SimpleNamespace(symbol="DRAM.US", eps_ttm=0, bps=0,
                        dividend_yield=0),
        # F 不出现（_EDGAR_EXCLUDE，长桥批量请求列表已排除）
    ]
    rec, cfg = _v_env(monkeypatch, _HOLDINGS, infos,
                      {1045810: NVDA_CC, 1577552: BABA_CC})
    stats = asyncio.run(valuation.run_valuation_round())
    # us=4 只（NVDA/BABA/QQQ/DRAM）；700.HK 非 US 不进、F 被 _EDGAR_EXCLUDE 排除
    assert stats["rows"] == 4
    # F 被 _EDGAR_EXCLUDE 排除，长桥请求列表都不含 F
    assert ("F", None, None, None) not in rec["static"]
    # ETF fund 门控：QQQ 置空落库（覆盖历史噪声脏值），不写有效数值
    assert ("QQQ", None, None, None) in rec["static"]
    assert not any(r[0] == "QQQ" and r[1] is not None for r in rec["static"])
    # no_data：DRAM eps=0
    assert ("DRAM", 0.0, 0.0, 0.0) in rec["static"]
    # 增速：NVDA/BABA 均写入（首跑无 TS 必跑增速）
    assert ("NVDA", pytest.approx(66.667, abs=0.01)) in [
        (s, g) for s, g in rec["growth"]]
    assert ("BABA", pytest.approx(-17.91, abs=0.01)) in [
        (s, g) for s, g in rec["growth"]]
    assert cfg["VALUATION_GROWTH_TS"] is not None


def test_run_valuation_round_growth_throttle(monkeypatch):
    """非周一且 TS 距今 1 天 → 不拉增速（省请求），static 正常。
    假时钟固定周三 12:00，杜绝周一运行时 weekday()==0 恒真的 flaky
    （批2审查 Important-3）。"""
    class _FakeDt(datetime):
        @staticmethod
        def now():
            return datetime(2026, 10, 14, 12, 0, 0)   # 周三
    monkeypatch.setattr(valuation, "datetime", _FakeDt)
    infos = [SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0, bps=9.5,
                             dividend_yield=0.52)]
    rec, cfg = _v_env(monkeypatch, _HOLDINGS[:1], infos, {},
                      growth_ts="2026-10-13 12:00:00")   # 假时钟前一天
    stats = asyncio.run(valuation.run_valuation_round())
    assert rec["static"] == [("NVDA", 8.0, 9.5, 0.52)]
    assert rec["growth"] == []                     # 节流期内零增速请求
    assert stats["growth_runs"] is False


def test_run_valuation_round_tmap_none(monkeypatch):
    """增速门控开启但 EDGAR 映射不可用（None）：跳过增速不炸循环，
    static 正常落库（批2审查 Important-2 回归锚）。"""
    class _FakeDt(datetime):
        @staticmethod
        def now():
            return datetime(2026, 10, 14, 12, 0, 0)   # 周三
    monkeypatch.setattr(valuation, "datetime", _FakeDt)
    infos = [SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0, bps=9.5,
                             dividend_yield=0.52)]
    rec, cfg = _v_env(monkeypatch, _HOLDINGS[:1], infos, {},
                      growth_ts="2026-10-13 12:00:00")
    # 覆写：TS 已 1 天但强制 run_growth（映射失败场景在门控开启时才触发）
    monkeypatch.setattr(valuation, "GROWTH_INTERVAL_SEC", 0)
    async def fake_map():
        return None
    monkeypatch.setattr(edgar_fetcher, "get_ticker_map", fake_map)
    stats = asyncio.run(valuation.run_valuation_round())
    assert rec["static"] == [("NVDA", 8.0, 9.5, 0.52)]
    assert rec["growth"] == []                     # 映射不可用零增速请求
    assert stats["rows"] == 1
    assert "VALUATION_GROWTH_TS" not in rec["set_cfg"]   # 水位未推进


def test_run_valuation_round_no_cik(monkeypatch):
    """长桥数值好但无 CIK 映射：static 写入、增速跳过。"""
    infos = [SimpleNamespace(symbol="GEV.US", eps_ttm=5.0, bps=30.0,
                             dividend_yield=0.5)]
    holdings = [{"symbol": "GEV", "market": "US", "asset_type": "stock"}]
    rec, cfg = _v_env(monkeypatch, holdings, infos, {})
    asyncio.run(valuation.run_valuation_round())
    assert rec["static"] == [("GEV", 5.0, 30.0, 0.5)]
    assert rec["growth"] == []
