"""daily_bars 指标计算测试。@author yys"""
from datetime import timedelta

import pytest

import daily_bars as dbar


def _bars(closes, highs=None, lows=None):
    """构造日K序列：high=close+1、low=close-1（未给时）。"""
    return [{"date": f"2026-01-{i+1:02d}", "close": c,
             "high": (highs[i] if highs else c + 1),
             "low": (lows[i] if lows else c - 1), "open": c, "volume": 0}
            for i, c in enumerate(closes)]


def test_sma_basic():
    out = dbar.sma([1, 2, 3, 4, 5], 3)
    assert out[:2] == [None, None]
    assert out[2] == pytest.approx(2.0)
    assert out[4] == pytest.approx(4.0)


def test_rsi_wilder_all_up_is_100():
    closes = [float(i) for i in range(1, 30)]
    out = dbar.rsi_wilder(closes, 14)
    assert out[-1] == pytest.approx(100.0)


def test_rsi_wilder_all_down_is_0():
    closes = [float(30 - i) for i in range(30)]
    out = dbar.rsi_wilder(closes, 14)
    assert out[-1] == pytest.approx(0.0)


def test_atr_constant_range():
    # 每根 TR = (high-low)=2 与前收跳空 0 → TR=2，ATR 恒 2
    bars = _bars([10.0] * 30)
    out = dbar.atr_wilder(bars, 22)
    assert out[-1] == pytest.approx(2.0)


def test_hhv_window():
    bars = _bars([1, 5, 3, 2, 8, 4], )
    out = dbar.hhv([b["high"] for b in bars], 3)
    assert out[4] == pytest.approx(9.0)    # max(3+1, 2+1, 8+1)=9


def test_compute_indicators_snapshot():
    bars = _bars([10.0 + 0.1 * i for i in range(210)])
    snap = dbar.compute_indicators(bars)
    assert snap["enough_history"] is True
    assert snap["ma200"] is not None and snap["rsi14"] == pytest.approx(100.0)
    assert snap["hhv22"] > snap["ma50"]
    assert snap["stop_level"] == pytest.approx(snap["hhv22"] - 3 * snap["atr22"])


def test_compute_indicators_short_history():
    snap = dbar.compute_indicators(_bars([1.0] * 60))
    assert snap["enough_history"] is False
    assert snap["ma200"] is None and snap["atr22"] is not None


def test_sma_n_gt_len_returns_none():
    assert dbar.sma([1.0, 2.0], 5) == [None, None]


def test_atr_n_gt_len():
    assert dbar.atr_wilder(_bars([1.0] * 5), 22) == [None] * 5


def test_compute_indicators_tiny_history_all_none():
    snap = dbar.compute_indicators(_bars([1.0] * 10))
    assert snap["stop_level"] is None
    assert snap["enough_history"] is False
    assert snap["ma50"] is None
    with pytest.raises(ValueError):
        dbar.compute_indicators([])


def test_rsi_flat_market():
    out = dbar.rsi_wilder([5.0] * 30, 14)
    assert out[-1] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# 增量合并 / 除权检测 / 长桥符号后缀（Task 3）
# ---------------------------------------------------------------------------


def test_merge_incremental_new_only():
    existing = {"2026-01-01": 10.0, "2026-01-02": 11.0}
    fetched = [{"date": "2026-01-02", "close": 11.0}, {"date": "2026-01-03", "close": 12.0}]
    new_rows, reload_needed = dbar.merge_bars(existing, fetched)
    assert reload_needed is False
    assert [r[0] for r in new_rows] == ["2026-01-03"]


def test_merge_detects_adjustment_event():
    existing = {"2026-01-02": 11.0}
    fetched = [{"date": "2026-01-02", "close": 5.5},   # 复权因子跳变 >0.5%
               {"date": "2026-01-03", "close": 5.6}]
    new_rows, reload_needed = dbar.merge_bars(existing, fetched)
    assert reload_needed is True


def test_symbols_need_us_suffix():
    assert dbar.lp_symbol("QQQ") == "QQQ.US"
    assert dbar.lp_symbol("QQQ.US") == "QQQ.US"


# ---- macd/kdj/aggregate_bars/ticks_to_bars（K线弹层增强）----

def test_ema_seeded_exact():
    """EMA 首值 seed=首个值；n=2 时手工精确值。"""
    out = dbar.ema([10.0, 10.0, 10.0, 20.0], 2)
    assert out[0] == 10.0 and out[1] == 10.0 and out[2] == 10.0
    assert abs(out[3] - 50.0 / 3.0) < 1e-9


def test_macd_structure_and_sign():
    """MACD：长度对齐、hist=2×(DIF−DEA)（国内惯例）、单调涨 DIF>0、横盘归零。"""
    rises = [float(i) for i in range(1, 41)]        # 40 根单调涨
    flat = [10.0] * 40
    for closes in (rises, flat):
        m = dbar.macd(closes)
        assert len(m["dif"]) == len(m["dea"]) == len(m["hist"]) == len(closes)
        for i in range(len(closes)):
            if m["dif"][i] is not None and m["dea"][i] is not None:
                assert abs(m["hist"][i] - 2.0 * (m["dif"][i] - m["dea"][i])) < 1e-9
    assert dbar.macd(rises)["dif"][-1] > 0           # 单调涨 → DIF 为正
    assert abs(dbar.macd(flat)["dif"][-1]) < 1e-6    # 横盘 → DIF≈0


def test_kdj_exact_n2():
    """KDJ n=2 手工精确值：RSV=66.667→K=55.556/D=51.852/J=62.963；首根样本不足 None。"""
    out = dbar.kdj([10.0, 12.0], [9.0, 10.0], [10.0, 11.0], n=2)
    assert out["k"][0] is None and out["d"][0] is None and out["j"][0] is None
    assert abs(out["k"][1] - 55.556) < 1e-3
    assert abs(out["d"][1] - 51.852) < 1e-3
    assert abs(out["j"][1] - 62.963) < 1e-3


def test_kdj_flat_hh_equals_ll():
    """高=低（一字）时 RSV=50 防除零。"""
    out = dbar.kdj([10.0] * 5, [10.0] * 5, [10.0] * 5, n=3)
    assert abs(out["k"][-1] - 50.0) < 1e-9


def test_aggregate_bars_week_and_month():
    """2026-09-28(周一)~10-02 五根日K：周K 聚成 1 根（ISO 周 40），月K 聚成 2 根。"""
    bars = [
        {"date": f"2026-09-{d}", "open": 10.0 + d, "high": 12.0 + d,
         "low": 9.0 + d, "close": 11.0 + d, "volume": 100 * d}
        for d in (28, 29, 30)
    ] + [
        {"date": f"2026-10-0{d}", "open": 10.0 + d, "high": 12.0 + d,
         "low": 9.0 + d, "close": 11.0 + d, "volume": 100 * d}
        for d in (1, 2)
    ]
    wk = dbar.aggregate_bars(bars, "week")
    assert len(wk) == 1
    assert wk[0]["date"] == "2026-09-28"
    assert wk[0]["open"] == 38.0 and wk[0]["close"] == 13.0    # 首开(10+28)/末收(11+2)
    assert wk[0]["high"] == 42.0 and wk[0]["low"] == 10.0      # 区间极值 max(12+d)/min(9+d)
    assert wk[0]["volume"] == 100 * (28 + 29 + 30 + 1 + 2)
    mo = dbar.aggregate_bars(bars, "month")
    assert len(mo) == 2 and mo[0]["date"] == "2026-09-28" and mo[1]["date"] == "2026-10-01"
    assert dbar.aggregate_bars(bars, "day") == bars            # day 原样返回


def test_ticks_to_bars_bucket_300s():
    """300s 分桶：开=桶首价、收=桶末价、高/低=桶内极值、量=tick 数；跨桶正确切分。"""
    from datetime import datetime, timezone
    def t(sec):
        return datetime(2026, 10, 2, 13, 30, 0, tzinfo=timezone.utc) \
            .replace(tzinfo=None) + timedelta(seconds=sec)
    rows = [
        {"ts": t(0), "price": 100.0}, {"ts": t(120), "price": 101.0},
        {"ts": t(299), "price": 99.0},                       # 桶 A：3 tick
        {"ts": t(300), "price": 98.0}, {"ts": t(480), "price": 97.0},  # 桶 B
    ]
    bars = dbar.ticks_to_bars(rows, bucket_sec=300)
    assert len(bars) == 2
    assert bars[0]["open"] == 100.0 and bars[0]["close"] == 99.0
    assert bars[0]["high"] == 101.0 and bars[0]["low"] == 99.0
    assert bars[0]["volume"] == 3
    assert bars[1]["open"] == 98.0 and bars[1]["close"] == 97.0
    assert bars[0]["date"] == "10-02 21:30"    # UTC 13:30 +8h 北京时间标签
    assert dbar.ticks_to_bars([]) == []


def test_aggregate_bars_empty_and_single():
    """空输入返回空列表；单根 bar 聚 week 后 OHLC/量原值保持。"""
    assert dbar.aggregate_bars([], "week") == []
    single = [{"date": "2026-09-28", "open": 10.0, "high": 12.0,
               "low": 9.0, "close": 11.0, "volume": 100}]
    wk = dbar.aggregate_bars(single, "week")
    assert len(wk) == 1
    assert wk[0] == {"date": "2026-09-28", "open": 10.0, "high": 12.0,
                     "low": 9.0, "close": 11.0, "volume": 100}


# ---------------- V2 规则支撑：week52_high / VIX 分位 / CSV 种子 ----------------

def _mk_bars(n, base=100.0):
    """构造 n 根单边递增日K（close=base+i）。"""
    return [{"date": f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}",
             "open": base + i, "high": base + i, "low": base + i,
             "close": base + i, "volume": 0} for i in range(n)]


def test_compute_indicators_week52_high():
    ind = dbar.compute_indicators(_mk_bars(300))
    # 300 根递增（100..399）：近 252 根最高 = 399
    assert ind["week52_high"] == 399.0
    ind2 = dbar.compute_indicators(_mk_bars(100))
    # 历史不足 252 根 → None（deep_dip 不触发）
    assert ind2["week52_high"] is None


def test_vix_percentile():
    # 末值 4：窗口 [5,1,2,3,4] 中 <=4 的有 4 个 → 80.0
    assert dbar._vix_percentile([5.0, 1.0, 2.0, 3.0, 4.0]) == 80.0
    # 末值为最小 → 1/N
    assert dbar._vix_percentile([9.0, 8.0, 1.0]) == pytest.approx(100.0 / 3)
    # 窗口截断：>504 根取尾
    closes = [10.0] * 503 + [1.0] + [100.0]   # 505 根，末值 100（全局最高）
    assert dbar._vix_percentile(closes) == 100.0   # 尾窗 504 根里 100 仍是最大


def test_get_vix_percentile_stale_guard(monkeypatch):
    monkeypatch.setattr(dbar, "VIX_PCTL", 15.0)
    monkeypatch.setattr(dbar, "VIX_ASOF", "2026-10-01")
    # VIX 落后 QQQ 基准日 → None（拉取失败退化）
    assert dbar.get_vix_percentile("2026-10-02") is None
    # 同步/领先 → 返回分位
    assert dbar.get_vix_percentile("2026-10-01") == 15.0
    assert dbar.get_vix_percentile("2026-09-30") == 15.0
    assert dbar.get_vix_percentile(None) == 15.0
    # 缓存空 → None
    monkeypatch.setattr(dbar, "VIX_PCTL", None)
    assert dbar.get_vix_percentile(None) is None


def test_vix_seed_parse_csv(tmp_path):
    import vix_seed
    p = tmp_path / "vix.csv"
    p.write_text("date,close\n2026-10-02,15.31\n2016-10-03,13.57\n", encoding="utf-8")
    rows = vix_seed.parse_csv(str(p))
    # 排序升序 + open/high/low=close 填充 + volume=0
    assert rows == [("2016-10-03", 13.57, 13.57, 13.57, 13.57, 0),
                    ("2026-10-02", 15.31, 15.31, 15.31, 15.31, 0)]


# ---------------- 市场底部信号底座：ma200_bias / fib 回撤 ----------------

def test_compute_indicators_fib_retracement():
    # 先涨后跌：低点段 close=10 → 高点段 close=20 → 末日回撤到 15；
    # high=close+1 / low=close-1（_bars 默认）→ H=21, L=9, depth=(21-15)/12=0.5
    closes = [10.0] * 105 + [20.0] * 105
    closes[-1] = 15.0
    snap = dbar.compute_indicators(_bars(closes))
    fib = snap["fib"]
    assert fib is not None
    assert fib["high"] == pytest.approx(21.0)
    assert fib["low"] == pytest.approx(9.0)
    assert fib["depth"] == pytest.approx(0.5)
    assert fib["levels"]["500"] == pytest.approx(15.0)
    assert fib["levels"]["382"] == pytest.approx(21.0 - 0.382 * 12.0)


def test_compute_indicators_fib_none_when_downtrend():
    # 先高后低（下跌延续段，L 晚于 H）：fib 必须为 None
    closes = [20.0] * 105 + [10.0] * 105
    snap = dbar.compute_indicators(_bars(closes))
    assert snap["fib"] is None


def test_compute_indicators_ma200_bias():
    bars = _bars([10.0 + 0.1 * i for i in range(210)])
    snap = dbar.compute_indicators(bars)
    expected = (snap["last_close"] - snap["ma200"]) / snap["ma200"] * 100.0
    assert snap["ma200_bias"] == pytest.approx(expected)


def test_compute_indicators_ma200_bias_none_when_short():
    snap = dbar.compute_indicators(_bars([1.0] * 60))
    assert snap["ma200"] is None and snap["ma200_bias"] is None
