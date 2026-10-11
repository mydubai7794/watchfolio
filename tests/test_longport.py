# -*- coding: utf-8 -*-
"""长桥行情模块与数据源架构升级（2026-10-03）单测。

覆盖：
- longport_fetcher：token exp 解析 / 错误码分流 / 可用性状态机 / 扩展价会话映射
- portfolio_fetcher：fetch_sina_panel 解析 / fetch_market_panel USDCNH 主备注入
- fetcher：期指东财恒主 / fetch_overnight 长桥优先+新浪补标普
全部 mock 网络层，不依赖真实外部服务。
"""

import asyncio
import time
from datetime import datetime, timedelta

import pytest

import longport_fetcher as lp
import portfolio_fetcher as pf
import fetcher as ft


# ---------------------------------------------------------------------------
# longport_fetcher：纯函数与状态机
# ---------------------------------------------------------------------------

def _fake_jwt(exp_epoch: int) -> str:
    """程序内构造纯伪造 JWT（hk_ 前缀 + base64url 三段），与真实 token 零关联。"""
    import base64
    import json as _json

    def _seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(
            _json.dumps(obj, separators=(",", ":")).encode()).decode().rstrip("=")
    header = {"alg": "RS256", "typ": "JWT", "kid": "unit-test-fake-kid"}
    payload = {"iss": "unit-test", "sub": "unit-test",
               "exp": exp_epoch, "ak": "fake"}
    return f"hk_m_{_seg(header)}.{_seg(payload)}.fake-signature"


FAKE_EXP = int(time.time()) + 90 * 86400   # 未来 90 天（未过期，剩余天数断言用）
FAKE_TOKEN = _fake_jwt(FAKE_EXP)


class TestTokenParse:
    def test_exp_parse(self):
        assert lp._parse_token_exp(FAKE_TOKEN) == float(FAKE_EXP)

    def test_exp_invalid(self):
        assert lp._parse_token_exp("not-a-jwt") is None
        assert lp._parse_token_exp("") is None

    def test_days_left_positive(self):
        # 用未过期 token 计算剩余天数（构造时约 90 天）
        days = lp._token_days_left(FAKE_TOKEN)
        assert days is not None and 0 < days < 365

    def test_days_left_expired(self):
        import base64, json as _json
        payload = base64.urlsafe_b64encode(
            _json.dumps({"exp": int(time.time() - 86400)}).encode()).decode().rstrip("=")
        assert lp._token_days_left(f"xx.{payload}.yy") < 0


class TestErrorCode:
    def test_extract_from_message(self):
        class E(Exception):
            pass
        assert lp._extract_error_code(E("OpenApiException: 401004 token invalid")) == "401004"
        assert lp._extract_error_code(E("rate limited code=301606")) == "301606"
        assert lp._extract_error_code(E("network timeout")) == ""

    def test_extract_from_code_attr(self):
        class E(Exception):
            code = 301604
        assert lp._extract_error_code(E("whatever")) == "301604"


class _LpState:
    """longport_fetcher 模块级状态快照/还原（跨用例隔离）。"""

    def __init__(self):
        self.snap = {}

    def __enter__(self):
        for k in ("fail_streak", "token_invalid", "_backoff_until",
                  "last_error", "last_error_code", "_ctx",
                  "_creds_cache", "_creds_ts", "_creds_warned",
                  "_creds_mixed_warned",
                  "_refresh_at", "_refresh_result"):
            self.snap[k] = getattr(lp, k)
        return self

    def __exit__(self, *a):
        for k, v in self.snap.items():
            setattr(lp, k, v)


class TestFailureRouting:
    """错误码分流：401004→停用；301603/301604→直接切冷备；301606→退避；其他→累计。"""

    def test_401004_token_invalid(self):
        with _LpState():
            lp.fail_streak = 0
            lp._mark_failure(Exception("OpenApiException: 401004 token invalid"))
            assert lp.token_invalid is True
            assert lp.is_available() is False

    def test_301604_direct_fallback(self):
        with _LpState():
            lp.fail_streak, lp.token_invalid = 0, False
            class E(Exception):
                code = 301604
            lp._mark_failure(E("permission"))
            assert lp.fail_streak >= lp._FAIL_THRESHOLD
            assert lp.is_available() is False

    def test_301606_backoff(self):
        with _LpState():
            lp.fail_streak, lp.token_invalid = 0, False
            lp._backoff_until = 0.0
            class E(Exception):
                code = 301606
            lp._mark_failure(E("rate"))
            assert time.monotonic() < lp._backoff_until   # 退避窗口生效
            assert lp.is_available() is False

    def test_other_error_accumulate(self):
        with _LpState():
            lp.fail_streak, lp.token_invalid = 0, False
            lp._mark_failure(Exception("connect timeout"))
            assert lp.fail_streak == 1
            assert lp.is_available() is True   # 未过阈值仍可用

    def test_three_strikes_unavailable(self):
        with _LpState():
            lp.fail_streak, lp.token_invalid = 0, False
            for _ in range(3):
                lp._mark_failure(Exception("timeout"))
            assert lp.is_available() is False
            lp._mark_success()
            assert lp.is_available() is True   # 恢复


class TestStatus:
    def test_status_shape(self):
        with _LpState():
            lp.fail_streak, lp.token_invalid = 1, False
            lp._creds_cache = {"access_token": FAKE_TOKEN}
            lp._creds_ts = time.monotonic()   # 命中热加载缓存
            s = lp.status()
            assert s["token_status"] in ("ok", "expiring", "expired", "unknown")
            assert {"available", "fallback_active", "fail_streak",
                    "last_error_code", "days_left", "token_expires_at"} <= set(s)

    def test_status_token_invalid(self):
        with _LpState():
            lp.token_invalid = True
            lp._creds_cache = {"access_token": FAKE_TOKEN}
            lp._creds_ts = time.monotonic()
            assert lp.status()["token_status"] == "invalid"


class _FakeSub:
    def __init__(self, price, ts=None):
        self.last_done = str(price)
        self.timestamp = ts


class _FakeQuote:
    def __init__(self, symbol, last, prev=0.0, pre=None, post=None, overnight=None):
        self.symbol = symbol
        self.last_done = str(last)
        self.prev_close = str(prev)
        self.pre_market_quote = pre
        self.post_market_quote = post
        self.overnight_quote = overnight


class TestExtFromQuote:
    def test_pre_session(self):
        q = _FakeQuote("AAPL.US", 333.0, pre=_FakeSub(332.85))
        price, ts = lp._ext_from_quote(q, "pre")
        assert price == 332.85
        assert ts == ""   # 无 timestamp 时为空，由 fetch_us_ext 兜底"长桥"标签

    def test_pre_session_with_ts(self):
        ts_dt = datetime(2026, 10, 3, 4, 30)
        q = _FakeQuote("AAPL.US", 333.0, pre=_FakeSub(332.85, ts=ts_dt))
        price, ts = lp._ext_from_quote(q, "pre")
        assert price == 332.85
        assert ts.startswith("长桥 ")   # 格式化为本地时间标签

    def test_post_session(self):
        q = _FakeQuote("AAPL.US", 333.0, post=_FakeSub(333.54))
        assert lp._ext_from_quote(q, "post")[0] == 333.54

    def test_closed_prefers_overnight(self):
        q = _FakeQuote("AAPL.US", 333.0, post=_FakeSub(333.54),
                       overnight=_FakeSub(331.38))
        assert lp._ext_from_quote(q, "closed")[0] == 331.38

    def test_closed_degrades_to_post(self):
        q = _FakeQuote("AAPL.US", 333.0, post=_FakeSub(333.54))
        assert lp._ext_from_quote(q, "closed")[0] == 333.54

    def test_none_sub(self):
        q = _FakeQuote("AAPL.US", 333.0)
        assert lp._ext_from_quote(q, "pre") == (0.0, "")


class TestFetchUsExtFallback:
    def test_unavailable_returns_none(self):
        with _LpState():
            lp.token_invalid = True
            assert asyncio.run(lp.fetch_us_ext(["AAPL"], "pre")) is None

    def test_available_path(self, monkeypatch):
        with _LpState():
            lp.token_invalid, lp.fail_streak = False, 0
            async def fake_quote_raw(symbols):
                assert symbols == ["AAPL.US", "MU.US"]
                return [_FakeQuote("AAPL.US", 333.0, pre=_FakeSub(332.85)),
                        _FakeQuote("MU.US", 1075.0, pre=_FakeSub(1105.08))]
            monkeypatch.setattr(lp, "_quote_raw", fake_quote_raw)
            out = asyncio.run(lp.fetch_us_ext(["AAPL", "MU"], "pre"))
            assert out == {"AAPL": {"ext_price": 332.85, "ext_time": "长桥"},
                           "MU": {"ext_price": 1105.08, "ext_time": "长桥"}}
            assert lp.fail_streak == 0   # 成功清账


class TestFetchQuoteMapProbe:
    def test_cold_backup_throttled(self):
        with _LpState():
            lp.fail_streak = 10   # 深度冷备
            lp._probe_at = time.monotonic()   # 刚探测过 → 节流内不再请求
            assert asyncio.run(lp.fetch_quote_map([".VIX.US"])) == {}


class _FakeCandle:
    def __init__(self, open_, close, ts):
        self.open = str(open_)
        self.close = str(close)
        self.timestamp = ts


class TestFetchAnchorQuotes:
    """一场口径锚点（长桥日K主源）：prev=昨日收盘(场基准)、close=当日常规收盘定格。"""

    def test_two_candles_full_anchor(self, monkeypatch):
        with _LpState():
            lp.token_invalid, lp.fail_streak = False, 0
            d1 = datetime(2026, 10, 1)
            d2 = datetime(2026, 10, 2)

            async def fake_candles(symbol):
                assert symbol == "QQQ.US"
                return [_FakeCandle(738.0, 742.03, d1), _FakeCandle(740.1, 749.58, d2)]
            monkeypatch.setattr(lp, "_candles_raw", fake_candles)
            out = asyncio.run(lp.fetch_anchor_quotes(["QQQ"]))
            assert out == {"QQQ": {"prev": 742.03, "open": 740.1, "close": 749.58}}

    def test_single_candle_prev_missing(self, monkeypatch):
        with _LpState():
            lp.token_invalid, lp.fail_streak = False, 0

            async def fake_candles(symbol):
                return [_FakeCandle(740.1, 749.58, datetime(2026, 10, 2))]
            monkeypatch.setattr(lp, "_candles_raw", fake_candles)
            out = asyncio.run(lp.fetch_anchor_quotes(["QQQ"]))
            # prev 缺失置 0（server 端退化用 open，与 TD 锚点同构）
            assert out == {"QQQ": {"prev": 0.0, "open": 740.1, "close": 749.58}}

    def test_out_of_order_sorted(self, monkeypatch):
        with _LpState():
            lp.token_invalid, lp.fail_streak = False, 0

            async def fake_candles(symbol):
                # SDK 异常乱序（新在前旧在后）→ 按 timestamp 排序后仍正确
                return [_FakeCandle(740.1, 749.58, datetime(2026, 10, 2)),
                        _FakeCandle(738.0, 742.03, datetime(2026, 10, 1))]
            monkeypatch.setattr(lp, "_candles_raw", fake_candles)
            out = asyncio.run(lp.fetch_anchor_quotes(["QQQ"]))
            assert out["QQQ"]["prev"] == 742.03 and out["QQQ"]["close"] == 749.58

    def test_fail_symbol_skipped(self, monkeypatch):
        with _LpState():
            lp.token_invalid, lp.fail_streak = False, 0

            async def fake_candles(symbol):
                if symbol == "PCG.US":
                    raise RuntimeError("boom")
                return [_FakeCandle(738.0, 742.03, datetime(2026, 10, 1)),
                        _FakeCandle(740.1, 749.58, datetime(2026, 10, 2))]
            monkeypatch.setattr(lp, "_candles_raw", fake_candles)
            out = asyncio.run(lp.fetch_anchor_quotes(["QQQ", "PCG"]))
            assert list(out) == ["QQQ"]   # 失败标的自然缺省

    def test_unavailable_empty(self):
        with _LpState():
            lp.token_invalid = True
            assert asyncio.run(lp.fetch_anchor_quotes(["QQQ"])) == {}


class TestSdkCallTimeout:
    """SDK 同步调用超时护栅：阻塞调用超时后废弃连接并计入失败分流。"""

    def test_timeout_discards_ctx(self, monkeypatch):
        import threading
        with _LpState():
            lp._SDK_CALL_TIMEOUT = 0.2
            released = threading.Event()

            def blocked_call(symbols):
                released.wait(5)   # 模拟 SDK WebSocket 卡死（永久阻塞）
                return []
            ctx = type("C", (), {"quote": staticmethod(blocked_call)})()
            monkeypatch.setattr(lp, "_get_ctx", lambda: _async_val(ctx))

            async def run():
                return await lp._quote_raw(["QQQ.US"])
            out = asyncio.run(run())
            released.set()   # 释放后台线程
            assert out == []                    # 超时 → 空结果
            assert lp._ctx is None              # 坏连接已废弃（下次重建）
            assert lp.last_error and "超时" in lp.last_error

    def test_timeout_backoff(self, monkeypatch):
        import threading
        with _LpState():
            lp._SDK_CALL_TIMEOUT = 0.2
            released = threading.Event()

            def blocked_call(symbol, period, count, adjust):
                released.wait(5)
                return []
            ctx = type("C", (), {"candlesticks": staticmethod(blocked_call)})()
            monkeypatch.setattr(lp, "_get_ctx", lambda: _async_val(ctx))

            async def run():
                return await lp._candles_raw("QQQ.US")
            out = asyncio.run(run())
            released.set()
            assert out == []
            assert time.monotonic() >= lp._backoff_until   # 已进入退避窗口


async def _async_val(v):
    return v


# ---------------------------------------------------------------------------
# token 自动续期（maybe_refresh_token）与看板指数批量（fetch_index_quotes）
# ---------------------------------------------------------------------------

def _mk_token(days: float) -> str:
    """构造指定剩余天数的 JWT 形 token（hk_ 前缀 + 可解析 exp）。"""
    import base64 as _b64, json as _json
    payload = _b64.urlsafe_b64encode(_json.dumps(
        {"exp": int(time.time() + days * 86400)}).encode()).decode().rstrip("=")
    return f"hk_m_xx.{payload}.yy"


class _FakeRefreshCfg:
    """替身 Config：refresh_access_token 返回类属性 new_token（Exception 则抛出）。"""
    new_token: object = "bad"

    def __init__(self, *a, **k):
        pass

    def refresh_access_token(self, expired_at=None):
        if isinstance(self.new_token, Exception):
            raise self.new_token
        return self.new_token


class TestMaybeRefresh:
    """到期自动续期：余量充足不触发 / 触发后写库+缓存失效 / 节流 / 异常值忽略。"""

    def _setup(self, monkeypatch, days_left):
        async def fake_load_creds():
            return {"app_key": "k", "app_secret": "s",
                    "access_token": _mk_token(days_left)}
        monkeypatch.setattr(lp, "_load_creds", fake_load_creds)
        monkeypatch.setattr(lp, "_SDK_OK", True)
        monkeypatch.setattr(lp, "_SDK_HAS_REFRESH", True)
        monkeypatch.setattr(lp, "_SDK_NAME", "longbridge")
        monkeypatch.setattr(lp, "_LpConfig", _FakeRefreshCfg)
        import database as db
        saved = []

        async def fake_set(key, value, remark=""):
            saved.append((key, value))
            return 1
        monkeypatch.setattr(db, "set_config", fake_set)
        return saved

    def test_days_sufficient_skip(self, monkeypatch):
        with _LpState():
            saved = self._setup(monkeypatch, 80)
            assert asyncio.run(lp.maybe_refresh_token()) is None
            assert saved == []   # 余量充足不触发，不消耗 refresh 次数

    def test_refresh_success_writes_db(self, monkeypatch):
        with _LpState():
            saved = self._setup(monkeypatch, 3)
            new_tok = _mk_token(90)
            _FakeRefreshCfg.new_token = new_tok
            out = asyncio.run(lp.maybe_refresh_token())
            assert out == new_tok
            assert saved and saved[0][0] == lp.CFG_KEY_ACCESS_TOKEN
            assert saved[0][1] == new_tok
            assert lp._creds_cache is None      # 凭证缓存加急失效
            assert lp.token_invalid is False    # 401004 标记自愈
            assert lp._refresh_result.startswith("ok")

    def test_throttled_within_6h(self, monkeypatch):
        with _LpState():
            self._setup(monkeypatch, 3)
            lp._refresh_at = time.monotonic()   # 刚尝试过 → 节流内跳过
            _FakeRefreshCfg.new_token = _mk_token(90)
            assert asyncio.run(lp.maybe_refresh_token()) is None

    def test_bad_new_token_ignored(self, monkeypatch):
        with _LpState():
            saved = self._setup(monkeypatch, 2)
            _FakeRefreshCfg.new_token = "not-a-jwt"   # 非法值不写库
            assert asyncio.run(lp.maybe_refresh_token()) is None
            assert saved == []
            assert "校验失败" in lp._refresh_result

    def test_sdk_without_refresh_skip(self, monkeypatch):
        with _LpState():
            self._setup(monkeypatch, 3)
            monkeypatch.setattr(lp, "_SDK_HAS_REFRESH", False)  # longport 旧 SDK
            assert asyncio.run(lp.maybe_refresh_token()) is None

    def test_refresh_exception_retries_later(self, monkeypatch):
        with _LpState():
            saved = self._setup(monkeypatch, 3)
            _FakeRefreshCfg.new_token = RuntimeError("network down")
            assert asyncio.run(lp.maybe_refresh_token()) is None
            assert saved == []
            assert lp._refresh_result.startswith("失败")
            assert time.monotonic() - lp._refresh_at < 5   # 已记录尝试时刻（6h 节流）


class TestFetchIndexQuotes:
    """看板指数批量：符号→短名映射、缺项自然省略。"""

    def test_map_and_missing(self, monkeypatch):
        async def fake_map(symbols):
            assert symbols == list(lp.INDEX_SYMBOLS)
            return {".VIX.US": {"price": 15.31, "prev": 15.0, "change_pct": 2.1},
                    ".SPX.US": {"price": 7722.72, "prev": 7700.0, "change_pct": 0.29}}
        monkeypatch.setattr(lp, "fetch_quote_map", fake_map)
        out = asyncio.run(lp.fetch_index_quotes())
        assert out == {"VIX": {"price": 15.31, "prev": 15.0, "change_pct": 2.1},
                       "SPX": {"price": 7722.72, "prev": 7700.0, "change_pct": 0.29}}

    def test_empty(self, monkeypatch):
        async def fake_map(symbols):
            return {}
        monkeypatch.setattr(lp, "fetch_quote_map", fake_map)
        assert asyncio.run(lp.fetch_index_quotes()) == {}


# ---------------------------------------------------------------------------
# portfolio_fetcher：新浪低频批与看板主备注入
# ---------------------------------------------------------------------------

SINA_PANEL_RAW = (
    'var hq_str_DINIW="美元指数,104.3000,104.0900,104.5100,104.9300,104.1100,'
    '104.6600,1234,104.3000,-0.2100,美元指数,104.5100";\n'
    'var hq_str_fx_susdcnh="美元人民币,7.1079,7.1129,7.1152,7.1129,7.1152,7.1130,'
    '7.1150,7.1079,7.1129,-0.0050,-0.0702,16:35:27";\n'
    'var hq_str_hf_OIL="71.120,70.000,71.100,72.000,70.500,71.200,70.800,70.950,'
    '12345,71.100,2,71.120,原油,0";\n'
    'var hq_str_hf_GC="4000.100,3950.000,3999.000,4010.000,3940.000,4005.000,'
    '3980.000,3960.500,54321,4000.000,3,4000.100,黄金,0";\n')


class TestFetchSinaPanel:
    def test_parse(self, monkeypatch):
        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            assert "DINIW" in url
            return SINA_PANEL_RAW
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        panel = asyncio.run(pf.fetch_sina_panel(proxy_urls=[]))
        assert panel["dxy"].get("price") == 104.3
        assert panel["cnh"].get("price") == 7.1079
        assert panel["hf"]["BZ"].get("price") == 71.12
        assert panel["hf"]["GC"].get("price") == 4000.1

    def test_fail_empty(self, monkeypatch):
        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            return None
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        assert asyncio.run(pf.fetch_sina_panel(proxy_urls=[])) == {}


class TestPanelUsdcnhInjection:
    """fetch_market_panel：USDCNH 主价 TD 注入优先、缺失回落新浪冷备价。"""

    def _run(self, monkeypatch, sina_panel, td_usdcnh):
        async def fake_sina(proxy_urls=None):
            return sina_panel
        monkeypatch.setattr(pf, "fetch_sina_panel", fake_sina)
        panel = asyncio.run(pf.fetch_market_panel(
            proxy_urls=[], yahoo_day={}, spark_map={},
            fetch_missing=False, sina_panel=sina_panel, td_usdcnh=td_usdcnh))
        return {p["key"]: p for p in panel}

    def test_td_primary(self, monkeypatch):
        by_key = self._run(monkeypatch,
                           {"dxy": {"price": 104.3}, "cnh": {"price": 7.1079},
                            "hf": {}}, td_usdcnh=6.9876)
        assert by_key["USDCNH"]["price"] == 6.9876

    def test_sina_fallback(self, monkeypatch):
        by_key = self._run(monkeypatch,
                           {"dxy": {"price": 104.3}, "cnh": {"price": 7.1079},
                            "hf": {}}, td_usdcnh=None)
        assert by_key["USDCNH"]["price"] == 7.1079
        assert by_key["DXY"]["price"] == 104.3

    def test_dxy_yahoo_primary(self, monkeypatch):
        """DXY 主价迁 Yahoo：yahoo_day 注入优先于新浪批冷备价。"""
        async def fake_sina(proxy_urls=None):
            return {"dxy": {"price": 104.3}, "cnh": {"price": 7.1079}, "hf": {}}
        monkeypatch.setattr(pf, "fetch_sina_panel", fake_sina)
        panel = asyncio.run(pf.fetch_market_panel(
            proxy_urls=[], yahoo_day={"DXY": {"price": 105.1, "change_pct": 0.5}},
            spark_map={}, fetch_missing=False,
            sina_panel={"dxy": {"price": 104.3}}, td_usdcnh=None))
        by_key = {p["key"]: p for p in panel}
        assert by_key["DXY"]["price"] == 105.1   # Yahoo 主价优先于新浪批


# ---------------------------------------------------------------------------
# fetcher：期指主备反转与隔夜指数
# ---------------------------------------------------------------------------

class TestFuturesPrimary:
    def test_eastmoney_always_primary(self):
        with _FtState():
            ft._futures_source_health = {"sina": True, "eastmoney": True}
            ft._futures_source_recover = {}
            for _ in range(5):
                assert ft._get_next_futures_source() == "eastmoney"

    def test_sina_only_when_em_down(self):
        with _FtState():
            ft._futures_source_health = {"sina": True, "eastmoney": False}
            ft._futures_source_recover = {}
            assert ft._get_next_futures_source() == "sina"


class _FtState:
    def __enter__(self):
        self.health = dict(ft._futures_source_health)
        self.recover = dict(ft._futures_source_recover)
        return self

    def __exit__(self, *a):
        ft._futures_source_health.clear()
        ft._futures_source_health.update(self.health)
        ft._futures_source_recover.clear()
        ft._futures_source_recover.update(self.recover)


SINA_INX_RAW = 'var hq_str_gb_$inx="标普500,5670.85,0.24,Oct 02 04:00AM EDT";\n'


class TestFetchOvernight:
    def test_longport_primary_full(self, monkeypatch):
        """新平台三项指数（含 .SPX.US）全长桥命中，新浪零请求。"""
        async def fake_map(symbols):
            assert symbols == [".DJI.US", ".NDX.US", ".SPX.US"]
            return {".DJI.US": {"price": 51176.96, "prev": 51021.0, "change_pct": 0.3},
                    ".NDX.US": {"price": 30807.93, "prev": 30700.0, "change_pct": 0.35},
                    ".SPX.US": {"price": 7722.72, "prev": 7700.0, "change_pct": 0.29}}
        monkeypatch.setattr(lp, "fetch_quote_map", fake_map)

        async def fail_http_get(url, source="", headers=None):
            raise AssertionError("长桥全中时不应请求新浪")
        monkeypatch.setattr(ft, "_http_get", fail_http_get)
        result = asyncio.run(ft.fetch_overnight())
        assert result["道琼斯"]["close"] == 51176.96
        assert result["纳斯达克"]["close"] == 30807.93
        assert result["标普500"]["close"] == 7722.72

    def test_longport_sp_missing_sina_inx(self, monkeypatch):
        async def fake_map(symbols):
            assert symbols == [".DJI.US", ".NDX.US", ".SPX.US"]
            return {".DJI.US": {"price": 51176.96, "prev": 51021.0, "change_pct": 0.3},
                    ".NDX.US": {"price": 30807.93, "prev": 30700.0, "change_pct": 0.35}}
        monkeypatch.setattr(lp, "fetch_quote_map", fake_map)
        captured = {}

        async def fake_http_get(url, source="", headers=None):
            captured["url"] = url
            assert source == "sina"
            return SINA_INX_RAW
        monkeypatch.setattr(ft, "_http_get", fake_http_get)
        result = asyncio.run(ft.fetch_overnight())
        assert result["道琼斯"]["close"] == 51176.96
        assert result["纳斯达克"]["close"] == 30807.93
        assert result["标普500"]["close"] == 5670.85
        assert "gb_$inx" in captured["url"]   # 新浪只补标普500 一项

    def test_longport_fail_sina_full(self, monkeypatch):
        async def fake_map(symbols):
            return {}
        monkeypatch.setattr(lp, "fetch_quote_map", fake_map)

        async def fake_http_get(url, source="", headers=None):
            return ('var hq_str_gb_$dji="道琼斯,51176.96,0.24,T";\n'
                    'var hq_str_gb_$ixic="纳斯达克,30807.93,0.35,T";\n'
                    + SINA_INX_RAW)
        monkeypatch.setattr(ft, "_http_get", fake_http_get)
        result = asyncio.run(ft.fetch_overnight())
        assert set(result) == {"道琼斯", "纳斯达克", "标普500"}


# ---------------------------------------------------------------------------
# 静态信息批量（fetch_static_info）：估值因子层数据底座（eps_ttm/bps/股息率）
# ---------------------------------------------------------------------------

def test_fetch_static_info_success_and_failure(monkeypatch):
    """成功：批量 .US 后缀调用 static_info 并原样返回；失败：异常记账后回 []。"""
    from types import SimpleNamespace

    async def fake_ctx():
        # 实现在调用 _sdk_call 前会求值 ctx.static_info，替身须带该属性
        return SimpleNamespace(static_info=lambda symbols: [])

    async def fake_call(fn, *args):
        assert args == (["NVDA.US", "BABA.US"],)
        return [SimpleNamespace(symbol="NVDA.US", eps_ttm=8.0)]

    with _LpState():   # 失败分支 _mark_failure 改模块级状态，按文件惯例隔离
        monkeypatch.setattr(lp, "_SDK_OK", True)
        monkeypatch.setattr(lp, "token_invalid", False)
        monkeypatch.setattr(lp, "_backoff_until", 0.0)
        monkeypatch.setattr(lp, "_get_ctx", fake_ctx)
        monkeypatch.setattr(lp, "_sdk_call", fake_call)
        infos = asyncio.run(lp.fetch_static_info(["NVDA", "BABA"]))
        assert infos and infos[0].symbol == "NVDA.US"

        async def fake_call_fail(fn, *args):
            raise RuntimeError("boom")

        monkeypatch.setattr(lp, "_sdk_call", fake_call_fail)
        assert asyncio.run(lp.fetch_static_info(["NVDA"])) == []
        # 失败必须记账（漏记账会导致长桥健康状态漂移，批1审查 Minor-4）
        assert lp.fail_streak >= 1 and lp.last_error
