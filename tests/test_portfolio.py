"""持仓模块离线单元测试：时段/解析/盈亏计算（不发真实网络请求）"""
import asyncio
import json
import time
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import portfolio_fetcher as pf
import server
from tencent_bus import TencentBus

ET = ZoneInfo("America/New_York")


class TestUsMarketSession:
    def test_pre(self):
        s, d, _ = pf.us_market_session(datetime(2026, 10, 1, 9, 0, tzinfo=ET))
        assert s == "pre" and d == date(2026, 10, 1)

    def test_regular(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 12, 0, tzinfo=ET))[0] == "regular"

    def test_post(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 17, 0, tzinfo=ET))[0] == "post"

    def test_closed_night(self):
        assert pf.us_market_session(datetime(2026, 10, 1, 21, 0, tzinfo=ET))[0] == "closed"

    def test_closed_weekend(self):
        # 2026-10-03 周六
        assert pf.us_market_session(datetime(2026, 10, 3, 12, 0, tzinfo=ET))[0] == "closed"


class TestShouldFetchExt:
    """扩展时段 ext 拉取判定：pre/post 恒拉；工作日 closed 的 16:00 后与凌晨 <04:00 也拉
    （新浪 gb_ 夜间返回当日最后盘后价，保证夜盘时段前端保留盘后价不"倒退"）；
    周五盘后残留延伸至周六凌晨 <04:00，周六 04:00 起不拉。"""

    def test_pre_true(self):
        assert pf.should_fetch_ext("pre", datetime(2026, 10, 1, 8, 0, tzinfo=ET)) is True

    def test_post_true(self):
        assert pf.should_fetch_ext("post", datetime(2026, 10, 1, 17, 0, tzinfo=ET)) is True

    def test_closed_friday_night_true(self):
        # 2026-10-02 周五 21:00 美东：当日盘后残留窗口，应拉取
        assert pf.should_fetch_ext("closed", datetime(2026, 10, 2, 21, 0, tzinfo=ET)) is True

    def test_closed_saturday_false(self):
        # 2026-10-03 周六：非工作日一律不拉
        assert pf.should_fetch_ext("closed", datetime(2026, 10, 3, 21, 0, tzinfo=ET)) is False

    def test_closed_wednesday_day_false(self):
        # 2026-09-30 周三 10:00（盘后已过、凌晨未到的常规 closed 空窗）：不拉
        assert pf.should_fetch_ext("closed", datetime(2026, 9, 30, 10, 0, tzinfo=ET)) is False

    def test_closed_wednesday_early_morning_false(self):
        # 2026-09-30 周三 05:00：凌晨盘后残留窗口已结束（>=04:00），不拉
        assert pf.should_fetch_ext("closed", datetime(2026, 9, 30, 5, 0, tzinfo=ET)) is False

    def test_closed_wednesday_2am_true(self):
        # 2026-09-30 周三 02:00：工作日凌晨盘后残留窗口，应拉取
        assert pf.should_fetch_ext("closed", datetime(2026, 9, 30, 2, 0, tzinfo=ET)) is True

    def test_closed_saturday_2am_true(self):
        # 2026-10-03 周六 02:00：周五盘后残留延伸至周六凌晨 4 点前，应拉取
        assert pf.should_fetch_ext("closed", datetime(2026, 10, 3, 2, 0, tzinfo=ET)) is True

    def test_closed_saturday_5am_false(self):
        # 2026-10-03 周六 05:00：周六凌晨残留窗口已结束（>=04:00），不拉
        assert pf.should_fetch_ext("closed", datetime(2026, 10, 3, 5, 0, tzinfo=ET)) is False


TENCENT_RAW = 'v_usMU="200~美光科技~MU.OQ~1065.11~1065.11~0.00~2125925~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~~2026-10-01 09:30:00~0.00~0.00~1083.50~1062.36~USD~1035004~193308777~0.01~12.55~~44.05~";\nv_usSKHY="200~SK海力士~SKHY.OQ~184.15~184.15~0.00~1035004~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~~2026-10-01 09:30:00~0.00~0.00~189.14~183.00~USD~1035004~193308777~0.01~12.55~~44.05~";\n'


class TestParseTencentUs:
    def test_basic(self):
        quotes = pf.parse_tencent_us(TENCENT_RAW, ["MU", "SKHY"])
        assert set(quotes) == {"MU", "SKHY"}
        mu = quotes["MU"]
        assert mu["name"] == "美光科技"
        assert mu["price"] == 1065.11
        assert mu["pre_close"] == 1065.11
        assert mu["currency"] == "USD"
        assert mu["quote_time"] == "2026-10-01 09:30:00"

    def test_missing_symbol(self):
        quotes = pf.parse_tencent_us(TENCENT_RAW, ["NOPE"])
        assert "NOPE" not in quotes

    def test_truncated_line(self):
        # 构造只有 37 个字段的截断行（< 守卫阈值 38）：不应抛 IndexError 且该 symbol 不入结果
        mu_fields = TENCENT_RAW.split('"')[1].split("~")[:37]
        truncated = 'v_usMU="' + "~".join(mu_fields) + '";\n'
        quotes = pf.parse_tencent_us(truncated, ["MU"])
        assert "MU" not in quotes


SINA_RAW = 'var hq_str_gb_mu="美光,1065.1100,0.00,2026-10-01 21:27:06,0.0000,0.0000,0.0000,0.0000,1254.8500,179.1950,0,25290278,1202386340212,44.80,23.770000,0.00,0.00,0.00,0.00,1128884660,85,1051.9950,-1.23,-13.12,Oct 01 09:27AM EDT,Oct 01 09:10AM EDT,1065.1100,2127019,1,2026,0.0000,1086.4100,1050.2500,2258052759.8623,1072.6400,1065.1100";\nvar hq_str_gb_skhy="SK海力士,184.1500,0.00,2026-10-01 21:27:04,0.0000,0.0000,0.0000,0.0000,199.8650,124.8000,0,20495622,1342205818250,0.00,--,0.00,-0.01,0.00,0.00,7288655000,0,0,0.00,0.00,,,184.1500,1035173,1,2026,0.0000,189.1400,183.0000,193308509.9160,183.0000,184.1500";\n'


class TestParseSinaUs:
    def test_with_ext(self):
        ext = pf.parse_sina_us(SINA_RAW, ["MU"])
        assert ext["MU"]["ext_price"] == 1051.9950
        assert ext["MU"]["pre_close"] == 1065.1100
        assert ext["MU"]["ext_time"] == "Oct 01 09:27AM EDT"

    def test_without_ext(self):
        ext = pf.parse_sina_us(SINA_RAW, ["SKHY"])
        assert ext["SKHY"]["ext_price"] == 0.0


class TestParseFx:
    def test_ok(self):
        raw = 'var hq_str_fx_susdhkd="21:55:24,7.846600,7.847600,7.846400,19,7.846700,7.847300,7.845400,7.846600,美元兑港元即期汇率,0.010000,0.000200,0.0002421,,7.847600,7.781500,,2026-10-01";'
        assert pf.parse_fx_usdhkd(raw) == 7.8466

    def test_out_of_range(self):
        raw = 'var hq_str_fx_susdhkd="21:55:24,70.8466,7.847,7.846,19,7.8467,7.847,7.845,7.8466,x,0.01,0.0002,0.0002,,7.847,7.781,,2026-10-01";'
        assert pf.parse_fx_usdhkd(raw) == 0.0

    def test_garbage(self):
        assert pf.parse_fx_usdhkd("") == 0.0


FT_DATA = {"data": {"Overview": {"fundname": "Templeton Select APAC Equity Income Fund",
    "shareclass": [{"shclname": "A (Mdis) HKD", "nav": {
        "navvalue": "$15.50", "navdate": "30/09/2026",
        "navchngvalue": "$-0.12", "navchngpct": "-0.77%", "ytdtotretatnav": "30.91%"}}]}}}


class TestParseFtNav:
    def test_ok(self):
        r = pf.parse_ft_nav(FT_DATA)
        assert r["nav"] == 15.50
        assert r["nav_date"] == date(2026, 9, 30)
        assert r["change_pct"] == -0.77
        assert r["ytd_pct"] == 30.91

    def test_bad(self):
        assert pf.parse_ft_nav({"data": {"Overview": {}}}) is None


HOLDINGS = [
    {"symbol": "MU", "name": "美光科技", "market": "US", "quantity": 100, "cost_price": 900.0, "currency": "USD"},
    {"symbol": "HK0000740297", "name": "邓普顿基金", "market": "HKFUND", "quantity": 5000, "cost_price": 13.0, "currency": "HKD"},
]
QUOTES = {"MU": {"name": "美光科技", "price": 1065.11, "pre_close": 1065.11,
                 "ext_price": 1051.995, "ext_time": "Oct 01 09:27AM EDT",
                 "change_pct": 0.0, "currency": "USD", "quote_time": "t"}}
FUND = {"nav": 15.50, "prev_nav": 15.62, "nav_date": date(2026, 9, 30), "nav_change_pct": -0.77}


class TestBuildPositions:
    def test_summary(self):
        positions, summary = pf.build_positions(HOLDINGS, QUOTES, FUND, fx_usdhkd=7.8)
        mu = next(p for p in positions if p["symbol"] == "MU")
        assert mu["price"] == 1051.995          # 扩展时段价优先
        assert mu["today_pnl"] == (1051.995 - 1065.11) * 100
        assert mu["value"] == 1051.995 * 100
        assert mu["position_pnl"] == (1051.995 - 900.0) * 100
        fund = next(p for p in positions if p["symbol"] == "HK0000740297")
        assert fund["value"] == 15.50 * 5000 / 7.8
        assert fund["today_pnl"] == (15.50 - 15.62) * 5000 / 7.8
        assert fund["position_pnl"] == (15.50 - 13.0) * 5000 / 7.8
        assert summary["total_value"] == mu["value"] + fund["value"]
        assert summary["base_value"] == 1065.11 * 100 + 15.62 * 5000 / 7.8

    def test_missing_quote(self):
        positions, summary = pf.build_positions(HOLDINGS, {}, FUND, 7.8)
        mu = next(p for p in positions if p["symbol"] == "MU")
        assert mu["price"] == 0 and mu["value"] == 0 and mu["stale"] is True
        # stale 行无收盘定格价 → 夜盘增量% 不可算，后端下发 None（前端省略）
        assert mu["overnight_change_pct"] is None

    def test_stale_cost_basis(self):
        # 行情与净值均缺失：stale 行的成本仍需按 USD 口径计入 cost_basis，total_value 归零
        positions, summary = pf.build_positions(HOLDINGS, {}, None, 7.8)
        mu = next(p for p in positions if p["symbol"] == "MU")
        fund = next(p for p in positions if p["symbol"] == "HK0000740297")
        assert mu["stale"] is True and fund["stale"] is True
        assert mu["cost_usd"] == 90000.0
        assert fund["cost_usd"] == 13.0 * 5000 / 7.8      # HKD 成本折算 USD，而非 13.0*5000
        assert summary["total_value"] == 0.0
        assert summary["cost_basis"] == 90000.0 + 13.0 * 5000 / 7.8
        assert summary["total_pnl"] == summary["cost_basis"] * -1


# 腾讯港股实测样例（v_hk03152，78 字段，币种在 f[75]）
TENCENT_HK_RAW = ('v_hk03152="100~A博时港元~03152~1134.900~1134.650~1135.000~9382.0~0~0~1134.900~'
                  '0~0~0~0~0~0~0~0~0~1134.900~0~0~0~0~0~0~0~0~0~9382.0~2026/09/30 16:08:33~0.250~0.02~'
                  '1135.150~1134.650~1134.900~9382.0~10647825.200~0~0.00~~0~0~0.04~1.9520~1.9520~'
                  'A BOS HKD MM~0.00~1138.100~1104.500~1.90~51.88~0~0~0~0~0~0.00~0.00~5.45~1~1.95~0.09~'
                  'GP-FUND~~~0.18~0.24~0.67~172000.00~172000.00~0.00~0.000~1134.921~1.96~HKD~1~50";\n')


class TestParseTencentHk:
    def test_tencent_hk(self):
        quotes = pf.parse_tencent_quotes(TENCENT_HK_RAW, ["03152"], {"03152": "hk"})
        q = quotes["03152"]
        assert q["price"] == 1134.9
        assert q["pre_close"] == 1134.65
        assert q["currency"] == "HKD"
        assert q["quote_time"] == "2026/09/30 16:08:33"

    def test_tencent_mixed(self):
        # 港股 + 美股混合一次解析，两 symbol 均返回
        raw = TENCENT_HK_RAW + TENCENT_RAW
        quotes = pf.parse_tencent_quotes(raw, ["03152", "MU"], {"03152": "hk", "MU": "us"})
        assert set(quotes) == {"03152", "MU"}
        assert quotes["03152"]["currency"] == "HKD"
        assert quotes["03152"]["price"] == 1134.9
        assert quotes["MU"]["currency"] == "USD"
        assert quotes["MU"]["price"] == 1065.11

    def test_hk_truncated(self):
        # 港股行裁到 75 字段（< 守卫阈值 76，无法安全访问 f[75]）：不抛 IndexError 且不入结果
        fields = TENCENT_HK_RAW.split('"')[1].split("~")[:75]
        truncated = 'v_hk03152="' + "~".join(fields) + '";\n'
        quotes = pf.parse_tencent_quotes(truncated, ["03152"], {"03152": "hk"})
        assert "03152" not in quotes


HK_HOLDINGS = [
    {"symbol": "03152", "name": "博时港元货币ETF", "market": "HK",
     "quantity": 1, "cost_price": 1126.6, "currency": "HKD"},
]
HK_QUOTES = {"03152": {"name": "A博时港元", "price": 1134.9, "pre_close": 1134.65,
                       "change_pct": 0.02, "high": 1135.15, "low": 1134.65,
                       "currency": "HKD", "quote_time": "2026/09/30 16:08:33",
                       "ext_price": 0.0, "ext_time": ""}}


class TestBuildPositionsHk:
    def test_hk_quote(self):
        positions, summary = pf.build_positions(HK_HOLDINGS, HK_QUOTES, None, fx_usdhkd=7.8)
        p = positions[0]
        assert p["value"] == 1134.9 / 7.8
        assert p["today_pnl"] == (1134.9 - 1134.65) / 7.8
        assert p["position_pnl"] == (1134.9 - 1126.6) / 7.8
        assert p["stale"] is False
        assert p["ext"] == "regular"

    def test_hk_stale(self):
        positions, summary = pf.build_positions(HK_HOLDINGS, {}, None, fx_usdhkd=7.8)
        p = positions[0]
        assert p["stale"] is True
        assert p["cost_usd"] == 1126.6 / 7.8


class TestMarketPanelParsers:
    def test_parse_sina_oil(self):
        """新浪布伦特 hf_OIL：f[0]现价 f[7]昨结 f[13]名称，涨跌幅自算。

        修正：旧实现误用 f[2]（买价）作昨结，导致涨跌幅口径错误。
        """
        raw = 'var hq_str_hf_OIL="101.18,3.21,101.09,101.12,101.88,96.55,00:45:07,98.03,97.66,0,6,7,2026-10-02,布伦特原油";\n'
        q = pf.parse_sina_oil(raw)
        assert q["price"] == 101.18 and q["pre_close"] == 98.03
        assert abs(q["change_pct"] - (101.18 - 98.03) / 98.03 * 100) < 1e-9
        assert q["name"] == "布伦特原油"

    def test_parse_sina_oil_bad(self):
        """字段不足/无匹配时返回空 dict，不抛异常。"""
        assert pf.parse_sina_oil('var hq_str_hf_OIL="1,2,3";') == {}
        assert pf.parse_sina_oil("") == {}

    def test_parse_tencent_hf_line(self):
        """腾讯混排返回中的 hf_ 行（逗号布局，无尾部成交量字段，区别于 us/hk 波浪线）。"""
        raw = 'v_hf_OIL="101.18,3.21,101.09,101.12,101.88,96.55,00:45:07,98.03,97.66,0,6,7,2026-10-02,布伦特原油";\n'
        q = pf.parse_tencent_quotes(raw, ["OIL"], {"OIL": "hf"})
        assert q["OIL"]["price"] == 101.18 and q["OIL"]["pre_close"] == 98.03
        assert q["OIL"]["name"] == "布伦特原油"
        # parse_tencent_quotes 统一对 change_pct 做 round(4)（与 us/hk 分支一致）
        assert q["OIL"]["change_pct"] == round((101.18 - 98.03) / 98.03 * 100, 4)

    def test_parse_tencent_hf_truncated(self):
        """hf 行裁到 13 字段（< 守卫阈值 14，无法安全访问 f[13] 名称）：不抛异常且不入结果。"""
        fields = "101.18,3.21,101.09,101.12,101.88,96.55,00:45:07,98.03,97.66,0,6,7,2026-10-02".split(",")
        truncated = 'v_hf_OIL="' + ",".join(fields) + '";\n'
        quotes = pf.parse_tencent_quotes(truncated, ["OIL"], {"OIL": "hf"})
        assert "OIL" not in quotes

    def test_parse_sina_diniw(self):
        """新浪美元指数 DINIW：f[1]现价 f[3]昨收 f[9]名称。"""
        raw = 'var hq_str_DINIW="00:45:25,102.1406,102.1406,101.4743,7389,101.4825,102.1766,101.4377,102.1406,美元指数,2026-10-02";\n'
        q = pf.parse_sina_diniw(raw)
        assert q["price"] == 102.1406 and q["pre_close"] == 101.4743
        assert q["name"] == "美元指数"
        assert abs(q["change_pct"] - (102.1406 - 101.4743) / 101.4743 * 100) < 1e-9

    def test_parse_sina_diniw_bad(self):
        """字段不足/无匹配时返回空 dict，不抛异常。"""
        assert pf.parse_sina_diniw('var hq_str_DINIW="1,2,3";') == {}
        assert pf.parse_sina_diniw("") == {}

    def test_parse_yahoo_chart(self):
        """Yahoo v8 chart：meta 现价/昨收 + adjclose 过滤空值后作近5日 spark。"""
        raw = json.dumps({"chart": {"result": [{"meta": {"regularMarketPrice": 5.599,
            "chartPreviousClose": 5.639},
            "indicators": {"adjclose": [{"adjclose": [5.60, 5.61, None, 5.599]}]}}]}, "error": None})
        q = pf.parse_yahoo_chart(raw)
        assert q["price"] == 5.599 and q["pre_close"] == 5.639
        assert q["spark"] == [5.60, 5.61, 5.599]
        assert abs(q["change_pct"] - (5.599 - 5.639) / 5.639 * 100) < 1e-9

    def test_parse_yahoo_chart_bad(self):
        """非法 JSON/结构缺失 → 空 dict，不抛异常。"""
        assert pf.parse_yahoo_chart("not-json") == {}
        assert pf.parse_yahoo_chart(json.dumps({"chart": {"error": "x"}})) == {}


class _FakeResp:
    """假 httpx 响应：200 + GBK 编码内容。"""
    status_code = 200
    content = "美元指数".encode("gbk")


class _FakeProxyClient:
    """假 httpx.AsyncClient：记录所用代理，get 恒返回 200 GBK 响应。"""
    tried: list = []

    def __init__(self, *args, proxy=None, **kwargs):
        _FakeProxyClient.tried.append(proxy)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None):
        return _FakeResp()


FX_OK_RAW = ('var hq_str_fx_susdhkd="21:55:24,7.846600,7.847600,7.846400,19,7.846700,'
             '7.847300,7.845400,7.846600,美元兑港元即期汇率,0.010000,0.000200,0.0002421,,'
             '7.847600,7.781500,,2026-10-01";')


class TestGetTextResilient:
    """新浪容错版 GET：直连优先，403/失败时代理候选依次重试。"""

    def test_direct_ok(self, monkeypatch):
        """直连成功：直接返回，不创建任何代理 client。"""
        async def fake_get_text(url, headers=None, timeout=10.0):
            return "ok"

        _FakeProxyClient.tried = []
        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        monkeypatch.setattr(pf.httpx, "AsyncClient", _FakeProxyClient)
        got = asyncio.run(pf._get_text_resilient("http://x", proxy_urls=["p1", "p2"]))
        assert got == "ok"
        assert _FakeProxyClient.tried == []          # 未发起任何代理请求

    def test_proxy_fallback(self, monkeypatch):
        """直连失败：按候选顺序试代理，首个 200 即返回 GBK 解码内容且不再续试。"""
        async def fake_get_text(url, headers=None, timeout=10.0):
            return None

        _FakeProxyClient.tried = []
        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        monkeypatch.setattr(pf.httpx, "AsyncClient", _FakeProxyClient)
        got = asyncio.run(pf._get_text_resilient("http://x", proxy_urls=["p1", "p2"]))
        assert got == "美元指数"                      # GBK 解码正确
        assert _FakeProxyClient.tried == ["p1"]      # 首个代理成功即止

    def test_all_fail_returns_none(self, monkeypatch):
        """直连与全部代理候选均失败：返回 None，不抛异常。"""
        async def fake_get_text(url, headers=None, timeout=10.0):
            return None

        class _FailClient(_FakeProxyClient):
            def __init__(self, *args, proxy=None, **kwargs):
                _FakeProxyClient.tried.append(proxy)

            async def get(self, url, headers=None):
                raise OSError("proxy down")

        _FakeProxyClient.tried = []
        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        monkeypatch.setattr(pf.httpx, "AsyncClient", _FailClient)
        got = asyncio.run(pf._get_text_resilient("http://x", proxy_urls=["p1", "p2"]))
        assert got is None
        assert _FakeProxyClient.tried == ["p1", "p2"]  # 两个候选都试过

    def test_no_proxies(self, monkeypatch):
        """无代理清单：直连失败即返回 None（等价旧直连行为）。"""
        async def fake_get_text(url, headers=None, timeout=10.0):
            return None

        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        assert asyncio.run(pf._get_text_resilient("http://x")) is None


class TestFetchFxUsesProxies:
    def test_proxy_passthrough(self, monkeypatch):
        """fetch_fx_usdhkd 新浪主源直连 403 时，代理清单透传 _get_text_resilient。"""
        captured = {}

        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            captured["url"] = url
            captured["proxy_urls"] = proxy_urls
            return None   # 新浪失败 → 回切 TD

        async def td_ok(pair):
            assert pair == "USD/HKD"
            return 7.8470

        monkeypatch.setattr(pf, "fetch_td_fx", td_ok)
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd(proxy_urls=["p9"]))
        assert rate == 7.8470 and fallback is True
        assert captured["proxy_urls"] == ["p9"]
        assert "susdhkd" in captured["url"]

    def test_sina_primary_no_td(self, monkeypatch):
        """fetch_fx_usdhkd 新浪主源成功：零 TD 请求（免费额度让位锚点恢复/USDCNH）。"""
        called = {"td": 0}

        async def td_spy(pair):
            called["td"] += 1
            return 7.8470

        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            return FX_OK_RAW

        monkeypatch.setattr(pf, "fetch_td_fx", td_spy)
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd(proxy_urls=["p9"]))
        assert rate == 7.8466 and fallback is False
        assert called["td"] == 0

    def test_all_fail_fallback(self, monkeypatch):
        """新浪与 TD 均失败：置 7.80 静态兜底。"""
        async def no_td(pair):
            return 0.0

        monkeypatch.setattr(pf, "fetch_td_fx", no_td)
        monkeypatch.setattr(pf, "_get_text_resilient",
                            lambda *a, **k: _async_none())
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd())
        assert rate == pf.FX_FALLBACK and fallback is True


async def _async_none():
    return None


class TestDetectIsolatedSpike:
    """tick 孤立毛刺检测：当前点大幅偏离前后邻居均值、且邻居彼此接近 → 坏值毛刺。

    阈值语义：偏离量 dev>60 且邻居互差 < dev*0.5（排除真实跳变后的平稳序列），
    两者同时满足才判毛刺；任一边界为 None（序列首尾/缺值）不判。
    """

    def test_real_spike_1667(self):
        # 线上实测 id=1667：p=87.38，前后邻居 281/280 → 孤立尖峰
        assert pf.detect_isolated_spike(281.0, 87.38, 280.0) is True

    def test_real_spike_1725(self):
        # 线上实测 id=1725：p=-299.60，前后邻居 272/270 → 孤立深坑
        assert pf.detect_isolated_spike(272.0, -299.60, 270.0) is True

    def test_stable_series_false(self):
        # 平稳序列：相邻点小幅波动，不判毛刺
        assert pf.detect_isolated_spike(280.0, 281.0, 280.5) is False

    def test_real_jump_false(self):
        # 真实跳变（如开盘跳空）：邻居互差大（≥ dev*0.5），不判毛刺
        assert pf.detect_isolated_spike(999.0, 1000.0, 100.0) is False

    def test_small_dev_false(self):
        # 偏离量未过 60 绝对阈值：不判毛刺
        assert pf.detect_isolated_spike(280.0, 270.0, 280.0) is False

    def test_none_boundary_false(self):
        # 任一边界为 None（序列首尾/缺值）：不判毛刺
        assert pf.detect_isolated_spike(None, 87.38, 280.0) is False
        assert pf.detect_isolated_spike(281.0, 87.38, None) is False
        assert pf.detect_isolated_spike(None, 87.38, None) is False
        assert pf.detect_isolated_spike(281.0, None, 280.0) is False

    def test_consecutive_bad_values_undetectable(self):
        # 连续 ≥2 点同向坏值不可检（无第二信息源）：坏点相邻时邻居互差
        # 不再远小于偏离量，判定条件天然不成立 → False（写入口延迟确认兜底）
        assert pf.detect_isolated_spike(281.0, 87.38, 87.5) is False


class TestDecideTickAction:
    """tick 写入口延迟确认决策：超阈值暂存(hold)→下一轮恢复丢弃(discard)/
    仍异常确认为真实跳变(confirm，如开盘跳空)。
    p 阈值 = max(60, 0.02×|last_v|)（总资产 2% 量纲；v 无独立跳变检测，
    现金/行情坏值不会使 v 脱离 p 单独突变）。"""

    def test_cold_start_write(self):
        # 冷启动首条无 last 基准：直接写入
        assert pf.decide_tick_action(None, None, 280.0, False) == "write"

    def test_normal_write(self):
        # 偏差在阈值内且无暂存：正常写入
        assert pf.decide_tick_action(280.0, 100000.0, 281.0, False) == "write"

    def test_replay_needle_571_hold(self):
        # 线上针2(id=1725) 写入口回放：last_p=272 → p=-299.6（dev≈571.6），
        # last_v=13000 → 阈值 max(60, 0.02×13000)=260 < 571.6 → hold。
        # 旧 5% 量纲阈值 650 > 571.6 会 write 漏过入库（本轮修复点）
        assert pf.decide_tick_action(272.0, 13000.0, -299.6, False) == "hold"

    def test_replay_needle_194_hold(self):
        # 线上针1(id=1667) 写入口回放：last_p=281 → p=87.38（dev≈193.6）。
        # last_v 处 2% 阈值生效区间(<194/0.02=9700，盘中基准如 9000)：
        # 阈值 180 < 193.6 → hold。last_v≈13000 时阈值 260 > 193.6 该针会漏过，
        # 由读出口 _strip_spike_points 孤立点剔除兜底（双端过滤分工，
        # 连续坏值边界见 detect_isolated_spike docstring）
        assert pf.decide_tick_action(281.0, 9000.0, 87.38, False) == "hold"

    def test_recover_discard(self):
        # 下一轮恢复到 last 附近（偏差 < 阈值一半 100）：上一轮为毛刺，丢弃暂存
        assert pf.decide_tick_action(272.0, 10000.0, 272.5, True) == "discard"

    def test_still_abnormal_confirm(self):
        # 连续两轮仍异常（偏差 ≥ 阈值一半）：真实跳变确认，补写暂存并接受本轮
        assert pf.decide_tick_action(272.0, 10000.0, -290.0, True) == "confirm"

    def test_absolute_floor_60(self):
        # last_v 很小时阈值取绝对下限 60：偏差 100 > 60 → 暂存
        assert pf.decide_tick_action(200.0, 500.0, 100.0, False) == "hold"


class TestUpdateSessionState:
    """一场口径状态机纯函数：base_closes/close_prices/base_logged 就地更新，
    冷启动退化经 base_fallback 传给调用方打日志（函数自身无 IO）。

    f[3]=腾讯现价（收盘后至次日盘前实测保持 16:00 收盘价）、f[4]=腾讯昨收
    （凌晨/盘前实证仍为上一场基准未切换）；边界覆盖：regular 开盘刷新基准、
    post/closed≥16:00 定格、凌晨<04:00 用 f[3] 定格（旧版误用 f[4] 致
    close==base、收盘定格段恒 0）、盘前仅缺失时 f[3] 兜底、冷启动空 state
    退化填 f[4]。"""

    Q = {"MU": {"price": 100.0, "pre_close": 95.0}}

    def test_regular_0930_refreshes_base(self):
        # 09:30 regular：f[4] 即一场基准，每轮刷新覆盖隔夜退化的旧基准；
        # 收盘定格跟随实时价（定格于 16:00 最后一条采样）
        state = {"base_closes": {"MU": 88.0}, "close_prices": {}, "base_logged": True}
        pf.update_session_state(state, "regular",
                                datetime(2026, 10, 1, 9, 30, tzinfo=ET), self.Q)
        assert state["base_closes"]["MU"] == 95.0
        assert state["close_prices"]["MU"] == 100.0
        assert state["base_fallback"] == []

    def test_post_1600_freezes_close(self):
        # 16:00 post：f[3] 实测保持收盘价 → 收盘定格；场基准沿用上一值不刷新
        state = {"base_closes": {"MU": 95.0}, "close_prices": {"MU": 99.0},
                 "base_logged": False}
        pf.update_session_state(state, "post",
                                datetime(2026, 10, 1, 16, 0, tzinfo=ET), self.Q)
        assert state["close_prices"]["MU"] == 100.0
        assert state["base_closes"]["MU"] == 95.0

    def test_dawn_closed_keeps_frozen_close(self):
        # 凌晨 closed(<04:00)：f[4] 实证仍为上一场基准（未切换，065f 实证
        # 2026-10-02 03:27/04:07 AMD f[4]=611.76=f[3]615.73 的基准），定格必须
        # 用 f[3]（旧版取 f[4] 会使 close==base → 收盘定格段恒 0，线上
        # 「收盘定格 -$22.14」根因）；每轮覆写，自愈历史污染值
        state = {"base_closes": {"MU": 95.0}, "close_prices": {"MU": 99.0},
                 "base_logged": False}
        pf.update_session_state(state, "closed",
                                datetime(2026, 10, 2, 2, 0, tzinfo=ET), self.Q)
        assert state["close_prices"]["MU"] == 100.0   # f[3] 定格，非 f[4]
        assert state["base_closes"]["MU"] == 95.0     # 基准沿用不覆盖

    def test_pre_fills_missing_close_only(self):
        # 盘前 pre：仅缺失时用 f[3] 兜底（实测保持收盘定格），不覆盖已定格值
        # （防盘前中后段 f[3] 可能切换为盘前成交价污染 16:00 定格/锚点恢复值）
        state = {"base_closes": {"MU": 95.0}, "close_prices": {"MU": 615.73},
                 "base_logged": False}
        pf.update_session_state(state, "pre",
                                datetime(2026, 10, 2, 8, 0, tzinfo=ET), self.Q)
        assert state["close_prices"]["MU"] == 615.73   # 已定格不被覆盖
        assert state["base_closes"]["MU"] == 95.0
        state2 = {"base_closes": {"MU": 95.0}, "close_prices": {},
                  "base_logged": True}
        pf.update_session_state(state2, "pre",
                                datetime(2026, 10, 2, 8, 0, tzinfo=ET), self.Q)
        assert state2["close_prices"]["MU"] == 100.0   # 缺失时 f[3] 兜底

    def test_closed_windows_branch_by_hour(self):
        # closed 按 et_hour 分支：晚间 ≥16:00 同 post（f[3] 定格）；
        # 凌晨 <04:00 亦用 f[3] 定格（f[4] 未切换、为场基准）
        state_night = {"base_closes": {}, "close_prices": {}, "base_logged": False}
        pf.update_session_state(state_night, "closed",
                                datetime(2026, 10, 1, 21, 0, tzinfo=ET), self.Q)
        assert state_night["close_prices"]["MU"] == 100.0
        state_dawn = {"base_closes": {"MU": 95.0}, "close_prices": {},
                      "base_logged": False}
        pf.update_session_state(state_dawn, "closed",
                                datetime(2026, 10, 2, 2, 0, tzinfo=ET), self.Q)
        assert state_dawn["close_prices"]["MU"] == 100.0   # f[3]，非 f[4]（95.0）

    def test_cold_start_empty_state_fallback(self):
        # 冷启动空 state：非 regular 时段退化用当轮 f[4] 充当场基准，
        # base_logged 置位 + base_fallback 传递（隔夜偏差 09:30 开盘自愈）
        state = {}
        pf.update_session_state(state, "post",
                                datetime(2026, 10, 1, 16, 30, tzinfo=ET), self.Q)
        assert state["base_closes"] == {"MU": 95.0}
        assert state["base_logged"] is True
        assert state["base_fallback"] == ["MU"]
        # 第二轮：基准已维护不再退化，标注不重复（base_fallback 清空）
        pf.update_session_state(state, "post",
                                datetime(2026, 10, 1, 16, 40, tzinfo=ET), self.Q)
        assert state["base_fallback"] == []
        assert state["base_closes"] == {"MU": 95.0}

    def test_partial_missing_fills_only_missing(self):
        # 多 symbol 场景：仅缺失基准的 symbol 退化，已维护的不被覆盖
        quotes = {"MU": {"price": 100.0, "pre_close": 95.0},
                  "QQQ": {"price": 500.0, "pre_close": 495.0}}
        state = {"base_closes": {"MU": 94.0}, "close_prices": {},
                 "base_logged": True}
        pf.update_session_state(state, "post",
                                datetime(2026, 10, 1, 17, 0, tzinfo=ET), quotes)
        assert state["base_closes"] == {"MU": 94.0, "QQQ": 495.0}
        assert state["base_fallback"] == []   # base_logged 已置位，不再标注


class TestNightOverrideAfterStateMachine:
    """24h数据源夜盘代理价覆盖时序：状态机必须先于覆盖执行（吃原始腾讯价），
    否则周末 closed+hour>=16 分支会把代理价写进 close_prices 定格，
    隔夜增量(代理价-被污染定格)语义错乱（edc2e67 评审修复的回归防线）。"""

    Q_RAW = {"AMD": {"price": 615.78, "pre_close": 611.65}}   # 腾讯原始（周六=f[3]保持周五收盘）

    def test_weekend_evening_close_not_polluted_by_h24(self):
        # 周六 18:00 closed（hour>=16 分支）：先状态机再覆盖 → 定格=615.78（周五收盘），
        # 覆盖后的 q["price"]=24h数据源代理价 620.9 只进展示/tick，不回写定格
        state = {"base_closes": {"AMD": 611.65}, "close_prices": {}, "base_logged": True}
        quotes = {"AMD": dict(self.Q_RAW["AMD"])}
        pf.update_session_state(state, "closed",
                                datetime(2026, 10, 3, 18, 0, tzinfo=ET), quotes)
        # 夜盘覆盖（server 顺序：状态机之后）
        quotes["AMD"]["price"] = 620.9
        assert state["close_prices"]["AMD"] == 615.78   # 定格未被代理价污染
        assert quotes["AMD"]["price"] == 620.9          # 展示价=代理价


class TestStripSpikePoints:
    """intraday 读出口毛刺清洗：孤立单点剔除（首尾不判），双端过滤的读侧兜底。"""

    def test_strip_two_real_spikes(self):
        # 线上两根针（87.38 / -299.60）嵌入平稳序列 → 两点剔除，其余保留
        points = [{"ts": "1", "v": 1.0, "p": 281.0},
                  {"ts": "2", "v": 1.0, "p": 87.38},
                  {"ts": "3", "v": 1.0, "p": 280.0},
                  {"ts": "4", "v": 1.0, "p": 272.0},
                  {"ts": "5", "v": 1.0, "p": -299.60},
                  {"ts": "6", "v": 1.0, "p": 270.0}]
        out = server._strip_spike_points(points)
        assert [p["p"] for p in out] == [281.0, 280.0, 272.0, 270.0]

    def test_keep_first_last(self):
        # 序列首尾即使数值异常也不剔除（无法三点判定），真实跳变同样保留
        points = [{"ts": "1", "v": 1.0, "p": 87.38},
                  {"ts": "2", "v": 1.0, "p": 280.0},
                  {"ts": "3", "v": 1.0, "p": -299.60}]
        assert server._strip_spike_points(points) == points

    def test_short_series_untouched(self):
        # 少于 3 点：原样返回
        points = [{"ts": "1", "v": 1.0, "p": 1.0}]
        assert server._strip_spike_points(points) == points


class TestTradingSessionKey:
    """一场口径交易日（session_key=开盘日）：工作日≥09:30→当日；
    工作日<09:30→上一工作日；周末→上一工作日（跨周末继续回退）。"""

    def test_weekday_open(self):
        # 周四 10:00（已开盘）→ 当日周四
        assert pf.trading_session_key(datetime(2026, 10, 1, 10, 0, tzinfo=ET)) == date(2026, 10, 1)

    def test_weekday_before_open(self):
        # 周四 08:00（盘前凌晨，属上一场）→ 周三
        assert pf.trading_session_key(datetime(2026, 10, 1, 8, 0, tzinfo=ET)) == date(2026, 9, 30)

    def test_saturday(self):
        # 周六 12:00 → 周五
        assert pf.trading_session_key(datetime(2026, 10, 3, 12, 0, tzinfo=ET)) == date(2026, 10, 2)

    def test_monday_early_morning(self):
        # 周一 03:00 → 上周五（周日为周末，回退至最近工作日）
        assert pf.trading_session_key(datetime(2026, 10, 5, 3, 0, tzinfo=ET)) == date(2026, 10, 2)

    def test_weekday_open_exact_930(self):
        # 工作日 09:30 整（开盘时刻含边界）→ 当日
        assert pf.trading_session_key(datetime(2026, 10, 1, 9, 30, tzinfo=ET)) == date(2026, 10, 1)

    def test_sunday_night(self):
        # 周日 21:00 → 周五
        assert pf.trading_session_key(datetime(2026, 10, 4, 21, 0, tzinfo=ET)) == date(2026, 10, 2)


class TestBuildPositionsSessionPnl:
    """一场口径三数值：实时累计(day_pnl/today_pnl)、收盘定格(close_pnl)、
    夜盘增量(overnight_pnl=day_pnl-close_pnl)，恒等式必须成立。"""

    H = [{"symbol": "MU", "name": "美光", "market": "US",
          "quantity": 10, "cost_price": 50.0, "currency": "USD"}]
    Q = {"MU": {"name": "美光", "price": 100.0, "pre_close": 95.0,
                "ext_price": 110.0, "ext_time": "t", "change_pct": 0.0,
                "currency": "USD", "quote_time": "t"}}

    def test_three_values_identity(self):
        # 盘后场景：腾讯昨收=95、场基准(base_overrides)=90、收盘定格(close_overrides)=100、
        # 盘后实时价(ext_price)=110
        positions, summary = pf.build_positions(
            self.H, self.Q, None, 7.8,
            base_overrides={"MU": 90.0}, close_overrides={"MU": 100.0})
        mu = positions[0]
        # 实时累计 = (盘后价 110 - 场基准 90) × 10 = 200（基准用 base_overrides 优先）
        assert mu["today_pnl"] == (110.0 - 90.0) * 10
        assert summary["day_pnl"] == (110.0 - 90.0) * 10
        # live_change_pct 基于场基准 90
        assert abs(mu["live_change_pct"] - (110.0 - 90.0) / 90.0 * 100) < 1e-9
        # 收盘定格涨跌幅（今日盈亏定格版分子）：(100-90)/90 = +11.11%
        assert abs(mu["day_close_pct"] - (100.0 - 90.0) / 90.0 * 100) < 1e-9
        # 收盘定格：position 带 close_price；定格盈亏 = (100-90)×10 = 100
        assert mu["close_price"] == 100.0
        assert summary["close_pnl"] == (100.0 - 90.0) * 10
        # 夜盘增量 = 实时累计 - 收盘定格 = 200 - 100 = 100
        assert summary["overnight_pnl"] == summary["day_pnl"] - summary["close_pnl"]
        # 夜盘增量% 后端统一下发：(现价110-定格100)/定格100 = +10%
        assert abs(mu["overnight_change_pct"] - 10.0) < 1e-9

    def test_no_overrides_fallback(self):
        # 不传 overrides：基准退化 pre_close(95)、close 退化实时价(110)→
        # overnight=0，与旧口径行为一致
        positions, summary = pf.build_positions(self.H, self.Q, None, 7.8)
        mu = positions[0]
        assert mu["today_pnl"] == (110.0 - 95.0) * 10
        assert mu["close_price"] == 110.0
        assert summary["close_pnl"] == (110.0 - 95.0) * 10
        assert summary["overnight_pnl"] == 0.0
        # close 退化实时价 → 夜盘增量% = 0
        assert mu["overnight_change_pct"] == 0.0

    def test_hk_overrides_fx(self):
        # HK 持仓：三数值按汇率折算 USD，恒等式保持
        h = [{"symbol": "03152", "name": "博时", "market": "HK",
              "quantity": 2, "cost_price": 1000.0, "currency": "HKD"}]
        q = {"03152": {"name": "博时", "price": 1050.0, "pre_close": 1049.0,
                       "change_pct": 0.0, "high": 0.0, "low": 0.0,
                       "currency": "HKD", "quote_time": "t",
                       "ext_price": 0.0, "ext_time": ""}}
        positions, summary = pf.build_positions(
            h, q, None, 7.8,
            base_overrides={"03152": 1040.0}, close_overrides={"03152": 1045.0})
        p = positions[0]
        assert p["today_pnl"] == (1050.0 - 1040.0) * 2 / 7.8
        assert summary["close_pnl"] == (1045.0 - 1040.0) * 2 / 7.8
        assert summary["overnight_pnl"] == summary["day_pnl"] - summary["close_pnl"]
        # 夜盘增量% 按 HKD 定格价计算，与金额口径同源
        assert abs(p["overnight_change_pct"] - (1050.0 - 1045.0) / 1045.0 * 100) < 1e-9

    def test_fund_close_is_nav(self):
        # HKFUND：场基准=prev_nav，收盘定格=当日净值（基金日更，无夜盘）→ overnight=0
        fund = {"nav": 15.50, "prev_nav": 15.62, "nav_date": date(2026, 9, 30),
                "nav_change_pct": -0.77}
        h = [{"symbol": "F1", "name": "邓普顿", "market": "HKFUND",
              "quantity": 100, "cost_price": 13.0, "currency": "HKD"}]
        positions, summary = pf.build_positions(h, {}, fund, 7.8)
        p = positions[0]
        assert p["close_price"] == 15.50
        assert summary["close_pnl"] == (15.50 - 15.62) * 100 / 7.8
        assert summary["overnight_pnl"] == 0.0
        # 基金日更无夜盘：现价=定格净值 → 夜盘增量% = 0
        assert p["overnight_change_pct"] == 0.0


class TestFetchOvernightQuotes:
    """夜盘代理价（24h数据源）：resolve_h24_map 映射/缓存/失败容错、
    fetch_h24_prices 中间价口径、fetch_overnight_quotes 插槽串联与降级。"""

    def test_resolve_h24_map_filters_trading_pairs(self, monkeypatch):
        """exchangeInfo 过滤 status=TRADING 的 代码+B+USDT 精确匹配，无对应缺省。"""
        async def fake_client_get(self, url, **kw):
            class R:
                def json(self):
                    return {"symbols": [
                        {"symbol": "AMDBUSDT", "status": "TRADING"},
                        {"symbol": "NVDABUSDT", "status": "TRADING"},
                        {"symbol": "QQQBUSDT", "status": "BREAK"},        # 非交易状态剔除
                        {"symbol": "XXYUSDT", "status": "TRADING"},      # 无关符号后缀不匹配
                    ]}
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        pf._h24_map_cache.clear()
        pf._h24_map_ts = 0.0
        m = asyncio.run(pf.resolve_h24_map(["AMD", "NVDA", "QQQ", "MU"]))
        assert m == {"AMD": "AMDBUSDT", "NVDA": "NVDABUSDT"}

    def test_resolve_h24_map_uses_cache_within_ttl(self, monkeypatch):
        """24h TTL 内直接命中缓存不再请求 exchangeInfo。"""
        calls = []

        async def fake_client_get(self, url, **kw):
            calls.append(url)
            raise AssertionError("缓存命中不应发起请求")

        pf._h24_map_cache.clear()
        pf._h24_map_cache.update({"AMD": "AMDBUSDT"})
        pf._h24_map_ts = time.monotonic()
        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        assert asyncio.run(pf.resolve_h24_map(["AMD"])) == {"AMD": "AMDBUSDT"}
        assert calls == []

    def test_fetch_h24_prices_batch_mid_price(self, monkeypatch):
        """bookTicker 批量一次拿全部：symbols=JSON 数组单参数，取 (bid+ask)/2 中间价。"""
        calls = []

        async def fake_client_get(self, url, **kw):
            calls.append(url)
            assert "symbols=" in url   # 批量参数形态（URL 编码 JSON 数组）

            class R:
                def json(self):
                    return [
                        {"symbol": "AMDBUSDT", "bidPrice": "620.02", "askPrice": "620.10"},
                        {"symbol": "MUBUSDT", "bidPrice": "1090.00", "askPrice": "1090.02"},
                    ]
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        out = asyncio.run(pf.fetch_h24_prices(
            {"AMD": "AMDBUSDT", "MU": "MUBUSDT"}))
        # 单次请求携带全部交易符号（攒批原则：次数要少，同批全查）
        assert len(calls) == 1
        assert out == {"AMD": {"price": pytest.approx(620.06)},
                       "MU": {"price": pytest.approx(1090.01)}}

    def test_fetch_h24_prices_bad_resp_falls_back(self, monkeypatch):
        """批量响应非 list（错误对象）→ 换代理候选重试，全失败返回 {}。"""
        attempts = []

        async def fake_client_get(self, url, **kw):
            attempts.append(url)

            class R:
                def json(self):
                    return {"code": -1121, "msg": "Invalid symbol."}   # 非批量数组
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        out = asyncio.run(pf.fetch_h24_prices(
            {"AMD": "AMDBUSDT"}, proxy_urls=["http://p1:9", "http://p2:9"]))
        assert out == {}
        # 直连 + 2 个代理候选逐个尝试后放弃
        assert len(attempts) == 3

    def test_fetch_overnight_quotes_degrades_to_empty(self, monkeypatch):
        """映射为空（全部无对应交易/接口失败）→ 恒 {}，自然降级定格口径。"""
        async def fake_resolve(us_symbols, proxy_urls=None):
            return {}
        monkeypatch.setattr(pf, "resolve_h24_map", fake_resolve)
        assert asyncio.run(pf.fetch_overnight_quotes(["QQQM"])) == {}

    def test_fetch_overnight_quotes_wires_h24(self, monkeypatch):
        """插槽串联：映射非空 → fetch_h24_prices 结果直出；境外代理透传。"""
        async def fake_resolve(us_symbols, proxy_urls=None):
            assert proxy_urls == ["http://ov1:9"]   # 境外代理经 fetch_overnight_quotes 透传
            return {"AMD": "AMDBUSDT"}

        async def fake_prices(bmap, proxy_urls=None):
            assert bmap == {"AMD": "AMDBUSDT"}
            return {"AMD": {"price": 620.06}}

        monkeypatch.setattr(pf, "resolve_h24_map", fake_resolve)
        monkeypatch.setattr(pf, "fetch_h24_prices", fake_prices)
        assert asyncio.run(pf.fetch_overnight_quotes(["AMD"], proxy_urls=["http://ov1:9"])) \
            == {"AMD": {"price": 620.06}}


class TestFetchEquityOvernightQuotes:
    """股票行情端点补齐：无交易标的（AMKR/PCG/F）夜盘价，中间价口径与容错。"""

    def test_equity_mid_price_per_symbol(self, monkeypatch):
        """逐只单 symbol 请求，取 (bid+ask)/2 中间价（与 bookTicker 同口径）。"""
        calls = []

        async def fake_client_get(self, url, **kw):
            calls.append(kw.get("params", {}).get("symbol"))

            class R:
                def json(self):
                    return {"symbol": kw["params"]["symbol"],
                            "bidPrice": "53.92", "askPrice": "54.60",
                            "bidSize": 100, "askSize": 100}
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        out = asyncio.run(pf.fetch_equity_overnight_quotes(["AMKR", "F", "PCG"]))
        assert sorted(calls) == ["AMKR", "F", "PCG"]   # 逐只补齐缺口标的
        assert out == {"AMKR": {"price": pytest.approx(54.26)},
                       "F": {"price": pytest.approx(54.26)},
                       "PCG": {"price": pytest.approx(54.26)}}

    def test_equity_bad_rows_skipped(self, monkeypatch):
        """异常/零价/symbol 不符的标的跳过，自然缺省维持定格口径。"""

        async def fake_client_get(self, url, **kw):
            sym = kw["params"]["symbol"]

            class R:
                def json(self):
                    if sym == "AMKR":
                        return {"symbol": "AMKR", "bidPrice": "0", "askPrice": "0"}
                    if sym == "F":
                        return {"symbol": "OTHER", "bidPrice": "12", "askPrice": "13"}
                    raise ValueError("gateway error")

            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        out = asyncio.run(pf.fetch_equity_overnight_quotes(["AMKR", "F", "PCG"]))
        assert out == {}

    def test_equity_empty_symbols_no_request(self, monkeypatch):
        """缺口为空 → 不发请求。"""
        async def fake_client_get(self, url, **kw):
            raise AssertionError("空列表不应发起请求")
        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)
        assert asyncio.run(pf.fetch_equity_overnight_quotes([])) == {}


class TestBuildCnySummary:
    """人民币累计口径看板：总资产 × USDCNH 对比净投入本金，缺输入降级 None。"""

    def test_cny_summary_math(self):
        """value=总资产×汇率；pnl=value-本金；pct 以本金为分母。"""
        out = pf.build_cny_summary(12000.0, 7.0, 84850.0)
        assert out == {"rate": 7.0, "invested": 84850.0,
                       "invested_gross": 84850.0, "expense": 0.0,
                       "value": 84000.0, "pnl": -850.0,
                       "pnl_pct": pytest.approx(-1.0, abs=0.01)}

    def test_cny_summary_net_invested(self):
        """净投入口径：消费从本金扣除，pnl/pct 以净投入为分母（与记账页一致）。"""
        out = pf.build_cny_summary(12000.0, 7.0, 84850.0, 246.32)
        assert out["invested"] == 84603.68
        assert out["invested_gross"] == 84850.0
        assert out["expense"] == 246.32
        assert out["pnl"] == pytest.approx(84000.0 - 84603.68, abs=0.01)
        assert out["pnl_pct"] == pytest.approx(-603.68 / 84603.68 * 100, abs=0.02)

    def test_cny_summary_missing_inputs(self):
        """汇率未就绪/本金未配置/总资产为空/净投入非正 → None（前端降级 --）。"""
        assert pf.build_cny_summary(12000.0, None, 84850.0) is None
        assert pf.build_cny_summary(12000.0, 7.0, None) is None
        assert pf.build_cny_summary(12000.0, 7.0, 0.0) is None
        assert pf.build_cny_summary(None, 7.0, 84850.0) is None
        assert pf.build_cny_summary(12000.0, 7.0, 100.0, 100.0) is None


class TestTwelveDataFallback:
    """twelvedata 接口型兜底（替换东财 ulist）：批量 ≤8 只轮换覆盖、字段映射。"""

    def _mock_quote_response(self, monkeypatch, resp_json):
        async def fake_client_get(self, url, params=None, **kw):
            assert url == pf.TWELVEDATA_QUOTE_URL and params.get("apikey") == pf.TWELVEDATA_API_KEY

            class R:
                def json(self):
                    return resp_json
            return R()
        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_client_get)

    def test_batch_limit_and_rotation(self, monkeypatch):
        """15 只持仓：单次批量 ≤8（免费 8 credits/min），轮换指针逐次覆盖全部。"""
        syms = [f"S{i}" for i in range(15)]
        self._mock_quote_response(monkeypatch, {
            s: {"symbol": s, "name": f"n{s}", "close": "10.5", "previous_close": "10.0",
                "percent_change": "5.0", "high": "11", "low": "9", "datetime": "2026-10-01"}
            for s in syms})

        async def no_text(url, headers=None, timeout=10.0):
            return None   # 主源失败触发兜底
        monkeypatch.setattr(pf, "_get_text", no_text)

        pf._td_fail_streak = 3
        pf._last_td_used = 0.0
        pf._td_cursor = 0
        q = asyncio.run(pf.fetch_quotes(syms, []))
        assert len(q) == 8                       # 首批 8 只
        assert set(q) == set(syms[:8])
        assert 0 < pf._td_cursor < len(syms)     # 指针后移
        pf._last_td_used = 0.0                   # 重置 60s 节流（测试立即触发第二批）
        q2 = asyncio.run(pf.fetch_quotes(syms, []))
        # 第二批 S8..S14 + 回绕 S0（批量恒 8，轮换指针取模环绕覆盖全部持仓）
        assert set(q2) == set(syms[8:]) | {"S0"}

    def test_field_mapping(self, monkeypatch):
        """字段映射：close→price、previous_close→pre_close、percent_change→change_pct。"""
        self._mock_quote_response(monkeypatch, {
            "AMD": {"symbol": "AMD", "name": "Advanced Micro Devices Inc.",
                    "close": "615.73", "previous_close": "611.76",
                    "percent_change": "0.64894250", "high": "619.3",
                    "low": "601.5", "datetime": "2026-10-01"}})

        async def no_text(url, headers=None, timeout=10.0):
            return None
        monkeypatch.setattr(pf, "_get_text", no_text)

        pf._td_fail_streak = 3
        pf._last_td_used = 0.0
        pf._td_cursor = 0
        q = asyncio.run(pf.fetch_quotes(["AMD"], []))
        assert q["AMD"]["price"] == 615.73
        assert q["AMD"]["pre_close"] == 611.76
        assert abs(q["AMD"]["change_pct"] - 0.648942) < 1e-6
        assert q["AMD"]["currency"] == "USD"

    def test_single_symbol_flat_response(self, monkeypatch):
        """单 symbol 请求返回扁平对象（非字典包裹），统一解析。"""
        self._mock_quote_response(monkeypatch, {
            "symbol": "AMD", "name": "AMD", "close": "615.73",
            "previous_close": "611.76", "percent_change": "0.64"})

        async def no_text(url, headers=None, timeout=10.0):
            return None
        monkeypatch.setattr(pf, "_get_text", no_text)

        pf._td_fail_streak = 3
        pf._last_td_used = 0.0
        pf._td_cursor = 0
        q = asyncio.run(pf.fetch_quotes(["AMD"], []))
        assert q["AMD"]["price"] == 615.73


SINA_PANEL_RAW = (
    'var hq_str_DINIW="00:45:25,102.1406,102.1406,101.4743,7389,101.4825,'
    '102.1766,101.4377,102.1406,美元指数,2026-10-02";\n'
    'var hq_str_hf_OIL="66.10,66.05,,,66.20,66.00,65.90,65.50,0.00,0.00,'
    '2026-10-02,0:00:00,0,布伦特原油,0,0";\n'
    'var hq_str_hf_GC="3860.5,3855.0,,,3865.0,3850.0,3848.0,3840.0,0.00,'
    '0.00,2026-10-02,0:00:00,0,COMEX黄金,0,0";\n'
)


class TestMarketPanelYahooSpark:
    """看板分时主价快照存库口径：Yahoo 5d/5m 解析（US30Y/US10Y/VIX 主价刷新
    顺带落库）、新浪 fx_susdcnh 主价、fetch_market_panel 组装（spark 读库注入
    [ts,price] 对），以及 USD/HKD 汇率走势 fetch_fx_spark（server fx_spark 下发、
    前端拼 USDHKD 方块用）。"""

    def test_day_symbols_mapping(self):
        """YAHOO_DAY_SYMBOLS 当日分时映射齐备（期货/美元指数/收益率/VIX/离岸人民币）；
        YAHOO_PANEL_SYMBOLS 精简为 USDHKD fx_spark 专用（5d 日线仅此一项保留）。"""
        assert pf.YAHOO_DAY_SYMBOLS["BZ"] == "BZ=F"
        assert pf.YAHOO_DAY_SYMBOLS["GC"] == "GC=F"
        assert pf.YAHOO_DAY_SYMBOLS["DXY"] == "DX-Y.NYB"
        assert pf.YAHOO_DAY_SYMBOLS["US30Y"] == "^TYX"
        assert pf.YAHOO_DAY_SYMBOLS["US10Y"] == "^TNX"
        assert pf.YAHOO_DAY_SYMBOLS["VIX"] == "^VIX"
        assert pf.YAHOO_DAY_SYMBOLS["USDCNH"] == "CNH=X"
        assert pf.YAHOO_PANEL_SYMBOLS == {"USDHKD": "HKD=X"}

    def test_market_panel_spark_marks(self):
        """MARKET_PANEL spark_source 口径：第 5 位仅作历史主源标记（现行分时统一
        读库组装，不再单独请求分时接口）；USDCNH 已作为第 11 项加入看板。"""
        src = {key: s for key, _, _, _, s in pf.MARKET_PANEL}
        assert len(pf.MARKET_PANEL) == 14 and "USDCNH" in src
        assert src["HSI"] == "tencent"
        assert src["QQQ"] == src["SPY"] == src["SOXX"] == "yahoo_day"
        assert src["BZ"] == src["GC"] == src["DXY"] == "yahoo_day"
        assert src["US30Y"] == src["US10Y"] == src["VIX"] == src["USDCNH"] == "yahoo_day"
        assert "yahoo" not in set(src.values())
        # 主价快照存库/读 spark 标的全集：10 个 Yahoo 项 + HSI
        assert set(pf.DAY_MINUTE_KEYS) == set(pf.YAHOO_DAY_SYMBOLS) | {"HSI"}

    def test_parse_yahoo_day_chart(self):
        """Yahoo 5d/5m 解析：meta 主价/昨收 + None 收盘点跳过 + epoch→UTC naive
        + 只保留最后一个 UTC 自然日（盘中=当日分时、闭市=上一时段全程，
        并规避 Yahoo 期货 range=1d 凌晨空窗 bug）。"""
        ts0 = int(datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc).timestamp())
        ts1 = int(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc).timestamp())
        raw = json.dumps({"chart": {"result": [{
            "meta": {"regularMarketPrice": 742.03, "chartPreviousClose": 739.77},
            "timestamp": [ts0, ts0 + 300, ts1, ts1 + 300, ts1 + 600],
            "indicators": {"quote": [{"close": [738.0, 739.0, None, 740.0, 742.03]}]}}]}})
        q = pf.parse_yahoo_day_chart(raw)
        # 昨收取最后 UTC 日之前的最后有效收盘点（739.0），而非 meta 的 5 日前收盘
        assert q["price"] == 742.03 and q["pre_close"] == 739.0
        assert abs(q["change_pct"] - (742.03 - 739.0) / 739.0 * 100) < 1e-9
        # 前一 UTC 日(09-30) 2 点被过滤；最后 UTC 日(10-01) None 收盘桶跳过：3 桶 → 2 点
        assert q["minutes"] == [(datetime(2026, 10, 1, 8, 5), 740.0),
                                (datetime(2026, 10, 1, 8, 10), 742.03)]

    def test_parse_yahoo_day_chart_empty_day(self):
        """5d 窗口全空（极端空窗）：返回结构但 minutes=[]，主价仍可用。"""
        ts0 = int(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc).timestamp())
        raw = json.dumps({"chart": {"result": [{
            "meta": {"regularMarketPrice": 102.2, "chartPreviousClose": 102.3},
            "timestamp": [ts0], "indicators": {"quote": [{"close": [None]}]}}]}})
        q = pf.parse_yahoo_day_chart(raw)
        assert q["price"] == 102.2 and q["minutes"] == []

    def test_parse_yahoo_day_chart_bad(self):
        assert pf.parse_yahoo_day_chart("not-json") == {}
        assert pf.parse_yahoo_day_chart('{"chart": {"result": []}}') == {}

    def test_parse_fx_usdcnh(self):
        """新浪 fx_susdcnh：f[1] 现价 f[3] 昨收，涨跌幅自算（实测布局与 susdhkd 一致）。"""
        raw = ('var hq_str_fx_susdcnh="11:46:24,6.706800,6.706900,6.713200,124,'
               '6.713600,6.717400,6.705000,6.706800,离岸人民币（香港）,-0.100000,'
               '-0.006400,0.001847,,6.995700,6.691300,,2026-10-02";')
        q = pf.parse_fx_usdcnh(raw)
        assert q["price"] == 6.7068 and q["pre_close"] == 6.7132
        assert abs(q["change_pct"] - (6.7068 - 6.7132) / 6.7132 * 100) < 1e-9

    def test_parse_fx_usdcnh_bad(self):
        # 字段不足 / 价格越界 / 无匹配 → {}（看板该项 price 置 None 不致命）
        assert pf.parse_fx_usdcnh('var hq_str_fx_susdcnh="11:46,6.70";') == {}
        assert pf.parse_fx_usdcnh('var hq_str_fx_susdcnh="09:00,9.9,1,6.7,1,1,1,1,1,x,1,1";') == {}
        assert pf.parse_fx_usdcnh("") == {}

    def test_market_panel_assembly(self, monkeypatch):
        """组装：spark 由 spark_map 注入（缺项空数组）；主价分工不变——BZ/GC=新浪 hf
        兜底、DXY=DINIW、USDCNH=fx_susdcnh、US30Y/US10Y/VIX=yahoo_day 缺失时内部补拉
        Yahoo 当日分时；腾讯分时死请求已消除（主价轮不打 minute 接口）。"""
        minute_urls: list[str] = []

        async def fake_get_text(url, headers=None, timeout=10.0):
            if "minute/query" in url:
                minute_urls.append(url)
            return ""    # 分钟线路径（走 _get_text 的仅剩 HSI 分钟线）

        async def fake_bus_get(codes):
            return ""    # 腾讯混排（攒批总线）空：BZ/GC 主价走新浪 hf 兜底

        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            return SINA_PANEL_RAW + (
                'var hq_str_fx_susdcnh="11:46:24,6.706800,6.706900,6.713200,124,'
                '6.713600,6.717400,6.705000,6.706800,离岸人民币（香港）,-0.100000,'
                '-0.006400,0.001847,,6.995700,6.691300,,2026-10-02";\n')

        yahoo_calls: list[str] = []

        async def fake_day(sym, proxy_urls=None):
            yahoo_calls.append(sym)
            return {"price": 5.6, "pre_close": 5.5, "change_pct": 1.8, "minutes": []}

        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        monkeypatch.setattr(pf.tencent_bus, "get", fake_bus_get)
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        monkeypatch.setattr(pf, "fetch_yahoo_day_minutes", fake_day)
        panel = asyncio.run(pf.fetch_market_panel(
            proxy_urls=["p1"],
            yahoo_day={"US30Y": {"price": 5.603, "pre_close": 5.5, "change_pct": 1.87}},
            spark_map={"QQQ": [1.0, 2.0, 3.0, 4.0, 5.0]}))
        got = {row["key"]: row for row in panel}
        # spark 注入：传入项取库内序列，未传项为空数组
        assert got["QQQ"]["spark"] == [1.0, 2.0, 3.0, 4.0, 5.0]
        assert got["BZ"]["spark"] == [] and got["USDCNH"]["spark"] == []
        # 主价分工：BZ/GC=新浪 hf 兜底、DXY=DINIW、USDCNH=fx_susdcnh
        assert got["BZ"]["price"] == 66.10
        assert got["GC"]["price"] == 3860.5
        assert got["DXY"]["price"] == 102.1406
        assert got["USDCNH"]["price"] == 6.7068
        # US30Y 已有 yahoo_day 数据（零补拉）；US10Y/VIX/DJI/NDX/SPX 缺失
        # → 内部补拉当日分时接口（按 MARKET_PANEL 顺序）
        assert yahoo_calls == ["^TNX", "^VIX", "^DJI", "^NDX", "^GSPC"]
        assert got["US30Y"]["price"] == 5.603 and got["US10Y"]["price"] == 5.6
        assert got["DJI"]["price"] == 5.6 and got["SPX"]["price"] == 5.6
        # 腾讯分时死请求已消除：主价轮不再打 minute 接口
        assert minute_urls == []

    def test_market_panel_fetch_missing_off(self, monkeypatch):
        """fetch_market_panel fetch_missing=False：缓存缺失项也零 Yahoo 请求，
        缺失项 price 置 None（前端隐藏涨跌）——server _market_loop 传入，
        Yahoo 补拉节流单点化到 server due 判定，防缓存空期间每轮 3 请求放大。"""
        async def fake_get_text(url, headers=None, timeout=10.0):
            return ""

        async def fake_bus_get(codes):
            return ""

        async def fake_resilient(url, headers=None, proxy_urls=None, timeout=10.0):
            return ""    # 新浪空：BZ/GC/DXY/USDCNH 全部缺价（本用例只看 Yahoo 侧）

        yahoo_calls: list[str] = []

        async def fake_day(sym, proxy_urls=None):
            yahoo_calls.append(sym)
            return {"price": 5.6, "pre_close": 5.5, "change_pct": 1.8, "minutes": []}

        monkeypatch.setattr(pf, "_get_text", fake_get_text)
        monkeypatch.setattr(pf.tencent_bus, "get", fake_bus_get)
        monkeypatch.setattr(pf, "_get_text_resilient", fake_resilient)
        monkeypatch.setattr(pf, "fetch_yahoo_day_minutes", fake_day)
        panel = asyncio.run(pf.fetch_market_panel(
            proxy_urls=["p1"], yahoo_day=None, spark_map=None,
            fetch_missing=False))
        got = {row["key"]: row for row in panel}
        # 缓存为空 + fetch_missing=False → 零 Yahoo 请求，三项 price 置 None
        assert yahoo_calls == []
        assert got["US30Y"]["price"] is None
        assert got["US10Y"]["price"] is None
        assert got["VIX"]["price"] is None

    def test_fetch_fx_spark(self, monkeypatch):
        """fetch_fx_spark：Yahoo HKD=X 仅取 spark 字段，代理清单透传。"""
        async def fake_ok(symbol, proxy_urls=None):
            assert symbol == "HKD=X" and proxy_urls == ["p1"]
            return {"price": 7.8, "pre_close": 7.8, "change_pct": 0.0,
                    "spark": [7.81, 7.82, 7.80]}

        monkeypatch.setattr(pf, "_fetch_yahoo_chart", fake_ok)
        assert asyncio.run(pf.fetch_fx_spark(proxy_urls=["p1"])) == [7.81, 7.82, 7.80]

    def test_fetch_fx_spark_fail(self, monkeypatch):
        """chart 失败（空 dict）→ 返回空数组，不抛异常。"""
        async def fake_fail(symbol, proxy_urls=None):
            return {}

        monkeypatch.setattr(pf, "_fetch_yahoo_chart", fake_fail)
        assert asyncio.run(pf.fetch_fx_spark()) == []


class TestH24AliasDerive:
    """同标的替代推导（QQQM→QQQ）：24h价百分点映射到自身收盘基准。"""

    def test_derive_alias_qqqm_from_qqq(self):
        # QQQ 24h价 745 / QQQ 收盘 740 = +0.6757% → QQQM = 304.64 × 比率
        overnight = {"QQQ": {"price": 745.0}}
        close_map = {"QQQM": 304.64, "QQQ": 740.0}
        out = pf.derive_alias_prices(overnight, close_map, ["QQQM", "QQQ"])
        assert set(out) == {"QQQM"}
        assert out["QQQM"]["price"] == pytest.approx(304.64 * 745.0 / 740.0)

    def test_derive_alias_skips_when_self_covered(self):
        # 自身已有 24h价（理论不该发生）→ 不推导不覆盖
        overnight = {"QQQ": {"price": 745.0}, "QQQM": {"price": 306.0}}
        out = pf.derive_alias_prices(overnight, {"QQQM": 304.64, "QQQ": 740.0},
                                     ["QQQM", "QQQ"])
        assert out == {}

    def test_derive_alias_requires_close_anchors(self):
        # 收盘定格缺失（任一侧）→ 放弃推导（保持定格口径，不猜价）
        overnight = {"QQQ": {"price": 745.0}}
        assert pf.derive_alias_prices(overnight, {"QQQ": 740.0}, ["QQQM"]) == {}
        assert pf.derive_alias_prices(overnight, {"QQQM": 304.64}, ["QQQM"]) == {}
        assert pf.derive_alias_prices(overnight, {}, ["QQQM", "QQQ"]) == {}

    def test_derive_alias_not_in_universe(self):
        # QQQM 不在持仓清单 → 不推导（推导只为持仓服务）
        out = pf.derive_alias_prices({"QQQ": {"price": 745.0}},
                                     {"QQQM": 304.64, "QQQ": 740.0}, ["QQQ"])
        assert out == {}


class TestTdSessionAnchors:
    """冷启动一场锚点恢复（twelvedata previous_close/close）：批量 8 只 + 60s 节流。"""

    def test_fetch_anchors_parses_prev_close(self, monkeypatch):
        pf._last_td_used = 0.0   # 重置节流

        async def fake_get(self, url, params=None, **kw):
            assert params["symbol"] == "AMD,MU"
            assert params["apikey"] == pf.TWELVEDATA_API_KEY

            class R:
                def json(self):
                    return {"AMD": {"open": "610.0", "close": "615.5",
                                    "previous_close": "611.76"},
                            "MU": {"open": "1080.0", "close": "1065.11"}}
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_get)
        out = asyncio.run(pf.fetch_td_session_anchors(["AMD", "MU"]))
        # base=previous_close（与 regular f[4] 场基准同口径）；previous_close
        # 缺失时退化 open（MU 用例）
        assert out == {"AMD": {"open": 610.0, "prev": 611.76, "close": 615.5},
                       "MU": {"open": 1080.0, "prev": 1080.0, "close": 1065.11}}

    def test_fetch_anchors_throttled(self, monkeypatch):
        # 距上次使用 <60s → 直接 {}，不发起请求（复用 twelvedata 免费限额节流）
        pf._last_td_used = time.monotonic()
        calls = []

        async def boom(self, *a, **kw):
            calls.append(1)
            raise AssertionError("节流期内不应请求")

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", boom)
        assert asyncio.run(pf.fetch_td_session_anchors(["AMD"])) == {}
        assert calls == []

    def test_fetch_anchors_batch_cap(self, monkeypatch):
        # 单批上限 8 只（对齐免费 8 credits/min），超出部分留给下批渐进
        pf._last_td_used = 0.0
        got = {}

        async def fake_get(self, url, params=None, **kw):
            got["symbols"] = params["symbol"].split(",")

            class R:
                def json(self):
                    return {s: {"open": "1.0", "close": "1.0"} for s in got["symbols"]}
            return R()

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_get)
        out = asyncio.run(pf.fetch_td_session_anchors([f"S{i}" for i in range(12)]))
        assert len(got["symbols"]) == 8
        assert len(out) == 8

    def test_fetch_anchors_fail_returns_empty(self, monkeypatch):
        pf._last_td_used = 0.0

        async def fake_get(self, *a, **kw):
            raise RuntimeError("network down")

        monkeypatch.setattr(pf.httpx.AsyncClient, "get", fake_get)
        assert asyncio.run(pf.fetch_td_session_anchors(["AMD"])) == {}


class TestTencentBus:
    """腾讯行情攒批发送路由：微批合并（次数要少，同批全查）。"""

    def _mk_bus(self):
        bus = TencentBus(min_interval=0.2, ttl=0.4)
        return bus

    def _patch_fetch(self, monkeypatch, responses):
        """responses: dict[prefixed_code, fields_str]；记录每次拉取的 codes。"""
        reqs = []

        async def fake_fetch(self):
            try:
                batch = sorted(self._pending)
                self._pending.clear()
                if not batch:
                    return
                reqs.append(batch)
                now = time.monotonic()
                self._last_fetch = now
                for c in batch:
                    if c in responses:
                        self._result[c] = f"v_{c}={responses[c]}"
                        self._ts[c] = now
            finally:
                self._inflight = None

        monkeypatch.setattr(TencentBus, "_fetch", fake_fetch)
        return reqs

    def test_two_consumers_share_one_request(self, monkeypatch):
        """两个消费方并发请求不同 code → 聚合为一次批量拉取（同批全查）。"""
        bus = self._mk_bus()
        reqs = self._patch_fetch(monkeypatch, {"usAMD": "1~AMD~620", "sz159501": "51~ETF~1.2"})

        # 模拟真实网络延迟：批在飞期间第二个消费方到达 → 其 code 并入同批
        orig_fetch = TencentBus._fetch

        async def slow_fetch(self):
            await asyncio.sleep(0.15)
            await orig_fetch(self)

        monkeypatch.setattr(TencentBus, "_fetch", slow_fetch)

        async def main():
            t1 = asyncio.create_task(bus.get(["usAMD"]))
            await asyncio.sleep(0.05)
            t2 = asyncio.create_task(bus.get(["sz159501"]))
            return await asyncio.gather(t1, t2)

        r1, r2 = asyncio.run(main())
        assert r1 == "v_usAMD=1~AMD~620"
        assert r2 == "v_sz159501=51~ETF~1.2"
        # 两次消费只发起一次真实拉取，且一批带全两个 code
        assert len(reqs) == 1 and set(reqs[0]) == {"usAMD", "sz159501"}

    def test_ttl_cache_hit_no_refetch(self, monkeypatch):
        """TTL 内重复请求同一 code → 直接回缓存，不重复拉取。"""
        bus = self._mk_bus()
        reqs = self._patch_fetch(monkeypatch, {"usAMD": "1~AMD~620"})

        async def main():
            a = await bus.get(["usAMD"])
            b = await bus.get(["usAMD"])
            return a, b

        a, b = asyncio.run(main())
        assert a == b == "v_usAMD=1~AMD~620"
        assert len(reqs) == 1

    def test_min_interval_coalesces(self, monkeypatch):
        """MIN_INTERVAL 内的第二次拉取需求被抑制（合入节奏，不放大请求）。"""
        bus = self._mk_bus()
        reqs = self._patch_fetch(monkeypatch, {"usAMD": "1~AMD~620", "usMU": "1~MU~1090"})

        async def main():
            await bus.get(["usAMD"])
            await asyncio.sleep(0.05)   # 仍在 min_interval(0.2s) 冷却内
            await bus.get(["usMU"])
            await asyncio.sleep(0.3)     # TTL(0.4s) 内：缓存直接命中，不再拉取
            await bus.get(["usMU"])

        asyncio.run(main())
        assert reqs == [["usAMD"], ["usMU"]]

    def test_join_drops_missing(self, monkeypatch):
        """部分 code 拉取失败（响应缺失）→ 拼接文本自然缺该段，不抛错。"""
        bus = self._mk_bus()
        self._patch_fetch(monkeypatch, {"usAMD": "1~AMD~620"})

        async def main():
            return await bus.get(["usAMD", "usBAD"])

        out = asyncio.run(main())
        assert out == "v_usAMD=1~AMD~620"
