"""持仓模块离线单元测试：时段/解析/盈亏计算（不发真实网络请求）"""
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import portfolio_fetcher as pf

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

    def test_parse_tencent_minute(self):
        """腾讯分时接口 → [(HHMM, 价格)]。"""
        raw = json.dumps({"code": 0, "data": {"usQQQ": {"data": {
            "data": ["0930 738.00 100", "0931 738.50 200"], "date": ""}}}})
        pts = pf.parse_tencent_minute(raw)
        assert pts == [("0930", 738.0), ("0931", 738.5)]

    def test_parse_tencent_minute_garbage(self):
        """非法 JSON/结构缺失 → 空列表，不抛异常。"""
        assert pf.parse_tencent_minute("not-json") == []
        assert pf.parse_tencent_minute(json.dumps({"code": 0, "data": {}})) == []
