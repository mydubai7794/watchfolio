"""macro_fetcher 解析纯逻辑测试（fixture 字符串，零网络）。@author yys"""
import asyncio

import macro_fetcher as mf


def test_parse_fred_csv_latest_skipping_dots():
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,1.5\n") == \
        ("2026-10-06", 1.5)
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,\n") is None
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n") is None


def test_parse_fred_csv_header_only():
    assert mf.parse_fred_csv("DATE,WILL5000PRFC\n") is None


def test_parse_multpl_cape_by_year_html():
    # 真实端点 /shiller-pe/table/by-year 首数据行即最新（值列含 &#x2002; 实体）
    t = ('<table><tr><th>Date</th><th>Value</th></tr>'
         '<tr><td>Oct 6, 2026</td><td>&#x2002;\n41.90</td></tr>'
         '<tr><td>Jan 1, 2026</td><td>&#x2002;\n39.65</td></tr></table>')
    assert mf.parse_multpl_cape(t) == ("2026-10-06", 41.9)
    # 年度行兜底（表首缺日线时）
    t2 = ('<table><tr><th>Date</th><th>Value</th></tr>'
          '<tr><td>Jan 1, 2026</td><td>&#x2002;\n39.65</td></tr></table>')
    assert mf.parse_multpl_cape(t2) == ("2026-01-01", 39.65)
    assert mf.parse_multpl_cape("<table><tr><td>x</td></tr></table>") is None
    assert mf.parse_multpl_cape("<html>oops") is None


def test_source_urls_and_anti_bot_headers():
    # multpl 真实端点是 by-year HTML 表格（table-json 是 404 臆造端点）
    assert mf.MULTPL_CAPE_URL == "https://www.multpl.com/shiller-pe/table/by-year"
    # CNN 需完整浏览器 UA + Referer（短 UA 得 200 但 body 是 teapot 反爬文本）
    assert mf.CNN_HEADERS["Referer"].startswith("https://edition.cnn.com/")
    assert "Chrome/" in mf.CNN_HEADERS["User-Agent"] and "Safari" in mf.CNN_HEADERS["User-Agent"]
    # FRED 歧视伪造浏览器 UA（实测 Chrome UA 挂起超时、默认 UA 200），不配浏览器头
    assert "User-Agent" not in mf.FRED_HEADERS
    # 巴菲特指标：Wilshire 系列已从 FRED 下架（WILL5000PRFC 等 404），
    # 改用非金融企业市值（NCBEILQ027S，百万$）÷ GDP（十亿$，单位差 1000 倍）
    assert mf.BUFFETT_MCAP_SID == "NCBEILQ027S"
    assert mf.BUFFETT_GDP_SID == "GDP"


def test_parse_cnn_fg():
    # CNN 真实顶层键为 fear_and_greed（曾误写 fear_greed 少 _and 导致线上永远 miss）
    t = '{"fear_and_greed":{"score":38.5,"rating":"Fear","previous":40}}'
    assert mf.parse_cnn_fg(t) == (38.5, "Fear")
    assert mf.parse_cnn_fg('{"fear_greed":{"score":1}}') is None
    assert mf.parse_cnn_fg("{}") is None
    assert mf.parse_cnn_fg("not json") is None


def test_fetchers_registry():
    assert {f.__name__ for f in mf.FETCHERS} == {
        "fetch_buffett", "fetch_cape", "fetch_yield_spread", "fetch_fear_greed",
        "fetch_cpi_yoy", "fetch_credit_spread"}


CPI_TEXT = "\n".join([
    "DATE,CPIAUCSL",
    "2025-07-01,320.2",                # 使 rows[-13] ≠ rows[0]，锁索引语义
    "2025-08-01,.",                    # 无效行跳过
    "2025-09-01,320.5",
    "2025-10-01,320.8",
    "2025-11-01,321.0",
    "2025-12-01,322.1",
    "2026-01-01,323.0",
    "2026-02-01,324.0",
    "2026-03-01,325.0",
    "2026-04-01,326.0",
    "2026-05-01,327.0",
    "2026-06-01,328.0",
    "2026-07-01,329.0",
    "2026-08-01,330.0",
    "2026-09-01,331.0",
])


def test_parse_fred_csv_tail():
    tail = mf.parse_fred_csv_tail(CPI_TEXT, 14)
    assert len(tail) == 14                 # '.' 行被跳过（15 数据行）
    assert tail[-1] == ("2026-09-01", 331.0)
    assert tail[0] == ("2025-07-01", 320.2)
    assert mf.parse_fred_csv_tail("DATE,X\n") == []


def test_fetch_cpi_yoy(monkeypatch):
    async def fake_get(url, headers=None, allow_proxy=True):
        assert "CPIAUCSL" in url
        return CPI_TEXT

    monkeypatch.setattr(mf, "_get_text", fake_get)
    r = asyncio.run(mf.fetch_cpi_yoy())
    assert r["name"] == "cpi_yoy"
    assert r["asof"] == "2026-09-01"
    assert r["value"] == round((331.0 / 320.5 - 1) * 100, 2)
    assert r["extra"]["year_ago"] == 320.5
    assert r["extra"]["year_ago_asof"] == "2025-09-01"   # rows[-13] 精确锁定


def test_fetch_cpi_yoy_insufficient_history(monkeypatch):
    short = "DATE,CPIAUCSL\n" + "\n".join(f"2026-0{i}-01,{320 + i}" for i in range(1, 4))

    async def fake_get(url, headers=None, allow_proxy=True):
        return short

    monkeypatch.setattr(mf, "_get_text", fake_get)
    assert asyncio.run(mf.fetch_cpi_yoy()) is None


def test_fetch_credit_spread(monkeypatch):
    async def fake_get(url, headers=None, allow_proxy=True):
        assert "BAMLH0A0HYM2" in url
        return "DATE,BAMLH0A0HYM2\n2026-10-10,3.18\n"

    monkeypatch.setattr(mf, "_get_text", fake_get)
    r = asyncio.run(mf.fetch_credit_spread())
    assert r == {"name": "credit_spread_hy", "asof": "2026-10-10",
                 "value": 3.18, "extra": {}}
