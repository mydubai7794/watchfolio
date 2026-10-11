"""edgar_fetcher 纯函数层测试（fixture 字符串，零网络）。@author yys"""
import json
from datetime import date, datetime

import edgar_fetcher as ef


def test_parse_ticker_map():
    text = json.dumps({
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 1045810, "ticker": "nvda", "title": "NVIDIA"},
        "2": {"cik_str": "bad", "ticker": "XX", "title": "坏行跳过"},
    })
    out = ef.parse_ticker_map(text)
    assert out == {"AAPL": 320193, "NVDA": 1045810}


def test_classify_asset_type_priority():
    assert ef.classify_asset_type(["10-K", "4", "8-K"]) == "stock"
    assert ef.classify_asset_type(["10-Q/A", "4"]) == "stock"
    assert ef.classify_asset_type(["20-F", "6-K"]) == "fpi"          # fpi 优先于其它
    assert ef.classify_asset_type(["20-F", "10-K"]) == "fpi"         # 混合申报 fpi 优先
    assert ef.classify_asset_type(["20-F", None]) == "fpi"           # None 元素防御
    assert ef.classify_asset_type(["N-1A", "NPORT-PART"]) == "etf"
    assert ef.classify_asset_type(["N-CSR"]) == "etf"
    assert ef.classify_asset_type(["6-K", "8-K"]) is None            # 无法归类下轮再试
    assert ef.classify_asset_type([]) is None


SUBMISSIONS_JSON = json.dumps({
    "cik": 1045810, "name": "NVIDIA CORP",
    "filings": {"recent": {
        "accessionNumber": ["0001045810-26-000001", "0001045810-26-000002",
                            "0001045810-26-000003"],
        "filingDate": ["2026-10-08", "2026-10-05", "2026-09-30"],
        "formType": ["4", "10-Q", "4"],
        "primaryDocument": ["xslF345X05/primary_doc.xml", "nvda-10q.htm",
                            "primary_doc.xml"],
    }}})


def test_parse_submissions_recent():
    all_rows = ef.parse_submissions_recent(SUBMISSIONS_JSON)
    assert [r["form"] for r in all_rows] == ["4", "10-Q", "4"]
    form4 = ef.parse_submissions_recent(SUBMISSIONS_JSON, form_filter="4")
    assert len(form4) == 2
    assert form4[0]["accession"] == "0001045810-26-000001"
    assert form4[0]["primary_doc"] == "xslF345X05/primary_doc.xml"
    assert ef.parse_submissions_recent('{"filings":{}}') == []


FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-10-08</periodOfReport>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001234567</rptOwnerCik>
      <rptOwnerName>JENSEN HUANG</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>President and CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>141000</value></transactionShares>
        <transactionPricePerShare><value>135.0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>20000</value></transactionShares>
        <transactionPricePerShare><value>136.5</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-07</value></transactionDate>
      <transactionCoding><transactionCode>G</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>0</value></transactionShares>
        <transactionPricePerShare><value></value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>"""


def test_parse_form4_xml():
    rows = ef.parse_form4_xml(FORM4_XML, "NVDA", "1045810",
                              "0001045810-26-000001", "2026-10-08")
    assert len(rows) == 2                       # 0 股行被跳过
    r0, r1 = rows
    assert r0["reporter"] == "JENSEN HUANG"
    assert r0["title"] == "President and CEO"   # officer 头衔优先于 Director
    assert r0["action"] == "A" and r0["shares"] == 141000
    assert r0["price"] == 135.0 and r0["value_usd"] == 19035000.0
    assert r0["tx_code"] == "P" and r0["trade_date"] == "2026-10-07"
    assert r0["filed_date"] == "2026-10-08" and r0["cik"] == "1045810"
    assert r1["action"] == "D" and r1["value_usd"] == 2730000.0
    assert ef.parse_form4_xml("<not-xml", "X", "1", "a", "d") == []
    # 无非衍生品表 → 空列表
    assert ef.parse_form4_xml(
        "<ownershipDocument><documentType>4</documentType></ownershipDocument>",
        "X", "1", "a", "d") == []


def test_build_insider_summary():
    today = "2026-10-11"
    rows = [
        {"filed_date": date(2026, 10, 1), "action": "A", "shares": 141000,
         "value_usd": 19035000.0},
        {"filed_date": date(2026, 9, 20), "action": "D", "shares": 5000,
         "value_usd": 700000.0},
        {"filed_date": date(2026, 1, 1), "action": "A", "shares": 999999,
         "value_usd": 9999999.0},                # 90 天窗口外剔除
        {"filed_date": date(2026, 8, 1), "action": "A", "shares": 1000,
         "value_usd": None},                     # 金额缺失按 0 计
    ]
    s = ef.build_insider_summary(rows, today)
    assert s["net_shares"] == 141000 - 5000 + 1000
    assert s["net_value_usd"] == 19035000.0 - 700000.0
    assert s["buys"] == 2 and s["sells"] == 1 and s["tx_count"] == 3
    assert s["since"] == "2026-07-13"
    assert ef.build_insider_summary([], today) is None


def test_form4_url_strips_xsl_prefix():
    assert ef.form4_url(1045810, "0001045810-26-000001",
                        "xslF345X05/primary_doc.xml") == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000001/primary_doc.xml"
    assert ef.form4_url(1045810, "0001045810-26-000001",
                        "primary_doc.xml") == \
        "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000001/primary_doc.xml"
    assert ef.form4_url(1045810, "0001045810-26-000001",
                        None).endswith("/primary_doc.xml")            # None 防御兜底


# ---------------------------------------------------------------------------
# 网络层：节流/退避/UA（monkeypatch httpx + 冻结时钟，零真实网络）
# ---------------------------------------------------------------------------
import asyncio

import httpx


class _FakeResp:
    def __init__(self, code, text="ok"):
        self.status_code = code
        self.text = text


def _patch_client(monkeypatch, responder):
    calls = []

    class FakeClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            calls.append(url)
            return responder(url)

    monkeypatch.setattr(ef.httpx, "AsyncClient", FakeClient)
    return calls


def test_edgar_get_throttle_and_backoff(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(round(s, 1))

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)   # 时钟冻结
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)             # 上次请求恰在当前时刻
    codes = iter([429, 429, 200])
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(next(codes)))
    resp = asyncio.run(ef._edgar_get("https://x", "UA"))
    assert resp.status_code == 200
    assert len(calls) == 3
    # 冻结时钟下每次 gap=2.0；退避序列 2.0/4.0
    assert sleeps == [2.0, 2.0, 2.0, 4.0, 2.0]


def test_edgar_get_503_exhausted(monkeypatch):
    async def fake_sleep(s):
        pass

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(503))
    assert asyncio.run(ef._edgar_get("https://x", "UA")) is None
    assert len(calls) == 4                       # 首试 + 3 次退避重试


def test_edgar_get_network_error_retries_once(monkeypatch):
    async def fake_sleep(s):
        pass

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(ef, "_last_req_ts", 100.0)
    calls = []

    class FlakyClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            calls.append(url)
            raise httpx.ConnectError("boom")

    monkeypatch.setattr(ef.httpx, "AsyncClient", FlakyClient)
    assert asyncio.run(ef._edgar_get("https://x", "UA")) is None
    assert len(calls) == 2                       # 网络异常仅重试 1 次


def test_resolve_ua_from_config(monkeypatch):
    import database as db

    async def fake_cfg(key):
        return "QDII-Portfolio admin@example.com" if key == "EDGAR_UA" else None

    monkeypatch.setattr(db, "get_config", fake_cfg)
    assert asyncio.run(ef._resolve_ua()) == "QDII-Portfolio admin@example.com"

    async def fake_cfg_empty(key):
        return None

    monkeypatch.setattr(db, "get_config", fake_cfg_empty)
    assert asyncio.run(ef._resolve_ua()) == ef.DEFAULT_UA


def test_get_ticker_map_cache_and_refresh(monkeypatch):
    import database as db

    monkeypatch.setattr(ef, "_TICKER_MAP", None)   # 冷启动起点，测试自包含
    cfg = {"EDGAR_TICKER_MAP_TS": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    set_calls = []

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        set_calls.append((key, value))

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))

    # 冷启动（内存空）→ 拉一次并落时间戳
    _patch_client(monkeypatch, lambda url: _FakeResp(
        200, json.dumps({"0": {"cik_str": 1, "ticker": "MU", "title": "x"}})))
    m1 = asyncio.run(ef.get_ticker_map())
    assert m1 == {"MU": 1}
    assert set_calls and set_calls[0][0] == "EDGAR_TICKER_MAP_TS"
    # 内存已有且时间戳新鲜 → 不再发请求
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(500))
    assert asyncio.run(ef.get_ticker_map()) == {"MU": 1}
    assert calls == []


def _async_ret(v):
    async def _f(*a, **kw):
        return v
    return _f


def test_edgar_get_no_wait_when_idle(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(round(s, 1))

    monkeypatch.setattr(ef.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ef.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(ef, "_last_req_ts", 50.0)              # 距上次已 50s
    calls = _patch_client(monkeypatch, lambda url: _FakeResp(200))
    resp = asyncio.run(ef._edgar_get("https://x", "UA"))
    assert resp.status_code == 200
    assert calls == ["https://x"]
    assert sleeps == []                                        # gap≤0 不等待


def test_get_ticker_map_bad_payload_not_poisoned(monkeypatch):
    import database as db

    monkeypatch.setattr(ef, "_TICKER_MAP", None)   # 冷启动起点，测试自包含
    set_calls = []

    async def fake_cfg(key):
        return None

    async def fake_set(key, value, remark=""):
        set_calls.append((key, value))

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))
    # 200 + 坏 JSON → 空解析不落缓存不落时间戳（防 7 天毒化），返回 None
    _patch_client(monkeypatch, lambda url: _FakeResp(200, "not-json"))
    assert asyncio.run(ef.get_ticker_map()) is None
    assert set_calls == []
    # 旧缓存 + 时间戳过期 + 拉取 500 → 旧缓存兜底返回
    monkeypatch.setattr(ef, "_TICKER_MAP", {"MU": 1})

    async def fake_cfg_stale(key):
        return "2020-01-01 00:00:00" if key == "EDGAR_TICKER_MAP_TS" else None

    monkeypatch.setattr(db, "get_config", fake_cfg_stale)
    _patch_client(monkeypatch, lambda url: _FakeResp(500))
    assert asyncio.run(ef.get_ticker_map()) == {"MU": 1}
