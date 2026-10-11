"""edgar_fetcher 纯函数层测试（fixture 字符串，零网络）。@author yys"""
import json
from datetime import date, datetime, timedelta

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
        "form": ["4", "10-Q", "4"],
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


def test_parse_submissions_recent_real_sec_schema():
    """回归：SEC 真实键名为 form（2026-10-11 线上首轮全 skip 的根因，
    fixture 曾以 formType 自证通过）；错误键名解析必须为空以防再犯。"""
    body = json.dumps({"filings": {"recent": {
        "form": ["10-Q", "4"],
        "filingDate": ["2026-10-05", "2026-10-08"],
        "accessionNumber": ["a-1", "a-2"],
        "primaryDocument": ["d1.xml", "d2.xml"],
    }}})
    rows = ef.parse_submissions_recent(body)
    assert [r["form"] for r in rows] == ["10-Q", "4"]
    assert ef.parse_submissions_recent(json.dumps(
        {"filings": {"recent": {"formType": ["4"],
                                "filingDate": ["2026-10-08"],
                                "accessionNumber": ["a-1"]}}})) == []


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
        {"filed_date": date(2026, 10, 1), "action": "A", "tx_code": "P",
         "shares": 141000, "value_usd": 19035000.0},      # 公开市场买
        {"filed_date": date(2026, 9, 20), "action": "D", "tx_code": "S",
         "shares": 5000, "value_usd": 700000.0},          # 公开市场卖
        {"filed_date": date(2026, 9, 15), "action": "A", "tx_code": "A",
         "shares": 30000, "value_usd": 8000000.0},        # RSU 授予：只计 tx_count
        {"filed_date": date(2026, 9, 17), "action": "D", "tx_code": "G",
         "shares": 438000, "value_usd": 99000000.0},      # 赠与：只计 tx_count
        {"filed_date": date(2026, 9, 17), "action": "D", "tx_code": "F",
         "shares": 45728, "value_usd": 10000000.0},       # 税务代扣：只计 tx_count
        {"filed_date": date(2026, 1, 1), "action": "A", "tx_code": "P",
         "shares": 999999, "value_usd": 9999999.0},       # 90 天窗口外剔除
        {"filed_date": date(2026, 8, 1), "action": "A", "tx_code": "P",
         "shares": 1000, "value_usd": None},              # 金额缺失按 0 计
    ]
    s = ef.build_insider_summary(rows, today)
    # 只有 P/S 计净额（授予/赠与/代扣 2026-10-11 对账修正前误计）
    assert s["net_shares"] == 141000 - 5000 + 1000
    assert s["net_value_usd"] == 19035000.0 - 700000.0
    assert s["buys"] == 2 and s["sells"] == 1
    assert s["tx_count"] == 6                 # 窗口内全部申报（含 A/G/F）
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


# ---------------------------------------------------------------------------
# 每日轮：探测分类 + Form 4 增量 + 水位（全部打桩零网络）
# ---------------------------------------------------------------------------


def _round_env(monkeypatch, holdings, forms_by_cik, form4_texts, watermark=None):
    """打桩 db 与网络层，返回记录器。forms_by_cik: {cik: submissions_json文本}。"""
    import database as db

    cfg = {"EDGAR_WATERMARK": watermark,
           "EDGAR_TICKER_MAP_TS": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    cfg = {k: v for k, v in cfg.items() if v is not None}
    rec = {"classified": [], "saved": [], "set_cfg": []}

    async def fake_cfg(key):
        return cfg.get(key)

    async def fake_set(key, value, remark=""):
        cfg[key] = value
        rec["set_cfg"].append((key, value))

    async def fake_holdings(enabled_only=True):
        return holdings

    async def fake_classify(symbol, at):
        rec["classified"].append((symbol, at))

    async def fake_save(rows):
        rec["saved"].extend(rows)
        return len(rows)

    monkeypatch.setattr(db, "get_config", fake_cfg)
    monkeypatch.setattr(db, "set_config", fake_set)
    monkeypatch.setattr(db, "get_holdings", fake_holdings)
    monkeypatch.setattr(db, "set_holding_asset_type", fake_classify)
    monkeypatch.setattr(db, "save_insider_tx", fake_save)
    monkeypatch.setattr(db, "get_insider_accessions", _async_ret(set()))
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))
    monkeypatch.setattr(ef, "_TICKER_MAP", {"MU": 723125, "BABA": 1577552,
                                            "SMH": 1234567})
    fetched = []

    async def fake_sub(cik, ua):
        return forms_by_cik.get(cik)

    async def fake_form4(cik, accession, doc, ua):
        fetched.append((cik, accession))
        return form4_texts.get(accession)

    monkeypatch.setattr(ef, "fetch_submissions", fake_sub)
    monkeypatch.setattr(ef, "fetch_form4_text", fake_form4)
    return rec, cfg, fetched


def _subs_json(cik, forms):
    return json.dumps({"cik": cik, "filings": {"recent": {
        "accessionNumber": [f[2] for f in forms],
        "filingDate": [f[1] for f in forms],
        "form": [f[0] for f in forms],
        "primaryDocument": ["primary_doc.xml"] * len(forms),
    }}})


def test_run_edgar_round_classify_and_poll(monkeypatch):
    forms_mu = _subs_json(723125, [
        ("10-Q", "2026-10-05", "0000723125-26-000002"),
        ("4", "2026-10-08", "0000723125-26-000001"),
    ])
    forms_baba = _subs_json(1577552, [("20-F", "2026-06-30", "x-baba-1")])
    forms_smh = _subs_json(1234567, [("N-CSR", "2026-09-30", "x-smh-1")])
    holdings = [
        {"symbol": "MU", "market": "US", "asset_type": None},      # 待探测→stock
        {"symbol": "BABA", "market": "US", "asset_type": None},    # 待探测→fpi
        {"symbol": "SMH", "market": "US", "asset_type": None},     # 待探测→etf
        {"symbol": "HK0000740297", "market": "HKFUND", "asset_type": None},
    ]
    form4 = {"0000723125-26-000001": FORM4_XML}
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings,
        {723125: forms_mu, 1577552: forms_baba, 1234567: forms_smh},
        form4, watermark="2026-10-01")
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    assert rec["classified"] == [("MU", "stock"), ("BABA", "fpi"), ("SMH", "etf")]
    # 只有 stock 拉 Form 4；fpi/etf 零 Form 4 请求
    assert fetched == [(723125, "0000723125-26-000001")]
    assert len(rec["saved"]) == 2                  # FORM4_XML 两笔有效交易
    # 水位推进到已处理最大 filingDate 再回撤 3 天（安全边际）
    assert dict(rec["set_cfg"])["EDGAR_WATERMARK"] == "2026-10-05"
    assert any(k == "EDGAR_LAST_OK" for k, _ in rec["set_cfg"])


def test_run_edgar_round_excludes_same_name_symbol(monkeypatch):
    """F（射频识别业务代号）严禁映射到 SEC 同名汽车类发行人：排除标记
    none，第二轮不再重写；其余标的正常轮询。"""
    forms_mu = _subs_json(723125, [("10-Q", "2026-10-05", "0000723125-26-000002"),
                                   ("4", "2026-10-08", "0000723125-26-000001")])
    holdings = [
        {"symbol": "F", "market": "US", "asset_type": None},
        {"symbol": "MU", "market": "US", "asset_type": "stock"},
    ]
    form4 = {"0000723125-26-000001": FORM4_XML}
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, form4,
        watermark="2026-10-01")
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    assert rec["classified"] == [("F", "none")]   # 排除标记，不进 stats
    assert fetched == [(723125, "0000723125-26-000001")]  # MU 正常拉 Form 4
    # 第二轮（F 已 none）不再重写、不发请求
    rec["classified"].clear()
    holdings[0]["asset_type"] = "none"
    asyncio.run(ef.run_edgar_round())
    assert rec["classified"] == []


def test_run_edgar_round_cik_override_for_etf(monkeypatch):
    """QQQ 不在 company_tickers 映射时经 _CIK_OVERRIDES 解析并分类 etf。"""
    forms = _subs_json(1067839, [("NPORT-P", "2026-10-01", "x-qqq-1")])
    holdings = [{"symbol": "QQQ", "market": "US", "asset_type": None}]
    rec, cfg, fetched = _round_env(monkeypatch, holdings, {1067839: forms},
                                   {}, watermark="2026-10-01")
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    assert rec["classified"] == [("QQQ", "etf")]
    assert fetched == []                          # etf 不拉 Form 4


def test_run_edgar_round_form4_fetch_fail_keeps_watermark(monkeypatch):
    forms_mu = _subs_json(723125, [("10-Q", "2026-10-05", "0000723125-26-000002"),
                                   ("4", "2026-10-08", "0000723125-26-000001")])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {},
        watermark="2026-10-01")                    # form4_texts 空 → XML 拉取失败
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True and r["new_tx"] == 0
    # 拉取失败的申报不推水位 → 次日重试
    assert not any(k == "EDGAR_WATERMARK" for k, _ in rec["set_cfg"])


def test_run_edgar_round_first_round_backfill_90d(monkeypatch):
    recent = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")
    forms_mu = _subs_json(723125, [
        ("4", recent, "0000723125-26-000001"),
        ("4", old, "0000723125-26-000000"),           # 90 天外：since 之前不拉
    ])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {"0000723125-26-000001": FORM4_XML},
        watermark=None)                            # 无水位 → 首轮回看 90 天
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True
    # 只有 recent（≥ today-90）被处理；old 在窗口外不拉（动态日期防时间炸弹）
    assert fetched == [(723125, "0000723125-26-000001")]


def test_run_edgar_round_watermark_rollback_replays_failed_symbol(monkeypatch):
    """全局水位回撤边际：B 当轮 submissions 失败被跳过、A 推进水位（10-08 回撤
    至 10-05），B 次日恢复后 10-06 申报仍落在重放窗口内被补拉，零漏申报。"""
    import database as db

    forms_mu = _subs_json(723125, [("4", "2026-10-08", "0000723125-26-000001")])
    forms_baba = _subs_json(1577552, [("4", "2026-10-06", "x-baba-f4-1")])
    holdings = [
        {"symbol": "MU", "market": "US", "asset_type": "stock"},
        {"symbol": "BABA", "market": "US", "asset_type": "stock"},
    ]
    form4 = {"0000723125-26-000001": FORM4_XML, "x-baba-f4-1": FORM4_XML}
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu, 1577552: forms_baba},
        form4, watermark="2026-10-01")

    # 已入库 accession 集合随 save 累积（模拟真实 DB 幂等）
    async def fake_accessions(sym, days=120):
        return {r["accession"] for r in rec["saved"] if r["symbol"] == sym}

    monkeypatch.setattr(db, "get_insider_accessions", fake_accessions)

    async def fail_baba(cik, ua):
        return None if cik == 1577552 else forms_mu

    monkeypatch.setattr(ef, "fetch_submissions", fail_baba)
    r1 = asyncio.run(ef.run_edgar_round())
    assert r1["ok"] is True and r1["skipped"] == 1   # BABA 当轮失败跳过
    assert fetched == [(723125, "0000723125-26-000001")]
    assert dict(rec["set_cfg"])["EDGAR_WATERMARK"] == "2026-10-05"  # 10-08 回撤 3 天

    async def ok_all(cik, ua):
        return {723125: forms_mu, 1577552: forms_baba}[cik]

    monkeypatch.setattr(ef, "fetch_submissions", ok_all)
    r2 = asyncio.run(ef.run_edgar_round())
    assert r2["ok"] is True and r2["new_tx"] == 2    # BABA 10-06 补拉成功
    assert fetched == [(723125, "0000723125-26-000001"),
                       (1577552, "x-baba-f4-1")]     # MU 已入库不再拉


def test_run_edgar_round_skips_processed_accession(monkeypatch):
    """已入库 accession（uk_tx 对 NULL price 不去重的补偿）不重复拉取/插入。"""
    import database as db

    forms_mu = _subs_json(723125, [("10-Q", "2026-10-05", "0000723125-26-000002"),
                                   ("4", "2026-10-08", "0000723125-26-000001")])
    holdings = [{"symbol": "MU", "market": "US", "asset_type": "stock"}]
    rec, cfg, fetched = _round_env(
        monkeypatch, holdings, {723125: forms_mu}, {"0000723125-26-000001": FORM4_XML},
        watermark="2026-10-01")

    async def fake_accessions(sym, days=120):
        return {"0000723125-26-000001"}            # 该申报已入库

    monkeypatch.setattr(db, "get_insider_accessions", fake_accessions)
    r = asyncio.run(ef.run_edgar_round())
    assert r["ok"] is True and r["new_tx"] == 0
    assert fetched == []                           # 不再拉 XML
    assert rec["saved"] == []                      # 不再插入


def test_fetch_companyconcept_url_padding(monkeypatch):
    """companyconcept URL 10 位补零 CIK + 默认概念名；穷尽失败 None 透传。"""
    captured = []

    async def fake_get(url, ua):
        captured.append(url)
        return _FakeResp(200, "{}")

    monkeypatch.setattr(ef, "_edgar_get", fake_get)
    monkeypatch.setattr(ef, "_resolve_ua", _async_ret("UA"))
    r = asyncio.run(ef.fetch_companyconcept(1045810))
    assert r == "{}"
    assert captured == [
        "https://data.sec.gov/api/xbrl/companyconcept/"
        "CIK0001045810/us-gaap/EarningsPerShareDiluted.json"]

    async def fake_none(url, ua):
        return None
    monkeypatch.setattr(ef, "_edgar_get", fake_none)
    assert asyncio.run(ef.fetch_companyconcept(2120882)) is None
