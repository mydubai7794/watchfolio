"""edgar_fetcher 纯函数层测试（fixture 字符串，零网络）。@author yys"""
import json
from datetime import date

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
