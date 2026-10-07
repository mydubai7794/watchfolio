"""macro_fetcher 解析纯逻辑测试（fixture 字符串，零网络）。@author yys"""
import macro_fetcher as mf


def test_parse_fred_csv_latest_skipping_dots():
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,1.5\n") == \
        ("2026-10-06", 1.5)
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n2026-10-06,\n") is None
    assert mf.parse_fred_csv("DATE,X\n2026-10-05,.\n") is None


def test_parse_fred_csv_header_only():
    assert mf.parse_fred_csv("DATE,WILL5000PRFC\n") is None


def test_parse_multpl_cape():
    t = '[{"date":"2026-10-06","value":"28.14"},{"date":"2026-09-29","value":"27.9"}]'
    assert mf.parse_multpl_cape(t) == ("2026-10-06", 28.14)
    assert mf.parse_multpl_cape("[]") is None
    assert mf.parse_multpl_cape("<html>oops") is None


def test_parse_cnn_fg():
    t = '{"fear_greed":{"score":38.5,"rating":"Fear","previous":40}}'
    assert mf.parse_cnn_fg(t) == (38.5, "Fear")
    assert mf.parse_cnn_fg("{}") is None
    assert mf.parse_cnn_fg("not json") is None


def test_fetchers_registry():
    assert {f.__name__ for f in mf.FETCHERS} == {
        "fetch_buffett", "fetch_cape", "fetch_yield_spread", "fetch_fear_greed"}
