"""全球指数（腾讯 gz 前缀国际指数）解析与刷新节流测试。

背景：2026-10-03 期指主备反转后新浪冷备被旁路，全球指数缓存无写入点；
东财 push2 ulist 实装后发现腾讯云服务器 IP 被全端点风控，最终切换到
qt.gtimg.cn gz 前缀（gzN225/gzKS11），走 tencent_bus 攒批与 A 股同请求，
60s 节流（A 股快刷窗口内不跟随 5s 候选轮）。
"""

import asyncio

import pytest

import fetcher

SAMPLE_GZ = (
    'v_gzN225="N225~日经225指数~2026-10-08 09:30:55~69385.99~-649.72~-0.93~AP~";'
    'v_gzKS11="KS11~韩国首尔综合~2026-10-08 09:30:50~6763.45~-40.45~-0.59~AP~";'
)


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    monkeypatch.setattr(fetcher, "_last_global_index_cache", {})
    monkeypatch.setattr(fetcher, "_global_index_fetch_at", 0.0)


def test_parse_tencent_gz_indices():
    fetcher._parse_and_cache_tencent_global_indices(SAMPLE_GZ, ["N225", "KS11"])
    n = fetcher._last_global_index_cache["N225"]
    assert n["price"] == pytest.approx(69385.99)
    assert n["pre_close"] == pytest.approx(70035.71)
    assert n["change_pct"] == pytest.approx(-0.93)
    assert n["name"] == "日经225指数"
    k = fetcher._last_global_index_cache["KS11"]
    assert k["price"] == pytest.approx(6763.45)
    assert k["change_pct"] == pytest.approx(-0.59)


def test_parse_skips_truncated_rows():
    bad = 'v_gzN225="N225~日经225指数~2026-10-08~69385.99~";'
    fetcher._parse_and_cache_tencent_global_indices(bad, ["N225"])
    assert "N225" not in fetcher._last_global_index_cache


def test_fetch_refreshes_via_tencent_bus_when_stale():
    calls = []

    class FakeBus:
        async def get(self, codes):
            calls.append(list(codes))
            return SAMPLE_GZ

    fetcher.tencent_bus = FakeBus()
    out = asyncio.run(fetcher.fetch_global_indices(["N225", "KS11"]))
    assert calls == [["gzN225", "gzKS11"]]
    assert [q["code"] for q in out] == ["N225", "KS11"]
    assert out[0]["price"] == pytest.approx(69385.99)


def test_fetch_throttled_within_window():
    calls = []

    class FakeBus:
        async def get(self, codes):
            calls.append(1)
            return SAMPLE_GZ

    fetcher.tencent_bus = FakeBus()
    asyncio.run(fetcher.fetch_global_indices(["N225", "KS11"]))
    asyncio.run(fetcher.fetch_global_indices(["N225", "KS11"]))
    asyncio.run(fetcher.fetch_global_indices(["KS11"]))
    assert len(calls) == 1


def test_bus_failure_returns_empty_without_raise():
    class FakeBus:
        async def get(self, codes):
            return ""

    fetcher.tencent_bus = FakeBus()
    out = asyncio.run(fetcher.fetch_global_indices(["N225"]))
    assert out == []
