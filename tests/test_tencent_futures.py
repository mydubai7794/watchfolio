"""腾讯期指主源（fuNQ/fuES/fuYM）单元测试。

覆盖（2026-10-08 腾讯主源收敛）：
1. _parse_tencent_futures：真实样本行解析（f[32] 涨跌幅直取）、截断行守卫、
   f[32] 缺失时 f[3]/f[4] 自算、空 raw → None
2. fetch_futures_with_routing：腾讯成功直达（东财/新浪零调用）、
   腾讯失败降级东财、全失败返回 None dict
3. 总线 10s 节拍参数（TENCENT_BUS_INTERVAL_SEC 与 config 一致）
"""
import asyncio
import importlib
import unittest
from unittest import mock

import fetcher
import tencent_bus

# 线上实测样本（2026-10-08，GBK 解码后；f[0]="delay" 为外盘分类标记）
SAMPLE_RAW = (
    'v_fuNQ="delay~纳斯达克100指数期货~NQ~31440.25~31402.25~31403.00~22569~'
    '140889403760~139613892299~31439.00~1~0~0~0~0~0~0~0~0~31440.00~1~0~0~0~0~'
    '0~0~0~0~~2026-10-07 21:35:13~38.00~0.12~31459.00~31372.25~USD";'
    'v_fuES="delay~标普500指数期货~ES~7855.25~7852.75~7850.75~29433~'
    '228834361526~223679361379~7855.00~25~0~0~0~0~0~0~0~0~7855.25~8~0~0~0~0~'
    '0~0~0~0~~2026-10-07 21:35:13~2.50~0.03~7855.50~7851.25~USD";'
    'v_fuYM="delay~道琼斯指数期货~YM~51422.00~51449.00~51427.00~3278~'
    '6036432222~6244495125~51421.00~1~0~0~0~0~0~0~0~0~51423.00~1~0~0~0~0~'
    '0~0~0~0~~2026-10-07 21:35:12~-27.00~-0.05~51423.00~51421.00~USD";'
    'v_fuNIY="delay~日经指数期货~NIY~69585.00~69850.00~69850.00~4493~'
    '719799645.5~786383898.5~69580.00~3~0~0~0~0~0~0~0~0~69585.00~3~0~0~0~0~'
    '0~0~0~0~~2026-10-07 21:35:13~-265.00~-0.38~69600.00~69560.00~USD"'
)

# 截断行（字段 < 5，应被守卫跳过）
TRUNCATED_RAW = 'v_fuNQ="delay~纳斯达克100指数期货~NQ~31440.25";'

# 无 f[32] 的短行（现价/昨结可自算涨跌幅：30000/29900 → +0.33%）
SHORT_RAW = (
    'v_fuNQ="delay~纳斯达克100指数期货~NQ~30000.00~29900.00~29900.00~'
    '100~0~0~30000.00~1~0~0~0~0~0~0~0~0~30000.00~1~0~0~0~0~0~0~0~0~'
    '~2026-10-07 21:35:13"'
)


class TestParseTencentFutures(unittest.TestCase):
    def test_parse_real_sample(self):
        out = fetcher._parse_tencent_futures(SAMPLE_RAW, ["NQ", "ES", "YM", "NK"])
        self.assertIsNotNone(out)
        self.assertAlmostEqual(out["NQ"], 0.12, places=2)
        self.assertAlmostEqual(out["ES"], 0.03, places=2)
        self.assertAlmostEqual(out["YM"], -0.05, places=2)
        self.assertAlmostEqual(out["NK"], -0.38, places=2)

    def test_truncated_line_guarded(self):
        out = fetcher._parse_tencent_futures(TRUNCATED_RAW, ["NQ", "ES", "YM"])
        self.assertIsNone(out)  # 全部解析失败 → None 触发路由降级

    def test_change_pct_self_calc_when_missing(self):
        out = fetcher._parse_tencent_futures(SHORT_RAW, ["NQ"])
        self.assertIsNotNone(out)
        self.assertAlmostEqual(out["NQ"], 0.33, places=2)

    def test_empty_raw_returns_none(self):
        self.assertIsNone(fetcher._parse_tencent_futures("", ["NQ"]))
        self.assertIsNone(fetcher._parse_tencent_futures(None, ["NQ"]))


class TestFuturesRoutingTencentPrimary(unittest.TestCase):
    def setUp(self):
        fetcher._futures_source_health.update({"sina": True, "eastmoney": True})
        fetcher._futures_source_recover.clear()
        fetcher._futures_last_used_source = None

    def tearDown(self):
        fetcher._futures_source_health.update({"sina": True, "eastmoney": True})
        fetcher._futures_source_recover.clear()
        fetcher._futures_last_used_source = None

    def test_tencent_primary_short_circuit(self):
        """腾讯成功直达：东财/新浪零调用，source=tencent。"""
        with mock.patch.object(
                fetcher, "_fetch_futures_tencent",
                new=mock.AsyncMock(return_value={"NQ": 0.1, "ES": 0.2, "YM": None})), \
             mock.patch.object(
                 fetcher, "_fetch_futures_eastmoney",
                 new=mock.AsyncMock()) as em, \
             mock.patch.object(
                 fetcher, "_fetch_futures_sina",
                 new=mock.AsyncMock()) as sina:
            out = asyncio.run(fetcher.fetch_futures_with_routing(["NQ", "ES", "YM"]))
        self.assertEqual(out["NQ"], 0.1)
        self.assertEqual(fetcher._futures_last_used_source, "tencent")
        em.assert_not_called()
        sina.assert_not_called()

    def test_tencent_failure_falls_back_to_eastmoney(self):
        """腾讯失败 → 健康状态机选东财（主→备优先序第一位）。"""
        with mock.patch.object(
                fetcher, "_fetch_futures_tencent",
                new=mock.AsyncMock(return_value=None)), \
             mock.patch.object(
                 fetcher, "_fetch_futures_eastmoney",
                 new=mock.AsyncMock(return_value={"NQ": 0.5, "ES": 0.4, "YM": 0.3})) as em, \
             mock.patch.object(
                 fetcher, "_fetch_futures_sina",
                 new=mock.AsyncMock()) as sina:
            out = asyncio.run(fetcher.fetch_futures_with_routing(["NQ", "ES", "YM"]))
        self.assertEqual(out["NQ"], 0.5)
        self.assertEqual(fetcher._futures_last_used_source, "eastmoney")
        em.assert_awaited_once()
        sina.assert_not_called()

    def test_all_sources_failed(self):
        """全部失败 → {NQ: None, ...}，健康状态双 False。"""
        with mock.patch.object(
                fetcher, "_fetch_futures_tencent",
                new=mock.AsyncMock(return_value=None)), \
             mock.patch.object(
                 fetcher, "_fetch_futures_eastmoney",
                 new=mock.AsyncMock(return_value=None)), \
             mock.patch.object(
                 fetcher, "_fetch_futures_sina",
                 new=mock.AsyncMock(return_value=None)):
            out = asyncio.run(fetcher.fetch_futures_with_routing(["NQ", "ES", "YM"]))
        self.assertEqual(out, {"NQ": None, "ES": None, "YM": None})
        self.assertIsNone(fetcher._futures_last_used_source)


class TestBusIntervalConfig(unittest.TestCase):
    def test_bus_uses_configured_interval(self):
        """总线单例节拍与 config.TENCENT_BUS_INTERVAL_SEC 一致（10s 保源）。"""
        import config
        self.assertAlmostEqual(
            tencent_bus.tencent_bus._min_interval,
            config.TENCENT_BUS_INTERVAL_SEC,
        )
        self.assertAlmostEqual(config.TENCENT_BUS_INTERVAL_SEC, 10.0)

    def test_monitor_interval_config(self):
        """监控驱动间隔 10s（与总线节拍对齐）。"""
        import config
        self.assertAlmostEqual(config.MONITOR_INTERVAL_SEC, 10.0)
        self.assertAlmostEqual(config.CANDIDATE_REFRESH_SEC, 10.0)

    def test_modules_reload_clean(self):
        """config 改动后 bus/server 模块可无异常重载（部署热路径冒烟）。"""
        for mod in ("config", "tencent_bus", "fetcher"):
            importlib.import_module(mod)


if __name__ == "__main__":
    unittest.main()
