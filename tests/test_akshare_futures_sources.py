"""测试 AKShare 中可用的全球股指期货数据源。

目的：
1. 验证东方财富数据源能获取哪些股指期货
2. 验证新浪数据源支持哪些股指期货
3. 测试通过代理请求的可行性
4. 为后续替换现有 fetcher.py 做准备

期指目标：
- NQ00Y: 纳斯达克100期货
- ES00Y: 标普500期货
- YM00Y: 道琼斯期货
- DAX: 德国DAX期货
- NK/NK225: 日经225期货
- CN00Y: 富时中国A50
"""

import time
from typing import Dict, List, Optional

import akshare as ak
import pandas as pd
import requests


# ===========================================================================
# 常量
# ===========================================================================

# 代理配置
PROXY_URL = "http://*REMOVED**REMOVED*:7779"

# 3秒间隔（与项目 MIN_INTERVAL_SEC 一致）
MIN_INTERVAL = 3.0
_last_request: float = 0.0


def _rate_limit():
    """简易限速：确保两次请求之间 >= MIN_INTERVAL。"""
    global _last_request
    elapsed = time.monotonic() - _last_request
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request = time.monotonic()


# ===========================================================================
# 东方财富数据源测试
# ===========================================================================

class TestEastmoneyGlobalFutures:
    """测试东方财富全球期货数据源。

    API: ak.futures_global_spot_em()
    数据源: quote.eastmoney.com (纯HTTP)
    """

    df: Optional[object] = None
    target_symbols = ["NQ00Y", "ES00Y", "YM00Y", "CN00Y", "DAX", "NK225"]

    @classmethod
    def setup_class(cls):
        """获取完整的全球期货列表。"""
        print("\n===== 东方财富全球期货数据源 =====")
        print("API: futures_global_spot_em()")
        print("数据源: quote.eastmoney.com")
        print()

        _rate_limit()
        cls.df = ak.futures_global_spot_em()

        print(f"总数据量: {len(cls.df)} 行")
        print(f"列: {list(cls.df.columns)}")
        print()

        # 打印前10行供参考
        print("前10行数据预览:")
        print(cls.df.head(10)[['代码', '名称', '最新价', '涨跌幅', '昨结']].to_string())
        print()

    def test_target_symbols_availability(self):
        """测试目标期指代码是否可用。"""
        print("\n--- 目标期指可用性检查 ---")

        for symbol in self.target_symbols:
            matches = self.df[self.df['代码'] == symbol]
            if not matches.empty:
                row = matches.iloc[0]
                price = row['最新价']
                price_str = f"{price:.2f}" if price and pd.notna(price) else "N/A"
                change = row['涨跌幅']
                change_str = f"{change:.2f}" if pd.notna(change) else "N/A"
                print(f"✓ {symbol:8s} | {row['名称']:20s} | "
                      f"最新价: {price_str:>10s} | "
                      f"涨跌幅: {change_str:>6s}%")
            else:
                # 尝试模糊匹配
                fuzzy = self.df[self.df['代码'].str.contains(symbol, na=False) |
                                self.df['名称'].str.contains(symbol, na=False)]
                if not fuzzy.empty:
                    print(f"? {symbol:8s} | 未精确匹配，但找到相关:")
                    for _, frow in fuzzy.head(3).iterrows():
                        print(f"    - {frow['代码']}: {frow['名称']}")
                else:
                    print(f"✗ {symbol:8s} | 不可用")

    def test_list_all_equity_index_futures(self):
        """列出所有可用的股指期货。"""
        print("\n--- 所有可用的股指/指数期货 ---")

        # 搜索名称包含"指"、"指数"、"纳指"、"标普"、"道指"等关键词
        keywords = ["指", "指数", "纳指", "标普", "道指", "A50", "恒生", "H股", "红筹"]
        pattern = "|".join(keywords)

        equity_futures = self.df[
            self.df['名称'].str.contains(pattern, na=False)
        ].copy()

        if equity_futures.empty:
            print("未找到股指期货")
        else:
            print(f"找到 {len(equity_futures)} 个股指期货:")
            print(equity_futures[['代码', '名称', '最新价', '涨跌幅', '昨结']].to_string())

    def test_eastmoney_http_direct(self):
        """测试直接HTTP请求东方财富接口（不经过akshare）。"""
        print("\n--- 测试直连东方财富HTTP接口 ---")

        # 东方财富期货列表接口
        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "50",
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0+t:6,f:!2,m:0+f:!2,m:1+t:6,f:!2,m:1+f:!2",  # 国际期货
            "fields": "f12,f13,f14,f2,f3,f4,f5,f6,f60,f62",
        }

        _rate_limit()
        resp = requests.get(url, params=params, timeout=10)
        print(f"状态码: {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
            if data.get("data"):
                items = data["data"].get("diff", [])
                print(f"返回数据量: {len(items)}")

                # 查找目标期指
                for item in items[:20]:
                    code = item.get("f12", "")
                    name = item.get("f14", "")
                    price = item.get("f2", 0)
                    change_pct = item.get("f3", 0)

                    # 简单过滤只显示期指
                    if any(k in name for k in ["指", "NQ", "ES", "YM", "A50"]):
                        print(f"  {code:8s} | {name:20s} | {price:>10.2f} | {change_pct:>6.2f}%")

    def test_eastmoney_with_proxy(self):
        """测试通过代理请求东方财富数据。"""
        print(f"\n--- 测试通过代理请求东方财富 ---")
        print(f"代理: {PROXY_URL.replace('*REMOVED*@', '***@')}")

        url = "http://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "20",
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "m:0+t:6,f:!2,m:0+f:!2,m:1+t:6,f:!2,m:1+f:!2",
            "fields": "f12,f13,f14,f2,f3,f4",
        }

        _rate_limit()
        try:
            resp = requests.get(
                url,
                params=params,
                proxies={"http": PROXY_URL, "https": PROXY_URL},
                timeout=10
            )
            print(f"状态码: {resp.status_code}")

            if resp.status_code == 200:
                data = resp.json()
                if data.get("data"):
                    items = data["data"].get("diff", [])
                    print(f"✓ 通过代理成功获取 {len(items)} 条数据")
            else:
                print(f"✗ 代理请求失败")
        except Exception as e:
            print(f"✗ 代理请求异常: {e}")


# ===========================================================================
# 新浪数据源测试
# ===========================================================================

class TestSinaGlobalFutures:
    """测试新浪外盘期货数据源。

    API: ak.futures_foreign_commodity_realtime()
    数据源: finance.sina.com.cn (纯HTTP)

    注意：此API主要针对商品期货，但尝试测试股指期货。
    """

    target_symbols = ["NQ", "ES", "YM", "DAX", "NK", "CN"]

    def test_sina_index_futures_directly(self):
        """直接测试新浪的 hf_ 接口（与现有fetcher.py相同的逻辑）。"""
        print("\n===== 新浪外盘期货数据源 =====")
        print("API: futures_foreign_commodity_realtime() / 直接HTTP")
        print("数据源: finance.sina.com.cn")
        print()

        # 使用与现有 fetcher.py 相同的 URL 格式
        symbols_to_test = ["hf_NQ", "hf_ES", "hf_YM", "hf_DAX", "hf_NK", "hf_CN"]
        url = "http://hq.sinajs.cn/list=" + ",".join(symbols_to_test)

        _rate_limit()
        resp = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=10)
        resp.encoding = "gbk"

        print(f"状态码: {resp.status_code}")
        print()
        print("--- 原始响应 ---")
        print(resp.text)
        print()

        # 解析响应
        import re
        for sym in symbols_to_test:
            match = re.search(rf'var hq_str_{sym}="([^"]*)"', resp.text)
            if match:
                value = match.group(1)
                if value:
                    fields = value.split(",")
                    print(f"{sym}: "
                          f"最新价={fields[0]}, 昨结算={fields[7] if len(fields) > 7 else 'N/A'}, "
                          f"名称={fields[13] if len(fields) > 13 else 'N/A'}")
                else:
                    print(f"{sym}: 返回值为空")
            else:
                print(f"{sym}: 未匹配到数据")

    def test_sina_with_proxy(self):
        """测试通过代理请求新浪数据。"""
        print(f"\n--- 测试通过代理请求新浪 ---")
        print(f"代理: {PROXY_URL.replace('*REMOVED*@', '***@')}")

        symbols_to_test = ["hf_NQ", "hf_ES", "hf_YM", "hf_DAX"]
        url = "http://hq.sinajs.cn/list=" + ",".join(symbols_to_test)

        _rate_limit()
        try:
            resp = requests.get(
                url,
                headers={"Referer": "http://finance.sina.com.cn"},
                proxies={"http": PROXY_URL, "https": PROXY_URL},
                timeout=10
            )
            resp.encoding = "gbk"

            print(f"状态码: {resp.status_code}")
            if resp.status_code == 200:
                import re
                for sym in symbols_to_test:
                    match = re.search(rf'var hq_str_{sym}="([^"]*)"', resp.text)
                    if match and match.group(1):
                        fields = match.group(1).split(",")
                        print(f"✓ {sym}: 最新价={fields[0]}")
                    else:
                        print(f"? {sym}: 未获取到数据")
        except Exception as e:
            print(f"✗ 代理请求异常: {e}")


# ===========================================================================
# 数据源对比总结
# ===========================================================================

def _summarize_findings():
    """打印数据源对比总结。"""
    print("\n" + "="*60)
    print("数据源对比总结")
    print("="*60)

    print("""
┌─────────────────┬────────────────────────────────────────────┐
│ 数据源          │ 覆盖期指                                   │
├─────────────────┼────────────────────────────────────────────┤
│ 东方财富        │ NQ00Y ✓, ES00Y ✓, YM00Y ✓, CN00Y ✓        │
│ futures_global  │ DAX ✗, NK225 ✗                            │
│ _spot_em()      │ 纯HTTP，直连可用，返回620+品种            │
├─────────────────┼────────────────────────────────────────────┤
│ 新浪 hf_        │ NQ ✓, ES ✓, YM ✓, DAX ✓                  │
│ futures_foreign │ NK ✗ (返回数据为空)                        │
│ _commodity      │ CN ? (A50可能需要其他代码)                │
│ _realtime()     │ 纯HTTP，直连可用，支持代理               │
└─────────────────┴────────────────────────────────────────────┘

建议方案：
1. 美股三大期指（NQ/ES/YM）: 使用东方财富源，数据更全
2. DAX: 使用新浪源（现有代码已支持）
3. NK225: 新浪源 hf_NK 无数据，需要寻找其他数据源

其他可能的数据源：
- 腾讯期指接口 (qt.gtimg.cn)
- 英为财情 (investing.com) - 需要爬虫
- 新交所 (SGX) 官方数据
- 大阪交易所 (OSE) 官方数据
""")


# ===========================================================================
# 探索其他可能的数据源
# ===========================================================================

class TestAlternativeSources:
    """测试其他可能的股指期货数据源。"""

    def test_tencent_futures_source(self):
        """测试腾讯期货数据源（与现有ETF数据源相同）。"""
        print("\n===== 腾讯期货数据源 =====")

        # 腾讯也支持外盘期货，格式类似新浪
        # 代码格式与新浪不同，需要探索
        test_codes = ["hf_NQ", "hf_ES", "hf_DAX"]  # 先用新浪格式测试

        url = "http://qt.gtimg.cn/q=" + ",".join(test_codes)

        _rate_limit()
        resp = requests.get(url, timeout=10)
        resp.encoding = "gbk"

        print(f"状态码: {resp.status_code}")
        if resp.status_code == 200:
            print("--- 原始响应 ---")
            print(resp.text[:500])  # 只打印前500字符

            # 简单解析
            if "v_hf_NQ" in resp.text:
                print("\n腾讯可能支持新浪格式的 hf_ 代码")
            else:
                print("\n腾讯不支持新浪格式的 hf_ 代码，需要探索其他格式")

    def test_nikkei_alternatives(self):
        """测试日经225期货的替代数据源。"""
        print("\n===== 日经225期货替代数据源探索 =====")

        # 1. 尝试新浪的NK相关代码
        nk_variants = ["hf_NK", "hf_NK225", "hf_NK225Y", "hf_NK00Y"]
        url = "http://hq.sinajs.cn/list=" + ",".join(nk_variants)

        _rate_limit()
        resp = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=10)
        resp.encoding = "gbk"

        print("\n--- 新浪NK相关代码测试 ---")
        for code in nk_variants:
            if f'hq_str_{code}="' in resp.text and f'="{resp.text.split(f'hq_str_{code}="')[1].split('"')[1]}"' != "=":
                value = resp.text.split(f'hq_str_{code}="')[1].split('"')[0]
                if value:
                    fields = value.split(",")
                    print(f"✓ {code}: {fields[13] if len(fields) > 13 else '未知'} = {fields[0]}")
                else:
                    print(f"? {code}: 返回为空")
            else:
                print(f"✗ {code}: 不支持")

        # 2. 建议其他方案
        print("""
        日经225期货数据获取建议：
        1. SGX（新加坡交易所）有日经期货，但可能需要专门的API
        2. 使用现有代码中的NK指数，可能已经满足需求
        3. 如果必须精确的NK225期货数据，考虑：
           - Yahoo Finance API (需要代理)
           - Alpha Vantage
           - 直接爬取大商所/新交所网站
        """)


if __name__ == "__main__":
    # 直接运行此文件执行所有测试
    import pytest
    pytest.main([__file__, "-v", "-s"])
