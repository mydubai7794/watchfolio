"""验证新浪期指 / 腾讯 ETF / 新浪 IOPV 的纯 HTTP 接口返回格式与解析逻辑。

目的：确认底层接口可用、字段索引位置正确，为后续替代 akshare 做准备。
所有测试发真实请求，打印原始响应供人工复核。
"""

import re
import time

import pytest
import requests

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

SINA_FUTURES_URL = "http://hq.sinajs.cn/list=hf_NQ,hf_YM,hf_DAX"
SINA_HEADERS = {"Referer": "http://finance.sina.com.cn"}

SINA_IOPV_URL = "http://hq.sinajs.cn/list=sz159561_iopv,sh513400_iopv,sz159941_iopv,sz159660_iopv,sh513100_iopv"

TENCENT_URL = "http://qt.gtimg.cn/q=sz159561,sh513400,sz159941,sz159660,sh513100"

ETFS = ["sz159561", "sh513400", "sz159941", "sz159660", "sh513100"]

# 3 秒间隔（与项目 MIN_INTERVAL_SEC 一致）
MIN_INTERVAL = 3.0
_last_request: float = 0.0


def _rate_limit():
    """简易限速：确保两次请求之间 >= MIN_INTERVAL。"""
    global _last_request
    elapsed = time.monotonic() - _last_request
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request = time.monotonic()


def _safe_float(s: str, default: float = 0.0) -> float:
    try:
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


# ===========================================================================
# 新浪期指接口
# ===========================================================================

class TestSinaFutures:
    """验证 http://hq.sinajs.cn/list=hf_NQ,hf_YM,hf_DAX

    实际格式 (逗号分隔, 15 字段):
      [0]  最新价
      [1]  涨跌额 (常为空)
      [2]  开盘价
      [3]  ?
      [4]  最高价
      [5]  最低价
      [6]  时间
      [7]  昨结算价
      [12] 日期
      [13] 合约中文名

    涨跌幅需手动算: (price - prev_settle) / prev_settle * 100
    """

    raw_text: str = ""

    @classmethod
    def setup_class(cls):
        _rate_limit()
        resp = requests.get(SINA_FUTURES_URL, headers=SINA_HEADERS, timeout=10)
        resp.encoding = "gbk"
        cls.raw_text = resp.text
        print(f"\n===== 新浪期指原始响应 =====\n{cls.raw_text}\n")

    def test_status_ok(self):
        resp = requests.get(SINA_FUTURES_URL, headers=SINA_HEADERS, timeout=10)
        assert resp.status_code == 200

    def test_parse_and_calc_change_pct(self):
        for symbol in ("hf_NQ", "hf_YM", "hf_DAX"):
            match = re.search(rf'var hq_str_{symbol}="([^"]*)"', self.raw_text)
            assert match, f"未匹配到 {symbol}"

            value = match.group(1)
            if not value:
                print(f"  {symbol}: 返回值为空（可能不在交易时段）")
                continue

            fields = value.split(",")
            print(f"\n  {symbol} 字段数: {len(fields)}")
            for i, f in enumerate(fields):
                print(f"    [{i:2d}] {f}")

            price = _safe_float(fields[0])
            prev_settle = _safe_float(fields[7])
            assert price > 0
            assert prev_settle > 0
            change_pct = (price - prev_settle) / prev_settle * 100
            print(f"  → {symbol}: 最新价={price}, 昨结算={prev_settle}, "
                  f"涨跌幅={change_pct:.3f}%")


# ===========================================================================
# 腾讯 ETF 接口
# ===========================================================================

class TestTencentETF:
    """验证 http://qt.gtimg.cn/q=... (~分隔, 88 字段)

    已验证字段:
      [3]  当前价          ✓
      [9]  买一价          ✓
      [10] 买一量(手)      ✓
      [19] 卖一价          ✓
      [20] 卖一量(手)      ✓
      [32] 涨跌幅(%)       ✓
      [78] IOPV 实时参考净值 ✓ (待本轮验证)
      [80] IOPV 涨跌(元)    ✓ (待本轮验证)
      [81] 昨日净值         ✓ (待本轮验证)
    """

    raw_text: str = ""

    @classmethod
    def setup_class(cls):
        _rate_limit()
        resp = requests.get(TENCENT_URL, timeout=10)
        resp.encoding = "gbk"
        cls.raw_text = resp.text
        print(f"\n===== 腾讯 ETF 原始响应 =====\n{cls.raw_text}\n")

    def test_status_ok(self):
        assert requests.get(TENCENT_URL, timeout=10).status_code == 200

    def test_parse_all_fields(self):
        """验证全部关键字段: 价格/盘口/涨跌幅/IOPV。"""
        for code in ETFS:
            match = re.search(rf'v_{code}="([^"]*)"', self.raw_text)
            assert match, f"未匹配到 {code}"

            value = match.group(1)
            if not value:
                print(f"  {code}: 返回值为空")
                continue

            fields = value.split("~")
            print(f"\n  {code} 字段数: {len(fields)}")
            for i, f in enumerate(fields):
                print(f"    [{i:2d}] {f}")

            price = _safe_float(fields[3])
            buy1_price = _safe_float(fields[9])
            buy1_vol = int(_safe_float(fields[10]))
            sell1_price = _safe_float(fields[19])
            sell1_vol = int(_safe_float(fields[20]))
            change_pct = _safe_float(fields[32])
            iopv = _safe_float(fields[78])
            iopv_change = _safe_float(fields[80])
            prev_nav = _safe_float(fields[81])

            assert price > 0, f"{code} 价格为 0"

            # IOPV 应接近价格 (溢价率通常 ±10% 以内)
            if iopv > 0:
                premium = (price - iopv) / iopv * 100
                assert abs(premium) < 15, (
                    f"{code} IOPV={iopv} 对价格={price} 溢价率={premium:.1f}% 异常"
                )
                print(f"  → {code}: 价格={price}, IOPV={iopv}, "
                      f"溢价率={premium:.2f}%, 昨日净值={prev_nav}")
            else:
                print(f"  → {code}: 价格={price}, IOPV=0 (未获取到)")

            print(f"     买一={buy1_price}×{buy1_vol}, 卖一={sell1_price}×{sell1_vol}, "
                  f"涨跌幅={change_pct}%, IOPV涨跌={iopv_change}")


# ===========================================================================
# 新浪 IOPV 专用接口 (交叉验证)
# ===========================================================================

class TestSinaIOPV:
    """验证 http://hq.sinajs.cn/list=sz159561_iopv,...

    返回格式: var hq_str_sz159561_iopv="日期,时间,IOPV";
    用于和腾讯 [78] 交叉验证。
    """

    raw_text: str = ""

    @classmethod
    def setup_class(cls):
        _rate_limit()
        resp = requests.get(SINA_IOPV_URL, headers=SINA_HEADERS, timeout=10)
        resp.encoding = "gbk"
        cls.raw_text = resp.text
        print(f"\n===== 新浪 IOPV 原始响应 =====\n{cls.raw_text}\n")

    def test_status_ok(self):
        assert requests.get(SINA_IOPV_URL, headers=SINA_HEADERS, timeout=10).status_code == 200

    def test_parse_iopv(self):
        for code in ETFS:
            match = re.search(rf'var hq_str_{code}_iopv="([^"]*)"', self.raw_text)
            assert match, f"未匹配到 {code}_iopv"

            value = match.group(1)
            if not value:
                print(f"  {code}_iopv: 返回值为空")
                continue

            # 格式: "2026-04-17,11:30:00,1.3126"
            parts = value.split(",")
            print(f"  {code}_iopv: 原始={value}, 字段数={len(parts)}")

            assert len(parts) >= 3, f"{code} IOPV 字段不足: {parts}"
            date, time_str, iopv_str = parts[0], parts[1], parts[2]
            iopv = _safe_float(iopv_str)

            assert iopv > 0, f"{code} IOPV 为 0"
            print(f"  → {code}: 日期={date}, 时间={time_str}, IOPV={iopv}")


# ===========================================================================
# 腾讯 [78] vs 新浪 IOPV 交叉验证
# ===========================================================================

class TestCrossValidateIOPV:
    """用新浪 IOPV 接口验证腾讯 [78] 确实是 IOPV。"""

    tencent_raw: str = ""
    sina_raw: str = ""

    @classmethod
    def setup_class(cls):
        _rate_limit()
        resp_t = requests.get(TENCENT_URL, timeout=10)
        resp_t.encoding = "gbk"
        cls.tencent_raw = resp_t.text

        _rate_limit()
        resp_s = requests.get(SINA_IOPV_URL, headers=SINA_HEADERS, timeout=10)
        resp_s.encoding = "gbk"
        cls.sina_raw = resp_s.text

    def test_tencent_field_78_matches_sina_iopv(self):
        """腾讯 [78] 和新浪 _iopv 接口返回的 IOPV 应基本一致。"""
        for code in ETFS:
            # 腾讯 [78]
            match_t = re.search(rf'v_{code}="([^"]*)"', self.tencent_raw)
            assert match_t
            fields_t = match_t.group(1).split("~")
            tencent_iopv = _safe_float(fields_t[78]) if len(fields_t) > 78 else 0

            # 新浪 IOPV
            match_s = re.search(rf'var hq_str_{code}_iopv="([^"]*)"', self.sina_raw)
            sina_iopv = 0.0
            if match_s and match_s.group(1):
                sina_iopv = _safe_float(match_s.group(1).split(",")[2])

            if tencent_iopv > 0 and sina_iopv > 0:
                diff_pct = abs(tencent_iopv - sina_iopv) / sina_iopv * 100
                print(f"  {code}: 腾讯[78]={tencent_iopv}, 新浪={sina_iopv}, "
                      f"偏差={diff_pct:.3f}%")
                assert diff_pct < 1.0, (
                    f"{code} 腾讯[78]={tencent_iopv} vs 新浪={sina_iopv} 偏差过大"
                )
            else:
                print(f"  {code}: 腾讯[78]={tencent_iopv}, 新浪={sina_iopv} "
                      f"(有一方为 0，可能非交易时段)")
