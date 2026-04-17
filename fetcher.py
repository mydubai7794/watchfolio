"""数据抓取模块：通过 akshare 获取 ETF 行情和全球期指数据。

关键设计：
- fund_etf_spot_em() 一次请求获取全部 ETF（含 IOPV、溢价率、委比、外盘/内盘）
- futures_global_spot_em() 全球期指行情
- 每轮仅 2 次请求（ETF 批量 + 期指批量），严格遵守 3 秒/IP 间隔
- 委比(%) = (委买-委卖)/(委买+委卖)*100，用于计算买卖不平衡度
- 代理轮转：通过环境变量设置 http_proxy/https_proxy
"""

import asyncio
import logging
import os
import time
from datetime import datetime

import akshare as ak
import pandas as pd

from config import FUTURES_NAME_MAP, MIN_INTERVAL_SEC

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 全局代理状态
# ---------------------------------------------------------------------------

_proxies: list[str] = []
_proxy_index: int = 0
_proxy_last_used: dict[str, float] = {}  # proxy_url -> last_used_timestamp
_last_request_time: float = 0.0  # 上次请求时间（用于直连限速）


def set_proxies(proxies: list[str]) -> None:
    """设置可用代理列表。"""
    global _proxies, _proxy_index
    _proxies = list(proxies)
    _proxy_index = 0
    logger.info("代理列表已更新，共 %d 个代理", len(_proxies))


def _clear_proxy_env() -> None:
    """清除代理环境变量。"""
    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        os.environ.pop(key, None)


def _set_proxy_env(proxy_url: str) -> None:
    """设置代理环境变量。"""
    _clear_proxy_env()
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url


def _pick_proxy() -> str | None:
    """选择下一个满足 MIN_INTERVAL_SEC 间隔的代理，无代理则返回 None。"""
    global _proxy_index
    if not _proxies:
        return None

    now = time.monotonic()
    for _ in range(len(_proxies)):
        candidate = _proxies[_proxy_index % len(_proxies)]
        _proxy_index += 1
        last_used = _proxy_last_used.get(candidate, 0)
        if now - last_used >= MIN_INTERVAL_SEC:
            _proxy_last_used[candidate] = now
            return candidate

    logger.warning("所有代理均在冷却中")
    return None


async def _wait_rate_limit(is_direct: bool) -> str | None:
    """等待速率限制并返回应使用的代理 URL（None 表示直连）。

    无论是直连还是代理，都严格遵守 MIN_INTERVAL_SEC 间隔。
    """
    global _last_request_time

    proxy = _pick_proxy()
    if proxy:
        _set_proxy_env(proxy)
        return proxy

    # 直连模式：确保距离上次请求 >= MIN_INTERVAL_SEC
    _clear_proxy_env()
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < MIN_INTERVAL_SEC:
        wait = MIN_INTERVAL_SEC - elapsed
        logger.info("直连限速等待 %.1fs", wait)
        await asyncio.sleep(wait)
    _last_request_time = time.monotonic()
    return None


# ---------------------------------------------------------------------------
# ETF 批量行情（含 IOPV、委比、外盘/内盘）
# ---------------------------------------------------------------------------

async def fetch_etf_batch() -> pd.DataFrame:
    """获取全部 ETF 实时行情（单次批量请求）。

    策略：先直连，失败后尝试代理。直连遵守 MIN_INTERVAL_SEC 限速。
    """
    # 抑制 akshare 内部的 tqdm 进度条
    os.environ["TQDM_DISABLE"] = "1"

    # 第一次尝试：直连
    await _wait_rate_limit(is_direct=True)
    try:
        df = await asyncio.to_thread(ak.fund_etf_spot_em)
        logger.info("ETF 批量行情获取成功(直连)，共 %d 条", len(df))
        return df
    except Exception as e:
        logger.warning("ETF 批量直连失败: %s", e)
    finally:
        _clear_proxy_env()

    # 第二次尝试：走代理
    proxy = _pick_proxy()
    if proxy:
        _set_proxy_env(proxy)
        try:
            df = await asyncio.to_thread(ak.fund_etf_spot_em)
            logger.info("ETF 批量行情获取成功(代理 %s)，共 %d 条", proxy, len(df))
            return df
        except Exception as e:
            logger.error("ETF 批量代理也失败: %s", e)
        finally:
            _clear_proxy_env()

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 全球期指行情
# ---------------------------------------------------------------------------

async def fetch_futures() -> pd.DataFrame:
    """获取全球期指实时行情（单次批量请求）。

    策略：先直连，失败后尝试代理。
    """
    # 抑制 akshare 内部的 tqdm 进度条
    os.environ["TQDM_DISABLE"] = "1"

    # 第一次尝试：直连
    await _wait_rate_limit(is_direct=True)
    try:
        df = await asyncio.to_thread(ak.futures_global_spot_em)
        logger.info("全球期指行情获取成功(直连)，共 %d 条", len(df))
        return df
    except Exception as e:
        logger.warning("期指直连失败: %s", e)
    finally:
        _clear_proxy_env()

    # 第二次尝试：走代理
    proxy = _pick_proxy()
    if proxy:
        _set_proxy_env(proxy)
        try:
            df = await asyncio.to_thread(ak.futures_global_spot_em)
            logger.info("期指行情获取成功(代理 %s)，共 %d 条", proxy, len(df))
            return df
        except Exception as e:
            logger.error("期指代理也失败: %s", e)
        finally:
            _clear_proxy_env()

    return pd.DataFrame()


def _parse_futures(futures_df: pd.DataFrame) -> dict[str, float | None]:
    """从期指 DataFrame 中提取 NQ/ES/YM/DAX/NK 的涨跌幅。"""
    result: dict[str, float | None] = {}
    if futures_df.empty:
        for key in FUTURES_NAME_MAP:
            result[key] = None
        return result

    for key, keyword in FUTURES_NAME_MAP.items():
        try:
            mask = futures_df["名称"].str.contains(keyword, na=False)
            matched = futures_df[mask]
            if matched.empty:
                result[key] = None
                continue
            row = matched.iloc[0]
            result[key] = float(row["涨跌幅"])
        except Exception as e:
            logger.warning("解析期指 %s (%s) 失败: %s", key, keyword, e)
            result[key] = None

    return result


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0) -> float:
    try:
        if pd.isna(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    try:
        if pd.isna(val):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _weibi_to_ratio(weibi: float) -> tuple[int, int]:
    """从委比(%)反算委买量/委卖量比值。

    委比 = (委买 - 委卖) / (委买 + 委卖) * 100
    设 ratio = 委买/委卖，则:
      委比 = (ratio - 1) / (ratio + 1) * 100
      ratio = (100 + 委比) / max(100 - 委比, 0.01)

    返回 (buy_vol, sell_vol) 使 buy_vol/sell_vol ≈ ratio
    取 sell_vol=1000 保证精度，buy_vol 按比例计算。
    """
    if abs(weibi) > 99.99:
        # 委比接近 ±100，一端几乎为零
        return (999900, 100) if weibi > 0 else (100, 999900)

    denom = max(100.0 - weibi, 0.01)
    ratio = (100.0 + weibi) / denom
    sell_base = 1000
    buy_base = int(ratio * sell_base)
    return (max(buy_base, 1), sell_base)


# ---------------------------------------------------------------------------
# 一轮完整抓取
# ---------------------------------------------------------------------------

async def fetch_round(etf_codes: list[str], proxy_urls: list[str]) -> dict:
    """执行一轮完整的数据抓取并聚合。

    每轮仅 2 次请求：ETF 批量行情 + 期指批量行情。
    严格遵守 MIN_INTERVAL_SEC (3s) 的请求间隔。

    Returns:
        {
            "etf_data": [ {code, name, price, iopv, premium_pct, change_pct,
                           buy1_vol, sell1_vol, buy1_price, sell1_price,
                           weibi, outer_vol, inner_vol, iopv_source} ],
            "futures_data": { "NQ": float|None, ... },
            "errors": [...],
            "fetch_time": "HH:MM:SS"
        }
    """
    errors: list[str] = []
    fetch_start = time.monotonic()

    # 1. 设置代理
    set_proxies(proxy_urls)

    # 2. 串行获取（严格限速）：先 ETF 批量，再期指批量
    etf_df = await fetch_etf_batch()
    futures_df = await fetch_futures()

    # 3. 解析期指
    futures_data = _parse_futures(futures_df)

    # 4. 从批量数据组装 ETF 列表
    etf_data: list[dict] = []

    if etf_df.empty:
        errors.append("ETF 批量行情为空")
    else:
        etf_df["代码_str"] = etf_df["代码"].astype(str)
        for code in etf_codes:
            try:
                rows = etf_df[etf_df["代码_str"] == code]
                if rows.empty:
                    errors.append(f"ETF {code} 未在批量行情中找到")
                    continue
                row = rows.iloc[0]

                price = _safe_float(row.get("最新价"), 0.0)
                iopv = _safe_float(row.get("IOPV实时估值"), 0.0)

                # 基金折价率：正数=折价，取反得溢价率
                discount_rate = _safe_float(row.get("基金折价率"), 0.0)
                premium_pct = -discount_rate

                change_pct = _safe_float(row.get("涨跌幅"), 0.0)
                name = str(row.get("名称", ""))

                # 委比 → 买卖量（用于得分计算）
                weibi = _safe_float(row.get("委比"), 0.0)
                buy1_vol, sell1_vol = _weibi_to_ratio(weibi)

                # 外盘/内盘（实际成交方向，用于展示）
                outer_vol = _safe_int(row.get("外盘"), 0)
                inner_vol = _safe_int(row.get("内盘"), 0)

                # 买卖一价格
                buy1_price = _safe_float(row.get("买一"), 0.0)
                sell1_price = _safe_float(row.get("卖一"), 0.0)

                iopv_source = "IOPV" if iopv > 0 else "N/A"

                etf_data.append({
                    "code": code,
                    "name": name,
                    "price": price,
                    "iopv": iopv,
                    "premium_pct": premium_pct,
                    "change_pct": change_pct,
                    "buy1_vol": buy1_vol,
                    "sell1_vol": sell1_vol,
                    "buy1_price": buy1_price,
                    "sell1_price": sell1_price,
                    "weibi": weibi,
                    "outer_vol": outer_vol,
                    "inner_vol": inner_vol,
                    "iopv_source": iopv_source,
                })
            except Exception as e:
                errors.append(f"ETF {code} 数据解析失败: {e}")
                logger.exception("解析 ETF %s 数据异常", code)

    elapsed = time.monotonic() - fetch_start
    fetch_time = datetime.now().strftime("%H:%M:%S")
    logger.info(
        "本轮抓取完成: %d 只ETF, 耗时 %.1fs, 错误 %d",
        len(etf_data), elapsed, len(errors),
    )

    return {
        "etf_data": etf_data,
        "futures_data": futures_data,
        "errors": errors,
        "fetch_time": fetch_time,
    }
