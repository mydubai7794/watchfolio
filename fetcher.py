"""数据抓取模块：通过 akshare 获取 ETF 行情、买卖盘、全球期指数据。

关键设计：
- fund_etf_spot_em() 一次请求获取全部 ETF（含 IOPV、溢价率等）
- stock_bid_ask_em(symbol) 单只 ETF 五档买卖盘
- futures_global_spot_em() 全球期指行情
- 代理轮转：通过环境变量设置 http_proxy/https_proxy，akshare 内部的 requests 会自动使用
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


def set_proxies(proxies: list[str]) -> None:
    """设置可用代理列表。"""
    global _proxies, _proxy_index
    _proxies = list(proxies)
    _proxy_index = 0
    logger.info("代理列表已更新，共 %d 个代理", len(_proxies))


def _clear_proxy_env() -> None:
    """清除代理环境变量（用于不走代理的请求）。"""
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)
    os.environ.pop("HTTP_PROXY", None)
    os.environ.pop("HTTPS_PROXY", None)


def _set_proxy_env(proxy_url: str) -> None:
    """设置代理环境变量（akshare 内部的 requests 会读取这些变量）。"""
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url


def _pick_proxy() -> str | None:
    """选择下一个可用代理（满足最小间隔），无代理则返回 None。"""
    global _proxy_index
    if not _proxies:
        return None

    now = time.monotonic()
    # 找到满足间隔的代理
    for _ in range(len(_proxies)):
        candidate = _proxies[_proxy_index % len(_proxies)]
        _proxy_index += 1
        last_used = _proxy_last_used.get(candidate, 0)
        if now - last_used >= MIN_INTERVAL_SEC:
            _proxy_last_used[candidate] = now
            return candidate

    # 所有代理都在冷却中，返回 None（将走直连）
    logger.warning("所有代理均在冷却中，本轮跳过代理")
    return None


# ---------------------------------------------------------------------------
# ETF 批量行情
# ---------------------------------------------------------------------------

async def fetch_etf_batch() -> pd.DataFrame:
    """获取全部 ETF 实时行情（单次请求）。

    返回 DataFrame 含列：代码, 名称, 最新价, IOPV实时估值, 基金折价率, 涨跌幅, ...
    """
    _clear_proxy_env()
    try:
        df = await asyncio.to_thread(ak.fund_etf_spot_em)
        logger.info("ETF 批量行情获取成功，共 %d 条", len(df))
        return df
    except Exception as e:
        logger.error("ETF 批量行情获取失败: %s", e)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# 单只 ETF 五档买卖盘
# ---------------------------------------------------------------------------

async def fetch_bid_ask(symbol: str) -> dict | None:
    """获取单只 ETF 的五档买卖盘数据。

    返回 dict 示例：
        {
            "sell_5": float, "sell_5_vol": int, ..., "sell_1": float, "sell_1_vol": int,
            "buy_1": float, "buy_1_vol": int, ..., "buy_5": float, "buy_5_vol": int,
            "最新": float, "均价": float, "涨幅": float, ...
        }
    失败返回 None。
    """
    proxy = _pick_proxy()
    if proxy:
        _set_proxy_env(proxy)
    else:
        _clear_proxy_env()

    try:
        df = await asyncio.to_thread(ak.stock_bid_ask_em, symbol=symbol)
        # 返回格式是 item/value 两列，转为 dict
        result = dict(zip(df["item"], df["value"]))
        return result
    except Exception as e:
        logger.warning("获取 %s 买卖盘失败: %s", symbol, e)
        return None
    finally:
        _clear_proxy_env()


# ---------------------------------------------------------------------------
# 并发获取多只 ETF 买卖盘
# ---------------------------------------------------------------------------

async def fetch_all_bid_asks(codes: list[str]) -> dict[str, dict]:
    """并发获取多只 ETF 的五档买卖盘。

    Args:
        codes: ETF 代码列表

    Returns:
        {code: bid_ask_dict, ...}，失败的 code 不在结果中。
    """
    semaphore = asyncio.Semaphore(5)  # 最多 5 个并发

    async def _fetch_one(code: str) -> tuple[str, dict | None]:
        async with semaphore:
            result = await fetch_bid_ask(code)
            return code, result

    tasks = [_fetch_one(code) for code in codes]
    results = await asyncio.gather(*tasks)

    bid_asks: dict[str, dict] = {}
    for code, data in results:
        if data is not None:
            bid_asks[code] = data

    logger.info("买卖盘获取完成：成功 %d / 请求 %d", len(bid_asks), len(codes))
    return bid_asks


# ---------------------------------------------------------------------------
# 全球期指行情
# ---------------------------------------------------------------------------

async def fetch_futures() -> pd.DataFrame:
    """获取全球期指实时行情（单次请求）。

    返回 DataFrame，需按 FUTURES_NAME_MAP 中的关键字过滤。
    """
    _clear_proxy_env()
    try:
        df = await asyncio.to_thread(ak.futures_global_spot_em)
        logger.info("全球期指行情获取成功，共 %d 条", len(df))
        return df
    except Exception as e:
        logger.error("全球期指行情获取失败: %s", e)
        return pd.DataFrame()


def _parse_futures(futures_df: pd.DataFrame) -> dict[str, float | None]:
    """从期指 DataFrame 中提取 NQ/ES/YM/DAX/NK 的涨跌幅。

    Returns:
        {"NQ": float|None, "ES": float|None, ...}
    """
    result: dict[str, float | None] = {}
    if futures_df.empty:
        for key in FUTURES_NAME_MAP:
            result[key] = None
        return result

    for key, keyword in FUTURES_NAME_MAP.items():
        try:
            # 按名称关键字过滤
            mask = futures_df["名称"].str.contains(keyword, na=False)
            matched = futures_df[mask]
            if matched.empty:
                result[key] = None
                continue
            # 取第一条匹配的涨跌幅
            row = matched.iloc[0]
            val = row["涨跌幅"]
            result[key] = float(val)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("解析期指 %s (%s) 涨跌幅失败: %s", key, keyword, e)
            result[key] = None

    return result


# ---------------------------------------------------------------------------
# 一轮完整抓取
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0) -> float:
    """安全转 float。"""
    try:
        if pd.isna(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    """安全转 int。"""
    try:
        if pd.isna(val):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default


async def fetch_round(etf_codes: list[str], proxy_urls: list[str]) -> dict:
    """执行一轮完整的数据抓取并聚合。

    Args:
        etf_codes: 需要抓取买卖盘的 ETF 代码列表
        proxy_urls: 可用代理 URL 列表

    Returns:
        {
            "etf_data": [ {code, name, price, iopv, premium_pct, change_pct,
                           buy1_vol, sell1_vol, buy1_price, sell1_price, iopv_source} ],
            "futures_data": { "NQ": float|None, "DAX": float|None, ... },
            "errors": [...],
            "fetch_time": "HH:MM:SS"
        }
    """
    errors: list[str] = []
    fetch_start = time.monotonic()

    # 1. 设置代理
    set_proxies(proxy_urls)

    # 2. 并发获取 ETF 批量行情、买卖盘、期指行情
    etf_batch_task = fetch_etf_batch()
    bid_ask_task = fetch_all_bid_asks(etf_codes)
    futures_task = fetch_futures()

    etf_df, bid_asks, futures_df = await asyncio.gather(
        etf_batch_task, bid_ask_task, futures_task
    )

    # 3. 解析期指
    futures_data = _parse_futures(futures_df)

    # 4. 组装 ETF 数据
    etf_data: list[dict] = []

    if etf_df.empty:
        errors.append("ETF 批量行情为空")
    else:
        # 建立 code -> 行 的映射
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

                # 基金折价率：正数=折价，需要取反得到溢价率
                discount_rate = _safe_float(row.get("基金折价率"), 0.0)
                premium_pct = -discount_rate

                change_pct = _safe_float(row.get("涨跌幅"), 0.0)
                name = str(row.get("名称", ""))

                # 买卖盘数据
                ba = bid_asks.get(code, {})
                buy1_vol = _safe_int(ba.get("buy_1_vol"), 0)
                sell1_vol = _safe_int(ba.get("sell_1_vol"), 0)
                buy1_price = _safe_float(ba.get("buy_1"), 0.0)
                sell1_price = _safe_float(ba.get("sell_1"), 0.0)

                # 确定 IOPV 来源
                iopv_source = "batch"
                if iopv == 0 and ba.get("最新"):
                    # 批量数据无 IOPV，使用买卖盘中的最新价
                    iopv = _safe_float(ba.get("最新"), 0.0)
                    iopv_source = "bid_ask"

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
