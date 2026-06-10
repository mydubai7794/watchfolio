"""数据抓取模块：通过纯 HTTP 接口获取 ETF 行情和全球期指数据。

数据源架构：
- 腾讯 HTTP 接口 (qt.gtimg.cn) 获取 ETF 行情（含 IOPV、盘口、涨跌幅）
- 期指数据采用智能路由，轮询以下两个数据源：
  1. 新浪 HTTP 接口 (hq.sinajs.cn): hf_NQ, hf_ES, hf_YM
  2. 东方财富接口 (futsseapi.eastmoney.com): NQ00Y, ES00Y, YM00Y

关键设计：
- 每轮仅 2 次 HTTP 请求（ETF 批量 + 期指批量），严格遵守 3 秒/IP/数据源间隔
- 速率限制按 (proxy, source) 独立计算：同一代理可连续请求腾讯和期指源，互不等待
- 智能路由：新浪和东方财富轮询使用，降低单一数据源风控风险
- 降级策略：优先数据源失败时自动切换到备用数据源
- 使用 httpx 异步客户端，原生支持 SOCKS5/HTTP 代理
"""

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime

import httpx

from config import FUTURES_NAME_MAP, MIN_INTERVAL_SEC

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 接口常量
# ---------------------------------------------------------------------------

TENCENT_BASE = "http://qt.gtimg.cn/q="
SINA_FUTURES_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {"Referer": "http://finance.sina.com.cn"}

# 东方财富期货接口 - 只请求 COBOT 交易所（美股期指）
EASTMONEY_FUTURES_URL = "https://futsseapi.eastmoney.com/list/COBOT"
EASTMONEY_PARAMS = {
    "orderBy": "dm",
    "sort": "desc",
    "pageSize": "500",  # 一次获取全部数据
    "pageIndex": "0",
    "token": "***REMOVED***",
    "field": "dm,sc,name,p,zsjd,zde,zdf,f152,o,h,l,zjsj,vol,wp,np,ccl",
    "blockName": "callback",
}

# ---------------------------------------------------------------------------
# 全局代理状态 — 速率限制按 (proxy, source) 独立计算
# ---------------------------------------------------------------------------

_proxies: list[str] = []
_proxy_index: int = 0
# key = (proxy_url, source_name), value = last_used monotonic timestamp
_source_last_used: dict[tuple[str, str], float] = {}
# 直连也按 source 独立限速
_direct_last_used: dict[str, float] = {}

# 期指数据源轮询状态
_futures_source_roundrobin: list[str] = ["sina", "eastmoney"]  # 轮询顺序
_futures_source_index: int = 0
# 数据源健康状态：True=可用, False=不可用
_futures_source_health: dict[str, bool] = {"sina": True, "eastmoney": True}

# 代理封禁追踪 — 断路器
# key = (proxy_url, source), value = {"fails": N, "banned_until": monotonic_ts}
_proxy_bans: dict[tuple[str, str], dict] = {}
_PROXY_BAN_BASE_SEC = 30.0        # 首次封禁冷却 30 秒
_PROXY_BAN_MAX_SEC = 600.0         # 最长冷却 10 分钟
_PROXY_BAN_BACKOFF = 2.0           # 指数退避倍数


def set_proxies(proxies: list[str]) -> None:
    """设置可用代理列表。"""
    global _proxies, _proxy_index
    _proxies = list(proxies)
    _proxy_index = 0
    logger.info("代理列表已更新，共 %d 个代理", len(_proxies))


def _get_next_futures_source() -> str:
    """获取下一个期指数据源（轮询）。"""
    global _futures_source_index, _futures_source_health

    # 找一个健康的数据源
    for _ in range(len(_futures_source_roundrobin)):
        source = _futures_source_roundrobin[_futures_source_index % len(_futures_source_roundrobin)]
        _futures_source_index += 1
        if _futures_source_health.get(source, True):
            return source

    # 如果都不可用，重置状态并重试
    logger.warning("所有期指数据源都不可用，重置状态")
    _futures_source_health = {"sina": True, "eastmoney": True}
    _futures_source_index = 0
    return "sina"


def _is_proxy_banned(proxy_url: str, source: str) -> bool:
    """检查代理是否被封禁中。"""
    key = (proxy_url, source)
    ban = _proxy_bans.get(key)
    if not ban:
        return False
    if time.monotonic() >= ban.get("banned_until", 0):
        # 冷却已过，自动解封
        del _proxy_bans[key]
        return False
    return True


def _record_proxy_ban(proxy_url: str, source: str):
    """记录一次封禁（403），设置指数退避冷却。"""
    key = (proxy_url, source)
    ban = _proxy_bans.get(key, {"fails": 0, "banned_until": 0})
    ban["fails"] += 1
    cooldown = min(
        _PROXY_BAN_BASE_SEC * (_PROXY_BAN_BACKOFF ** (ban["fails"] - 1)),
        _PROXY_BAN_MAX_SEC,
    )
    ban["banned_until"] = time.monotonic() + cooldown
    _proxy_bans[key] = ban
    logger.warning(
        "代理被封 %s (%s): 连续%d次403, 冷却%.0fs",
        proxy_url[:40], source, ban["fails"], cooldown,
    )


def _clear_proxy_ban(proxy_url: str, source: str):
    """请求成功，清除封禁计数。"""
    key = (proxy_url, source)
    if key in _proxy_bans:
        del _proxy_bans[key]


async def _acquire_proxy(source: str) -> str | None:
    """为指定数据源获取一个满足 MIN_INTERVAL_SEC 间隔的代理。

    优先使用代理。当所有代理对该数据源都在冷却时，等待最短冷却时间
    而不是降级为直连。仅在没有配置代理时才使用直连。
    """
    global _proxy_index

    if not _proxies:
        # 无代理，直连限速
        now = time.monotonic()
        last = _direct_last_used.get(source, 0)
        if now - last < MIN_INTERVAL_SEC:
            wait = MIN_INTERVAL_SEC - (now - last)
            logger.debug("直连 %s 限速等待 %.1fs", source, wait)
            await asyncio.sleep(wait)
        _direct_last_used[source] = time.monotonic()
        return None

    now = time.monotonic()
    # 尝试找一个已冷却且未被封禁的代理
    for _ in range(len(_proxies)):
        candidate = _proxies[_proxy_index % len(_proxies)]
        _proxy_index += 1
        if _is_proxy_banned(candidate, source):
            continue
        last = _source_last_used.get((candidate, source), 0)
        if now - last >= MIN_INTERVAL_SEC:
            _source_last_used[(candidate, source)] = now
            return candidate

    # 所有代理对该数据源都在冷却中或被封禁 — 等最短等待时间
    min_wait = MIN_INTERVAL_SEC
    for proxy in _proxies:
        ban = _proxy_bans.get((proxy, source))
        if ban:
            remaining = max(0, ban["banned_until"] - now)
            if 0 < remaining < min_wait:
                min_wait = remaining
        elapsed = now - _source_last_used.get((proxy, source), 0)
        remaining = MIN_INTERVAL_SEC - elapsed
        if 0 < remaining < min_wait:
            min_wait = remaining

    if min_wait > 0:
        logger.debug("所有代理对 %s 冷却/封禁中，等待 %.1fs", source, min_wait)
        await asyncio.sleep(min_wait)

    # 等完后重新选取（自动跳过仍在封禁期的代理）
    for _ in range(len(_proxies)):
        candidate = _proxies[_proxy_index % len(_proxies)]
        _proxy_index += 1
        if not _is_proxy_banned(candidate, source):
            _source_last_used[(candidate, source)] = time.monotonic()
            return candidate

    # 全部仍在封禁中（不应该到这里），降级直连
    logger.warning("所有代理对 %s 仍被封禁，降级直连", source)
    _direct_last_used[source] = time.monotonic()
    return None


async def _http_get(url: str, *, source: str = "", headers: dict | None = None, retries: int = 1) -> str | None:
    """发一次 HTTP GET，按 source 独立限速。失败时换代理重试。"""
    last_proxy = None
    for attempt in range(1 + retries):
        proxy_url = await _acquire_proxy(source)
        client_kwargs: dict = {"timeout": 10}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
            mode = f"代理({source}) {proxy_url[:30]}..."
            last_proxy = proxy_url
        else:
            mode = f"直连({source})"
            last_proxy = None

        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    if proxy_url:
                        _clear_proxy_ban(proxy_url, source)
                    return resp.text
                # 403/407 = 被封禁
                if resp.status_code in (403, 407) and proxy_url:
                    _record_proxy_ban(proxy_url, source)
                logger.warning("GET %s (%s) → HTTP %d", url[:60], mode, resp.status_code)
        except Exception as e:
            logger.warning("GET %s (%s) 失败: %s", url[:60], mode, e)

        if attempt < retries:
            logger.info("重试 %s (第%d次, 换代理)", source, attempt + 1)

    return None


async def _http_get_json(url: str, *, source: str = "", params: dict | None = None, retries: int = 1) -> dict | None:
    """发一次 HTTP GET 获取 JSON，按 source 独立限速。"""
    last_proxy = None
    for attempt in range(1 + retries):
        proxy_url = await _acquire_proxy(source)
        client_kwargs: dict = {"timeout": 10}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
            mode = f"代理({source}) {proxy_url[:30]}..."
            last_proxy = proxy_url
        else:
            mode = f"直连({source})"
            last_proxy = None

        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    if proxy_url:
                        _clear_proxy_ban(proxy_url, source)
                    return resp.json()
                if resp.status_code in (403, 407) and proxy_url:
                    _record_proxy_ban(proxy_url, source)
                logger.warning("GET %s (%s) → HTTP %d", url[:60], mode, resp.status_code)
        except Exception as e:
            logger.warning("GET %s (%s) 失败: %s", url[:60], mode, e)

        if attempt < retries:
            logger.info("重试 %s (第%d次, 换代理)", source, attempt + 1)

    return None


# ---------------------------------------------------------------------------
# ETF 代码前缀转换
# ---------------------------------------------------------------------------

def _add_prefix(code: str) -> str:
    """纯数字代码 → 带市场前缀 (sz/sh)。"""
    if code.startswith(("sz", "sh")):
        return code
    return f"sh{code}" if code[0] in "56789" else f"sz{code}"


def _strip_prefix(code: str) -> str:
    """带前缀代码 → 纯数字。"""
    return code.removeprefix("sz").removeprefix("sh")


# ---------------------------------------------------------------------------
# ETF 行情解析 (腾讯接口)
# ---------------------------------------------------------------------------

# 腾讯接口字段索引
_T_PRICE = 3
_T_NAME = 1
_T_CHANGE_PCT = 32
_T_BUY1_PRICE = 9
_T_BUY1_VOL = 10
_T_SELL1_PRICE = 19
_T_SELL1_VOL = 20
_T_OUTER = 7
_T_INNER = 8
_T_IOPV = 78

# 5 档买量索引
_T_PRE_CLOSE = 4
_T_TOTAL_VOL = 36
_T_AMOUNT = 37
_T_BUY_VOLS = [10, 12, 14, 16, 18]
_T_SELL_VOLS = [20, 22, 24, 26, 28]


def _safe_float(s: str | None, default: float = 0.0) -> float:
    try:
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


def _parse_tencent_etf(raw: str, etf_codes: list[str]) -> tuple[list[dict], list[str], dict[str, str]]:
    """解析腾讯 ETF 批量响应，返回 (etf_data, errors, naming)。"""
    etf_data: list[dict] = []
    errors: list[str] = []
    naming: dict[str, str] = {}

    for code in etf_codes:
        prefixed = _add_prefix(code)
        match = re.search(rf'v_{prefixed}="([^"]*)"', raw)
        if not match:
            errors.append(f"ETF {code} 未在腾讯接口中找到")
            continue

        value = match.group(1)
        if not value:
            errors.append(f"ETF {code} 返回值为空")
            continue

        fields = value.split("~")
        if len(fields) < 30:
            errors.append(f"ETF {code} 字段数不足: {len(fields)}")
            continue

        try:
            price = _safe_float(fields[_T_PRICE])
            name = fields[_T_NAME]
            change_pct = _safe_float(fields[_T_CHANGE_PCT])
            pre_close = _safe_float(fields[_T_PRE_CLOSE])
            buy1_price = _safe_float(fields[_T_BUY1_PRICE])
            buy1_vol = int(_safe_float(fields[_T_BUY1_VOL]))
            sell1_price = _safe_float(fields[_T_SELL1_PRICE])
            sell1_vol = int(_safe_float(fields[_T_SELL1_VOL]))
            outer_vol = int(_safe_float(fields[_T_OUTER]))
            inner_vol = int(_safe_float(fields[_T_INNER]))
            iopv = _safe_float(fields[_T_IOPV]) if len(fields) > _T_IOPV else 0.0
            total_volume = int(_safe_float(fields[_T_TOTAL_VOL])) if len(fields) > _T_TOTAL_VOL else 0
            amount = _safe_float(fields[_T_AMOUNT]) if len(fields) > _T_AMOUNT else 0.0

            # 溢价率 = (价格 - IOPV) / IOPV × 100
            premium_pct = (price - iopv) / iopv * 100 if iopv > 0 else 0.0

            # 委比 = (总买量 - 总卖量) / (总买量 + 总卖量) × 100
            total_buy = sum(int(_safe_float(fields[i])) for i in _T_BUY_VOLS if i < len(fields))
            total_sell = sum(int(_safe_float(fields[i])) for i in _T_SELL_VOLS if i < len(fields))
            total = total_buy + total_sell
            weibi = (total_buy - total_sell) / total * 100 if total > 0 else 0.0

            iopv_source = "IOPV" if iopv > 0 else "N/A"

            naming[code] = name
            etf_data.append({
                "code": code,
                "name": name,
                "price": price,
                "iopv": iopv,
                "premium_pct": premium_pct,
                "change_pct": change_pct,
                "pre_close": pre_close,
                "buy1_vol": buy1_vol,
                "sell1_vol": sell1_vol,
                "buy1_price": buy1_price,
                "sell1_price": sell1_price,
                "buy_total_vol": total_buy,
                "sell_total_vol": total_sell,
                "total_volume": total_volume,
                "amount": amount,
                "weibi": weibi,
                "outer_vol": outer_vol,
                "inner_vol": inner_vol,
                "iopv_source": iopv_source,
            })
        except Exception as e:
            errors.append(f"ETF {code} 解析失败: {e}")
            logger.exception("解析 ETF %s 异常", code)

    return etf_data, errors, naming


# ---------------------------------------------------------------------------
# 通用腾讯行情解析 (股票/ETF/基金/指数 — 非期指的 A 股品种)
# ---------------------------------------------------------------------------

def _parse_tencent_quote(raw: str, codes: list[str]) -> tuple[list[dict], dict[str, str]]:
    """解析腾讯批量响应，返回 (quote_data, naming)。

    与 _parse_tencent_etf 类似但输出字段更通用，
    适用于股票、基金、指数等品种。
    """
    quote_data: list[dict] = []
    naming: dict[str, str] = {}

    for code in codes:
        prefixed = _add_prefix(code)
        match = re.search(rf'v_{prefixed}="([^"]*)"', raw)
        if not match:
            continue

        value = match.group(1)
        if not value:
            continue

        fields = value.split("~")
        if len(fields) < 30:
            continue

        try:
            price = _safe_float(fields[_T_PRICE])
            name = fields[_T_NAME]
            change_pct = _safe_float(fields[_T_CHANGE_PCT])
            pre_close = _safe_float(fields[_T_PRE_CLOSE])
            total_volume = int(_safe_float(fields[_T_TOTAL_VOL])) if len(fields) > _T_TOTAL_VOL else 0
            amount = _safe_float(fields[_T_AMOUNT]) if len(fields) > _T_AMOUNT else 0.0
            iopv = _safe_float(fields[_T_IOPV]) if len(fields) > _T_IOPV else 0.0

            premium_pct = (price - iopv) / iopv * 100 if iopv > 0 else 0.0

            naming[code] = name
            item = {
                "code": code,
                "name": name,
                "price": price,
                "pre_close": pre_close,
                "change_pct": change_pct,
                "iopv": iopv,
                "premium_pct": premium_pct,
                "total_volume": total_volume,
                "amount": amount,
            }

            if len(fields) > _T_BUY1_VOL:
                item["buy1_price"] = _safe_float(fields[_T_BUY1_PRICE])
                item["buy1_vol"] = int(_safe_float(fields[_T_BUY1_VOL]))
                item["sell1_price"] = _safe_float(fields[_T_SELL1_PRICE])
                item["sell1_vol"] = int(_safe_float(fields[_T_SELL1_VOL]))

                total_buy = sum(int(_safe_float(fields[i])) for i in _T_BUY_VOLS if i < len(fields))
                total_sell = sum(int(_safe_float(fields[i])) for i in _T_SELL_VOLS if i < len(fields))
                item["buy_total_vol"] = total_buy
                item["sell_total_vol"] = total_sell

            quote_data.append(item)
        except Exception as e:
            logger.exception("解析行情 %s 异常", code)

    return quote_data, naming


# ---------------------------------------------------------------------------
# 期指行情解析 - 双数据源支持
# ---------------------------------------------------------------------------

# 新浪 -> 内部代码映射
_SINA_TO_INTERNAL = {
    "NQ": "NQ",
    "ES": "ES",
    "YM": "YM",
}

# 东方财富 -> 内部代码映射
_EASTMONEY_TO_INTERNAL = {
    "NQ00Y": "NQ",
    "ES00Y": "ES",
    "YM00Y": "YM",
}


def _parse_sina_futures(raw: str, futures_indices: list[str]) -> dict[str, float | None]:
    """解析新浪期指响应，返回 {NQ: float|None, ...}。"""
    result: dict[str, float | None] = {}
    for key in futures_indices:
        if key not in _SINA_TO_INTERNAL:
            result[key] = None
            continue

        symbol = f"hf_{key}"
        match = re.search(rf'var hq_str_{symbol}="([^"]*)"', raw)
        if not match or not match.group(1):
            result[key] = None
            continue

        fields = match.group(1).split(",")
        price = _safe_float(fields[0])
        prev_settle = _safe_float(fields[7])

        if price > 0 and prev_settle > 0:
            result[key] = (price - prev_settle) / prev_settle * 100
        else:
            result[key] = None
    return result


def _parse_eastmoney_futures(data: dict, futures_indices: list[str]) -> dict[str, float | None]:
    """解析东方财富期指响应，返回 {NQ: float|None, ...}。"""
    result: dict[str, float | None] = {}

    if not data or "list" not in data:
        return {k: None for k in futures_indices}

    # 建立代码到数据的映射
    code_map = {}
    for item in data["list"]:
        code = item.get("dm", "")
        if code in _EASTMONEY_TO_INTERNAL:
            # 东方财富直接返回涨跌幅百分比
            change_pct = _safe_float(item.get("zdf"))
            code_map[code] = change_pct

    # 按请求的期指顺序返回
    for key in futures_indices:
        # 找到对应的东方财富代码
        em_code = None
        for ec, internal in _EASTMONEY_TO_INTERNAL.items():
            if internal == key:
                em_code = ec
                break

        if em_code and em_code in code_map:
            result[key] = code_map[em_code]
        else:
            result[key] = None

    return result


# ---------------------------------------------------------------------------
# 期指数据获取 - 智能路由
# ---------------------------------------------------------------------------

async def _fetch_futures_sina(futures_indices: list[str]) -> dict[str, float | None] | None:
    """使用新浪数据源获取期指。"""
    symbols = ",".join(f"hf_{k}" for k in futures_indices if k in _SINA_TO_INTERNAL)
    if not symbols:
        return None

    url = SINA_FUTURES_URL.format(symbols=symbols)
    raw = await _http_get(url, source="sina_futures", headers=SINA_HEADERS)

    if raw:
        result = _parse_sina_futures(raw, futures_indices)
        active = sum(1 for v in result.values() if v is not None)
        logger.info("新浪期指: %d/%d 成功", active, len(futures_indices))
        return result

    return None


async def _fetch_futures_eastmoney(futures_indices: list[str]) -> dict[str, float | None] | None:
    """使用东方财富数据源获取期指。"""
    # 只获取我们需要的期指对应的代码
    needed_codes = [
        code for code, internal in _EASTMONEY_TO_INTERNAL.items()
        if internal in futures_indices
    ]
    if not needed_codes:
        return None

    data = await _http_get_json(EASTMONEY_FUTURES_URL, source="eastmoney_futures", params=EASTMONEY_PARAMS)

    if data:
        result = _parse_eastmoney_futures(data, futures_indices)
        active = sum(1 for v in result.values() if v is not None)
        logger.info("东方财富期指: %d/%d 成功", active, len(futures_indices))
        return result

    return None


async def fetch_futures_with_routing(futures_indices: list[str]) -> dict[str, float | None]:
    """使用智能路由获取期指数据。

    轮询策略：
    1. 优先使用轮询选择的数据源
    2. 失败时降级到备用数据源
    3. 更新数据源健康状态

    Returns:
        {NQ: float|None, ES: float|None, YM: float|None}
    """
    if not futures_indices:
        return {}

    primary_source = _get_next_futures_source()
    backup_source = "eastmoney" if primary_source == "sina" else "sina"

    logger.debug("期指路由: 优先=%s, 备用=%s", primary_source, backup_source)

    # 1. 尝试优先数据源
    if primary_source == "sina":
        result = await _fetch_futures_sina(futures_indices)
    else:
        result = await _fetch_futures_eastmoney(futures_indices)

    if result:
        _futures_source_health[primary_source] = True
        return result

    # 2. 优先源失败，尝试备用源
    logger.warning("期指数据源 %s 失败，降级到 %s", primary_source, backup_source)

    if backup_source == "sina":
        result = await _fetch_futures_sina(futures_indices)
    else:
        result = await _fetch_futures_eastmoney(futures_indices)

    if result:
        _futures_source_health[backup_source] = True
        _futures_source_health[primary_source] = False  # 标记优先源不可用
        return result

    # 3. 两个源都失败
    logger.error("所有期指数据源均失败")
    _futures_source_health["sina"] = False
    _futures_source_health["eastmoney"] = False
    return {k: None for k in futures_indices}


# ---------------------------------------------------------------------------
# 一轮完整抓取
# ---------------------------------------------------------------------------

async def fetch_round(
    etf_codes: list[str], proxy_urls: list[str], futures_indices: list[str] | None = None,
) -> dict:
    """执行一轮完整的数据抓取并聚合。

    每轮仅 2 次 HTTP 请求：
      1. 腾讯 ETF 批量行情（含 IOPV、盘口）
      2. 期指行情（智能路由：新浪/东方财富轮询）

    严格遵守 MIN_INTERVAL_SEC (3s) 的请求间隔。

    Args:
        etf_codes: ETF 代码列表
        proxy_urls: 代理 URL 列表
        futures_indices: 需要获取的期指列表，如 ["NQ", "ES", "YM"]。
                         为 None 时从 FUTURES_NAME_MAP 取默认值。

    Returns:
        {
            "etf_data": [...],
            "futures_data": { "NQ": float|None, ... },
            "futures_source": "sina" | "eastmoney",
            "errors": [...],
            "fetch_time": "HH:MM:SS"
        }
    """
    if not futures_indices:
        futures_indices = list(FUTURES_NAME_MAP.keys())

    errors: list[str] = []
    fetch_start = time.monotonic()

    # 1. 设置代理
    set_proxies(proxy_urls)

    # 2. ETF 批量行情 (腾讯)
    prefixed = [_add_prefix(c) for c in etf_codes]
    tencent_url = f"{TENCENT_BASE}{','.join(prefixed)}"
    etf_data: list[dict] = []

    raw_etf = await _http_get(tencent_url, source="tencent")
    naming: dict[str, str] = {}
    if raw_etf:
        etf_data, errors, naming = _parse_tencent_etf(raw_etf, etf_codes)
        logger.info("ETF 行情获取成功，共 %d 只", len(etf_data))
    else:
        errors.append("ETF 腾讯接口请求失败")

    # 3. 期指行情（智能路由）
    futures_data = await fetch_futures_with_routing(futures_indices)

    # 4. 回写 ETF 名称到数据库（以接口返回为准）
    if naming:
        try:
            import database as db
            updated = await db.update_etf_names(naming)
            if updated:
                logger.info("ETF 名称已更新，%d 条变更", updated)
        except Exception as e:
            logger.warning("ETF 名称回写失败: %s", e)

    elapsed = time.monotonic() - fetch_start
    fetch_time = datetime.now().strftime("%H:%M:%S")

    # 确定使用的数据源
    futures_source = "unknown"
    if futures_data:
        if any(v is not None for v in futures_data.values()):
            # 根据轮询索引判断用了哪个源
            idx = (_futures_source_index - 1) % len(_futures_source_roundrobin)
            futures_source = _futures_source_roundrobin[idx]

    logger.info(
        "本轮抓取完成: %d 只ETF, 期指源=%s, 耗时 %.1fs, 错误 %d",
        len(etf_data), futures_source, elapsed, len(errors),
    )

    return {
        "etf_data": etf_data,
        "futures_data": futures_data,
        "futures_source": futures_source,
        "errors": errors,
        "fetch_time": fetch_time,
    }


# ---------------------------------------------------------------------------
# 隔夜外盘指数 (启动时查一次)
# ---------------------------------------------------------------------------

OVERNIGHT_INDICES = {
    "gb_$dji": "道琼斯",
    "gb_$ixic": "纳斯达克",
    "gb_$inx": "标普500",
}


async def fetch_overnight() -> dict:
    """获取隔夜美股收盘数据，启动时调用一次。

    Returns:
        {"道琼斯": {"close": 48578.72, "change_pct": 0.24}, ...}
    """
    symbols = ",".join(OVERNIGHT_INDICES.keys())
    url = SINA_FUTURES_URL.format(symbols=symbols)
    raw = await _http_get(url, source="sina", headers=SINA_HEADERS)

    result: dict[str, dict] = {}
    if not raw:
        return result

    for symbol, name in OVERNIGHT_INDICES.items():
        match = re.search(rf'var hq_str_{re.escape(symbol)}="([^"]*)"', raw)
        if not match or not match.group(1):
            continue
        fields = match.group(1).split(",")
        # 格式: 名称,收盘价,涨跌幅%,日期时间,...
        result[name] = {
            "close": _safe_float(fields[1]) if len(fields) > 1 else 0,
            "change_pct": _safe_float(fields[2]) if len(fields) > 2 else 0,
            "time": fields[3] if len(fields) > 3 else "",
        }
        logger.info("隔夜 %s: 收盘=%.2f, 涨跌幅=%.2f%%", name,
                     result[name]["close"], result[name]["change_pct"])
    return result


# ---------------------------------------------------------------------------
# ETF 日K线 (前复权) — 用于计算昨日涨跌幅，不依赖 history_ticks
# ---------------------------------------------------------------------------

TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


async def fetch_etf_prev_day_changes(etf_codes: list[str]) -> dict[str, float]:
    """批量获取各ETF上一个交易日的涨跌幅（分批并发 + 代理轮转限速）。

    按代理池大小分批，每批并发请求使用不同代理；
    批间遵守 MIN_INTERVAL_SEC 间隔，避免触发风控。

    3个代理 + 20只ETF → 7批 × 3秒 ≈ 21秒（vs 串行60秒）

    Returns:
        {code: change_pct}  如 {"513100": -0.7863}
    """
    if not etf_codes:
        return {}

    today_str = datetime.now().strftime("%Y-%m-%d")

    async def _fetch_one(code: str) -> tuple[str, float | None]:
        prefixed = _add_prefix(code)
        url = f"{TENCENT_KLINE_URL}?param={prefixed},day,,,5,qfq"
        try:
            raw = await _http_get(url, source="tencent_kline")
            if not raw:
                return code, None
            data = json.loads(raw)
            stock_data = data.get("data", {}).get(prefixed, {})
            day_key = "qfqday" if "qfqday" in stock_data else "day"
            klines = stock_data.get(day_key, [])
            if len(klines) < 2:
                return code, None

            last_date = klines[-1][0]
            if last_date >= today_str:
                if len(klines) < 3:
                    return code, None
                yesterday_close = _safe_float(klines[-2][2])
                day_before_close = _safe_float(klines[-3][2])
            else:
                yesterday_close = _safe_float(klines[-1][2])
                day_before_close = _safe_float(klines[-2][2])

            if day_before_close > 0:
                change = round(
                    (yesterday_close - day_before_close) / day_before_close * 100, 4
                )
                return code, change
        except Exception as e:
            logger.warning("获取ETF %s 日K线失败: %s", code, e)
        return code, None

    # 按代理数量分批并发
    batch_size = max(len(_proxies), 1)
    result: dict[str, float] = {}

    for i in range(0, len(etf_codes), batch_size):
        batch = etf_codes[i:i + batch_size]
        tasks = [_fetch_one(code) for code in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in batch_results:
            if isinstance(r, Exception):
                continue
            code, change = r
            if change is not None:
                result[code] = change
                logger.info("ETF %s 昨日涨跌: %+.2f%%", code, change)

        # 批间等待（最后一批不等）
        if i + batch_size < len(etf_codes):
            await asyncio.sleep(MIN_INTERVAL_SEC)

    logger.info("ETF昨日涨跌获取完成: %d/%d (%d代理, batch=%d)",
                len(result), len(etf_codes), len(_proxies), batch_size)
    return result
