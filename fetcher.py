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
import os
import random
import re
import time
from datetime import datetime

import httpx

from config import FUTURES_NAME_MAP, MIN_INTERVAL_SEC
from tencent_bus import tencent_bus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 接口常量
# ---------------------------------------------------------------------------

TENCENT_BASE = "http://qt.gtimg.cn/q="
SINA_FUTURES_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {"Referer": "http://finance.sina.com.cn"}

# 东方财富期货接口 - 只请求 COBOT 交易所（美股期指）
EASTMONEY_FUTURES_URL = "https://futsseapi.eastmoney.com/list/COBOT"

# 腾讯全球指数（gz 前缀国际指数，走 tencent_bus 攒批与 A 股同请求，零额外 HTTP）
_TENCENT_GZ_INDEX_MAP = {
    "N225": "gzN225",
    "KS11": "gzKS11",
}

# 腾讯股指期货（fu 前缀外盘期货，走 tencent_bus 攒批零额外 HTTP）
# 行布局（美式）：f[1]名称 f[2]代码 f[3]现价 f[4]昨结 f[31]涨跌额 f[32]涨跌幅；
# f[0]="delay" 为外盘品种分类标记（实测价格/时间戳实时跳动，非延迟标记）
_TENCENT_FUTURES_MAP = {
    "NQ": "fuNQ",
    "ES": "fuES",
    "YM": "fuYM",
    "NK": "fuNIY",   # CME 日经指数期货（东财/新浪映射本就无 NK，存量缺失项补齐）
}

_SINA_GLOBAL_INDEX_MAP = {
    "N225": "b_NKY",
    "KS11": "b_KOSPI",
}

_last_global_index_cache: dict[str, dict] = {}
# 全球指数刷新节流：A股时段候选轮 5s 快刷，指数 60s 一次（与窗口外候选轮同节奏）
_GLOBAL_INDEX_REFRESH_SEC = 60.0
_global_index_fetch_at: float = 0.0
# 东财接口 token 属敏感凭据：优先从环境变量读取，缺省回退现值保持部署零影响
EASTMONEY_PARAMS = {
    "orderBy": "dm",
    "sort": "desc",
    "pageSize": "500",  # 一次获取全部数据
    "pageIndex": "0",
    "token": os.getenv("QDII_EASTMONEY_TOKEN", "***REMOVED***"),
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

# 期指数据源选择状态（2026-10-08 腾讯主源收敛：fuNQ/fuES/fuYM 经总线搭批
# 恒为主源——腾讯基本不封禁，不进健康状态机，失败下轮自然重试；东财/新浪
# 降为降级链（封禁严厉度：东财 > 新浪 = 雅虎，服务器 IP 风控实测）。保留
# roundrobin 结构仅为兼容既有降级状态机语义。
_futures_source_roundrobin: list[str] = ["eastmoney", "sina"]  # 主→备优先序
_futures_source_index: int = 0
# 上一轮实际成功使用的期指数据源（主源失败降级备源时如实记录，供 fetch_round 下发）
_futures_last_used_source: str | None = None
# 数据源健康状态：True=可用, False=不可用
_futures_source_health: dict[str, bool] = {"sina": True, "eastmoney": True}
_futures_source_recover: dict[str, float] = {}  # source -> monotonic ts when it can recover
_FUTURES_SOURCE_COOLDOWN_SEC = 60.0

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
    """获取期指数据源（东财恒为主源，新浪冷备）。

    东财健康时恒选东财（不再 50/50 轮询，新浪请求量降为冷备量级）；
    不健康的源在冷却 60 秒后自动恢复。东财不可用时选新浪。
    """
    global _futures_source_index, _futures_source_health

    now = time.monotonic()
    for source, ts in list(_futures_source_recover.items()):
        if now >= ts:
            _futures_source_health[source] = True
            del _futures_source_recover[source]
            logger.info("期指数据源 %s 冷却恢复，重新参与轮询", source)

    # 找一个健康的数据源（主→备优先序扫描：东财健康即恒选，新浪仅冷备）
    for source in _futures_source_roundrobin:
        if _futures_source_health.get(source, True):
            return source

    # 如果都不可用，重置状态并重试
    logger.warning("所有期指数据源都不可用，重置状态")
    _futures_source_health = {"sina": True, "eastmoney": True}
    _futures_source_recover.clear()
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


def _record_proxy_ban(proxy_url: str, source: str, base_sec: float = 0, max_sec: float = 0):
    """记录一次封禁，设置指数退避冷却。"""
    key = (proxy_url, source)
    ban = _proxy_bans.get(key, {"fails": 0, "banned_until": 0})
    ban["fails"] += 1
    _base = base_sec or _PROXY_BAN_BASE_SEC
    _max = max_sec or _PROXY_BAN_MAX_SEC
    cooldown = min(
        _base * (_PROXY_BAN_BACKOFF ** (ban["fails"] - 1)),
        _max,
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


async def _http_get_json(url: str, *, source: str = "", params: dict | None = None, retries: int = 1, follow_redirects: bool = False) -> dict | None:
    """发一次 HTTP GET 获取 JSON，按 source 独立限速。"""
    last_proxy = None
    for attempt in range(1 + retries):
        proxy_url = await _acquire_proxy(source)
        client_kwargs: dict = {"timeout": 10, "follow_redirects": follow_redirects}
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
                elif proxy_url and resp.status_code in (502, 503):
                    _record_proxy_ban(proxy_url, source)
                logger.warning("GET %s (%s) → HTTP %d", url[:60], mode, resp.status_code)
        except Exception as e:
            logger.warning("GET %s (%s) 失败: %s", url[:60], mode, e)
            if proxy_url:
                _record_proxy_ban(proxy_url, source)

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
# 东方财富全球指数
# ---------------------------------------------------------------------------

async def fetch_global_indices(codes: list[str]) -> list[dict]:
    """读取全球指数缓存；缓存缺失或超龄（60s）时经腾讯总线刷新一次。

    主刷新通道为 qt.gtimg.cn gz 前缀国际指数（tencent_bus 攒批，与 A 股
    ETF 同一次批量请求；东财 push2 与新浪 hq.sinajs 对服务器 IP 均有风控，
    实测不可达/不稳）。新浪期指路径的顺带解析保留为兜底写入点。刷新无论
    成败都推进节流时间戳，避免故障时的失败风暴。
    """
    global _global_index_fetch_at
    if codes:
        stale = time.monotonic() - _global_index_fetch_at >= _GLOBAL_INDEX_REFRESH_SEC
        missing = any(
            c in _TENCENT_GZ_INDEX_MAP and c not in _last_global_index_cache
            for c in codes
        )
        if stale or missing:
            try:
                gz_codes = [gz for c, gz in _TENCENT_GZ_INDEX_MAP.items() if c in codes]
                if gz_codes:
                    raw = await tencent_bus.get(gz_codes)
                    _parse_and_cache_tencent_global_indices(raw, codes)
            except Exception as e:
                logger.warning("腾讯全球指数刷新异常: %s", e)
            _global_index_fetch_at = time.monotonic()

    return [_last_global_index_cache[c] for c in codes if c in _last_global_index_cache]


def _parse_and_cache_tencent_global_indices(raw: str, codes: list[str] | None) -> None:
    """解析腾讯 gz 前缀国际指数行，存入 _last_global_index_cache。

    行格式（与 A 股布局不同）：f[0]代码 f[1]名称 f[2]时间 f[3]现价
    f[4]涨跌额 f[5]涨跌幅；昨收 = 现价 - 涨跌额 推导。
    """
    global _last_global_index_cache

    if not codes or not raw:
        return
    valid_codes = [c for c in codes if c in _TENCENT_GZ_INDEX_MAP]
    if not valid_codes:
        return

    new_cache: dict[str, dict] = {}
    for line in raw.split(";"):
        line = line.strip()
        if not line or '="' not in line:
            continue
        var_part, value_part = line.split('="', 1)
        qt_code = var_part.replace("v_", "", 1)
        internal_code = None
        for ic, gz in _TENCENT_GZ_INDEX_MAP.items():
            if gz == qt_code:
                internal_code = ic
                break
        if not internal_code:
            continue

        parts = value_part.rstrip('";"\r\n').split("~")
        if len(parts) < 6:
            continue
        price = _safe_float(parts[3])
        change_amt = _safe_float(parts[4])
        change_pct = _safe_float(parts[5])
        if price <= 0:
            continue
        new_cache[internal_code] = {
            "code": internal_code,
            "name": parts[1] or internal_code,
            "price": price,
            "pre_close": round(price - change_amt, 2),
            "change_pct": change_pct,
        }

    if new_cache:
        _last_global_index_cache.update(new_cache)
        for code, q in new_cache.items():
            logger.info("腾讯全球指数 %s(%s): price=%.2f, change=%.2f%%",
                        code, q["name"], q["price"], q["change_pct"])


# ---------------------------------------------------------------------------
# 期指数据获取 - 智能路由
# ---------------------------------------------------------------------------

async def _fetch_futures_tencent(futures_indices: list[str]) -> dict[str, float | None] | None:
    """腾讯主源获取期指（qt.gtimg.cn fu 前缀，经 tencent_bus 攒批与 A 股
    ETF/全球指数同一次 HTTP，零额外请求）。

    腾讯基本不封禁，恒为主源且不进健康状态机——失败时由路由函数降级
    东财/新浪，下轮自然回切腾讯。
    """
    fu_codes = [_TENCENT_FUTURES_MAP[k] for k in futures_indices
                if k in _TENCENT_FUTURES_MAP]
    if not fu_codes:
        return None

    raw = await tencent_bus.get(fu_codes)
    return _parse_tencent_futures(raw, futures_indices)


def _parse_tencent_futures(raw: str, futures_indices: list[str]) -> dict[str, float | None] | None:
    """解析腾讯 fu 前缀期指行，返回 {NQ: change_pct, ...}（部分失败项为 None）。

    行格式（美式布局）：v_fuNQ="delay~名称~NQ~现价~昨结~开盘~..."
    涨跌幅优先取 f[32]（与东财口径实测一致），字段缺失时按 f[3]/f[4] 自算。
    全部解析失败返回 None（触发路由降级）。
    """
    if not raw:
        return None

    code_map: dict[str, float] = {}
    for line in raw.split(";"):
        line = line.strip()
        if not line or '="' not in line:
            continue
        var_part, value_part = line.split('="', 1)
        qt_code = var_part.replace("v_", "", 1)
        internal = None
        for ic, fc in _TENCENT_FUTURES_MAP.items():
            if fc == qt_code:
                internal = ic
                break
        if not internal:
            continue
        parts = value_part.rstrip('";"\r\n').split("~")
        if len(parts) < 5:
            continue
        price = _safe_float(parts[3])
        settle = _safe_float(parts[4])
        if price <= 0:
            continue
        change_pct = _safe_float(parts[32]) if len(parts) > 32 else 0.0
        if not change_pct and settle > 0:
            change_pct = round((price - settle) / settle * 100, 2)
        code_map[internal] = change_pct

    if not code_map:
        return None

    result: dict[str, float | None] = {k: code_map.get(k) for k in futures_indices}
    active = sum(1 for v in result.values() if v is not None)
    logger.info("腾讯期指: %d/%d 成功", active, len(futures_indices))
    return result


async def _fetch_futures_sina(
    futures_indices: list[str],
    global_index_codes: list[str] | None = None,
) -> dict[str, float | None] | None:
    """使用新浪数据源获取期指，同时顺带获取全球指数（合并为一次 HTTP 请求）。

    成功时同时更新：
    - 期指结果（返回值）
    - _last_global_index_cache（模块级全球指数缓存）
    """
    global _last_global_index_cache

    futures_symbols = ",".join(f"hf_{k}" for k in futures_indices if k in _SINA_TO_INTERNAL)

    global_symbols: list[str] = []
    if global_index_codes:
        global_symbols = [
            _SINA_GLOBAL_INDEX_MAP[c] for c in global_index_codes
            if c in _SINA_GLOBAL_INDEX_MAP
        ]

    all_parts = [p for p in [futures_symbols, ",".join(global_symbols)] if p]
    if not all_parts:
        return None

    symbols = ",".join(all_parts)

    url = SINA_FUTURES_URL.format(symbols=symbols)
    raw = await _http_get(url, source="sina_futures", headers=SINA_HEADERS)

    if raw:
        result = _parse_sina_futures(raw, futures_indices)
        active = sum(1 for v in result.values() if v is not None)
        logger.info("新浪期指: %d/%d 成功", active, len(futures_indices))

        _parse_and_cache_global_indices(raw, global_index_codes)
        return result

    return None


def _parse_and_cache_global_indices(raw: str, codes: list[str] | None) -> None:
    """从新浪原始响应中解析全球指数，存入 _last_global_index_cache。

    成功写入时同步推进刷新节流时间戳（新浪已供给，推迟东财请求）。"""
    global _last_global_index_cache, _global_index_fetch_at

    if not codes:
        return

    valid_codes = [c for c in codes if c in _SINA_GLOBAL_INDEX_MAP]
    if not valid_codes:
        return

    new_cache: dict[str, dict] = {}
    for line in raw.split(";"):
        line = line.strip()
        if not line or '="' not in line:
            continue
        var_part, value_part = line.split('="', 1)
        sina_code = var_part.split("hq_str_")[-1]
        value_part = value_part.rstrip('"')
        if not value_part:
            continue

        parts = value_part.split(",")
        if len(parts) < 4:
            continue

        internal_code = None
        for ic, sc in _SINA_GLOBAL_INDEX_MAP.items():
            if sc == sina_code:
                internal_code = ic
                break
        if not internal_code:
            continue

        name = parts[0]
        price = _safe_float(parts[1])
        change_pct = _safe_float(parts[3])
        new_cache[internal_code] = {
            "code": internal_code,
            "name": name,
            "price": price,
            "pre_close": 0,
            "change_pct": change_pct,
        }
        logger.info("全球指数 %s(%s): price=%.2f, change=%.2f%%", internal_code, name, price, change_pct)

    if new_cache:
        _last_global_index_cache = new_cache
        _global_index_fetch_at = time.monotonic()


async def _fetch_futures_eastmoney(futures_indices: list[str]) -> dict[str, float | None] | None:
    """使用东方财富数据源获取期指。全球指数缓存不受影响。"""
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


async def fetch_futures_with_routing(
    futures_indices: list[str],
    global_index_codes: list[str] | None = None,
) -> dict[str, float | None]:
    """使用智能路由获取期指数据。

    降级策略（2026-10-08 腾讯主源收敛）：
    1. 腾讯恒主源（fuNQ/fuES/fuYM 经总线搭批零额外 HTTP；腾讯基本不封禁，
       不进健康状态机，失败下轮自然重试）
    2. 腾讯失败时走东财 → 新浪降级链（既有健康状态机 + 冷却恢复）
    3. 全部失败返回 {NQ: None, ...}

    当降级到 sina 时，global_index_codes 会合并到同一次
    HTTP 请求中，避免对 hq.sinajs.cn 重复请求。

    Returns:
        {NQ: float|None, ES: float|None, YM: float|None}
    """
    global _futures_last_used_source

    if not futures_indices and not global_index_codes:
        return {}

    # 1. 腾讯主源（总线搭批，与 A 股/全球指数同一次请求）
    tencent_result = await _fetch_futures_tencent(futures_indices)
    if tencent_result:
        _futures_last_used_source = "tencent"
        return tencent_result
    logger.warning("腾讯期指失败，降级东财/新浪链")

    primary_source = _get_next_futures_source()
    backup_source = "eastmoney" if primary_source == "sina" else "sina"

    logger.debug("期指路由: 优先=%s, 备用=%s", primary_source, backup_source)

    if primary_source == "sina":
        result = await _fetch_futures_sina(futures_indices, global_index_codes)
    else:
        result = await _fetch_futures_eastmoney(futures_indices)

    if result:
        _futures_source_health[primary_source] = True
        _futures_last_used_source = primary_source  # 记录实际使用源（防降级后误报）
        return result

    logger.warning("期指数据源 %s 失败，降级到 %s", primary_source, backup_source)

    if backup_source == "sina":
        result = await _fetch_futures_sina(futures_indices, global_index_codes)
    else:
        result = await _fetch_futures_eastmoney(futures_indices)

    if result:
        _futures_source_health[backup_source] = True
        _futures_source_health[primary_source] = False  # 标记优先源不可用
        _futures_source_recover[primary_source] = time.monotonic() + _FUTURES_SOURCE_COOLDOWN_SEC
        _futures_last_used_source = backup_source  # 实际由备源成功，如实记录
        return result

    # 3. 两个源都失败
    logger.error("所有期指数据源均失败")
    _futures_source_health["sina"] = False
    _futures_source_health["eastmoney"] = False
    _futures_source_recover["sina"] = time.monotonic() + _FUTURES_SOURCE_COOLDOWN_SEC
    _futures_source_recover["eastmoney"] = time.monotonic() + _FUTURES_SOURCE_COOLDOWN_SEC
    _futures_last_used_source = None
    return {k: None for k in futures_indices}


# ---------------------------------------------------------------------------
# 一轮完整抓取
# ---------------------------------------------------------------------------

async def fetch_round(
    etf_codes: list[str], proxy_urls: list[str], futures_indices: list[str] | None = None,
) -> dict:
    """执行一轮完整的数据抓取并聚合。

    每轮仅 1~2 次 HTTP 请求：
      1. 腾讯批量行情（ETF + 全球指数 gz + 期指 fu，经攒批总线一次发出）
      2. 期指降级请求（仅腾讯失败时：东财 futsseapi → 新浪 hf_）

    Args:
        etf_codes: ETF 代码列表
        proxy_urls: 代理 URL 列表
        futures_indices: 需要获取的期指列表，如 ["NQ", "ES", "YM"]。
                         为 None 时从 FUTURES_NAME_MAP 取默认值。

    Returns:
        {
            "etf_data": [...],
            "futures_data": { "NQ": float|None, ... },
            "futures_source": "tencent" | "eastmoney" | "sina",
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

    # 2. ETF 批量行情 (腾讯，经攒批发送路由与候选池/持仓共用节奏)
    prefixed = [_add_prefix(c) for c in etf_codes]
    etf_data: list[dict] = []

    # 期指 fu 代码并入同一批（10s 节拍下分批 get 会各等一个节拍串成 20s；
    # 并入后 _fetch_futures_tencent 的 get 命中 TTL 缓存零等待）
    fu_codes = [_TENCENT_FUTURES_MAP[k] for k in futures_indices
                if k in _TENCENT_FUTURES_MAP]
    raw_etf = await tencent_bus.get(prefixed + fu_codes)
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

    # 确定使用的数据源（以路由函数实际记录的成功源为准，
    # 主源失败降级备源成功时如实下发备源名，不再按轮询索引推算）
    futures_source = "unknown"
    if futures_data:
        if any(v is not None for v in futures_data.values()):
            futures_source = _futures_last_used_source or "unknown"

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
    """获取隔夜美股收盘数据，每天 09:15 调用一次。

    主源长桥新平台（.DJI/.NDX/.SPX.US 三项一次批量，含涨跌幅自算；
    指数必须带 .US 后缀——旧平台 .INX 静默空的结论仅适用旧平台，新平台
    标普符号为 .SPX.US）；长桥整体失败时新浪 gb_ 全量三项兜底。
    返回 {"道琼斯": {"close": 48578.72, "change_pct": 0.24}, ...}。
    """
    result: dict[str, dict] = {}
    # 1. 长桥主源：道指/纳指/标普一次批量（quote 直连 .cn，免代理）
    try:
        import longport_fetcher as lp
        quotes = await lp.fetch_quote_map([".DJI.US", ".NDX.US", ".SPX.US"])
        name_map = {".DJI.US": "道琼斯", ".NDX.US": "纳斯达克",
                    ".SPX.US": "标普500"}
        for sym, name in name_map.items():
            q = quotes.get(sym)
            if q and q.get("price"):
                result[name] = {"close": q["price"],
                                "change_pct": round(q.get("change_pct") or 0.0, 2),
                                "time": "长桥"}
                logger.info("隔夜 %s: 收盘=%.2f, 涨跌幅=%.2f%% (长桥)", name,
                            q["price"], q.get("change_pct") or 0.0)
    except Exception as e:
        logger.warning("长桥隔夜指数获取异常: %s", e)

    # 2. 新浪：长桥缺的项补齐（常态仅标普500 一项；长桥全挂时全量三项兜底）
    sina_syms = [s for s, n in OVERNIGHT_INDICES.items() if n not in result]
    if sina_syms:
        url = SINA_FUTURES_URL.format(symbols=",".join(sina_syms))
        raw = await _http_get(url, source="sina", headers=SINA_HEADERS)
        if raw:
            for symbol, name in OVERNIGHT_INDICES.items():
                if symbol not in sina_syms:
                    continue
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


# ---------------------------------------------------------------------------
# 候选池行情抓取 (股票/ETF/基金/指数 + 期指)
# ---------------------------------------------------------------------------

async def fetch_candidates(tencent_codes: list[str], futures_codes: list[str] | None = None, global_index_codes: list[str] | None = None) -> dict:
    """抓取候选池行情数据。

    按数据源分组批量请求：
    1. 腾讯 API 批量获取 A 股品种（股票/ETF/基金/指数）
    2. 期指路由获取期货数据
    3. 东方财富获取全球指数（如日经225）

    Args:
        tencent_codes: 腾讯数据源的代码列表（纯数字，如 ['600519', '513100']）
        futures_codes: 期指代码列表（如 ['NQ', 'ES', 'YM']），为 None 则不获取
        global_index_codes: 全球指数代码列表（如 ['N225']），为 None 则不获取

    Returns:
        {
            "quotes": [{"code": "600519", "name": "...", "price": ..., ...}, ...],
            "futures": {"NQ": float|None, ...},
            "global_indices": [{"code": "N225", ...}, ...],
            "errors": [...],
            "fetch_time": "HH:MM:SS"
        }
    """
    errors: list[str] = []
    fetch_start = time.monotonic()

    quote_data: list[dict] = []

    if tencent_codes:
        prefixed = [_add_prefix(c) for c in tencent_codes]

        # 攒批发送路由：与 A股监控/持仓行情共用一个批量节奏（少请求，同批全查）
        raw = await tencent_bus.get(prefixed)
        if raw:
            quote_data, naming = _parse_tencent_quote(raw, tencent_codes)
            logger.info("候选池腾讯行情: %d/%d 成功", len(quote_data), len(tencent_codes))

            if naming:
                try:
                    import database as db
                    await db.update_candidate_names(naming)
                except Exception as e:
                    logger.warning("候选池名称回写失败: %s", e)
        else:
            errors.append("候选池腾讯接口请求失败")

    futures_data: dict[str, float | None] = {}
    if futures_codes or global_index_codes:
        futures_data = await fetch_futures_with_routing(
            futures_codes or [], global_index_codes
        )

    global_indices: list[dict] = []
    if global_index_codes:
        global_indices = await fetch_global_indices(global_index_codes)

    elapsed = time.monotonic() - fetch_start
    fetch_time = datetime.now().strftime("%H:%M:%S")

    logger.info(
        "候选池抓取完成: %d 条行情, %d 期指, %d 全球指数, 耗时 %.1fs",
        len(quote_data), len(futures_data), len(global_indices), elapsed,
    )

    return {
        "quotes": quote_data,
        "futures": futures_data,
        "global_indices": global_indices,
        "errors": errors,
        "fetch_time": fetch_time,
    }
