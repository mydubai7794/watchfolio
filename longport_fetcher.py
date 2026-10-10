"""长桥 OpenAPI 行情模块：美股扩展价(pre/post/overnight)/指数/VIX/锚点主源。

平台与 SDK（2026-10-04 切新平台）：PyPI 包已由 longport 更名为 longbridge，
新旧平台域名并存（旧 openapi.longportapp.cn / 新 openapi.longbridge.cn，新 app
仅新平台有效、旧 app 双平台通用）。本模块 longbridge 优先、longport 兜底，
无论哪个包都显式指向新平台境内加速域名。坑点全记录见项目根
《长桥OpenAPI踩坑记录.md》（401004 排查/平台迁移/指数符号等）。

凭证配置（sys_config 表优先热加载，环境变量 QDII_LP_* 兜底；到期换 token
无需改代码/重启，60s 内热加载）：
- LONGPORT_APP_KEY / LONGPORT_APP_SECRET / LONGPORT_ACCESS_TOKEN 三个键
  （sys_config）；环境变量 QDII_LP_APP_KEY / QDII_LP_APP_SECRET /
  QDII_LP_ACCESS_TOKEN 作为库缺项时的逐项回落；两处均未配置视为长桥
  不可用，自动走既有降级链（新浪冷备 / twelvedata 回切）
- 换 token 操作：长桥后台「开放平台 → 访问令牌」重新生成（选正式账户身份，
  模拟盘身份 token 会被行情网关 401004 拒绝）后执行
  UPDATE sys_config SET value='<新token>' WHERE `key`='LONGPORT_ACCESS_TOKEN';
- token 约 90 天有效：最后 7 天 token_status=expiring（前端横幅提醒）；且
  longbridge SDK 下自动续期（maybe_refresh_token：到期前 7 天触发、6h 节流、
  默认续 90 天，成功写回 sys_config 热加载）。longport 旧 SDK 无 refresh
  能力，维持手动换 token。
- 行情权限（正式账户实测）：美股 LV1 现货+盘前盘后+夜盘 ✅；港股/A股 ETF ✅；
  指数必须带 .US 后缀（.DJI.US/.NDX.US/.SPX.US/.VIX.US——不带后缀或 .HSI.HK
  静默空返回）；OPRA 美股期权实时行情 ❌（301604 无权限，期权链免费可查）；
  US30Y/US10Y/商品期货/外汇长桥无品种。

数据源分工：长桥为美股扩展价/指数/锚点主源，新浪 gb_ 降为冷备（长桥连续
失败 >=3 轮自动回切新浪，恢复后自动切回）。

错误码分流（约定口径）：
- 401004 token 失效        → 置 token_invalid 停止请求（自动续期成功后自愈）
- 301604/301603 权限/额度  → 立即切新浪冷备（fail_streak 直接推过阈值）
- 301606 请求频率超限      → 退避（60s 内不再请求，期间走新浪）
- 其他/网络错误            → fail_streak+1，>=3 轮自动回切新浪冷备；
  冷备期间每 300s 探测一次长桥，探测成功即恢复主源
"""

import asyncio
import base64
import json
import logging
import os
import time
from datetime import datetime, timezone

import config   # 触发 load_env_file：凭证环境变量在本模块使用前注入

logger = logging.getLogger(__name__)

# SDK 可选依赖：longbridge（新 SDK，包已改名）优先，longport（旧 SDK）兜底；
# 均未安装/导入失败时模块优雅降级（长桥不可用 → 调用方走新浪冷备）。
# 两包均提供 Config/QuoteContext/Period/AdjustType，差异在 Config 构造参数
# （longbridge 无 enable_overnight）与 refresh_access_token 能力（仅 longbridge）。
try:
    from longbridge.openapi import (Config as _LpConfig, QuoteContext as _LpQuoteContext,
                                    Period as _LpPeriod, AdjustType as _LpAdjustType)
    _SDK_OK, _SDK_NAME = True, "longbridge"
except Exception:
    try:
        from longport.openapi import (Config as _LpConfig, QuoteContext as _LpQuoteContext,
                                      Period as _LpPeriod, AdjustType as _LpAdjustType)
        _SDK_OK, _SDK_NAME = True, "longport"
    except Exception as _e:   # pragma: no cover - 部署环境缺包时的降级路径
        _SDK_OK, _SDK_NAME = False, ""
        _SDK_ERR = str(_e)
        _LpConfig = _LpQuoteContext = _LpPeriod = _LpAdjustType = None

# longbridge SDK 才有 Config.refresh_access_token（token 自动续期能力开关）
_SDK_HAS_REFRESH = bool(_SDK_OK and hasattr(_LpConfig, "refresh_access_token"))

# 新平台境内加速域名：新 app 仅新平台有效；旧 app 双平台通用（实测全通）。
# 两个 SDK 的 Config 均支持显式 http_url/quote_ws_url 覆盖，故无论装哪个包
# 都钉死新平台 .cn，避免 longport 旧 SDK 默认选旧平台域名（openapi.longportapp.cn）
LONGBRIDGE_HTTP_URL = "https://openapi.longbridge.cn"
LONGBRIDGE_QUOTE_WS_URL = "wss://openapi-quote.longbridge.cn/v2"

# ---------------------------------------------------------------------------
# 凭证（sys_config 优先热加载；库缺项回落环境变量 QDII_LP_*；全空视为未配置）
# ---------------------------------------------------------------------------

CFG_KEY_APP_KEY = "LONGPORT_APP_KEY"
CFG_KEY_APP_SECRET = "LONGPORT_APP_SECRET"
CFG_KEY_ACCESS_TOKEN = "LONGPORT_ACCESS_TOKEN"

ENV_KEY_APP_KEY = "QDII_LP_APP_KEY"
ENV_KEY_APP_SECRET = "QDII_LP_APP_SECRET"
ENV_KEY_ACCESS_TOKEN = "QDII_LP_ACCESS_TOKEN"

_CREDS_TTL_SEC = 60.0        # 凭证热加载周期（换 token 后最长 60s 生效）
_FAIL_THRESHOLD = 3          # 连续失败 >=3 轮 → 回切新浪冷备
_PROBE_INTERVAL_SEC = 300.0  # 冷备期间长桥探测间隔（恢复即切回主源）
_BACKOFF_SEC = 60.0          # 301606 频率超限退避窗口
_TOKEN_RENEW_WARN_DAYS = 7   # token 到期前 N 天进入 expiring 提醒（并触发自动续期）
_REFRESH_RETRY_SEC = 6 * 3600.0  # token 自动续期尝试节流（失败 6h 后重试）
_SDK_CALL_TIMEOUT = 15.0     # SDK 同步调用超时护栅（WebSocket 异常永久阻塞防护）
_REFRESH_CALL_TIMEOUT = 30.0 # refresh_access_token 超时护栅（HTTP 一次请求）
_LP_SEM = asyncio.Semaphore(2)  # 长桥并发护栅：官方限并发≤5、1s≤10次，SDK 内部
                                # 超限自动延迟（线程挂起排队）——调用侧主动收敛

# 模块级状态（进程内单例语义，与项目 fetcher 风格一致）
_creds_cache: dict | None = None
_creds_ts = 0.0
_creds_warned = False         # 凭证未配置 WARNING 只打一次的节流标记
_creds_mixed_warned = False   # 凭证来源混配 WARNING 只打一次的节流标记
_ctx = None                    # QuoteContext 惰性单例（同步对象）
_ctx_token: str | None = None  # 建ctx时的 token 指纹（换 token 重建）
_ctx_lock = asyncio.Lock()

fail_streak = 0                # 连续失败轮数（>=_FAIL_THRESHOLD 视为不可用）
last_error: str = ""           # 最近一次错误描述（状态页展示）
last_error_code: str = ""      # 最近一次错误码（401004/301603/301604/301606/...）
token_invalid = False          # 401004 后置位：停止请求直至换 token（热加载指纹变化）
_backoff_until = 0.0           # 301606 退避截止时刻（monotonic）
_probe_at = 0.0                # 冷备期间上次探测时刻（monotonic）
_token_warn_at = 0.0           # expiring 日志告警节流（每天一条）
_refresh_at = 0.0              # 上次 token 自动续期尝试时刻（monotonic，6h 节流）
_refresh_result = ""           # 最近一次续期结果（状态页展示，如 "ok 10-04 +90d"）


def _parse_token_exp(token: str) -> float | None:
    """解码 JWT payload 的 exp（UTC 秒）；非 JWT/解析失败返回 None。"""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        exp = float(data.get("exp") or 0)
        return exp if exp > 0 else None
    except Exception:
        return None


def _token_days_left(token: str) -> float | None:
    exp = _parse_token_exp(token)
    if exp is None:
        return None
    return (exp - time.time()) / 86400.0


def _env_creds() -> dict:
    """环境变量兜底凭证（QDII_LP_* 前缀；sys_config 缺项时逐项回落）。@author yys"""
    return {"app_key": os.getenv(ENV_KEY_APP_KEY, ""),
            "app_secret": os.getenv(ENV_KEY_APP_SECRET, ""),
            "access_token": os.getenv(ENV_KEY_ACCESS_TOKEN, "")}


async def _load_creds() -> dict:
    """读凭证：sys_config 优先（热加载），库缺项回落环境变量；全空视为未配置。"""
    global _creds_cache, _creds_ts, _creds_warned, _creds_mixed_warned
    now = time.monotonic()
    if _creds_cache and now - _creds_ts < _CREDS_TTL_SEC:
        return _creds_cache
    creds = _env_creds()
    srcs = {"app_key": "env", "app_secret": "env", "access_token": "env"}
    try:
        import database as db
        for cfg_key, field in ((CFG_KEY_APP_KEY, "app_key"),
                               (CFG_KEY_APP_SECRET, "app_secret"),
                               (CFG_KEY_ACCESS_TOKEN, "access_token")):
            v = await db.get_config(cfg_key)
            if v:
                creds[field] = v
                srcs[field] = "sys_config"
    except Exception as e:
        logger.warning("长桥凭证读库失败(用环境变量兜底): %s", e)
    if all(creds.values()):
        _creds_warned = False   # 凭证恢复配置后允许下次缺失再告警
    elif not _creds_warned:
        _creds_warned = True
        logger.warning("长桥凭证未配置(sys_config 与 QDII_LP_* 环境变量均缺)，"
                       "长桥源停用，行情走既有降级链")
    if len(set(srcs.values())) > 1:
        # 混配（如 key 来自 sys_config、secret/token 来自 env）：多半是换凭证时
        # 只改了一处，签名必败——节流告警一次，混配消除后重置允许再告警
        if not _creds_mixed_warned:
            _creds_mixed_warned = True
            logger.warning("长桥凭证来源混配：key=%s secret=%s token=%s，"
                           "如遇签名失败请核对 sys_config 与 qdii.env 是否同一应用",
                           srcs["app_key"], srcs["app_secret"],
                           srcs["access_token"])
    else:
        _creds_mixed_warned = False
    _creds_cache, _creds_ts = creds, now
    return creds


async def _get_ctx():
    """惰性建立 QuoteContext（to_thread 内同步调用；换 token 自动重建）。

    重建时机：凭证热加载后 token 指纹变化 → 旧 ctx 关闭重建，同时清
    token_invalid 标记（新 token 给一次机会，401004 会重新置位）。
    """
    global _ctx, _ctx_token, token_invalid
    if not _SDK_OK:
        return None
    creds = await _load_creds()
    if not all(creds.values()):
        return None   # 凭证未配置：不建 ctx（视为长桥不可用，走既有降级链）
    token = creds["access_token"]
    async with _ctx_lock:
        if _ctx is not None and _ctx_token == token:
            return _ctx
        old, _ctx, _ctx_token = _ctx, None, token
        token_invalid = False
        try:
            if old is not None:
                try:
                    old.close()
                except Exception:
                    pass
            def _build():
                # 显式钉死新平台 .cn 域名（新 app 仅新平台有效；旧 app 双平台
                # 通用）。longbridge 新 SDK 无 enable_overnight 参数（夜盘价
                # 默认可用）；longport 旧 SDK 开 enable_overnight 后 quote 的
                # overnight_quote 子对象才有值（极旧版本不支持域名参数时回退
                # 缺省构造，SDK 自动选域）
                if _SDK_NAME == "longbridge":
                    cfg = _LpConfig(creds["app_key"], creds["app_secret"], token,
                                    http_url=LONGBRIDGE_HTTP_URL,
                                    quote_ws_url=LONGBRIDGE_QUOTE_WS_URL)
                else:
                    try:
                        cfg = _LpConfig(creds["app_key"], creds["app_secret"], token,
                                        http_url=LONGBRIDGE_HTTP_URL,
                                        quote_ws_url=LONGBRIDGE_QUOTE_WS_URL,
                                        enable_overnight=True)
                    except TypeError:
                        cfg = _LpConfig(creds["app_key"], creds["app_secret"], token,
                                        enable_overnight=True)
                return _LpQuoteContext(cfg)
            _ctx = await asyncio.wait_for(asyncio.to_thread(_build),
                                          _SDK_CALL_TIMEOUT)
            logger.info("长桥 QuoteContext 已建立 (token 指纹 %s…)", token[:18])
        except Exception as e:
            logger.warning("长桥 QuoteContext 建立失败: %s", e)
        return _ctx


def _extract_error_code(exc: Exception) -> str:
    """从 OpenApiException 提取错误码（消息形如 ... 401004 ... / code=301606）。"""
    msg = str(exc)
    code = getattr(exc, "code", None)
    if code:
        return str(code)
    import re as _re
    m = _re.search(r"\b(40100[0-9]|30160[0-9])\b", msg)
    return m.group(1) if m else ""


def _mark_failure(exc: Exception | None, code: str = "", detail: str = "") -> None:
    """失败记账与错误码分流（模块级状态，供回切判定与状态页）。"""
    global fail_streak, token_invalid, _backoff_until, last_error, last_error_code
    last_error = detail or (str(exc)[:160] if exc else "unknown")
    last_error_code = code or (_extract_error_code(exc) if exc else "")
    if last_error_code == "401004":
        token_invalid = True
        logger.error("长桥 token 失效(401004)：请在长桥后台重新生成（正式账户身份）"
                     "并 UPDATE sys_config 的 %s，60s 内热加载", CFG_KEY_ACCESS_TOKEN)
    elif last_error_code in ("301603", "301604"):
        fail_streak = _FAIL_THRESHOLD   # 权限/额度问题直接切冷备
        logger.warning("长桥权限/额度受限(%s)：切换新浪冷备", last_error_code)
    elif last_error_code == "301606":
        _backoff_until = time.monotonic() + _BACKOFF_SEC
        fail_streak += 1
        logger.warning("长桥频率超限(301606)：退避 %ss", int(_BACKOFF_SEC))
    else:
        fail_streak += 1


def _mark_success() -> None:
    global fail_streak, last_error, last_error_code
    if fail_streak or last_error:
        logger.info("长桥行情恢复主源（此前 %s%s）", last_error_code or "网络失败",
                    f"/{fail_streak}轮" if fail_streak else "")
    fail_streak = 0
    last_error, last_error_code = "", ""


def is_available() -> bool:
    """长桥当前是否可用（供调用方决定主/冷备与状态页）。"""
    if not _SDK_OK or token_invalid:
        return False
    if time.monotonic() < _backoff_until:
        return False
    return fail_streak < _FAIL_THRESHOLD


def status() -> dict:
    """数据源状态（/api/datasource/status 下发、前端横幅依据）。"""
    token = ((_creds_cache or {}).get("access_token")
             or os.getenv(ENV_KEY_ACCESS_TOKEN, ""))
    days = _token_days_left(token)
    if not _SDK_OK or not token:
        token_status = "missing"
    elif token_invalid:
        token_status = "invalid"
    elif days is None:
        token_status = "unknown"
    elif days <= 0:
        token_status = "expired"
    elif days <= _TOKEN_RENEW_WARN_DAYS:
        token_status = "expiring"
        now = time.time()
        global _token_warn_at
        if now - _token_warn_at > 86400:   # 到期提醒日志：每天一条
            _token_warn_at = now
            if _SDK_HAS_REFRESH:
                logger.warning("长桥 token 将于 %.1f 天后到期（自动续期将触发，"
                               "结果见 %s）", days, _refresh_result or "尚未尝试")
            else:
                logger.warning("长桥 token 将于 %.1f 天后到期，请尽快在长桥后台"
                               "重新生成并 UPDATE sys_config（当前 SDK 无自动"
                               "续期能力）", days)
    else:
        token_status = "ok"
    exp = _parse_token_exp(token)
    return {
        "sdk_ok": _SDK_OK,
        "sdk_name": _SDK_NAME,
        "auto_refresh": _SDK_HAS_REFRESH,
        "available": is_available(),
        "fallback_active": _SDK_OK and not is_available(),
        "fail_streak": fail_streak,
        "last_error": last_error,
        "last_error_code": last_error_code,
        "token_status": token_status,
        "token_expires_at": (datetime.fromtimestamp(exp, tz=timezone.utc)
                             .strftime("%Y-%m-%d") if exp else None),
        "days_left": round(days, 1) if days is not None else None,
        "token_refresh": _refresh_result or None,
    }


async def maybe_refresh_token() -> str | None:
    """token 到期自动续期（新平台 Legacy API Key 模式，默认续 90 天）。

    触发条件：token 可解析 exp 且 days_left <= _TOKEN_RENEW_WARN_DAYS（含已
    过期）；节流 _REFRESH_RETRY_SEC（成功后新 token 天数充足自然退出循环，
    失败 6h 后重试）。成功链路：校验新 token（hk_ 前缀 + exp 可解析 + 余量
    足够）→ UPSERT sys_config → 失效凭证缓存（加急跳过 60s 热加载 TTL）→
    清 token_invalid；QuoteContext 由 token 指纹变化自动重建。
    仅 longbridge SDK 具备 refresh 能力（longport 旧 SDK 无此方法）。
    返回新 token（未触发/失败返回 None）；内部全捕获，调用方可放心每轮调。
    """
    global _refresh_at, _refresh_result, _creds_cache, _creds_ts, token_invalid
    if not _SDK_OK or not _SDK_HAS_REFRESH:
        return None
    if time.monotonic() - _refresh_at < _REFRESH_RETRY_SEC:
        return None
    creds = await _load_creds()
    token = creds["access_token"]
    days = _token_days_left(token)
    if days is None or days > _TOKEN_RENEW_WARN_DAYS:
        return None   # 余量充足，不消耗 refresh 次数
    _refresh_at = time.monotonic()
    try:
        def _do_refresh():
            cfg = _LpConfig(creds["app_key"], creds["app_secret"], token,
                            http_url=LONGBRIDGE_HTTP_URL,
                            quote_ws_url=LONGBRIDGE_QUOTE_WS_URL)
            return cfg.refresh_access_token()   # 默认续 90 天
        new_token = await asyncio.wait_for(asyncio.to_thread(_do_refresh),
                                           _REFRESH_CALL_TIMEOUT)
        new_token = str(new_token or "")
        nd = _token_days_left(new_token)
        if (not new_token.startswith("hk_") or nd is None
                or nd <= _TOKEN_RENEW_WARN_DAYS):
            _refresh_result = f"校验失败(days={nd})"
            logger.warning("长桥 token 自动续期返回异常值，忽略（6h 后重试）")
            return None
        import database as db
        await db.set_config(CFG_KEY_ACCESS_TOKEN, new_token, "长桥行情 token（自动续期）")
        _creds_cache, _creds_ts = None, 0.0   # 加急失效，下轮 _load_creds 立即重读
        token_invalid = False
        _refresh_result = f"ok {datetime.now().strftime('%m-%d %H:%M')} +{nd:.0f}天"
        logger.info("长桥 token 自动续期成功（剩余 %.0f 天，已写库热加载）", nd)
        return new_token
    except Exception as e:
        _refresh_result = f"失败: {str(e)[:120]}"
        logger.warning("长桥 token 自动续期失败(6h 后重试): %s", e)
        return None


# ---------------------------------------------------------------------------
# 行情取数（quote 为同步 RPC，to_thread 包装；一次批量带全部 symbol）
# ---------------------------------------------------------------------------

def _ext_from_quote(q, session: str) -> tuple[float, str]:
    """从 SecurityQuote 取扩展价：pre/post 段取对应真实扩展价，closed 夜间
    取 overnight（官方夜盘价）退化 post（与新浪夜间「最后盘后价」口径对齐）。
    返回 (ext_price, ext_time)；无扩展价返回 (0.0, "")。
    """
    src = None
    if session == "pre":
        src = getattr(q, "pre_market_quote", None)
    elif session == "post":
        src = getattr(q, "post_market_quote", None)
    else:
        src = getattr(q, "overnight_quote", None) or getattr(q, "post_market_quote", None)
    price = 0.0
    ts_text = ""
    if src is not None:
        price = float(str(getattr(src, "last_done", 0) or 0) or 0)
        ts = getattr(src, "timestamp", None)
        if ts is not None:
            try:
                ts_text = "长桥 " + ts.astimezone().strftime("%m-%d %H:%M")
            except Exception:
                ts_text = "长桥"
    return price, ts_text


async def _sdk_call(fn, *args):
    """SDK 同步调用线程化 + 并发信号量 + 超时护栅。

    长桥限制：一个账号一条长连接、1s≤10 次调用、并发请求≤5，SDK 内部超限
    会自动延迟请求（调用线程挂起排队）；若叠加 WebSocket 断线重连，排队
    可能长期不消费（曾挂死持仓与看板双循环）——调用侧信号量收敛并发≤2，
    wait_for 超时后放弃等待并废弃当前 ctx（下次调用重建新连接）。
    """
    global _ctx
    async with _LP_SEM:
        try:
            return await asyncio.wait_for(asyncio.to_thread(fn, *args),
                                          _SDK_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            _ctx = None
            _mark_failure(None, "", f"SDK 调用超时(>{_SDK_CALL_TIMEOUT:.0f}s), 废弃连接")
            raise


async def _quote_raw(symbols: list[str]) -> list:
    """批量 quote（SDK 同步调用线程化）。失败/不可用返回 []。"""
    if not _SDK_OK or token_invalid or not symbols:
        return []
    if time.monotonic() < _backoff_until:
        return []
    ctx = await _get_ctx()
    if ctx is None:
        return []
    try:
        quotes = await _sdk_call(ctx.quote, symbols)
        return quotes or []
    except asyncio.TimeoutError:
        return []   # 已在 _sdk_call 记账（超时废弃连接）
    except Exception as e:
        _mark_failure(e)
        return []


async def fetch_us_ext(us_symbols: list[str], session: str) -> dict[str, dict] | None:
    """美股扩展价（主源入口）：一次批量带全部美股持仓。

    返回 {sym: {ext_price, ext_time}}（与 parse_sina_us 出参字段同构，
    fetch_quotes 直接合并）；长桥不可用/本轮失败返回 None → 调用方回切新浪。
    """
    if not us_symbols:
        return None
    quotes = await _quote_raw([f"{s}.US" for s in us_symbols])
    if not quotes:
        if is_available():
            _mark_failure(None, "", "空响应")
        return None
    _mark_success()
    out: dict[str, dict] = {}
    for q in quotes:
        sym = str(getattr(q, "symbol", "")).replace(".US", "", 1)
        price, ts_text = _ext_from_quote(q, session)
        if price > 0:
            out[sym] = {"ext_price": price, "ext_time": ts_text or "长桥"}
    return out


async def fetch_quote_map(symbols: list[str]) -> dict[str, dict]:
    """通用批量行情（VIX/.DJI/.NDX 等）：{symbol: {price, prev, change_pct}}。

    冷备期间做恢复探测（每 _PROBE_INTERVAL_SEC 一次，成功即回主源）。
    """
    global _probe_at
    probing = (not is_available()) and time.monotonic() - _probe_at >= _PROBE_INTERVAL_SEC
    if probing:
        _probe_at = time.monotonic()
    if not is_available() and not probing:
        return {}
    quotes = await _quote_raw(symbols)
    if not quotes:
        return {}
    _mark_success()
    out: dict[str, dict] = {}
    for q in quotes:
        last = float(str(getattr(q, "last_done", 0) or 0) or 0)
        prev = float(str(getattr(q, "prev_close", 0) or 0) or 0)
        if last > 0:
            out[str(getattr(q, "symbol", ""))] = {
                "price": last, "prev": prev,
                "change_pct": (last - prev) / prev * 100 if prev > 0 else 0.0,
            }
    return out


async def _candles_raw(symbol: str) -> list:
    """单只日K（SDK 同步调用线程化）：最近 2 根、不复权。失败/不可用返回 []。"""
    if not _SDK_OK or token_invalid or not symbol:
        return []
    if time.monotonic() < _backoff_until:
        return []
    ctx = await _get_ctx()
    if ctx is None:
        return []
    try:
        ks = await _sdk_call(ctx.candlesticks, symbol, _LpPeriod.Day, 2,
                             _LpAdjustType.NoAdjust)
        return list(ks or [])
    except asyncio.TimeoutError:
        return []   # 已在 _sdk_call 记账（超时废弃连接）
    except Exception as e:
        _mark_failure(e)
        return []


async def fetch_anchor_quotes(us_symbols: list[str]) -> dict[str, dict]:
    """一场口径锚点恢复（长桥日K主源）：{sym: {"prev", "open", "close"}}。

    - prev=昨日收盘（=场基准，与腾讯 f[4]/TD previous_close 同口径）；
    - close=当日常规收盘（=16:00 定格，夜盘时段日K最后一根必为已收盘完整K线）；
    - open=当日开盘（prev 缺失时的退化锚，与 TD 锚点出参同构）。
    逐只顺序调用（SDK 单 symbol 接口；官方限并发≤5、SDK 内部超限自动延迟
    排队——gather 并发只多占线程不提速，顺序逐只贴合频控设计）；单只失败
    自然缺省 → 调用方回切 twelvedata 兜底（锚点恢复不再与外汇主价挤占
    TD 免费额度）。
    """
    if not us_symbols:
        return {}
    out: dict[str, dict] = {}
    for s in us_symbols:
        try:
            ks = await _candles_raw(f"{s}.US")
        except Exception as e:
            _mark_failure(e)
            continue
        ks = sorted(ks or [], key=lambda k: getattr(k, "timestamp", None) or
                    datetime.min.replace(tzinfo=timezone.utc))
        if not ks:
            continue
        close = float(str(getattr(ks[-1], "close", 0) or 0) or 0)
        if close <= 0:
            continue
        prev = float(str(getattr(ks[-2], "close", 0) or 0) or 0) if len(ks) >= 2 else 0.0
        out[s] = {"prev": prev,
                  "open": float(str(getattr(ks[-1], "open", 0) or 0) or 0),
                  "close": close}
    return out


async def fetch_vix() -> dict | None:
    """VIX 恐慌指数（长桥 .VIX.US 实时，替代 Yahoo ^VIX 代理 backfill）。"""
    m = await fetch_quote_map([".VIX.US"])
    return m.get(".VIX.US")


# 看板指数符号映射（新平台指数必须带 .US 后缀：.NDX 静默空、.NDX.US 正常；
# .SPX.US=标普500（旧平台 .INX 静默空是旧平台现象）；.HSI.HK 静默空恒指走腾讯）
INDEX_SYMBOLS = {".VIX.US": "VIX", ".DJI.US": "DJI",
                 ".NDX.US": "NDX", ".SPX.US": "SPX"}


async def fetch_index_quotes() -> dict[str, dict]:
    """看板指数批量（VIX/道指/纳指100/标普500，一次 quote 调用取全四项）。

    返回 {短名: {price, prev, change_pct}}（缺项自然省略）；冷备期间复用
    fetch_quote_map 的 300s 探测节流。供 server _market_loop 60s 轮询注入
    yahoo_day（长桥不可用交还 Yahoo ^VIX/^DJI/^NDX/^GSPC 兜底）。
    """
    m = await fetch_quote_map(list(INDEX_SYMBOLS))
    return {name: m[sym] for sym, name in INDEX_SYMBOLS.items() if sym in m}


async def probe_available() -> bool:
    """冷备期间的主动探测（供外部循环低频调用）：成功即恢复主源。"""
    if is_available():
        return True
    m = await fetch_quote_map([".VIX.US"])
    return bool(m)


async def fetch_daily_bars_raw(symbol: str, count: int = 1000) -> list:
    """长桥前复权日K（count≤1000）。失败/不可用返回 []。@author yys

    日K底座主源（daily_bars 增量落库 + 除权检测）：前复权 ForwardAdjust
    使指标与收益口径在除权/拆股事件后天然连续（回测已实测该参数可用）。
    单 symbol 逐只调用，调用方需自行串行分散（限流契约 1s≤10 次）。
    """
    if not _SDK_OK or token_invalid or not symbol:
        return []
    if time.monotonic() < _backoff_until:
        return []
    ctx = await _get_ctx()
    if ctx is None:
        return []
    try:
        ks = await _sdk_call(ctx.candlesticks, symbol, _LpPeriod.Day,
                             max(2, min(int(count), 1000)),
                             _LpAdjustType.ForwardAdjust)
        return list(ks or [])
    except asyncio.TimeoutError:
        return []   # 已在 _sdk_call 记账（超时废弃连接）
    except Exception as e:
        _mark_failure(e)
        return []
