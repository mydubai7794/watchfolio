"""长桥推送（WS 订阅）：美股持仓实时 quote → quotes_hub（shadow/live 双模式）。

模式开关 sys_config.LP_PUSH_MODE（默认 shadow，60s 缓存热切换）：
- shadow：只计数 + 周期统计日志，不写 quotes_hub、不触发提醒评估（安全观测期）；
- live：  quotes_hub.push_tick 喂入内存真相源 + _pending_eval 排队交提醒引擎，
  产生的提醒事件经模块级 on_events 钩子交宿主分发（server.py 注册：邮件
  入队 + SSE 广播），回调异常全捕获不炸循环。

连接裁决（踩坑记录 §2.6 官方限流契约「单账号一条长连接」）：不复刻独立
QuoteContext（那会是第二条长连接，可能被网关顶号/拒绝），而是**复用
longport_fetcher._get_ctx() 单例**——订阅与拉取共用一条连接；fetcher 侧
调用超时废弃 ctx / 换 token 重建都会更换 ctx 实例，守护循环按对象身份检测
变化后自动重挂回调与订阅（重挂幂等，身份未变不重复订阅）。

线程契约：_handle_quote/_handle_depth 在长桥 SDK 回调线程执行——只做轻量
路由与计数（quotes_hub.push_tick 线程安全可直调；_pending_eval 单键赋值
原子），提醒求值统一回事件循环线程由 eval_drain 批量消费；回调壳全捕获，
异常绝不外抛炸 SDK 线程。

避坑（详见《长桥OpenAPI踩坑记录.md》）：订阅符号必须带 .US 后缀，回调路由
前剥离；Decimal 统一 float 归一；订阅失败（如 Depth 无权限 301604 同类）
降级仅 Quote，不炸主循环；401004 token 失效不反复撞墙，30s 间隔等待换
token / 自动续期热加载自愈。@author yys
"""
import asyncio
import logging
import time

import alert_engine
import database as db
import longport_fetcher as lp
import quotes_hub

logger = logging.getLogger("qdii.lp_push")

# SubType 与 Config 同层（longbridge.openapi / longport.openapi）；fetcher
# 已判定 SDK 可用性（_SDK_OK），此处仅订阅类型枚举，缺包时保持 None 降级
try:
    from longbridge.openapi import SubType as _SubType
except Exception:
    try:
        from longport.openapi import SubType as _SubType
    except Exception:
        _SubType = None

# 模块级状态（进程内单例语义）
_mode_cache: dict = {"v": "shadow", "at": 0.0}   # LP_PUSH_MODE 缓存（60s 热加载）
_pending_eval: dict[str, float] = {}             # live 待评估价 {sym: price}
_stats = {"connected_at": 0.0, "quotes": 0, "depths": 0, "reconnects": 0}
_armed_ctx = None                                # 已挂回调/订阅的 ctx（身份比对）
_armed_symbols: list[str] = []                   # 已订阅标的集（含退订失败待重试残留）
_drift_check_at = 0.0                            # 标的漂移复核节流（monotonic）
_log_at = 0.0                                    # 统计日志节流（monotonic）

# 提醒事件分发钩子（宿主接线点）：server.py lifespan 注册为 async callable
# （邮件入队 + SSE 广播）；None 时事件仅落库不通知。避免 server.py 的
# _broadcast/mailer 被 import 进推送模块内部造成反向耦合。
on_events = None

_MODE_TTL = 60.0        # 模式热切换检查周期（sys_config 变更下一检查点生效）
_DRAIN_INTERVAL = 1.0   # 守护循环节拍（eval_drain 1s 节流由本循环保证）
_DRIFT_TTL = 300.0      # 订阅标的漂移复核周期（跟随持仓增删）
_BACKOFF_MAX = 60.0     # 断连重连退避上限
_TOKEN_WAIT_SEC = 30.0  # 401004 等待自愈间隔
_EMPTY_WAIT_SEC = 300.0 # 无美股持仓等待间隔

_sleep = asyncio.sleep   # 测试注入点（生产即 asyncio.sleep）


def _mode() -> str:
    """当前推送模式（缓存值直读，SDK 回调线程安全——无 IO）。"""
    return _mode_cache["v"] or "shadow"


async def _refresh_mode():
    """LP_PUSH_MODE 热加载（60s 节流；失败回落 shadow 不放大请求）。

    时间戳先推进再读库：读库挂起/失败也保持节流节奏，不会退化成高频重试。
    """
    now = time.monotonic()
    if now - _mode_cache["at"] < _MODE_TTL:
        return
    _mode_cache["at"] = now
    try:
        v = await db.get_config("LP_PUSH_MODE")
        _mode_cache["v"] = (v or "shadow").strip() or "shadow"
    except Exception:
        _mode_cache["v"] = "shadow"


def on_price(symbol: str, price: float | None, bid: float | None = None,
             ask: float | None = None):
    """报价路由（SDK 回调线程调用，只做轻量分发）。

    shadow：仅计数（观测靠周期统计日志）；live：喂 quotes_hub + 排队提醒
    评估。.US 后缀在此剥离（订阅侧加、回调侧剥，展示与 hub 全用裸符号）。
    """
    _stats["quotes"] += 1
    sym = symbol[:-3] if symbol.endswith(".US") else symbol
    if not sym or not price or price <= 0:
        return                      # 零价/空价不喂（防污染内存真相源）
    if _mode() != "live":
        return
    try:
        quotes_hub.push_tick(sym, price, source="lp_push", bid=bid, ask=ask)
    except Exception as e:
        logger.warning("推送 tick 写入异常 %s: %s", sym, e)
        return
    _pending_eval[sym] = price


def _to_f(v) -> float | None:
    """SDK 数值归一（Decimal/str → float）；无效/非正值返回 None。"""
    try:
        f = float(str(v)) if v is not None else 0.0
    except Exception:
        return None
    return f if f > 0 else None


def _handle_quote(ctx, event):
    """SDK quote 回调壳（SDK 线程）：全捕获，异常绝不上抛炸 SDK 线程。"""
    try:
        on_price(str(getattr(event, "symbol", "") or ""),
                 _to_f(getattr(event, "last_done", None)),
                 _to_f(getattr(event, "bid", None)),
                 _to_f(getattr(event, "ask", None)))
    except Exception as e:
        logger.warning("长桥推送 quote 回调异常: %s", e)


def _handle_depth(ctx, event):
    """SDK depth 回调壳：LV1 深度只计数（价格真相源仅 quote，深度不喂价）。"""
    try:
        _stats["depths"] += 1
    except Exception:
        pass


async def eval_drain():
    """把待评估价批量交给提醒引擎（1s 节流由守护循环保证）。

    非 live（含切回 shadow）时清空残留——模式切换不泄漏陈旧价；求值异常
    内部消化（提醒引擎自身有全捕获，此处双保险保证守护循环不炸）。

    线程论证（引用交换 vs dict copy+clear vs 加锁）：SDK 回调线程经
    on_price 尾部对 _pending_eval 做单键写入（每次经模块全局名查找拿
    最新引用），与本循环的消费并发。用「引用交换」——交换是单条
    STORE_GLOBAL 字节码（CPython GIL 下原子），回调写入要么落入旧 dict
    随本批消费、要么落入新 dict 留待下轮，两个方向都不丢；而
    copy+clear 两步之间回调写入会被 clear 吞掉（丢更新窗口）。相比
    threading.Lock：本处竞争点只有 swap 一步，交换对回调热路径零锁
    开销；quotes_hub Sampler 需要锁是因为其内部是跨线程多步读-改-写，
    互斥粒度需求不同，二者风格各自成立。
    """
    global _pending_eval
    await _refresh_mode()
    if not _pending_eval or _mode() != "live":
        _pending_eval.clear()
        return []
    prices = _pending_eval
    _pending_eval = {}
    try:
        return await alert_engine.evaluate_round(prices)
    except Exception as e:
        logger.warning("推送侧提醒评估异常（丢弃本批 %d 只）: %s", len(prices), e)
        return []


async def _load_us_symbols() -> list[str]:
    """订阅标的：启用持仓中的美股（与 daily_bars/alert_engine 同口径）。"""
    holdings = await db.get_holdings(enabled_only=True)
    return [h["symbol"] for h in holdings if h.get("market") == "US"]


def _sub_types() -> list:
    """订阅类型（Quote 必选；Depth 可用则带上，回调侧只计数观测）。"""
    if _SubType is None:
        return []
    types = [_SubType.Quote]
    if getattr(_SubType, "Depth", None) is not None:
        types.append(_SubType.Depth)
    return types


async def _call(fn, *args):
    """SDK 同步调用轻量护栅：复用 fetcher 的并发信号量与超时栅。

    刻意不走 lp._sdk_call——其超时/失败会做拉取侧错误码分流记账（如
    301604 会把拉取侧直接打冷备），订阅侧失败不应污染该状态机；此处仅
    保留并发收敛与超时护栅，超时废弃连接交由双方按 ctx 身份自愈。
    """
    async with lp._LP_SEM:
        try:
            return await asyncio.wait_for(asyncio.to_thread(fn, *args),
                                          lp._SDK_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            lp._ctx = None            # 连接疑已坏态：废弃，双方各自重建
            raise


async def _arm(ctx, syms: list[str]):
    """在共享 ctx 上挂回调 + 订阅/退订差集。

    幂等：同 ctx 且标的集一致时不做任何动作；同 ctx 标的集变化（持仓
    漂移）时先对差集退订（gone=旧-新，防订阅集只增不减），再全量订阅
    新集合。Depth 订阅失败（如无权限 301604 同类）降级仅 Quote；仅
    Quote 也失败则上抛交外层退避重试。退订失败仅 warning 且把 gone
    保留在 _armed_symbols（下轮差集非空自然重试），不阻断订阅。
    """
    global _armed_ctx, _armed_symbols
    prev = sorted(_armed_symbols or [])
    want = sorted(syms)
    same_ctx = ctx is _armed_ctx
    if same_ctx and want == prev:
        return
    pending_gone: list[str] = []
    if same_ctx:
        gone = sorted(set(prev) - set(want))
        if gone:
            try:
                await _call(ctx.unsubscribe, [f"{s}.US" for s in gone],
                            _sub_types())
            except Exception as e:
                pending_gone = gone
                logger.warning("长桥退订失败 %s（下轮差集重试）: %s", gone, e)
    await _call(ctx.set_on_quote, _handle_quote)
    try:
        await _call(ctx.set_on_depth, _handle_depth)
    except Exception as e:
        logger.warning("长桥 set_on_depth 注册失败（深度计数降级）: %s", e)
    sub = [f"{s}.US" for s in syms]     # 长桥铁律：订阅符号必须带 .US 后缀
    types = _sub_types()
    try:
        await _call(ctx.subscribe, sub, types)
    except Exception as e:
        if len(types) > 1:
            logger.warning("长桥 Quote+Depth 订阅失败(%s)，降级仅 Quote", e)
            await _call(ctx.subscribe, sub, types[:1])
        else:
            raise
    _armed_ctx = ctx
    # 退订失败的 gone 计入已订阅集（实际仍挂着）：下轮差集非空触发重试
    _armed_symbols = sorted(set(want) | set(pending_gone))


async def _dispatch_events(events: list[dict]):
    """提醒事件分发（live 独家窗口触发，事件已落库+冷却置位）。

    经模块级 on_events 钩子交宿主接线（server.py lifespan 注册：邮件入队
    + SSE 广播）；未注册（独立运行/未集成）时静默跳过。钩子异常全捕获
    ——通知通道故障绝不炸推送守护循环，丢通知不丢状态（事件已落库）。
    """
    cb = on_events
    if cb is None:
        return
    try:
        await cb(events)
    except Exception as e:
        logger.warning("长桥推送事件回调异常（%d 条通知丢失）: %s", len(events), e)


async def _symbols_drifted() -> bool:
    """订阅标的漂移复核（300s 节流）：持仓增删后自动跟随重订阅。

    比对基准用模块级 _armed_symbols（实际已订阅集，含退订失败待重试
    残留）而非外层局部盘点值：退订失败残留时 armed ⊋ latest，差集非空
    即判 drift → 触发外层重盘 → _arm 差集重试退订；若以局部值为基准，
    残留场景下 latest == 局部值恒等，drift 恒 False，重试死路。
    """
    global _drift_check_at
    now = time.monotonic()
    if now - _drift_check_at < _DRIFT_TTL:
        return False
    _drift_check_at = now
    try:
        latest = await _load_us_symbols()
    except Exception as e:
        logger.warning("长桥推送标的复核失败: %s", e)
        return False
    if set(latest) != set(_armed_symbols):
        logger.info("长桥推送标的集变化 %s → %s，准备重订阅",
                    sorted(_armed_symbols), sorted(latest))
        return True
    return False


def _stats_log_throttled():
    """周期统计日志（60s 一行）：shadow 观测期 quotes/depths 计数即心跳。"""
    global _log_at
    now = time.monotonic()
    if now - _log_at < _MODE_TTL:
        return
    _log_at = now
    logger.info("长桥推送统计 %s", status())


def status() -> dict:
    """推送观测状态（shadow 观测期 / 排障出口）。"""
    return {"mode": _mode(), "armed": _armed_ctx is not None,
            "symbols": len(_armed_symbols), "sdk": lp._SDK_NAME,
            **_stats}


async def push_loop(us_symbols: list[str] | None = None,
                    _iterations: int | None = None):
    """后台守护循环：武装（挂回调+订阅）→ 1s 节拍 drain → 自愈。

    断连重连：外层指数退避 1s 起步、×2、上限 60s，成功武装后归 1；
    拉取侧废弃/token 重建换 ctx 实例 → 内层按身份检测 break → 外层重挂
    （不用 SDK 断连回调 API，避免依赖未实证签名，身份比对语义等价且
    对 SDK 内部自动重连透明）。_iterations 仅测试注入（限制轮数）。
    """
    if not lp._SDK_OK:
        logger.warning("长桥 SDK 不可用，推送循环退出（实时价由轮询链路兜底）")
        return
    backoff = 1.0
    iters = 0
    while _iterations is None or iters < _iterations:
        iters += 1
        try:
            if lp.token_invalid:
                await _sleep(_TOKEN_WAIT_SEC)   # 401004 等换 token/自动续期自愈
                continue
            if us_symbols is None:
                us_symbols = await _load_us_symbols()
            if not us_symbols:
                await _sleep(_EMPTY_WAIT_SEC)   # 无美股持仓：等待后重新盘点
                us_symbols = None
                continue
            ctx = await lp._get_ctx()
            if ctx is None:
                raise RuntimeError("长桥 QuoteContext 不可用（建立失败）")
            await _arm(ctx, us_symbols)
            _stats["connected_at"] = time.time()
            _stats["reconnects"] += 1
            backoff = 1.0
            logger.info("长桥推送已连接并订阅 %d 只（%s SDK，mode=%s）",
                        len(us_symbols), lp._SDK_NAME, _mode())
            while _iterations is None or iters < _iterations:
                iters += 1
                await _sleep(_DRAIN_INTERVAL)
                _evs = await eval_drain()       # 内含模式热加载（60s 节流）
                if _evs:
                    await _dispatch_events(_evs)  # live 提醒事件 → 宿主通知接线
                _stats_log_throttled()
                if lp._ctx is not ctx:          # 连接被废弃/重建 → 重挂
                    logger.info("长桥连接已重建，推送重新挂载回调与订阅")
                    ctx = None
                    break
                if await _symbols_drifted():    # 基准=_armed_symbols（含退订残留）
                    us_symbols = None           # 下轮外层重新盘点持仓
                    break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("长桥推送异常，%.0fs 后重试: %s", backoff, e)
            try:
                await _sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2, _BACKOFF_MAX)
