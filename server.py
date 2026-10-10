"""FastAPI 主服务：SSE 推送 + REST 管理 API + 后台监控任务。"""

import asyncio
import base64
import hmac
import io
import json
import logging
import random
import re
import secrets
import time
from datetime import datetime, timedelta, timezone, date
from typing import Optional

from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import alert_backtest
import alert_engine
import database as db
import daily_bars
import email_trader as et
import fetcher
import ledger
import longport_push as lpp
import macro_fetcher
import mailer
import portfolio_fetcher as pf
import quotes_hub as qh
import scorer
from config import (
    MONITOR_INTERVAL_SEC,
    SERVER_PORT,
    TRADING_SESSIONS,
    CANDIDATE_REFRESH_SEC,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class EtfInput(BaseModel):
    code: str
    name: str = ""
    futures_index: str = ""


class ProxyInput(BaseModel):
    url: str


class ConfigInput(BaseModel):
    value: str


class LoginInput(BaseModel):
    username: str
    password: str
    captcha_id: str = ""
    captcha_answer: str = ""


# ---------------------------------------------------------------------------
# App & globals
# ---------------------------------------------------------------------------

# 登录会话: 存储在 MySQL sessions 表（token -> {username, expires}），
# 服务重启后会话不丢，过期由 DB 层校验
SESSION_TTL = 30 * 24 * 3600  # 30 天有效期

# 验证码: captcha_id -> {"answer": int, "expires": float}
_captchas: dict[str, dict] = {}
CAPTCHA_TTL = 300  # 验证码 5 分钟有效
# 登录限速: ip -> [时间戳...]
_login_attempts: dict[str, list[float]] = {}
# 验证码下发限速: ip -> [时间戳...]
_captcha_attempts: dict[str, list[float]] = {}


def _is_expired(expires) -> bool:
    """判断会话是否过期。expires 兼容 datetime（DB 返回）与 float（时间戳）。"""
    if isinstance(expires, datetime):
        return datetime.now() > expires
    return time.time() > expires


async def _create_session(username: str) -> str:
    """生成 token 并写入 DB sessions 表。"""
    token = secrets.token_urlsafe(32)
    await db.create_session(token, username, SESSION_TTL)
    return token


async def _check_session(request: Request) -> bool:
    """检查请求是否携带有效会话 token（查 DB sessions 表）。"""
    token = request.cookies.get("session_token")
    if not token:
        return False
    session = await db.get_session(token)
    if not session:
        return False
    # DB 层 get_session 已删除过期行，这里仅兜底防御过期数据
    if _is_expired(session["expires"]):
        return False
    return True


def _is_auth_path(path: str) -> bool:
    """判断请求路径是否为登录相关或静态登录页（免鉴权）。"""
    return (
        path == "/"
        or path.startswith("/api/login")
        or path.startswith("/api/logout")
        or path == "/static/login.html"
        or path.startswith("/api/quote")
        or path.startswith("/api/captcha")
        or path.startswith("/api/agent/")     # 外部 agent 接口：端点内 API Key 自鉴权
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: startup and shutdown."""
    await db.init_tables()
    logger.info("Database tables initialized")

    # SIGUSR1 任务栈转储（生产诊断）：后台协程静默挂起时 py-spy 不可见
    # asyncio 栈——kill -USR1 <pid> 后全部活动 task 的 await 位置落日志
    import signal as _signal

    def _dump_tasks(signum, _frame):
        try:
            loop = asyncio.get_event_loop()
            tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
            logger.warning("=== task dump (%d live) ===", len(tasks))
            for t in tasks:
                frames = t.get_stack(limit=10) or []
                where = frames[-1] if frames else None
                logger.warning("TASK %s%s", t.get_name(),
                               "" if where is None else " → %s:%s in %s" % (
                                   where.f_code.co_filename,
                                   where.f_lineno, where.f_code.co_name))
        except Exception as e:
            logger.warning("task dump failed: %s", e)

    _signal.signal(_signal.SIGUSR1, _dump_tasks)

    # 启动时立即执行一次静态数据初始化
    await _daily_init()

    # 注册 09:15 每日定时任务（自动刷新隔夜数据 + 昨日涨跌幅）
    global daily_task, scheduler_task
    daily_task = asyncio.create_task(_daily_schedule_loop())

    # 注册交易时间自动调度器
    scheduler_task = asyncio.create_task(_trading_hours_scheduler())

    global candidate_refresh_task
    candidate_refresh_task = asyncio.create_task(_candidate_refresh_loop())

    global portfolio_task, fund_nav_task, fund_nav_cache, market_task
    fund_nav_cache = await db.get_latest_fund_nav()
    fund_nav_task = asyncio.create_task(_fund_nav_loop())
    portfolio_task = asyncio.create_task(_portfolio_loop())
    market_task = asyncio.create_task(_market_loop())
    global h24_probe_task
    h24_probe_task = asyncio.create_task(_h24_probe_loop())

    # HSBC 成交邮件轮询（延迟 60s 首跑，之后每 30 分钟）
    global email_task
    email_task = asyncio.create_task(_email_loop())

    # ECB 参考价每日补拉（记账模块汇率缓存）
    global ledger_fx_task
    ledger_fx_task = asyncio.create_task(_ledger_fx_loop())

    # 宏观估值指标日更（22:40，纯看板展示不进 alerts）
    global macro_task
    macro_task = asyncio.create_task(macro_fetcher.macro_daily_loop())

    # 提醒模块：邮件队列 worker + 日K每日刷新（07:00 拉取+指标重算；
    # 提醒三表建表已并入 db.init_tables 内部，无需单独初始化）
    global alert_mail_task, daily_bars_task
    alert_mail_task = asyncio.create_task(mailer.mail_worker())
    daily_bars_task = asyncio.create_task(daily_bars.daily_refresh_loop())

    # 二期实时链路：长桥推送守护循环（shadow/live 由 sys_config.LP_PUSH_MODE
    # 热切换，shadow 不写 quotes_hub 不动线上展示）+ 内存真相源攒批落库
    # + SSE quote 快照节流广播；live 提醒事件接线与轮询侧同款处理
    global lp_push_task, quotes_flush_task, sse_quote_task, sse_overview_task
    lp_push_task = asyncio.create_task(lpp.push_loop())
    quotes_flush_task = asyncio.create_task(qh.flush_loop())
    sse_quote_task = asyncio.create_task(_sse_quote_loop())
    sse_overview_task = asyncio.create_task(_sse_overview_loop())
    lpp.on_events = _lp_on_events

    yield
    global monitor_running
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
    if portfolio_task:
        portfolio_task.cancel()
    if fund_nav_task:
        fund_nav_task.cancel()
    if market_task:
        market_task.cancel()
    if h24_probe_task:
        h24_probe_task.cancel()
    if email_task:
        email_task.cancel()
    if ledger_fx_task:
        ledger_fx_task.cancel()
    if macro_task:
        macro_task.cancel()
    if candidate_refresh_task:
        candidate_refresh_task.cancel()
    if daily_task:
        daily_task.cancel()
    if scheduler_task:
        scheduler_task.cancel()
    if alert_mail_task:
        alert_mail_task.cancel()
    if daily_bars_task:
        daily_bars_task.cancel()
    if lp_push_task:
        lp_push_task.cancel()
    if quotes_flush_task:
        quotes_flush_task.cancel()
    if sse_quote_task:
        sse_quote_task.cancel()
    if sse_overview_task:
        sse_overview_task.cancel()
    await db.close_pool()
    logger.info("Server shutdown complete")


app = FastAPI(title="QDII Monitor", lifespan=lifespan)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """鉴权中间件：未登录用户只能访问登录页和登录接口。"""
    path = request.url.path

    # 放行登录相关路径
    if _is_auth_path(path):
        # 已登录用户访问根路径 → 跳转持仓页（首选入口）
        if path == "/" and await _check_session(request):
            return RedirectResponse(url="/static/portfolio.html", status_code=302)
        return await call_next(request)

    # 放行 OPTIONS 预检
    if request.method == "OPTIONS":
        return await call_next(request)

    # 其余路径必须登录
    if not await _check_session(request):
        # API 请求返回 401，页面请求跳转登录
        if path.startswith("/api/"):
            return JSONResponse(
                status_code=401,
                content={"ok": False, "message": "未登录或会话已过期"},
            )
        return RedirectResponse(url="/static/login.html", status_code=302)

    return await call_next(request)

# SSE client queues
clients: list[asyncio.Queue] = []

# Background monitor state
monitor_task: Optional[asyncio.Task] = None
daily_task: Optional[asyncio.Task] = None
scheduler_task: Optional[asyncio.Task] = None
monitor_running: bool = False
overnight_data: dict = {}

# ETF昨日涨跌幅缓存 {code: change_pct}，每天一次 _daily_init 更新
prev_day_changes_cache: dict[str, float] = {}
prev_day_changes_date: str = ""  # 缓存对应的日期

candidate_cache: dict = {}
candidate_cache_time: float = 0
candidate_refresh_task: Optional[asyncio.Task] = None

# 全球指数缓存（由 fetcher._last_global_index_cache 写入，跟随期指路由频率）
global_index_cache: dict = {}

# 持仓模块状态
portfolio_cache: dict = {"ok": False, "positions": [], "summary": {}, "errors": []}
fund_nav_cache: dict | None = None   # {"nav","prev_nav","nav_date","nav_change_pct"}
fx_state: dict = {"rate": 7.80, "fallback": True, "time": ""}
fx_spark: list[float] = []           # USD/HKD 近5日走势（Yahoo HKD=X，随汇率刷新一并拉取）
market_cache: list[dict] = []        # 市场参考看板缓存（QQQ/SPY/SOXX/HSI/BZ/GC/DXY/US30Y/US10Y/VIX/DJI/NDX/SPX/USDCNH，
                                     # spark 为 market_ticks 当日分时累积，闭市回退上一时段全程）

# 一场口径状态机（模块级，进程生命周期内持久；重启后冷启动自愈，09:30 开盘刷新基准）。
# 维护逻辑已提取为纯函数 pf.update_session_state（无 IO、可离线测试），此处仅持有状态：
# base_closes: {symbol: 场基准价}——regular 时段用腾讯昨收(f[4])每轮刷新（数据源保证
#              =一场基准）；非 regular 沿用上一值；冷启动/重启落在非 regular 时段时
#              退化用当轮 f[4]（次日凌晨该值语义可能已切换，存在隔夜偏差，09:30 自愈，
#              循环侧 logger.info 仅标注一次）
# close_prices: {symbol: 常规收盘定格价}——regular 时段跟随实时价（定格于 16:00 最后
#              一条）；post/closed(≥16:00) 取腾讯现价(f[3]，实测盘后保持收盘价)；
#              次日凌晨(<04:00)/盘前取 f[4]（实测已切换为昨日常规收盘）
# base_logged: 冷启动退化标注开关（每次进程生命周期仅标注一次）
# base_fallback: 本轮冷启动退化填充的 symbol（纯函数→循环侧的日志传递通道，读完即 pop）
portfolio_session_state: dict = {"base_closes": {}, "close_prices": {}, "base_logged": False}
portfolio_task: Optional[asyncio.Task] = None
fund_nav_task: Optional[asyncio.Task] = None
market_task: Optional[asyncio.Task] = None
email_task: Optional[asyncio.Task] = None   # HSBC 成交邮件轮询任务
alert_mail_task: Optional[asyncio.Task] = None   # 提醒邮件队列 worker
daily_bars_task: Optional[asyncio.Task] = None   # 日K每日刷新循环（拉取+指标重算）
lp_push_task: Optional[asyncio.Task] = None       # 长桥推送守护循环（shadow/live）
quotes_flush_task: Optional[asyncio.Task] = None  # 内存真相源攒批落库循环
sse_quote_task: Optional[asyncio.Task] = None     # SSE quote 快照节流广播循环
sse_overview_task: Optional[asyncio.Task] = None  # SSE overview 推送价重建广播循环
macro_task: Optional[asyncio.Task] = None         # 宏观估值指标日更循环


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _broadcast(event: str, data: dict):
    """向所有 SSE 客户端广播事件。

    队列为无界 asyncio.Queue，put_nowait 不阻塞不抛异常，断连客户端由
    event_stream 的 is_disconnected 检测后自行出队清理（此处旧 try/except
    死代码已移除，避免误导）。
    """
    # default=str：date/datetime 等（fund nav_date）兜底转 ISO 串，与
    # HTTP 端点 FastAPI jsonable_encoder 行为一致
    payload = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    for queue in clients:
        queue.put_nowait(payload)


async def _sse_quote_loop(interval: float = 1.0):
    """SSE quote 快照：1s 节流批量广播内存真相源（一期轮询写入时同样生效）。

    quotes_hub 为内存真相源（推送 live / 轮询 poll 双入口写入），本协程
    只读快照广播、零外部请求；前端以 source=lp_push 的条目刷新展示价。
    """
    while True:
        await asyncio.sleep(interval)
        if not clients:
            continue  # 无 SSE 客户端时空转短路，跳过快照与序列化（少算不放大）
        try:
            items = qh.snapshot()
            if items:
                await _broadcast("quote", {"items": [
                    {"symbol": i["symbol"], "price": i["price"],
                     "bid": i.get("bid"), "ask": i.get("ask"), "ts": str(i.get("ts")),
                     "source": i.get("source")} for i in items]})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("SSE quote 广播失败: %s", e)


# 推送价重建 overview 的上轮计算上下文（_portfolio_loop 每轮刷新引用；
# SSE 循环只读 + 浅拷贝合并，纯内存零请求）
_pf_ctx: dict = {"holdings": [], "quotes": {}, "cash_usd": 0.0,
                 "fx": 0.0, "base": {}, "close": {}, "at": 0.0}

_LIVE_FRESH_SEC = 90.0   # lp_push 价新鲜度窗口：收盘后 SDK 停推的陈旧价
                         # 不得覆盖夜盘 24h 活价（防口径倒退）


def _fresh_lp_price(sym: str) -> float | None:
    """quotes_live 中该标的的新鲜 lp_push 价（过期/缺失返回 None）。"""
    lv = qh.quotes_live.get(sym)
    if not lv or lv.get("source") != "lp_push":
        return None
    try:
        ts = datetime.fromisoformat(str(lv.get("ts")))
        if (datetime.now(timezone.utc) - ts).total_seconds() > _LIVE_FRESH_SEC:
            return None
    except Exception:
        return None
    p = lv.get("price")
    return p if p and p > 0 else None


def _live_overview_payload() -> dict | None:
    """推送价重建 overview（纯内存）：lp_push 新鲜价覆盖美股 quotes 现价，
    复用 build_positions + 现金/cny 组装——与 _portfolio_loop 计算尾段
    同一口径函数，零漂移；港股/基金不受推送覆盖。"""
    ctx = _pf_ctx
    if not ctx["holdings"] or not ctx["quotes"] or not portfolio_cache.get("ok"):
        return None
    quotes = {s: dict(q) for s, q in ctx["quotes"].items()}
    for s, q in quotes.items():
        p = _fresh_lp_price(s)
        if p:
            q["price"] = p
            q["ext_price"] = 0.0    # 推送价即当下最新价，清掉盘前盘后
            q["ext_time"] = "lp_push"   # 扩展价定格优先级（与夜盘 24h 合并同款）
    positions, summary = pf.build_positions(
        ctx["holdings"], quotes, fund_nav_cache, ctx["fx"],
        base_overrides=ctx["base"], close_overrides=ctx["close"])
    summary["holdings_value"] = summary["total_value"]
    summary["cash_usd"] = round(ctx["cash_usd"], 2)
    summary["total_value"] = round(summary["holdings_value"] + ctx["cash_usd"], 2)
    denom = summary["base_value"] + ctx["cash_usd"]
    if denom > 0:
        summary["day_pnl_pct"] = summary["day_pnl"] / denom * 100
    data = dict(portfolio_cache)
    data["positions"] = positions
    data["summary"] = summary
    data["fetch_time"] = datetime.now().strftime("%H:%M:%S")
    data["fund"] = fund_nav_cache
    data["market"] = market_cache
    data["fx_spark"] = fx_spark
    usdcnh = next((m.get("price") for m in (market_cache or [])
                   if m.get("key") == "USDCNH" and m.get("price")), None)
    cny = pf.build_cny_summary(summary["total_value"], usdcnh,
                               _cny_cfg["invested"], _cny_cfg["expense"])
    if cny:
        data["cny"] = cny
    return data


async def _sse_overview_loop(interval: float = 2.0):
    """SSE overview 推送：lp_push 价驱动的持仓总览秒级重建广播。

    纯内存（_pf_ctx 快照 + quotes_hub 实时价），零外部请求；无客户端
    空转短路。nav/recent_trades 不在推送载荷（日级 TWR 与留痕数据秒级
    无意义），前端用上次轮询值兜底合并。
    """
    while True:
        await asyncio.sleep(interval)
        if not clients:
            continue
        try:
            data = _live_overview_payload()
            if data:
                await _broadcast("overview", data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("SSE overview 广播失败: %s", e)


async def _lp_on_events(events: list[dict]):
    """长桥 live 推送提醒事件接线（push_loop → on_events 钩子 → 宿主）。

    与 _portfolio_loop 轮询侧完全同款：邮件入队（完整事件含 body）+
    SSE 广播（剥离 body——/api/stream 为存量无鉴权通道，价格/成本等详情
    不经它外泄，前端收到事件后经有鉴权的 GET /api/alerts 拉详情）。
    事件已在引擎内落库+冷却置位，此处仅通知分发；字段取值用 .get 温和
    防御（缺字段降级为 None 广播，不在通知层炸推送循环）。
    """
    for ev in events:
        mailer.enqueue(ev)
        await _broadcast("alert", {
            "id": ev.get("id"), "symbol": ev.get("symbol"),
            "rule": ev.get("rule"), "level": ev.get("level"),
            "title": ev.get("title")})


def _safe_float(val, default=0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


def _is_trading_hours() -> bool:
    """判断当前是否在工作日交易时间内。"""
    now = datetime.now()
    if now.weekday() >= 5:  # 周六、周日
        return False
    minutes = now.hour * 60 + now.minute
    for sh, sm, eh, em in TRADING_SESSIONS:
        if sh * 60 + sm <= minutes <= eh * 60 + em:
            return True
    return False


def _calc_prev_day_changes(
    etf_data: list[dict],
    prev_close_map: dict[str, float],
) -> tuple[dict[str, float], list[str]]:
    """用行情的 pre_close + DB 的 prev_close 本地计算昨日涨跌幅。

    昨日涨跌幅 = (pre_close - prev_close) / prev_close × 100

    当 pre_close == prev_close 时（周末/同天重复），结果不可靠，
    该 ETF 被标记为 missing，需要回退 K 线接口。

    Args:
        etf_data: 行情数据（含 pre_close）
        prev_close_map: {code: prev_close} 从 DB 读取

    Returns:
        (results, missing_codes)
    """
    results: dict[str, float] = {}
    missing: list[str] = []

    for etf in etf_data:
        code = etf["code"]
        pre_close = etf.get("pre_close", 0)
        prev_close = prev_close_map.get(code, 0)

        if prev_close > 0 and pre_close > 0 and abs(pre_close - prev_close) > 0.0001:
            results[code] = round((pre_close - prev_close) / prev_close * 100, 4)
        else:
            missing.append(code)

    return results, missing


# ---------------------------------------------------------------------------
# Auth API (登录/登出)
# ---------------------------------------------------------------------------

def _generate_captcha() -> tuple[int, str]:
    """生成算术验证码，返回 (答案, PNG base64)。"""
    from PIL import Image, ImageDraw, ImageFont
    a, b = random.randint(2, 19), random.randint(2, 19)
    if random.random() < 0.5:
        text, answer = f"{a}+{b}=?", a + b
    else:
        hi, lo = max(a, b), min(a, b)
        text, answer = f"{hi}-{lo}=?", hi - lo
    img = Image.new("RGB", (150, 46), (26, 26, 46))
    draw = ImageDraw.Draw(img)
    # 干扰线
    for _ in range(5):
        x1, y1 = random.randint(0, 150), random.randint(0, 46)
        x2, y2 = random.randint(0, 150), random.randint(0, 46)
        draw.line([(x1, y1), (x2, y2)], fill=(70, 70, 110), width=1)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 26)
    except Exception:
        font = ImageFont.load_default()
    # 逐字符随机偏移上色
    colors = [(233, 69, 96), (0, 184, 148), (253, 203, 110), (116, 185, 255)]
    x = 14
    for ch in text:
        draw.text((x, random.randint(4, 12)), ch, fill=random.choice(colors), font=font)
        x += 24
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return answer, base64.b64encode(buf.getvalue()).decode()


@app.get("/api/captcha")
async def captcha(request: Request):
    """下发算术验证码（免鉴权，5 分钟有效，一次性）。"""
    # 下发限速：每 IP 每分钟最多 30 次（防公网无限刷 PIL 生成）
    client_ip = request.client.host if request.client else "?"
    now_ts = time.time()
    attempts = [t for t in _captcha_attempts.get(client_ip, []) if now_ts - t < 60]
    if not attempts:
        # 60s 窗口外无记录，回收 key 防止无界增长
        _captcha_attempts.pop(client_ip, None)
    if len(attempts) >= 30:
        return JSONResponse(status_code=429, content={"ok": False, "message": "请求过于频繁"})
    attempts.append(now_ts)
    _captcha_attempts[client_ip] = attempts
    captcha_id = secrets.token_hex(8)
    answer, b64 = _generate_captcha()
    _captchas[captcha_id] = {"answer": answer, "expires": time.time() + CAPTCHA_TTL}
    # 清理过期验证码
    for k in [k for k, v in _captchas.items() if time.time() > v["expires"]]:
        _captchas.pop(k, None)
    return {"ok": True, "captcha_id": captcha_id, "image": f"data:image/png;base64,{b64}"}


@app.post("/api/login")
async def login(data: LoginInput, request: Request):
    """用户名密码登录，成功后下发会话 cookie。"""
    # 验证码校验（一次性）
    cap = _captchas.pop(data.captcha_id, None) if data.captcha_id else None
    if not cap or time.time() > cap["expires"] or data.captcha_answer.strip() != str(cap["answer"]):
        return JSONResponse(status_code=401, content={"ok": False, "message": "验证码错误或已过期"})
    # 登录限速：每 IP 每分钟最多 10 次
    client_ip = request.client.host if request.client else "?"
    now_ts = time.time()
    attempts = [t for t in _login_attempts.get(client_ip, []) if now_ts - t < 60]
    if not attempts:
        # 60s 窗口外无记录，回收 key 防止无界增长
        _login_attempts.pop(client_ip, None)
    if len(attempts) >= 10:
        return JSONResponse(status_code=429, content={"ok": False, "message": "尝试过于频繁，请稍后再试"})
    attempts.append(now_ts)
    _login_attempts[client_ip] = attempts
    user = await db.get_user(data.username.strip())
    if not user or not db.verify_password(data.password, user["password"]):
        return JSONResponse(
            status_code=401,
            content={"ok": False, "message": "用户名或密码错误"},
        )
    # 存量明文命中 → 自动升级 pbkdf2 哈希（一次性迁移；失败不阻断登录，下次重试）
    if not user["password"].startswith("pbkdf2_sha256$") and user.get("id") is not None:
        try:
            await db.update_user_password(user["id"], db.hash_password(data.password))
        except Exception as e:
            logger.warning("用户 %s 密码哈希升级失败(下次登录重试): %s", user["username"], e)
    token = await _create_session(user["username"])
    resp = JSONResponse(content={"ok": True, "username": user["username"]})
    resp.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=SESSION_TTL,
        path="/",
    )
    return resp


@app.post("/api/logout")
async def logout(request: Request):
    """登出，清除会话。"""
    token = request.cookies.get("session_token")
    if token:
        await db.delete_session(token)
    resp = JSONResponse(content={"ok": True})
    resp.delete_cookie(key="session_token", path="/")
    return resp


@app.get("/api/session")
async def session_check(request: Request):
    """检查当前会话是否有效。"""
    token = request.cookies.get("session_token")
    session = await db.get_session(token) if token else None
    if session and not _is_expired(session["expires"]):
        return {"ok": True, "username": session["username"]}
    return JSONResponse(status_code=401, content={"ok": False, "message": "未登录"})


# ---------------------------------------------------------------------------
# SSE endpoint
# ---------------------------------------------------------------------------

@app.get("/api/stream")
async def event_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    clients.append(queue)
    logger.info("SSE client connected, total clients: %d", len(clients))

    async def generate():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30)
                    yield message
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            clients.remove(queue)
            logger.info("SSE client disconnected, total clients: %d", len(clients))

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Monitor control
# ---------------------------------------------------------------------------

async def _do_monitor_start() -> bool:
    """Internal: start the monitor loop. Returns True if started."""
    global monitor_task, monitor_running
    if monitor_running:
        return False
    monitor_running = True
    monitor_task = asyncio.create_task(_monitor_loop())
    return True


async def _do_monitor_stop() -> bool:
    """Internal: stop the monitor loop. Returns True if stopped."""
    global monitor_running, monitor_task
    if not monitor_running:
        return False
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        monitor_task = None

    await _broadcast("status", {
        "running": False,
        "proxy_count": 0,
        "etf_count": 0,
        "interval_sec": MONITOR_INTERVAL_SEC,
        "timestamp": datetime.now().isoformat(),
        "in_trading_hours": _is_trading_hours(),
    })
    return True


@app.get("/api/monitor/status")
async def monitor_status():
    proxy_list = await db.get_alive_proxies()
    etf_list = await db.get_etf_list(enabled_only=True)
    return {
        "running": monitor_running,
        "proxy_count": len(proxy_list),
        "etf_count": len(etf_list),
        "interval_sec": MONITOR_INTERVAL_SEC,
        "overnight": overnight_data,
        "in_trading_hours": _is_trading_hours(),
    }


@app.get("/api/datasource/status")
async def datasource_status():
    """数据源健康状态（前端横幅依据）。

    - longport：美股扩展价/VIX/隔夜指数主源。token_status ∈ ok/expiring
      （<=7天）/expired/invalid/missing；available=False 且 fallback_active
      =True 表示已自动回切新浪冷备，扩展价/夜盘价不受影响但精度降级；
      token 失效/临期请到长桥后台重新生成（正式账户身份）并 UPDATE
       sys_config 的 LONGPORT_ACCESS_TOKEN（60s 热加载，无需重启）。
    """
    import longport_fetcher as lp
    return {"longport": lp.status()}


# ---------------------------------------------------------------------------
# ETF CRUD
# ---------------------------------------------------------------------------

@app.get("/api/etf")
async def etf_list():
    rows = await db.get_etf_list()
    # Convert datetime objects to string for JSON serialization
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/etf")
async def etf_add(data: EtfInput):
    # code 白名单校验：仅允许字母数字与点（新浪/腾讯代码口径），防注入与 onclick 属性逃逸
    if not re.fullmatch(r"[0-9A-Za-z.]{1,16}", data.code or ""):
        return JSONResponse(status_code=400, content={"ok": False, "message": "code 仅允许字母数字与点"})
    affected = await db.add_etf(data.code, data.name, data.futures_index)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=409, content={"ok": False, "message": "ETF already exists"})


@app.delete("/api/etf/{code}")
async def etf_delete(code: str):
    affected = await db.delete_etf(code)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "ETF not found"})


@app.post("/api/etf/{code}/toggle")
async def etf_toggle(code: str, enabled: Optional[bool] = Query(None)):
    if enabled is not None:
        # Explicit set: 1=enabled, 0=disabled
        affected = await db.execute(
            "UPDATE etf_targets SET enabled = %s WHERE code = %s",
            (1 if enabled else 0, code),
        )
    else:
        # Toggle
        affected = await db.toggle_etf(code)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "ETF not found"})


# ---------------------------------------------------------------------------
# Proxy CRUD
# ---------------------------------------------------------------------------

@app.get("/api/proxy")
async def proxy_list():
    rows = await db.get_proxy_list()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/proxy")
async def proxy_add(data: ProxyInput):
    try:
        affected = await db.add_proxy(data.url)
        if affected:
            return {"ok": True}
        return JSONResponse(status_code=409, content={"ok": False, "message": "Proxy already exists"})
    except Exception as e:
        return JSONResponse(status_code=409, content={"ok": False, "message": str(e)})


@app.delete("/api/proxy/{proxy_id}")
async def proxy_delete(proxy_id: int):
    affected = await db.delete_proxy(proxy_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "Proxy not found"})


@app.post("/api/proxy/{proxy_id}/check")
async def proxy_check(proxy_id: int):
    """Check proxy connectivity and update status in DB."""
    import httpx

    # Fetch proxy URL from DB
    rows = await db.execute_dict("SELECT url FROM proxy_pool WHERE id = %s", (proxy_id,))
    if not rows:
        return JSONResponse(status_code=404, content={"ok": False, "message": "Proxy not found"})

    proxy_url = rows[0]["url"]
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url, timeout=10, verify=False
        ) as client:
            # 海外代理检测目标：baidu 对境外出口会误判，统一用 ipinfo
            resp = await client.get("https://ipinfo.io/ip")
            latency_ms = int((time.monotonic() - start) * 1000)
            if resp.status_code == 200:
                await db.update_proxy_status(proxy_id, alive=1, latency_ms=latency_ms)
                return {"ok": True, "alive": True, "latency_ms": latency_ms}
            else:
                await db.update_proxy_status(proxy_id, alive=0, latency_ms=latency_ms)
                return {"ok": True, "alive": False, "latency_ms": latency_ms, "status_code": resp.status_code}
    except Exception as e:
        latency_ms = int((time.monotonic() - start) * 1000)
        await db.update_proxy_status(proxy_id, alive=0, latency_ms=latency_ms)
        return {"ok": True, "alive": False, "latency_ms": latency_ms, "error": str(e)}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# 敏感配置键片段（键名含任一片段即脱敏返回）
SENSITIVE_KEY_FRAGMENTS = ("TOKEN", "KEY", "SECRET", "PASSWORD", "AUTH_CODE")


def is_sensitive_key(key: str) -> bool:
    """键名含敏感片段（TOKEN/KEY/SECRET/PASSWORD/AUTH_CODE）即视为敏感配置。"""
    k = (key or "").upper()
    return any(frag in k for frag in SENSITIVE_KEY_FRAGMENTS)


def mask_secret(value: str | None) -> str:
    """敏感值掩码：长度>8 显示前2+****+后2，否则全掩码。"""
    if not value:
        return "****"
    if len(value) > 8:
        return value[:2] + "****" + value[-2:]
    return "****"


@app.get("/api/config")
async def config_list():
    rows = await db.get_configs()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
        sensitive = is_sensitive_key(row.get("key", ""))
        row["masked"] = sensitive
        if sensitive:
            row["value"] = mask_secret(row.get("value"))
    return rows


@app.put("/api/config/{key}")
async def config_set(key: str, data: ConfigInput):
    # 留空 = 不修改；敏感键提交值等于当前掩码形态 = 未改动，均跳过落库
    if data.value == "":
        return {"ok": True, "skipped": True}
    if is_sensitive_key(key):
        current = await db.get_config(key)
        if current is not None and data.value == mask_secret(current):
            return {"ok": True, "skipped": True}
    await db.set_config(key, data.value)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Quote API (候选池行情 — 供外部客户端消费)
# ---------------------------------------------------------------------------

@app.get("/api/quote")
async def quote_list(codes: str = Query(default="", description="逗号分隔的代码列表，为空则返回全部")):
    """返回候选池缓存的行情数据。支持带 sz/sh 前缀的代码和全球指数代码。"""
    if codes:
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        items = []
        for c in code_list:
            stripped = c.removeprefix("sz").removeprefix("sh") if c.startswith(("sz", "sh")) else c
            found = False
            for key in (c, stripped, f"sz{stripped}", f"sh{stripped}"):
                if key in candidate_cache:
                    items.append(candidate_cache[key])
                    found = True
                    break
            if not found and c in global_index_cache:
                items.append(global_index_cache[c])
    elif candidate_cache:
        items = list(candidate_cache.values())
    else:
        items = []

    if not items:
        return {"items": [], "total": 0, "fetch_time": ""}

    return {
        "items": items,
        "total": len(items),
        "fetch_time": datetime.now().strftime("%H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Candidate Pool CRUD (候选池管理)
# ---------------------------------------------------------------------------

class CandidateInput(BaseModel):
    code: str
    name: str = ""
    type: str = "stock"
    source: str = "tencent"


@app.get("/api/candidate")
async def candidate_list():
    rows = await db.get_candidate_list()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/candidate")
async def candidate_add(data: CandidateInput):
    try:
        affected = await db.add_candidate(data.code, data.name, data.type, data.source)
        if affected:
            return {"ok": True}
        return JSONResponse(status_code=409, content={"ok": False, "message": "Candidate already exists"})
    except Exception as e:
        return JSONResponse(status_code=409, content={"ok": False, "message": str(e)})


@app.delete("/api/candidate/{candidate_id}")
async def candidate_delete(candidate_id: int):
    affected = await db.delete_candidate(candidate_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "Candidate not found"})


@app.post("/api/candidate/{candidate_id}/toggle")
async def candidate_toggle(candidate_id: int, enabled: Optional[bool] = Query(None)):
    if enabled is not None:
        affected = await db.execute(
            "UPDATE candidate_pool SET enabled = %s WHERE id = %s",
            (1 if enabled else 0, candidate_id),
        )
    else:
        affected = await db.toggle_candidate(candidate_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "Candidate not found"})


# ---------------------------------------------------------------------------
# 每日静态数据初始化（隔夜美股 + ETF昨日涨跌幅 + prev_close 更新）
# ---------------------------------------------------------------------------

def candidate_refresh_due(now_utc: datetime) -> bool:
    """A 股候选池快刷窗口判定（纯函数，供 _candidate_refresh_loop 与单测）。

    候选池均为 A 股 ETF/期指：仅北京时间 09:15-15:00 按 CANDIDATE_REFRESH_SEC
    快刷；窗口外（夜间/周末/假日）价格不动，降频慢轮即可（价不变幂等，
    期指路由随本循环自然放慢）。北京时间固定 UTC+8 无夏令时。
    """
    bj = now_utc + timedelta(hours=8)
    mins = bj.hour * 60 + bj.minute
    return 9 * 60 + 15 <= mins <= 15 * 60


async def _candidate_refresh_loop():
    """后台定时刷新候选池缓存：A 股时段（北京 09:15-15:00）按
    CANDIDATE_REFRESH_SEC 快刷，窗口外降频 60s（候选价不动，省请求）。"""
    global candidate_cache, candidate_cache_time, global_index_cache
    logger.info("候选池刷新循环启动 (快刷间隔 %.0fs, 时段外 60s)", CANDIDATE_REFRESH_SEC)

    while True:
        interval = CANDIDATE_REFRESH_SEC if candidate_refresh_due(
            datetime.now(timezone.utc).replace(tzinfo=None)) else 60.0
        try:
            candidates = await db.get_candidate_list(enabled_only=True)
            if not candidates:
                candidate_cache = {}
                await asyncio.sleep(interval)
                continue

            tencent_codes = [c["code"] for c in candidates if c["source"] == "tencent"]
            futures_codes = [c["code"] for c in candidates if c["source"] in ("sina_futures", "eastmoney_futures")]
            global_index_codes = [c["code"] for c in candidates if c["source"] == "eastmoney_index"]

            proxy_rows = await db.get_alive_proxies()
            proxy_urls = [row["url"] for row in proxy_rows]
            fetcher.set_proxies(proxy_urls)

            result = await fetcher.fetch_candidates(
                tencent_codes,
                futures_codes or None,
                global_index_codes or None,
            )

            if result.get("global_indices"):
                global_index_cache = {q["code"]: q for q in result["global_indices"]}

            cache: dict = {}
            for q in result.get("quotes", []):
                cache[q["code"]] = q

            for code, change_pct in result.get("futures", {}).items():
                name = next((c["name"] for c in candidates if c["code"] == code), code)
                cache[code] = {
                    "code": code,
                    "name": name,
                    "type": "futures",
                    "change_pct": change_pct,
                    "price": 0,
                    "pre_close": 0,
                }

            # 使用全球指数缓存
            for code in global_index_codes:
                if code in global_index_cache:
                    q = global_index_cache[code]
                    q.setdefault("type", "index")
                    cache[code] = q

            for c in candidates:
                if c["code"] not in cache:
                    cache[c["code"]] = {
                        "code": c["code"],
                        "name": c["name"],
                        "type": c["type"],
                        "price": 0,
                        "pre_close": 0,
                        "change_pct": 0,
                    }
                else:
                    cache[c["code"]].setdefault("type", c["type"])

            candidate_cache = cache
            candidate_cache_time = time.monotonic()

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("候选池刷新循环已停止")
            break
        except Exception as e:
            logger.exception("候选池刷新错误: %s", e)
            await asyncio.sleep(max(interval, 60.0))


async def _daily_init():
    """一次性获取所有静态数据，写入内存缓存。

    执行时机：
    1. 服务启动时立刻执行一次
    2. 每天 09:15 定时执行

    隔夜美股数据每次都会刷新（09:15 可能需要更新）。
    ETF 昨日涨跌幅同一天只计算一次。
    """
    global overnight_data, prev_day_changes_cache, prev_day_changes_date, candidate_cache, candidate_cache_time, global_index_cache

    logger.info("===== 每日静态数据初始化 开始 =====")

    # 0. 清理过期登录会话（独立 try：失败不阻断后续静态数据初始化）
    try:
        purged = await db.purge_expired_sessions()
        if purged:
            logger.info("已清理过期登录会话 %d 行", purged)
    except Exception as e:
        logger.warning("清理过期登录会话失败: %s", e)

    try:
        # 1. 加载代理池
        proxy_rows = await db.get_alive_proxies()
        proxy_urls = [row["url"] for row in proxy_rows]
        fetcher.set_proxies(proxy_urls)
        logger.info("代理池: %d 个可用", len(proxy_urls))

        # 2. 隔夜美股收盘数据 — 每次都刷新
        overnight_data = await fetcher.fetch_overnight()
        logger.info("隔夜美股: %d 个指数", len(overnight_data))

        # 2.5. 首次填充候选池缓存（不含全球指数，等 refresh loop 获取）
        if not candidate_cache:
            candidates = await db.get_candidate_list(enabled_only=True)
            tencent_codes = [c["code"] for c in candidates if c["source"] == "tencent"]
            futures_codes = [c["code"] for c in candidates if c["source"] in ("sina_futures", "eastmoney_futures")]
            global_index_codes = [c["code"] for c in candidates if c["source"] == "eastmoney_index"]
            if tencent_codes or futures_codes:
                cresult = await fetcher.fetch_candidates(tencent_codes, futures_codes or None, None)
                cache_init: dict = {}
                for q in cresult.get("quotes", []):
                    cache_init[q["code"]] = q
                for code, change_pct in cresult.get("futures", {}).items():
                    name = next((c["name"] for c in candidates if c["code"] == code), code)
                    cache_init[code] = {"code": code, "name": name, "type": "futures", "change_pct": change_pct, "price": 0, "pre_close": 0}
                for q in cresult.get("global_indices", []):
                    q.setdefault("type", "index")
                    cache_init[q["code"]] = q
                    global_index_cache[q["code"]] = q
                for c in candidates:
                    if c["code"] not in cache_init:
                        cache_init[c["code"]] = {"code": c["code"], "name": c["name"], "type": c["type"], "price": 0, "pre_close": 0, "change_pct": 0}
                    else:
                        cache_init[c["code"]].setdefault("type", c["type"])
                candidate_cache = cache_init
                candidate_cache_time = time.monotonic()
                logger.info("候选池首次加载: %d 条", len(cache_init))

        # 3. ETF 昨日涨跌幅 — 同一天只算一次
        today_str = datetime.now().strftime("%Y-%m-%d")
        if prev_day_changes_date == today_str and prev_day_changes_cache:
            logger.info("每日初始化: 隔夜美股已刷新, 昨日涨跌已完成 (%s)", today_str)
            return

        etf_rows = await db.get_etf_list(enabled_only=True)
        etf_codes = [r["code"] for r in etf_rows]
        if not etf_codes:
            logger.warning("无启用的 ETF，跳过昨日涨跌计算")
            prev_day_changes_date = today_str
            return

        # 发一次行情请求拿 pre_close（走代理池）
        round_result = await fetcher.fetch_round(etf_codes, proxy_urls, [])
        etf_data = round_result.get("etf_data", [])

        prev_close_map = await db.get_prev_close_map()
        local_result, missing_codes = _calc_prev_day_changes(etf_data, prev_close_map)

        if missing_codes:
            # K线回退（分批并发 + 代理池限速）
            kline_data = await fetcher.fetch_etf_prev_day_changes(missing_codes)
            local_result.update(kline_data)
            logger.info("昨日涨跌: %d 本地 + %d K线回退",
                        len(local_result) - len(kline_data), len(kline_data))
        else:
            logger.info("昨日涨跌: %d 只全部本地命中（0额外请求）",
                        len(local_result))

        # 写入内存缓存（监控循环只读）
        prev_day_changes_cache = local_result
        prev_day_changes_date = today_str

        # 更新 DB prev_close = pre_close（为下一个交易日准备）
        await db.update_prev_close(etf_data)
        logger.info("===== 每日静态数据初始化 完成 =====")

    except Exception as e:
        logger.exception("每日静态数据初始化失败: %s", e)


async def _daily_schedule_loop():
    """后台定时任务：每天 09:15 执行 _daily_init。

    计算距下次 09:15 的秒数，sleep 到点后执行。
    如果错过了当天（比如 09:15 之前启动时已经执行过），则等明天。
    """
    while True:
        now = datetime.now()
        target = now.replace(hour=9, minute=15, second=0, microsecond=0)

        # 如果今天的目标时间已过，定到明天
        if now >= target:
            # 检查今天是否已执行
            today_str = now.strftime("%Y-%m-%d")
            if prev_day_changes_date == today_str and prev_day_changes_cache:
                # 今天已执行，等明天
                target = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
            else:
                # 今天还没执行但已过 09:15 → 立即执行
                logger.info("定时任务: 已过 09:15 且今日未初始化，立即执行")
                await _daily_init()
                # 执行完后，定到明天
                target = now.replace(hour=9, minute=15, second=0, microsecond=0)
                if now >= target:
                    target = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)

        delay = (target - now).total_seconds()
        logger.info("每日定时任务: 下次执行 %s (%.0f 秒后)",
                    target.strftime("%Y-%m-%d %H:%M:%S"), delay)
        await asyncio.sleep(delay)

        logger.info("===== 定时任务触发 (09:15) =====")
        await _daily_init()


# ---------------------------------------------------------------------------
# Trading-hours auto scheduler
# ---------------------------------------------------------------------------

async def _trading_hours_scheduler():
    """后台定时任务：根据交易时间全自动启停监控。

    每 30 秒检查一次：
    - 进入交易时间 且监控未运行 → 自动启动
    - 离开交易时间 且监控运行中 → 自动停止
    """
    logger.info("交易时间调度器启动 (全自动模式)")
    while True:
        try:
            in_trading = _is_trading_hours()

            if in_trading and not monitor_running:
                logger.info("交易时间调度: 自动启动监控")
                await _do_monitor_start()

            elif not in_trading and monitor_running:
                logger.info("交易时间调度: 自动停止监控（非交易时间）")
                await _do_monitor_stop()

            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("交易时间调度器已停止")
            break
        except Exception as e:
            logger.exception("交易时间调度器错误: %s", e)
            await asyncio.sleep(30)


# ---------------------------------------------------------------------------
# Background monitor loop
# ---------------------------------------------------------------------------

async def _monitor_loop():
    """后台监控主循环。"""
    global monitor_running
    logger.info("Monitor loop started")

    while monitor_running:
        loop_start = time.monotonic()

        try:
            # 1. Load config from DB (4步决策模型参数)
            #    一次批量读取内存过滤，替代逐键 7 次 DB 往返（「能缓存不重查」）
            cfg_map = {r["key"]: r.get("value") for r in await db.get_configs()}
            futures_beta          = _safe_float(cfg_map.get("futures_beta"), 1.15)
            scenario_a_fut_drop   = _safe_float(cfg_map.get("scenario_a_fut_drop"), -0.6)
            scenario_b_ndx_drop   = _safe_float(cfg_map.get("scenario_b_ndx_drop"), -0.5)
            scenario_b_fut_rise   = _safe_float(cfg_map.get("scenario_b_fut_rise"), 0.3)
            scenario_a_overshot   = _safe_float(cfg_map.get("scenario_a_overshot"), 1.0)
            scenario_b_lag        = _safe_float(cfg_map.get("scenario_b_lag"), 0.3)
            buysell_ratio_thresh  = _safe_float(cfg_map.get("buysell_ratio_threshold"), 2.0)

            # 2. Load ETF list and proxy pool from DB
            etf_rows = await db.get_etf_list(enabled_only=True)
            proxy_rows = await db.get_alive_proxies()

            etf_codes = [row["code"] for row in etf_rows]
            proxy_urls = [row["url"] for row in proxy_rows]
            futures_indices = list({row["futures_index"] for row in etf_rows if row["futures_index"]})

            if not etf_codes:
                logger.debug("No enabled ETFs, skipping round")
                await _broadcast("status", {
                    "running": True,
                    "proxy_count": len(proxy_urls),
                    "interval_sec": MONITOR_INTERVAL_SEC,
                    "etf_count": 0,
                    "timestamp": datetime.now().isoformat(),
                })
                await asyncio.sleep(MONITOR_INTERVAL_SEC)
                continue

            # 3. Fetch data
            round_result = await fetcher.fetch_round(etf_codes, proxy_urls, futures_indices)

            # 4. Merge futures_change into each ETF based on its futures_index
            futures_data = round_result.get("futures_data", {})
            etf_data = round_result.get("etf_data", [])

            # Build a lookup: code -> futures_index
            code_to_futures = {row["code"]: row["futures_index"] for row in etf_rows}

            for etf in etf_data:
                fi = code_to_futures.get(etf["code"], "")
                etf["futures_index"] = fi
                etf["futures_change"] = futures_data.get(fi)

            # 5. Run 4-step decision model
            #    昨日涨跌幅是静态常量，已在启动阶段计算并缓存，循环中只读
            prev_day_changes = prev_day_changes_cache if prev_day_changes_cache else None

            decision_result = scorer.run_decision_model(
                etf_data,
                futures_data=futures_data,
                overnight_data=overnight_data,
                prev_day_changes=prev_day_changes,
                futures_beta=futures_beta,
                scenario_a_fut_drop=scenario_a_fut_drop,
                scenario_b_ndx_drop=scenario_b_ndx_drop,
                scenario_b_fut_rise=scenario_b_fut_rise,
                scenario_a_overshot=scenario_a_overshot,
                scenario_b_lag=scenario_b_lag,
                buysell_ratio_threshold=buysell_ratio_thresh,
            )

            ranked = decision_result["etf_data"]

            # 5.5. 保存历史快照到数据库（用于回测）
            try:
                saved_count = await db.save_history_ticks_batch(
                    ranked,
                    round_result.get("fetch_time", "")
                )
                if saved_count > 0:
                    logger.debug("已保存 %d 条历史快照", saved_count)
            except Exception as e:
                logger.warning("保存历史快照失败: %s", e)

            # 6. Broadcast snapshot event
            snapshot_items = []
            for etf in ranked:
                snapshot_items.append({
                    "code": etf["code"],
                    "name": etf.get("name", ""),
                    "price": etf["price"],
                    "pre_close": etf.get("pre_close", 0),
                    "iopv": etf["iopv"],
                    "change_pct": etf["change_pct"],
                    "premium_pct": etf["premium_pct"],
                    "buy1_vol": etf["buy1_vol"],
                    "sell1_vol": etf["sell1_vol"],
                    "buy1_price": etf["buy1_price"],
                    "sell1_price": etf["sell1_price"],
                    "buy_total_vol": etf.get("buy_total_vol", 0),
                    "sell_total_vol": etf.get("sell_total_vol", 0),
                    "total_volume": etf.get("total_volume", 0),
                    "amount": etf.get("amount", 0),
                    "ratio": etf.get("ratio", 0),
                    "premium": etf.get("premium", 0),
                    "score": etf.get("score", 0),
                    "futures_index": etf.get("futures_index", ""),
                    "futures_change": etf.get("futures_change"),
                    "iopv_source": etf.get("iopv_source", "N/A"),
                    "weibi": etf.get("weibi", 0),
                    "outer_vol": etf.get("outer_vol", 0),
                    "inner_vol": etf.get("inner_vol", 0),
                    # 决策模型新增字段
                    "scenario": etf.get("scenario", ""),
                    "premium_change": etf.get("premium_change"),
                    "premium_tag": etf.get("premium_tag", ""),
                    "step2_pass": etf.get("step2_pass", False),
                    "step2_reason": etf.get("step2_reason", ""),
                    "buy_sell_ratio": etf.get("buy_sell_ratio", 0),
                    "step3_pass": etf.get("step3_pass", False),
                    "is_target": etf.get("is_target", False),
                    "rec_action": etf.get("rec_action", ""),
                    "rec_price": etf.get("rec_price", 0),
                })

            await _broadcast("snapshot", {
                "items": snapshot_items,
                "futures": {k: v for k, v in futures_data.items()},
                "overnight": overnight_data,
                "global_indices": global_index_cache,
                "fetch_time": round_result.get("fetch_time", ""),
                "decision": {
                    "scenario": decision_result["scenario"],
                    "scenario_desc": decision_result["scenario_desc"],
                    "adjusted_fut_change": decision_result.get("adjusted_fut_change"),
                    "target_code": decision_result["target"]["code"] if decision_result["target"] else None,
                    "action": decision_result["decision"],
                    "reason": decision_result["reason"],
                },
            })

            # 7. Calculate sleep interval (总循环 >= MONITOR_INTERVAL_SEC，与总线 10s 节拍对齐)
            elapsed = time.monotonic() - loop_start
            interval = max(0, MONITOR_INTERVAL_SEC - elapsed)

            # 8. Broadcast status event
            await _broadcast("status", {
                "running": True,
                "proxy_count": len(proxy_urls),
                "interval_sec": round(interval, 2),
                "etf_count": len(etf_codes),
                "timestamp": datetime.now().isoformat(),
                "in_trading_hours": _is_trading_hours(),
            })

            # 9. Sleep
            await asyncio.sleep(interval)

        except asyncio.CancelledError:
            logger.info("Monitor loop cancelled")
            break
        except Exception as e:
            logger.exception("Monitor loop error: %s", e)
            await _broadcast("error", {"message": str(e)})
            await asyncio.sleep(MONITOR_INTERVAL_SEC)

    logger.info("Monitor loop stopped")


# ---------------------------------------------------------------------------
# 持仓模块：行情采集循环 + 基金净值/汇率循环
# ---------------------------------------------------------------------------

_anchor_task: asyncio.Task | None = None   # 长桥锚点后台恢复任务（进程内单发）
_cny_cfg = {"invested": 0.0, "expense": 0.0}  # 人民币累计本金配置缓存（低频读库）


async def _anchor_restore_task(symbols: list[str], state: dict) -> None:
    """长桥锚点后台恢复：逐只日K、每只间隔 4s（13 只 ≈60s 内摊完）。

    长桥限制：1s≤10 次调用、并发请求≤5，SDK 内部超限自动延迟请求（调用线程
    挂起排队，曾挂死双循环）——candlesticks 无批量接口，故请求分散摊薄且
    后台化（不阻塞持仓主循环）。单只失败自然缺省，下一轮 portfolio 循环
    missing 重触发补漏。结果直写 portfolio_session_state（asyncio 单线程
    无竞态）。
    """
    import longport_fetcher as lp
    base_map = state.setdefault("base_closes", {})
    close_map = state.setdefault("close_prices", {})
    restored = state.setdefault("base_restored", set())
    done = 0
    for i, s in enumerate(symbols):
        try:
            anchors = await lp.fetch_anchor_quotes([s])
        except Exception:
            anchors = {}
        if anchors.get(s):
            a = anchors[s]
            # base=昨日收盘（=场基准，与 f[4]/TD previous_close 同口径），缺失退化 open；
            # close=当日常规收盘定格
            base_map[s] = a.get("prev") or a["open"]
            close_map[s] = a["close"]
            restored.add(s)
            done += 1
        if i < len(symbols) - 1:
            await asyncio.sleep(4.0)
    if done:
        logger.info("锚点后台恢复完成 %d/%d 只", done, len(symbols))


async def _portfolio_loop():
    """持仓行情采集：盘中10s/扩展时段30s/休市按24h活价60s或定格600s，成功才写 tick。"""
    global portfolio_cache, _h24_direct_ok
    # tick 毛刺延迟确认状态（进程内存，重启即失效重建）：
    # last_tick_state：上一条已写入 tick 的 {p: day_pnl, v: total_value} 基准；
    # pending_tick：偏差超阈值被暂存待确认的 tick 行（下一轮恢复→丢弃，仍异常→补写确认）
    last_tick_state: dict = {"p": None, "v": None}
    pending_tick: tuple | None = None
    anchor_fail_streak = 0   # 锚点恢复连续失败计数（>=3 放弃，退化兜底）
    overnight_used = 0       # 上一轮 24h活价生效标的数（closed 段降频判定用，跨轮保留）
    logger.info("持仓行情采集循环启动")
    while True:
        interval = 600.0
        try:
            # 美东时钟循环内直取一次复用（时段判断/状态机/夜盘标记/归场同一快照，
            # 消除对 us_market_session 展示串的 strptime 反解析）
            et_dt = datetime.now(pf.ET_TZ)
            session, _et_date, et_now = pf.us_market_session(et_dt)
            # 24h窗口：收盘→开盘全段（盘后+夜盘+盘前+周末/假日），非 regular 即有活源
            h24_window = session != "regular"
            # closed 段 24h窗口恒真（600s 兜底分支曾不可达），按上一轮 24h活价
            # 是否生效降频：生效→60s 跟价（24h 曲线连续），失效/无交易→600s 定格慢轮
            interval = {"regular": 10.0, "pre": 30.0, "post": 30.0}.get(
                session, 60.0 if overnight_used > 0 else 600.0)
            holdings = await db.get_holdings(enabled_only=True)
            us_symbols = [h["symbol"] for h in holdings if h["market"] == "US"]
            hk_symbols = [h["symbol"] for h in holdings if h["market"] == "HK"]
            if not holdings:
                await asyncio.sleep(interval)
                continue
            # 新浪盘前盘后扩展价直连 403 时代理池重试（腾讯主源仍直连，取法仿 _market_loop）
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            quotes = await pf.fetch_quotes(us_symbols, hk_symbols, session,
                                           proxy_urls=proxies) if (us_symbols or hk_symbols) else {}
            if quotes:
                try:
                    # 仅回填 name 为空的持仓（行情接口取名）；用户手动改过
                    # 名称的不再覆盖，保留自定义名称
                    await db.update_holding_names({
                        h["symbol"]: quotes[h["symbol"]]["name"]
                        for h in holdings
                        if not (h.get("name") or "").strip()
                        and quotes.get(h["symbol"], {}).get("name")})
                except Exception:
                    pass
            # ---- 一场口径锚点恢复（冷启动，先于状态机）----
            # 重启落在非 regular 时段：补齐锚点（base=昨日收盘与 regular f[4]
            # 同口径、close=当日常规收盘），保证 day_pnl=close+overnight 恒等式
            # 跨重启一致；主源长桥日K（一次逐只并发、无 twelvedata 免费限额
            # 冲突——TD 8 credits/min 还要供 USDCNH 主价），长桥不可用/失败
            # 回切 TD 批量（8 只/批渐进）；以 base_restored 标记区分真锚点与
            # 状态机退化值（退化值每轮都可被恢复覆盖），连续失败>=3 次放弃
            # （腾讯兜底：实证凌晨/盘前 f[4]=场基准、f[3]=定格，语义恰好正确，
            # 09:30 自然对齐）
            if session != "regular" and us_symbols and anchor_fail_streak < 3:
                restored = portfolio_session_state.setdefault("base_restored", set())
                missing = [s for s in us_symbols
                           if s not in restored
                           or s not in portfolio_session_state.get("base_closes", {})
                           or s not in portfolio_session_state.get("close_prices", {})]
                if missing:
                    # 主源长桥日K：后台任务逐只分散（4s/只，官方频控 1s≤10 次、
                    # 并发≤5，SDK 内部超限延迟排队曾挂死双循环）——不阻塞主循环；
                    # 长桥不可用/已退避 → twelvedata 批量兜底（8 只/批渐进，
                    # 内部自带节流）；两者连续失败>=3 次放弃（腾讯兜底：实证
                    # 凌晨/盘前 f[4]=场基准、f[3]=定格，语义恰好正确，09:30
                    # 自然对齐）
                    lp_ok = False
                    try:
                        import longport_fetcher as lp
                        lp_ok = lp.is_available()
                    except Exception:
                        lp_ok = False
                    if lp_ok:
                        global _anchor_task
                        if _anchor_task is None or _anchor_task.done():
                            _anchor_task = asyncio.create_task(
                                _anchor_restore_task(missing, portfolio_session_state))
                            logger.info("锚点后台恢复启动(%d 只待补, 4s/只): %s",
                                        len(missing), missing)
                    else:
                        anchors = await pf.fetch_td_session_anchors(missing)
                        base_map = portfolio_session_state.setdefault("base_closes", {})
                        close_map = portfolio_session_state.setdefault("close_prices", {})
                        for s, a in anchors.items():
                            # base=previous_close（昨日收盘，与 regular 时段 f[4] 场基准
                            # 同口径，day_pnl=close+overnight 恒等式跨重启一致），缺失退化 open
                            base_map[s] = a.get("prev") or a["open"]
                            close_map[s] = a["close"]
                            restored.add(s)
                        if anchors:
                            anchor_fail_streak = 0
                        else:
                            anchor_fail_streak += 1
                        still = [s for s in missing if s not in anchors]
                        if still:
                            logger.info("锚点恢复待续(%d 只已补, %d 只待下批): %s",
                                        len(anchors), len(still), still)
            # ---- 一场口径状态机维护（base=场基准 / close=常规收盘定格）----
            # 必须先于 24h覆盖：状态机吃原始腾讯价（f[3]/f[4] 口径），确保
            # close_prices 定格不被 24h数据源代理价污染（尤其周末
            # closed+hour>=16 分支会用 q["price"] 写定格——若先覆盖，隔夜
            # 增量语义错乱）
            pf.update_session_state(portfolio_session_state, session, et_dt, quotes)
            fallback_syms = portfolio_session_state.pop("base_fallback", [])
            if fallback_syms:
                logger.info("一场口径基准冷启动退化: 非 regular 时段用腾讯昨收充当场基准 "
                            "(存在隔夜偏差, 09:30 开盘自愈): %s", fallback_syms)
            # ---- 24h数据源覆盖（状态机维护之后，仅影响展示与 tick）----
            # 窗口=收盘→开盘全段（盘后+夜盘+盘前+周末/假日）：7×24 有价（偏差
            # 0.02%-0.1%）覆盖美股现价 → 组合总值 24h 曲线连续；盘前盘后真实
            # 扩展价（腾讯/新浪 ext）优先，24h价兜底 ext 缺失标的；请求直连
            # 优先（探测循环确认），不可用时境外分类代理→通用池；无对应交易/
            # 失败的标的自然缺省，维持盘后价定格口径
            overnight_used = 0
            if h24_window and us_symbols:
                # 直连优先（行情镜像境内可达，实测 200）：24h数据源
                # 探测循环确认直连可用时传空候选（免代理开销）；直连不可用时
                # 境外分类代理优先，再回退通用池（fetcher 内逐候选尝试）
                probe_ok = _h24_direct_ok
                ov_urls: list[str] = []
                if not probe_ok:
                    ov_rows = await db.get_overseas_proxies()
                    ov_urls = [r["url"] for r in ov_rows] or proxies   # 境外优先，空回退通用池
                overnight = await pf.fetch_overnight_quotes(us_symbols, proxy_urls=ov_urls)
                # 同标的替代推导（QQQM 无交易）：夜盘价 = 自身收盘定格 ×
                # 替代标的(QQQ)涨跌幅——把 24h价的百分点映射过来得最新净值
                overnight.update(pf.derive_alias_prices(
                    overnight, portfolio_session_state.get("close_prices", {}), us_symbols))
                # bookTicker 无交易且别名推导仍缺的标的（如 AMKR/PCG/F），
                # 经 24h数据源股票行情端点逐只补齐（单 symbol weight=1，
                # 网关境内直连免代理）；失败标的自然缺省，维持定格口径
                equity_gap = [s for s in us_symbols if s not in overnight]
                if equity_gap:
                    overnight.update(await pf.fetch_equity_overnight_quotes(equity_gap))
                for sym, oq in overnight.items():
                    q = quotes.get(sym)
                    if q and oq.get("price"):
                        # pre/post 段真实扩展价优先（腾讯/新浪盘前盘后成交价
                        # 更真实，24h价只兜底无盘口标的）；closed 段的
                        # ext_price 是夜间残留定格盘后价，直接用 24h活价覆盖
                        real_ext = q.get("ext_price") and session in ("pre", "post")
                        if not real_ext:
                            q["price"] = oq["price"]
                            q["ext_price"] = 0.0
                            q["ext_time"] = "24h源"
                            overnight_used += 1
            base_overrides = {s: b for s, b in portfolio_session_state["base_closes"].items() if b}
            close_overrides = {s: c for s, c in portfolio_session_state["close_prices"].items() if c}
            positions, summary = pf.build_positions(
                holdings, quotes, fund_nav_cache, fx_state["rate"],
                base_overrides=base_overrides, close_overrides=close_overrides)
            # 现金并入总资产（现金无盈亏，今日/持仓盈亏口径不变）
            cash_rows = await db.get_cash_accounts()
            cash_usd = sum(r["amount"] for r in cash_rows if r["currency"] == "USD") + \
                sum(r["amount"] / fx_state["rate"] for r in cash_rows if r["currency"] == "HKD")
            summary["holdings_value"] = summary["total_value"]
            summary["cash_usd"] = round(cash_usd, 2)
            summary["total_value"] = round(summary["holdings_value"] + cash_usd, 2)
            # 口径契约：total_value(v)含现金 / holdings_value(p)不含现金 / base_value不含现金；
            # day_pnl_pct 分母必须含现金，与前端展示分母 total_value 一致（build_positions
            # 纯函数层只算持仓分母，此处现金并入后统一重算覆盖）
            denom = summary["base_value"] + cash_usd
            if denom > 0:
                summary["day_pnl_pct"] = summary["day_pnl"] / denom * 100
            # 人民币累计口径看板：总资产 × USDCNH（看板缓存复用，零新增请求）
            # 对比净投入本金 = cny_invested − expense_cny（个人消费折 CNY，
            # 与记账页 invest_net_cny 同口径）；未配置/汇率未就绪 → None。
            # 本金配置低频缓存到 _cny_cfg（端点实时重算 cny 用，见 portfolio_overview）
            try:
                cny_invested = float(await db.get_config("cny_invested") or 0)
                _cny_cfg["invested"] = cny_invested
            except (TypeError, ValueError):
                cny_invested = 0.0
            try:
                expense_cny = float(await db.get_config("expense_cny") or 0)
                _cny_cfg["expense"] = expense_cny
            except (TypeError, ValueError):
                expense_cny = 0.0
            usdcnh = next((m.get("price") for m in market_cache
                           if m.get("key") == "USDCNH" and m.get("price")), None)
            cny_summary = pf.build_cny_summary(summary["total_value"], usdcnh,
                                               cny_invested, expense_cny)
            # 一场口径交易日：次日凌晨(<09:30)采样归上一交易场，24h 曲线连续累计不重置
            session_key = pf.trading_session_key(et_dt)
            portfolio_cache = {
                "ok": bool(quotes),
                "fetch_time": datetime.now().strftime("%H:%M:%S"),
                "session": session,
                "et_now": et_now,
                "trade_date": str(session_key),
                # 24h窗口标记（结构化）：active=24h数据源代理价生效中，
                # 前端据此展示提示；全部标的无对应交易或直连/代理不可用时
                # active=False 退回定格提示。note 按时段：盘后/夜盘/盘前/假日
                "night_session": {"active": overnight_used > 0,
                                  "window": h24_window,
                                  "source": "h24_market",
                                  "note": {
                                      "post": "盘后·24h数据源(允许微小偏差)",
                                      "pre": "盘前·24h数据源(允许微小偏差)",
                                  }.get(session,
                                        "闭市·24h数据源(允许微小偏差)"
                                        if session == "closed" else "")},
                "fx_usdhkd": fx_state["rate"],
                "fx_fallback": fx_state["fallback"],
                "fx_time": fx_state["time"],
                "cny": cny_summary,
                "positions": positions,
                "summary": summary,
                "cash": cash_rows,
                "errors": [] if quotes else ["行情源暂不可用"],
            }
            # 推送价重建上下文（SSE overview 循环消费）：引用快照不复制，
            # 浅拷贝合并发生在消费侧（_live_overview_payload）
            _pf_ctx.update(holdings=holdings, quotes=quotes, cash_usd=cash_usd,
                           fx=fx_state["rate"], base=base_overrides,
                           close=close_overrides, at=time.time())
            # 常规时段写 tick；夜盘/周末（closed）在 24h数据源代理价生效时
            # 同样写 tick，24h 曲线跨夜盘/周末连续累积（日线聚合取每场最后一条不受影响）
            if quotes and (session != "closed" or overnight_used > 0):
                # tick 口径：总资产(含现金折算)，现金变动当日需保持曲线连续；
                # trade_date 用一场口径 session_key（凌晨归上一场）；day_pnl=实时累计
                # 写入口毛刺延迟确认：偏差超阈值的 tick 先暂存一轮不写库，下一轮
                # 恢复到 last 附近→上轮为瞬时坏值丢弃；仍异常→确认为真实跳变
                # （如开盘跳空），补写暂存 tick 保持曲线连续并接受本轮
                p_now, v_now = summary["day_pnl"], summary["total_value"]
                tick_row = (datetime.now(), session_key, v_now, p_now,
                            summary["total_pnl"], summary["day_pnl_pct"],
                            summary["total_pnl_pct"])
                action = pf.decide_tick_action(
                    last_tick_state["p"], last_tick_state["v"], p_now,
                    pending_tick is not None)
                if action == "write":
                    await db.save_portfolio_tick(*tick_row)
                    last_tick_state.update(p=p_now, v=v_now)
                elif action == "hold":
                    pending_tick = tick_row
                elif action == "discard":
                    pending_tick = None
                    await db.save_portfolio_tick(*tick_row)
                    last_tick_state.update(p=p_now, v=v_now)
                else:  # confirm：真实跳变，先补写暂存 tick 保持曲线连续
                    await db.save_portfolio_tick(*pending_tick)
                    pending_tick = None
                    await db.save_portfolio_tick(*tick_row)
                    last_tick_state.update(p=p_now, v=v_now)
            # ---- 提醒引擎：本轮最新价全量求值（引擎不感知数据来源）----
            try:
                _prices = {s: q.get("price") for s, q in quotes.items()
                           if s in us_symbols and q.get("price")}
                if _prices:
                    # 轮询价同步写内存真相源（float 归一 Decimal）：SSE quote
                    # 快照双入口之一，仅进内存不落库（tick 落库仅经 Sampler）
                    for _s, _p in _prices.items():
                        qh.update(_s, float(_p), source="poll")
                    _events = await alert_engine.evaluate_round(_prices)
                    for _ev in _events:
                        mailer.enqueue(_ev)
                        # SSE 广播剥离 body：/api/stream 为存量无鉴权通道，价格/成本等
                        # 详情不经它外泄；前端收到事件后经有鉴权的 GET /api/alerts 拉详情
                        await _broadcast("alert", {
                            "id": _ev.get("id"), "symbol": _ev["symbol"],
                            "rule": _ev["rule"], "level": _ev["level"],
                            "title": _ev["title"]})
                    if _events:
                        logger.info("提醒触发 %d 条: %s", len(_events),
                                    [e["title"] for e in _events])
            except Exception as e:
                logger.warning("提醒引擎评估异常: %s", e)
        except asyncio.CancelledError:
            logger.info("持仓行情采集循环已停止")
            break
        except Exception as e:
            logger.exception("持仓采集循环错误: %s", e)
            # 故障时钳制最小间隔，避免 10s 高频刷异常日志
            interval = max(interval, 30.0)
        await asyncio.sleep(interval)


async def _email_loop():
    """HSBC 成交邮件轮询：每 30 分钟拉取最近 1 天邮件，解析全部执行交易并幂等入账。"""
    logger.info("HSBC 成交邮件轮询循环启动 (间隔 %ss, 首跑延迟 %ss)",
                et.POLL_INTERVAL_SEC, et.FIRST_DELAY_SEC)
    await asyncio.sleep(et.FIRST_DELAY_SEC)
    while True:
        try:
            await et.poll_once(days=1)
        except asyncio.CancelledError:
            logger.info("HSBC 成交邮件轮询循环已停止")
            break
        except Exception as e:
            logger.exception("HSBC 邮件轮询错误: %s", e)
        await asyncio.sleep(et.POLL_INTERVAL_SEC)


def _next_fund_nav_run(now: datetime) -> datetime:
    """计算基金净值任务下一次执行时刻（北京 14:00/20:00，过点顺延次日）。"""
    targets = [now.replace(hour=14, minute=0, second=0, microsecond=0),
               now.replace(hour=20, minute=0, second=0, microsecond=0)]
    return next((t for t in targets if t > now), targets[0] + timedelta(days=1))


def _next_ledger_fx_run(now: datetime) -> datetime:
    """ECB 参考价下次拉取时刻：每日 22:35（ECB 定盘约 22:00 北京时间后）。"""
    target = now.replace(hour=22, minute=35, second=0, microsecond=0)
    if now >= target:
        target = (now + timedelta(days=1)).replace(hour=22, minute=35, second=0, microsecond=0)
    return target


async def _ledger_fx_sync_once() -> int:
    """补齐 ECB 参考价缓存：从 ledger 起点或库内最新日期次日到今天，一次区间请求。"""
    last = await db.get_ledger_fx_for_date(date.today())
    start = (date.fromisoformat(last["rate_date"]) + timedelta(days=1)).isoformat() \
        if last else ledger.LEDGER_FX_START
    end = date.today().isoformat()
    if start > end:
        return 0
    proxies = [r["url"] for r in await db.get_alive_proxies()]
    rows = await ledger.fetch_ecb_rates(start, end, proxy_urls=proxies)
    for d, cny, hkd in rows:
        await db.upsert_ledger_fx_rate(d, cny, hkd)
    if rows:
        logger.info("ECB 参考价缓存 +%d 条(%s..%s)", len(rows), rows[0][0], rows[-1][0])
    return len(rows)


async def _ledger_fx_loop():
    """每日 22:35 补拉 ECB 参考价；失败次日再试（不做短周期重试放大请求）。"""
    logger.info("ECB 参考价循环启动 (每日 22:35)")
    while True:
        try:
            delay = (_next_ledger_fx_run(datetime.now()) - datetime.now()).total_seconds()
            await asyncio.sleep(max(1, delay))
            await _ledger_fx_sync_once()
        except asyncio.CancelledError:
            logger.info("ECB 参考价循环已停止")
            break
        except Exception as e:
            logger.warning("ECB 参考价同步失败(次日重试): %s", e)
            await asyncio.sleep(3600)


async def _refresh_fx_spark(proxies: list[str]) -> None:
    """拉取 USD/HKD 近5日走势写入 fx_spark 全局（Yahoo HKD=X，失败置空数组不致命）。

    走势为 5 日日线、随汇率刷新（每天北京 14:00/20:00 各一轮）一并拉取即可保持新鲜，
    无需更高频；失败轮（30 分钟重试）重复拉取的请求量也很低，故不再额外限频。
    """
    global fx_spark
    try:
        fx_spark = await pf.fetch_fx_spark(proxy_urls=proxies)
    except Exception as e:
        logger.warning("USD/HKD 走势获取失败: %s", e)
        fx_spark = []


async def _fund_nav_loop():
    """基金净值+汇率+港元走势：启动立即执行，此后每天北京时间 14:00/20:00；失败 30 分钟后重试。"""
    global fund_nav_cache, fx_state
    logger.info("基金净值/汇率任务启动")
    while True:
        ok = False
        try:
            # 主源腾讯 whUSDHKD 总线攒批（不走代理），失败回切 twelvedata 冷备；
            # proxy_urls 仅签名兼容保留；取代理仿 _market_loop 供基金 NAV 使用
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            rate, fallback = await pf.fetch_fx_usdhkd(proxy_urls=proxies)
            fx_state = {"rate": rate, "fallback": fallback,
                        "time": datetime.now().strftime("%m-%d %H:%M")}
            # 汇率刷新处一并拉 USD/HKD 走势（低频日线，随轮更新）
            await _refresh_fx_spark(proxies)
            nav = await pf.fetch_fund_nav(proxies)
            if nav:
                await db.upsert_fund_nav(nav["nav_date"], nav["nav"], nav["change_pct"])
                logger.info("基金净值已更新: %s = %s", nav["nav_date"], nav["nav"])
            latest = await db.get_latest_fund_nav()
            if latest:
                fund_nav_cache = latest
            ok = nav is not None
        except asyncio.CancelledError:
            logger.info("基金净值任务已停止")
            break
        except Exception as e:
            logger.exception("基金净值任务错误: %s", e)
        if not ok:
            await asyncio.sleep(1800)
            continue
        now = datetime.now()
        nxt = _next_fund_nav_run(now)
        await asyncio.sleep((nxt - now).total_seconds())


_BACKFILL_MAX_AGE_SEC = 1800.0   # Yahoo 主价刷新阈值：库内最新 tick 距今超 30 分钟才拉一次
_BACKFILL_COOLDOWN_SEC = 14400.0  # 刷新成功后冷却 4h（价停滞会持续满足条件，幂等补拉限频）
_BACKFILL_RETRY_SEC = 300.0       # 刷新失败后 5min 重试（防源持续故障时请求放大）


def backfill_keys(last_map: dict[str, tuple | None], now_dt: datetime,
                  keys: list[str], max_age: float = _BACKFILL_MAX_AGE_SEC) -> list[str]:
    """返回需要 Yahoo 主价刷新的看板 keys（纯函数，供 _market_loop 与单测）。

    判定：库内无该标的 tick，或最新 tick 距今超 max_age（价停滞/服务重启/
    新标的/数据断档）。当前仅用于 US30Y/US10Y/VIX 主价刷新节流；分时由
    主价快照（60s 轮询价变即写）累积，不另请求分时接口——「少请求、
    多数据、回显全用库」原则。
    """
    out: list[str] = []
    for k in keys:
        last = last_map.get(k)
        if last is None or last[0] is None:
            out.append(k)
            continue
        ts = last[0]
        age = (now_dt - ts).total_seconds() if ts.tzinfo is None else (
            now_dt.replace(tzinfo=None) - ts).total_seconds()
        if age > max_age:
            out.append(k)
    return out


def yahoo_due_keys(yahoo_day: dict[str, dict], last_map: dict[str, tuple | None],
                   now_dt: datetime,
                   keys: tuple[str, ...] = ("US30Y", "US10Y", "VIX"),
                   max_age: float = _BACKFILL_MAX_AGE_SEC) -> tuple[list[str], list[str]]:
    """Yahoo 主价刷新双轨判定（纯函数，供 _market_loop 与单测）。

    返回 (missing, stale)：
    - missing：主价缓存无 price（冷启动/持续失败）→ 缺价即拉，循环侧仅受
      5min 失败节流（backfill_fail），不等 30min 断档，启动即自愈；
    - stale：库内最新 tick 断档超 max_age（例行 backfill）→ 受 4h 成功冷却
      （backfill_at）+ 5min 失败节流。同一 key 同属两类时只入 missing
      （立即拉、短节流），避免重复请求。
    """
    missing = [k for k in keys if not (yahoo_day.get(k) or {}).get("price")]
    stale = [k for k in backfill_keys(last_map, now_dt, list(keys), max_age)
             if k not in missing]
    return missing, stale


def yahoo_refresh_plan(yahoo_day: dict[str, dict], last_map: dict[str, tuple | None],
                       now_dt: datetime, now_mono: float,
                       backfill_at: dict[str, float], backfill_fail: dict[str, float],
                       keys: tuple[str, ...] = ("US30Y", "US10Y", "VIX")) -> list[str]:
    """合并双轨判定与节流状态，返回本轮应刷新的 Yahoo 主价 keys（纯函数）。

    - missing（缓存无 price）：缺价即拉，仅受 5min 失败节流——冷启动/
      Yahoo 故障恢复后立即自愈，不等 30min 断档；
    - stale（断档>30min）：受 4h 成功冷却 + 5min 失败节流，维持既有例行
      backfill 节奏；成功后 backfill_at 计 4h 冷却回归低频。
    """
    missing, stale = yahoo_due_keys(yahoo_day, last_map, now_dt, keys)
    due = [k for k in missing
           if now_mono - backfill_fail.get(k, 0.0) >= _BACKFILL_RETRY_SEC]
    due += [k for k in stale
            if now_mono - backfill_at.get(k, 0.0) >= _BACKFILL_COOLDOWN_SEC
            and now_mono - backfill_fail.get(k, 0.0) >= _BACKFILL_RETRY_SEC]
    return due


def hsi_session_open(now_utc: datetime) -> bool:
    """HSI 主价快照写入窗口（纯函数）：港股交易时段 09:30-16:00（港股与北京
    同时区、无夏令时）对应 UTC 01:30-08:00。

    腾讯收盘后仍返回微变价格，窗外写库会给当日分时拖出收盘后杂点尾段
    （实测拖到 ~16:54），故写入口按窗口过滤；午休 12:00-13:00 价不变本就
    不触发写入，窗口内连续判定即可。
    """
    mins = now_utc.hour * 60 + now_utc.minute
    return 90 <= mins <= 480


_H24_PROBE_INTERVAL_SEC = 1800.0   # 境外代理分类探测周期（30min，低频省资源）
_H24_PROBE_FIRST_DELAY_SEC = 90.0  # 首跑延迟（等代理池首轮抓取完成）
_h24_direct_ok = False             # 24h数据源直连可用性（探测循环维护）


async def _h24_probe_loop():
    """境外代理分类探测循环：给代理池打 region 标签。

    24h数据源端点为公开行情镜像域名（境内直连可达）。
    探测分两层：①服务器直连成功 → 数据源整体可用，代理无需境外分类（全部保持
    unknown，夜盘走直连）；②直连失败时才逐个探测代理，GET H24_PING_URL
    （weight 0，免 key）成功标记 region='overseas'（失败保持 unknown，下轮再测，
    不做永久否定标记）。已分类 overseas 的不重复探测。
    """
    logger.info("境外代理分类探测循环启动 (间隔 %ss)", _H24_PROBE_INTERVAL_SEC)
    await asyncio.sleep(_H24_PROBE_FIRST_DELAY_SEC)
    while True:
        try:
            direct_ok = False
            try:
                async with httpx.AsyncClient(timeout=8) as client:
                    resp = await client.get(pf.H24_PING_URL)
                direct_ok = resp.status_code == 200
            except Exception:
                direct_ok = False
            global _h24_direct_ok
            _h24_direct_ok = direct_ok
            if direct_ok:
                logger.info("24h数据源直连可用，代理无需境外分类")
            else:
                rows = await db.get_alive_proxies()
                unknown = [r for r in rows if r.get("region", "unknown") != "overseas"]
                for r in unknown:
                    try:
                        async with httpx.AsyncClient(timeout=8, proxy=r["url"]) as client:
                            resp = await client.get(pf.H24_PING_URL)
                        if resp.status_code == 200:
                            await db.mark_proxy_region(r["id"], "overseas")
                            logger.info("代理境外可达已标记 overseas: %s", r["url"])
                    except Exception:
                        pass   # 探测失败保持 unknown，下轮再测
        except asyncio.CancelledError:
            logger.info("境外代理分类探测循环已停止")
            break
        except Exception as e:
            logger.exception("境外代理分类探测错误: %s", e)
        await asyncio.sleep(_H24_PROBE_INTERVAL_SEC)


async def _market_loop():
    """市场参考看板采集（主价快照存库模式）：

    - 主价 60s 快轮（腾讯混排）→ **价变即写 market_ticks**，分时由主价
      快照自然累积（不再单独请求分时接口，「少请求、多数据、回显全用库」；
      闭市价不变不堆重复点）；
    - US30Y/US10Y/DXY 主价源为 Yahoo：缺价即拉（缓存无主价触发，仅受 5min
      失败节流）+ 例行 backfill（库内最新 tick 距今 >30min 且 4h 冷却到期），
      一次请求两用——meta 现价刷新主价缓存、当日 5m 分时顺带落库（零额外
      请求）；面板组装传 fetch_missing=False，Yahoo 节流单点化到本循环；
    - VIX/DJI/NDX/SPX 四项指数主价源为长桥新平台（.VIX/.DJI/.NDX/.SPX.US
      一次批量 60s 实时直连，免代理），价经 yahoo_day 注入面板与价变即写
      链路；长桥不可用时 pop 缓存交还 Yahoo 兜底（代理池 backfill，恢复后
      自动切回长桥）；长桥 token 到期自动续期也挂本循环（轮首检查，最后
      7 天触发、6h 节流，成功写回 sys_config 热加载）；
    - USDCNH 主价源为 twelvedata（30min 节流 48 credits/天，根除免费配额
      429；失败回落新浪批冷备价）；DXY 主价随 Yahoo 组 backfill（4h），缺失
      窗口回落新浪批冷备；新浪批降级纯兜底（BZ/GC 兜底 + USDCNH/DXY 冷备）
      15min 节流（成功固定 900s、失败指数退避 600s→1800s 封顶），产物缓存
      透传面板组装；
    - spark 统一读库并按 [ts,price] 对下发（前端悬浮展示时间+价格，闭市回退
      上一交易时段）；异常钳制 120s。
    """
    global market_cache
    logger.info("市场看板采集循环启动")
    yahoo_day: dict[str, dict] = {}     # US30Y/US10Y/DXY(/VIX 兜底) 主价缓存（backfill 轮顺带刷新）
    backfill_at: dict[str, float] = {}  # key → 上次 backfill 成功的 monotonic 时刻（4h 冷却）
    backfill_fail: dict[str, float] = {}  # key → 上次 backfill 失败时刻（5min 重试节流）
    # 新浪看板批低频节流状态（数据源架构升级：新浪降为低频互备/冷备）
    sina_panel_cache: dict = {}          # fetch_sina_panel 产物缓存
    sina_panel_at = 0.0                  # 上次成功拉取时刻（monotonic）
    sina_panel_fail = 0                  # 连续失败轮数（指数退避）
    sina_panel_interval = 900.0          # 成功固定 15min（纯兜底批）；失败 min(600*2^n, 1800)
    # twelvedata USDCNH 主价状态（30min 节流 48 credits/天，根除免费配额 429；
    # 连续无价回落新浪冷备）
    td_usdcnh: float | None = None
    td_fx_at = 0.0        # 上次尝试时刻（30min 节流）
    td_fx_ok_at = 0.0     # 上次成功时刻（>90min 无有效价回落新浪）
    lp_idx_ok = False                    # 长桥指数可用性（VIX/DJI/NDX/SPX，失败交还 Yahoo 兜底）
    while True:
        interval = 60.0
        try:
            # 长桥 token 到期自动续期（最后 7 天触发、6h 节流；内部全捕获）
            import longport_fetcher as lp
            await lp.maybe_refresh_token()

            # Yahoo 链路国内直连不稳：仿 _fund_nav_loop 从代理池取存活代理透传
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            now_mono = time.monotonic()

            # ---- 新浪看板批（纯兜底批：BZ/GC 兜底 + USDCNH/DXY 冷备）：15min 节流 ----
            if now_mono - sina_panel_at >= sina_panel_interval:
                fetched = await pf.fetch_sina_panel(proxy_urls=proxies)
                if fetched:
                    sina_panel_cache, sina_panel_at = fetched, now_mono
                    if sina_panel_fail:
                        logger.info("新浪看板批恢复（此前连续失败 %d 轮）", sina_panel_fail)
                    sina_panel_fail, sina_panel_interval = 0, 900.0
                else:
                    sina_panel_fail += 1
                    sina_panel_interval = min(600.0 * (2 ** (sina_panel_fail - 1)), 1800.0)
                    logger.warning("新浪看板批失败(退避至 %ds): 冷备价维持上次值",
                                   int(sina_panel_interval))

            # ---- twelvedata USDCNH 主价（30min 节流，失败沿用上次值，>90min 无
            #      有效价置 None 回落新浪批冷备价）----
            if now_mono - td_fx_at >= 1800.0:
                td_fx_at = now_mono
                v = await pf.fetch_td_fx("USD/CNH")
                if 6.0 < v < 8.5:
                    td_usdcnh, td_fx_ok_at = v, now_mono
                elif td_usdcnh is not None and now_mono - td_fx_ok_at < 5400.0:
                    pass   # 瞬时失败沿用上次值（汇率低频波动）
                else:
                    td_usdcnh = None

            # ---- 长桥指数主价（VIX/DJI/NDX/SPX 一次批量 60s 实时；不可用时
            #      pop 缓存交还 Yahoo 兜底）----
            try:
                idx = await lp.fetch_index_quotes()
                ok_keys = [k for k in ("VIX", "DJI", "NDX", "SPX")
                           if (idx.get(k) or {}).get("price")]
                for k in ("VIX", "DJI", "NDX", "SPX"):
                    if k in ok_keys:
                        yahoo_day[k] = {"price": idx[k]["price"],
                                        "change_pct": idx[k].get("change_pct")}
                    else:
                        yahoo_day.pop(k, None)
                if ok_keys and not lp_idx_ok:
                    logger.info("长桥指数主价生效（%s，实时直连）", "/".join(ok_keys))
                elif not ok_keys and lp_idx_ok:
                    logger.warning("长桥指数不可用，交还 Yahoo 兜底")
                lp_idx_ok = bool(ok_keys)
            except Exception as e:
                lp_idx_ok = False
                for k in ("VIX", "DJI", "NDX", "SPX"):
                    yahoo_day.pop(k, None)
                logger.warning("长桥指数获取异常: %s", e)

            # ---- 主价快照写入去重用的库内最新 tick ----
            last_map: dict[str, tuple | None] = {}
            for key in pf.DAY_MINUTE_KEYS:
                try:
                    last_map[key] = await db.get_market_last_tick(key)
                except Exception:
                    last_map[key] = None

            # ---- US30Y/US10Y/DXY(/VIX 兜底) 主价刷新（缺价即拉 + 例行 backfill 双轨节流）：
            #      这三项无腾讯/长桥主价源时由 Yahoo 一次请求两用——meta 现价刷新主价
            #      缓存、当日 5m 分时顺带落库；其余 8 项分时纯由主价快照累积。
            #      判定/节流抽 yahoo_refresh_plan 纯函数（见其 docstring）----
            yahoo_keys = ("US30Y", "US10Y", "DXY") + (
                () if lp_idx_ok else ("VIX", "DJI", "NDX", "SPX"))
            due = yahoo_refresh_plan(yahoo_day, last_map, now_utc, now_mono,
                                     backfill_at, backfill_fail, yahoo_keys)

            async def _refresh_yahoo(key: str) -> bool:
                q = await pf.fetch_yahoo_day_minutes(pf.YAHOO_DAY_SYMBOLS[key], proxies)
                if q.get("minutes"):
                    await db.save_market_ticks(key, q["minutes"])
                if not q.get("price"):
                    return False   # 无主价视同失败：缓存仍空，不记成功冷却防每轮空转
                yahoo_day[key] = q   # 按 price 缓存：极端空窗日有价无分时不放大补拉
                return True

            if due:
                results = await asyncio.gather(*(_refresh_yahoo(k) for k in due),
                                               return_exceptions=True)
                for key, r in zip(due, results):
                    if isinstance(r, Exception):
                        logger.warning("看板主价 %s Yahoo 刷新失败(5min 后重试): %s", key, r)
                        backfill_fail[key] = now_mono   # 失败短冷却，防持续故障请求放大
                    elif r:
                        backfill_at[key] = now_mono   # 成功才计 4h 冷却
                    else:
                        logger.warning("看板主价 %s Yahoo 刷新无主价(5min 后重试)", key)
                        backfill_fail[key] = now_mono

            # ---- spark 读库（每标的当日全程分时，降采样 ≤60 点，[ts,price] 对下发）
            #      + 主价快轮组装 ----
            spark_map: dict[str, list] = {}
            for key in pf.DAY_MINUTE_KEYS:
                try:
                    spark_map[key] = [[ts, p] for ts, p in await db.get_market_day_spark(key)]
                except Exception as e:
                    logger.warning("看板分时 %s 读库失败: %s", key, e)
                    spark_map[key] = []
            market_cache = await pf.fetch_market_panel(
                proxy_urls=proxies, yahoo_day=yahoo_day, spark_map=spark_map,
                fetch_missing=False,   # Yahoo 补拉已由上方 due 判定单点节流
                sina_panel=sina_panel_cache, td_usdcnh=td_usdcnh)

            # ---- 主价快照存库：价与库内最新 tick 不同才写（UTC，价变即积累分时）----
            for item in market_cache:
                key, price = item.get("key"), item.get("price")
                if price is None:
                    continue
                # HSI 仅在港股交易时段写库（腾讯收盘后仍回微变价，窗外写库
                # 会拖出收盘后杂点尾段）
                if key == "HSI" and not hsi_session_open(now_utc):
                    continue
                try:
                    # Yahoo 主价刷新项本轮分时刚落库，last_map 是落库前快照，
                    # 保留重查（长桥常态零落库、Yahoo 兜底期才需要）；
                    # 其余项复用轮首 last_map，省一轮 DB 往返
                    if key in ("US30Y", "US10Y", "DXY", "VIX", "DJI", "NDX", "SPX"):
                        last = await db.get_market_last_tick(key)
                    else:
                        last = last_map.get(key)
                    if last is None or last[0] is None or last[1] != float(price):
                        await db.save_market_ticks(key, [(now_utc, float(price))])
                except Exception as e:
                    logger.warning("主价快照 %s 写库失败: %s", key, e)
        except asyncio.CancelledError:
            logger.info("市场看板采集循环已停止")
            break
        except Exception as e:
            logger.exception("市场看板采集错误: %s", e)
            interval = 120.0
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Alert API（提醒/规则/K线/指标/回测）
# ---------------------------------------------------------------------------

async def _auth_or_401(request: Request):
    """session cookie 鉴权（惯用法同 /api/me）。@author yys"""
    token = request.cookies.get("session_token")
    session = await db.get_session(token) if token else None
    if not session:
        return JSONResponse(status_code=401, content={"ok": False, "message": "未登录"})
    return None


class RuleInput(BaseModel):
    params: dict = {}
    enabled: int = 1


# 规则键白名单：从 alert_engine.DEFAULT_PARAMS 同源派生——新规则只改引擎
# 默认参数表，此处自动跟随，杜绝两处清单漂移
_RULE_KEYS = tuple(alert_engine.DEFAULT_PARAMS)

# 规则数值参数取值规格：(下限, 上限, 下限是否开区间)，None=该侧无界。
# 键存在时严格数值（bool 是 int 子类须排除、字符串数字拒绝）：畸形值会让
# 引擎 float() 抛异常中断该标的求值，或比较恒假使规则静默失效——入库前拦下
_NUMERIC_PARAM_SPECS: dict[str, tuple[float | None, float | None, bool]] = {
    "profit_pct": (0.0, None, True),       # 浮盈阈值 >0
    "vix_pctile_max": (0.0, 100.0, False),  # VIX 分位上界 0~100
    "dip_pct": (0.0, None, True),           # 回撤阈值 >0
    "k": (0.0, None, True),                 # ATR 止损倍数 >0
    "rsi_low": (0.0, 100.0, False),         # 超卖阈值 0~100
    "bias_low": (None, None, False),        # 乖离阈值：数值即可，允许负
    "band_pct": (0.0, 20.0, True),          # 年线贴近带宽 >0 且 ≤20（pct）
    "depth": (0.3, 0.9, False),             # 斐波那契回撤阈值 0.3~0.9
}


def _validate_rule_params(params: dict) -> str | None:
    """策略包白名单校验：返回错误消息（None=合法）。@author yys

    畸形配置（非 dict 值、levels 含非数值项、数值参数越界/非数值）会让
    _merged_params 产出坏包，导致 kline 500 与该标的引擎静默失效——入库前拦下。
    """
    for key, val in params.items():
        if key not in _RULE_KEYS:
            return f"未知规则键: {key}"
        if not isinstance(val, dict):
            return f"规则 {key} 的配置必须为对象"
        levels = val.get("levels")
        if levels is not None:
            if not isinstance(levels, list):
                return f"规则 {key} 的 levels 必须为列表"
            for p in levels:
                # 严格类型：字符串 "8.0" 能过 float() 但会让下游
                # p/100.0 TypeError、f"{pct:+g}" ValueError——直接拒绝
                if isinstance(p, bool) or not isinstance(p, (int, float)):
                    return f"规则 {key} 的 levels 含非数值项: {p!r}"
        for pkey, (lo, hi, lo_open) in _NUMERIC_PARAM_SPECS.items():
            if pkey not in val:
                continue
            v = val[pkey]
            # v != v 排除 NaN（json.loads 可解析 NaN 字面量，入库后比较恒假致规则静默失效）
            if isinstance(v, bool) or not isinstance(v, (int, float)) or v != v:
                return f"规则 {key} 的 {pkey} 须为数值"
            if lo is not None and (v <= lo if lo_open else v < lo):
                return f"规则 {key} 的 {pkey} 超出取值范围: {v}"
            if hi is not None and v > hi:
                return f"规则 {key} 的 {pkey} 超出取值范围: {v}"
    return None


@app.get("/api/alert/rules")
async def alert_rules(request: Request):
    """全部规则配置（含默认值合成）。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    return {"ok": True, "rules": await db.get_alert_rules(),
            "defaults": alert_engine.DEFAULT_PARAMS}


@app.put("/api/alert/rules/{symbol}")
async def alert_rule_save(symbol: str, body: RuleInput, request: Request):
    """保存单票策略包（仅更新 params/enabled 两列，fired_state 列不受影响，
    消除与求值循环读-改-写的丢更新竞态）。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    bad = _validate_rule_params(body.params)
    if bad:
        return JSONResponse(status_code=400, content={"ok": False, "message": bad})
    await db.update_alert_rule_params(symbol, body.params, body.enabled)
    return {"ok": True}


@app.get("/api/alerts")
async def alerts_list(request: Request, unread: int = 0, limit: int = 50):
    """提醒历史（unread=1 只看未读）。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    rows = await db.get_alerts(unread_only=bool(unread), limit=limit)
    return {"ok": True, "total": len(rows), "alerts": rows}


@app.post("/api/alerts/{alert_id}/read")
async def alert_read(alert_id: int, request: Request):
    """标记单条提醒已读。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    await db.mark_alert_read(alert_id)
    return {"ok": True}


@app.post("/api/alerts/read_all")
async def alert_read_all(request: Request):
    """全部提醒标记已读。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    await db.mark_all_alerts_read()
    return {"ok": True}


@app.get("/api/indicators/{symbol}")
async def indicators(symbol: str, request: Request):
    """指标缓存快照 + MA200 趋势状态（策略管理页展示）。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    ind = daily_bars.get_indicators(symbol)
    rule = await db.get_alert_rule(symbol)
    bars = await db.get_daily_bars(symbol)
    trend = alert_engine.eval_trend(bars, alert_engine._merged_params(rule))
    return {"ok": True, "symbol": symbol, "indicators": ind, "trend": trend}


# 宏观估值四指标状态判定规则：返回 cheap/neutral/expensive/risk 四态，
# 阈值与 macro_fetcher 抓取口径一一对应（buffett=巴菲特指数百分位、cape=席勒市盈率、
# yield_2s10s=美债 2s10s 利差、fear_greed=恐惧贪婪指数）
_MACRO_STATUS_RULES = {
    "buffett": lambda v: "cheap" if v < 80 else ("neutral" if v <= 120 else "expensive"),
    "cape": lambda v: "cheap" if v < 15 else ("neutral" if v <= 25 else "expensive"),
    "yield_2s10s": lambda v: "risk" if v < 0 else ("neutral" if v <= 1 else "cheap"),
    "fear_greed": lambda v: "cheap" if v < 25 else ("neutral" if v <= 55 else "expensive"),
}


@app.get("/api/macro")
async def macro_view(request: Request):
    """宏观估值四指标：最新值 + 近一年历史 + 状态判定（纯看板展示，
    不进 alerts 铃声流）。@author yys"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    latest = {r["name"]: r for r in await db.get_macro_latest()}
    indicators = []
    status = {}
    for name in ("buffett", "cape", "yield_2s10s", "fear_greed"):
        row = latest.get(name)
        if not row:
            continue
        try:
            extra = json.loads(row.get("extra")) if row.get("extra") else {}
        except Exception:
            extra = {}
        hist = await db.get_macro_history(name, days=365)
        indicators.append({
            "name": name, "value": float(row["value"]),
            "asof": str(row["asof"]), "extra": extra,
            "history": [[str(h["asof"]), float(h["value"])] for h in hist],
        })
        rule = _MACRO_STATUS_RULES.get(name)
        status[name] = rule(float(row["value"])) if rule else "neutral"
    return {"ok": True, "indicators": indicators, "status": status}


_KLINE_CACHE: dict[str, tuple[float, dict]] = {}    # (symbol, tf) -> (monotonic, 全量 payload)


def _round_series(vals: list, nd: int) -> list:
    """指标序列统一取整（None 占位保留）。@author yys"""
    return [round(v, nd) if v is not None else None for v in vals]


def _kl_series(bars: list[dict]) -> dict:
    """对 bars 计算 RSI/MACD/KDJ 三指标序列（与 bars 等长）。"""
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    macd = daily_bars.macd(closes)
    kdj = daily_bars.kdj(highs, lows, closes)
    return {"rsi_series": _round_series(daily_bars.rsi_wilder(closes), 2),
            "macd_series": {k: _round_series(v, 3) for k, v in macd.items()},
            "kdj_series": {k: _round_series(v, 2) for k, v in kdj.items()}}


@app.get("/api/kline/{symbol}")
async def kline(symbol: str, request: Request, range: int = 120, tf: str = "day"):
    """K线 + 指标 + 标注：tf=day/week/month（daily_bars 聚合）/5m（market_ticks
    聚合，库内分时）。零新增长桥历史K请求；(symbol,tf) 60s 内存缓存，
    缓存存全量、range 仅截尾响应。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    range = max(int(range), 0)    # 负值入口钳制：防误传触发无意义全量查询/截尾异常 @author yys
    if tf not in ("day", "week", "month", "5m"):
        return JSONResponse(status_code=400,
                            content={"ok": False, "message": f"invalid tf: {tf}"})
    cache_key = f"{symbol}:{tf}"
    hit = _KLINE_CACHE.get(cache_key)
    if not (hit and time.monotonic() - hit[0] < 60.0):
        rule = await db.get_alert_rule(symbol)
        params = alert_engine._merged_params(rule)
        holdings = await db.get_holdings(enabled_only=True)
        cost = next((float(h["cost_price"]) for h in holdings
                     if h["symbol"] == symbol), None)
        levels = params["price_plan"].get("levels", [])
        ind = daily_bars.get_indicators(symbol) if tf == "day" else None
        marks = {
            "cost": cost,
            "levels": [{"pct": p, "price": round(cost * (1 + p / 100.0), 2)}
                       for p in levels] if cost else [],
            "stop_level": ind.get("stop_level") if ind else None,
            "hhv22": ind.get("hhv22") if ind else None,
        }
        if tf == "5m":
            rows = await db.get_market_ticks_range(symbol)
            bars = daily_bars.ticks_to_bars(rows, bucket_sec=300)
            volume_mode = "ticks"
        else:
            limit = None if tf != "day" else (None if range >= 1000 else range)
            bars = await db.get_daily_bars(symbol, limit=limit)
            if tf != "day":
                bars = daily_bars.aggregate_bars(bars, tf)
            volume_mode = "volume"
        overlays: dict = {}
        if tf == "day":
            closes = [float(b["close"]) for b in bars]
            overlays = {
                "ma50_series": _round_series(
                    daily_bars.sma(closes, daily_bars.MA_FAST), 2),
                "ma200_series": _round_series(
                    daily_bars.sma(closes, daily_bars.MA_SLOW), 2),
                "fib": (ind or {}).get("fib") or {},
            }
        payload = {"ok": True, "symbol": symbol, "tf": tf, "bars": bars,
                   "volume_mode": volume_mode, "marks": marks,
                   "overlays": overlays,
                   "indicators": {k: ind.get(k) for k in
                                  ("ma50", "ma200", "rsi14", "stop_level",
                                   "enough_history", "asof")} if ind else {}}
        payload.update(_kl_series(bars))
        _KLINE_CACHE[cache_key] = (time.monotonic(), payload)
        hit = _KLINE_CACHE[cache_key]
    p = hit[1]
    n = len(p["bars"])
    keep = n if range >= n else range        # 截尾窗口（series 与 bars 同步截）
    if keep < n:
        cut = n - keep
        out = dict(p)
        out["bars"] = p["bars"][cut:]
        out["rsi_series"] = p["rsi_series"][cut:]
        out["macd_series"] = {k: v[cut:] for k, v in p["macd_series"].items()}
        out["kdj_series"] = {k: v[cut:] for k, v in p["kdj_series"].items()}
        ov = p.get("overlays") or {}
        out["overlays"] = {**ov,
                           "ma50_series": (ov.get("ma50_series") or [])[cut:],
                           "ma200_series": (ov.get("ma200_series") or [])[cut:]}
        return out
    return p


class BacktestInput(BaseModel):
    symbols: list[str] = []
    rule: str = "atr_stop"
    params: dict = {}
    days: int = 750


@app.post("/api/backtest/run")
async def backtest_run(body: BacktestInput, request: Request):
    """四类规则历史回测（daily_bars 日K回放；symbols 空=全部美股持仓）。"""
    deny = await _auth_or_401(request)
    if deny:
        return deny
    symbols = body.symbols or [h["symbol"] for h in
                               await db.get_holdings(enabled_only=True)
                               if h["market"] == "US"]
    out = {}
    for sym in symbols:
        bars = await db.get_daily_bars(sym)
        if len(bars) < 30:
            out[sym] = {"skipped": "insufficient_data"}
            continue
        out[sym] = await asyncio.to_thread(
            alert_backtest.run, sym, body.rule, body.params, bars, days=body.days)
    return {"ok": True, "results": out}


# ---------------------------------------------------------------------------
# Portfolio API (持仓)
# ---------------------------------------------------------------------------

class HoldingInput(BaseModel):
    symbol: str
    name: str = ""
    market: str = "US"
    quantity: float = 0
    cost_price: float = 0
    currency: str = "USD"
    deduct_cash: bool = False


class CashInput(BaseModel):
    currency: str
    amount: float


class HoldingUpdateInput(BaseModel):
    name: str = ""
    market: str = "US"
    quantity: float = 0
    cost_price: float = 0
    currency: str = "USD"
    enabled: int = 1
    sort_order: int = 0


@app.get("/api/portfolio/overview")
async def portfolio_overview():
    """持仓总览（读缓存，含汇总/持仓明细/时段/汇率/市场看板/最近成交）。"""
    data = dict(portfolio_cache)
    data["fund"] = fund_nav_cache
    data["market"] = market_cache
    data["fx_spark"] = fx_spark
    # 人民币累计实时化：cny 依赖的 USDCNH 在 market_cache（60s 级），若随
    # portfolio_cache 快照下发，closed 段 24h活价失效切 600s 慢轮时 cny 会被拖住
    # 10 分钟不动——端点每次用最新看板价与缓存本金重算覆盖
    if portfolio_cache.get("summary"):
        usdcnh = next((m.get("price") for m in (market_cache or [])
                       if m.get("key") == "USDCNH" and m.get("price")), None)
        cny = pf.build_cny_summary(portfolio_cache["summary"]["total_value"],
                                   usdcnh, _cny_cfg["invested"],
                                   _cny_cfg["expense"])
        if cny:
            data["cny"] = cny
        # 顶层 nav 块（独立于 cny 降级）：实时总资产覆盖末点的 TWR/年化
        try:
            daily, events = await _get_nav_ctx()
            nav = ledger.build_nav_series(
                daily, events,
                latest_total_value=(portfolio_cache.get("summary") or {}).get("total_value"))
            if nav:
                data["nav"] = {"twr_pct": nav["twr_pct"], "ann_pct": nav["ann_pct"]}
        except Exception as e:
            logger.warning("TWR 计算失败（本轮跳过）: %s", e)
    # 最近 HSBC 成交记录（轻量，前端暂不展示只备数据）
    try:
        data["recent_trades"] = await db.get_recent_trades(10)
    except Exception:
        data["recent_trades"] = []
    return data


def _strip_spike_points(points: list[dict]) -> list[dict]:
    """intraday 读出口毛刺清洗：day_pnl 孤立坏值点剔除（序列首尾不判），其余原样保留。

    针对"单持仓行情瞬时坏值"已落库的孤立单点（前后邻居相近、当前点大幅偏离），
    在 day/hours 两分支出口统一兜底，过滤写入口延迟确认阈值以下的漏网小毛刺。
    """
    if len(points) < 3:
        return points
    out: list[dict] = []
    for i, pt in enumerate(points):
        if 0 < i < len(points) - 1 and pf.detect_isolated_spike(
                points[i - 1]["p"], pt["p"], points[i + 1]["p"]):
            continue
        out.append(pt)
    return out


@app.get("/api/portfolio/intraday")
async def portfolio_intraday(trade_date: str = Query(default=""),
                             hours: int = Query(default=0, ge=0, le=48)):
    """当日（或指定美东交易日）分时点；hours>0 时返回最近 N 小时窗口（跨日分时）。"""
    if hours > 0:
        rows = await db.get_portfolio_recent(hours)
        points = [
            {"ts": r["ts"].strftime("%m-%d %H:%M") if isinstance(r["ts"], datetime) else str(r["ts"]),
             "v": float(r["total_value_usd"]), "p": float(r["day_pnl_usd"])} for r in rows]
        return {"ok": True, "window": f"{hours}h", "points": _strip_spike_points(points)}
    if not trade_date:
        trade_date = portfolio_cache.get("trade_date") or datetime.now().strftime("%Y-%m-%d")
    rows = await db.get_portfolio_intraday(trade_date)
    points = [
        {"ts": r["ts"].strftime("%H:%M:%S") if isinstance(r["ts"], datetime) else str(r["ts"]),
         "v": float(r["total_value_usd"]), "p": float(r["day_pnl_usd"])} for r in rows]
    return {"ok": True, "window": "day", "trade_date": trade_date,
            "points": _strip_spike_points(points)}


@app.get("/api/portfolio/daily")
async def portfolio_daily(days_range: str = Query(default="30", alias="range")):
    """历史日线（每美东交易日最后一条采样）。range: 30/90/all。"""
    days = 3650 if days_range == "all" else (90 if days_range == "90" else 30)
    rows = await db.get_portfolio_daily(days)
    return {"ok": True, "days": [
        {"date": str(r["date"]), "v": float(r["total_value"]),
         "p": float(r["day_pnl"]), "pct": float(r["total_pnl_pct"])} for r in rows]}


# 净值重放上下文（60s 内存缓存：日线采样 + 事件全量；重放本身纯内存微秒级，
# 缓存只为省掉 overview 高频轮询下的重复 DB 读——「少请求多数据」）
_nav_ctx_cache = {"ts": 0.0, "daily": None, "events": None}


async def _get_nav_ctx() -> tuple[list[dict], list[dict]]:
    """净值重放上下文：全量日线 + 全量记账事件（60s 缓存）。@author yys"""
    now = time.time()
    if _nav_ctx_cache["daily"] is not None and now - _nav_ctx_cache["ts"] < 60:
        return _nav_ctx_cache["daily"], _nav_ctx_cache["events"]
    daily, events = await db.get_portfolio_daily(3650), await db.get_ledger_events()
    _nav_ctx_cache.update(ts=now, daily=daily, events=events)
    return daily, events


@app.get("/api/portfolio/nav")
async def portfolio_nav(days_range: str = Query(default="30", alias="range")):
    """单位净值（TWR）序列：ledger 现金流事件重放，剔除入金影响。

    range 仅裁剪 series 输出窗口（30/90/all）；twr_pct/ann_pct 恒为全历史。
    @author yys
    """
    daily, events = await _get_nav_ctx()
    nav = ledger.build_nav_series(daily, events)
    if not nav:
        return {"ok": True, "series": [], "twr_pct": None, "ann_pct": None, "days": 0}
    n = len(nav["series"]) if days_range == "all" else (
        90 if days_range == "90" else 30)
    series = nav["series"][-n:]
    return {"ok": True, "series": series, "twr_pct": nav["twr_pct"],
            "ann_pct": nav["ann_pct"], "days": nav["days"]}


async def _require_agent_key(request: Request) -> None:
    """agent 接口公共鉴权：sys_config.AGENT_API_KEY 未配置 → 404（视为禁用），
    key 不匹配 → 401（常量时间比较）；DB 异常 fail-closed 同 404 并记日志。@author yys"""
    try:
        api_key = await db.get_config("AGENT_API_KEY") or ""
    except Exception as e:
        logger.warning("AGENT_API_KEY 读取失败（本轮视为未配置）: %s", e)
        api_key = ""
    if not api_key:
        raise HTTPException(status_code=404, detail="接口未启用")
    provided = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(provided.encode(), api_key.encode()):
        raise HTTPException(status_code=401, detail="无效的 API Key")


# 外部 agent 只读接口域：router 级统一鉴权，新端点挂此 router 即受保护
agent_router = APIRouter(prefix="/api/agent",
                         dependencies=[Depends(_require_agent_key)])


@agent_router.get("/portfolio")
async def agent_portfolio():
    """外部 agent 持仓快照（X-API-Key 鉴权，读缓存零额外行情请求）。

    供外部 agent 做持仓鉴定/再平衡分析；鉴权由 router 级依赖统一收口
    （AGENT_API_KEY 未配置 → 404 视为禁用，key 不匹配 → 401 常量时间比较）。
    cny 与 overview 同逻辑：用最新看板 USDCNH + 本金配置实时重算，
    不受 600s 慢轮快照拖累。@author yys
    """
    data = dict(portfolio_cache)
    usdcnh = next((m.get("price") for m in (market_cache or [])
                   if m.get("key") == "USDCNH" and m.get("price")), None)
    cny = pf.build_cny_summary((data.get("summary") or {}).get("total_value"),
                               usdcnh, _cny_cfg["invested"], _cny_cfg["expense"])
    return {
        "ok": True,
        # 行情快照是否就绪：false=服务冷启动中（summary 空对象、盈亏不可用）
        "ready": bool(data.get("ok")),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session": data.get("session"),
        "trade_date": data.get("trade_date"),
        "summary": data.get("summary") or {},
        "holdings": [
            {"symbol": p.get("symbol"), "name": p.get("name", ""),
             "market": p.get("market"), "currency": p.get("currency"),
             "quantity": p.get("quantity"), "cost_price": p.get("cost_price"),
             "price": p.get("price"), "value": p.get("value"),
             "today_pnl": p.get("today_pnl"),
             "position_pnl": p.get("position_pnl"),
             "position_pnl_pct": p.get("position_pnl_pct"),
             "cost_usd": p.get("cost_usd"), "stale": p.get("stale", False),
             "ext": p.get("ext")}
            for p in (data.get("positions") or [])],
        "cash": data.get("cash") or [],
        "fx": {"usdhkd": data.get("fx_usdhkd"), "usdcnh": usdcnh},
        "cny": cny,
    }


app.include_router(agent_router)


@app.get("/api/portfolio/holding")
async def holding_list():
    """持仓列表（编辑用，含停用）。"""
    rows = await db.get_holdings(enabled_only=False)
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.post("/api/portfolio/holding")
async def holding_add(data: HoldingInput):
    symbol = data.symbol.strip().upper()
    if not symbol:
        return JSONResponse(status_code=400, content={"ok": False, "message": "symbol 不能为空"})
    # market 枚举校验（规范化大小写后校验，防非法值静默入库）
    mkt = data.market.strip().upper()
    if mkt not in ("US", "HK", "HKFUND"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "market 必须为 US、HK 或 HKFUND"})
    # currency 枚举校验
    cur = data.currency.strip().upper()
    if cur not in ("USD", "HKD"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "currency 必须为 USD 或 HKD"})
    # 勾选扣现时禁止对已存在 symbol 二次录入：upsert 覆盖会伴随再次扣现
    if data.deduct_cash:
        exist = await db.execute_dict("SELECT id FROM holdings WHERE symbol = %s", (symbol,))
        if exist:
            return JSONResponse(status_code=400, content={
                "ok": False,
                "message": "该代码已存在，重复录入会覆盖持仓并再次扣现；如需修正请用编辑功能"})
    await db.upsert_holding(symbol, data.name.strip(), mkt,
                            data.quantity, data.cost_price, cur)
    # 录入持仓时可选同步扣减现金（quantity×cost_price 原币种金额）；扣现失败不影响持仓保存
    if data.deduct_cash and data.quantity > 0 and data.cost_price > 0:
        mkt_currency = "USD" if mkt == "US" else cur
        try:
            balance = await db.deduct_cash(mkt_currency, data.quantity * data.cost_price)
        except Exception as e:
            logger.warning("录入 %s 扣减 %s 现金失败: %s", symbol, mkt_currency, e)
            return {"ok": True, "cash_warning": str(e)}
        logger.info("录入 %s 扣减 %s 现金 %.2f，余额 %.2f",
                    symbol, mkt_currency, data.quantity * data.cost_price,
                    balance if balance is not None else float("nan"))
        if balance is not None:
            return {"ok": True, "cash_balance": balance}
    return {"ok": True}


@app.put("/api/portfolio/holding/{holding_id}")
async def holding_update(holding_id: int, data: HoldingUpdateInput):
    # market 枚举校验（规范化大小写后校验，防非法值静默入库）
    mkt = data.market.strip().upper()
    if mkt not in ("US", "HK", "HKFUND"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "market 必须为 US、HK 或 HKFUND"})
    # currency 枚举校验
    cur = data.currency.strip().upper()
    if cur not in ("USD", "HKD"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "currency 必须为 USD 或 HKD"})
    affected = await db.update_holding(holding_id, data.name.strip(), mkt,
                                       data.quantity, data.cost_price, cur,
                                       data.enabled, data.sort_order)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "holding not found"})


@app.put("/api/portfolio/cash")
async def cash_update(data: CashInput):
    """设置某币种现金余额。"""
    curr = data.currency.strip().upper()
    if curr not in ("USD", "HKD"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "currency 必须为 USD 或 HKD"})
    await db.set_cash(curr, data.amount)
    return {"ok": True}


@app.delete("/api/portfolio/holding/{holding_id}")
async def holding_delete(holding_id: int):
    affected = await db.delete_holding(holding_id)
    if affected:
        return {"ok": True}
    return JSONResponse(status_code=404, content={"ok": False, "message": "holding not found"})


# ---------------------------------------------------------------------------
# Ledger (记账事件流)
# ---------------------------------------------------------------------------

_LEDGER_TYPES = {"inflow", "fx", "fee", "reward", "expense", "transfer", "realized", "balance"}
_LEDGER_ACCOUNTS = {"za_bank", "za_sec", "hsbc", "zf", "ccb", "cmb", "cib", "external"}
_LEDGER_CCYS = {"CNY", "HKD", "USD"}


@app.post("/api/ledger/events")
async def ledger_events_post(data: dict):
    """批量幂等入库记账事件（AI 唯一录入通道；同 ref_no=覆盖更新）。"""
    events = data.get("events") or []
    if not events or not isinstance(events, list):
        return JSONResponse(status_code=400, content={"ok": False, "message": "events 非空列表"})
    for ev in events:
        if ev.get("event_type") not in _LEDGER_TYPES:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": f"非法 event_type: {ev.get('event_type')}"})
        if ev.get("account") not in _LEDGER_ACCOUNTS:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": f"非法 account: {ev.get('account')}"})
        if ev.get("direction") not in ("+", "-"):
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": "direction 仅 + 或 -"})
        if ev.get("amount_ccy") not in _LEDGER_CCYS:
            return JSONResponse(status_code=400,
                                content={"ok": False, "message": "amount_ccy 仅 CNY/HKD/USD"})
        if not ev.get("ref_no") or not ev.get("event_date"):
            return JSONResponse(status_code=400, content={"ok": False, "message": "ref_no/event_date 必填"})
    for ev in events:
        await db.upsert_ledger_event(
            event_date=ev["event_date"], event_type=ev["event_type"],
            account=ev["account"], channel=ev.get("channel"),
            direction=ev["direction"], amount_ccy=ev["amount_ccy"],
            amount=ev["amount"], recv_ccy=ev.get("recv_ccy"),
            recv_amount=ev.get("recv_amount"), usd_equivalent=ev.get("usd_equivalent"),
            cny_amount=ev.get("cny_amount"), mid_rate=ev.get("mid_rate"),
            note=ev.get("note"), ref_no=ev["ref_no"])
    # 派生值同步：重算 cny_invested=Σ inflow.cny_amount 覆盖写（幂等），
    # 直写 _cny_cfg 令持仓页 CNY 卡片零延迟生效（不等下一持仓轮低频刷新）
    sync = await db.sync_ledger_cny_invested()
    _cny_cfg["invested"] = sync["after"]
    return {"ok": True, "applied": len(events),
            "derived_sync": {"cny_invested": sync}}


@app.get("/api/ledger/events")
async def ledger_events_get(event_type: Optional[str] = Query(None, alias="type"),
                            year: Optional[int] = Query(None),
                            account: Optional[str] = Query(None)):
    return {"ok": True, "events": await db.get_ledger_events(event_type, year, account)}


@app.get("/api/ledger/report")
async def ledger_report():
    """全套记账报表（纯实时计算，口径=盘点v3勘误版）。"""
    events = await db.get_ledger_events()
    total_assets = await db.get_latest_portfolio_value()
    cash = await db.get_cash_accounts()
    usdcnh = next((m.get("price") for m in market_cache
                   if m.get("key") == "USDCNH" and m.get("price")), None)
    report = ledger.build_report(events, total_assets_usd=total_assets,
                                 usdcnh=usdcnh, cash_accounts=cash)
    report["ok"] = True
    return report


# ---------------------------------------------------------------------------
# Static files & root redirect
# ---------------------------------------------------------------------------

@app.get("/")
async def root(request: Request):
    """根路径：已登录跳转持仓页（首选入口），未登录跳转登录页。"""
    # 注意 await：_check_session 为协程，漏 await 会恒真导致未登录也跳持仓页
    if await _check_session(request):
        return RedirectResponse(url="/static/portfolio.html")
    return RedirectResponse(url="/static/login.html")


# Mount static directory
app.mount("/static", StaticFiles(directory="static", html=True), name="static")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
