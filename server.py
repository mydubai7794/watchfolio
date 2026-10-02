"""FastAPI 主服务：SSE 推送 + REST 管理 API + 后台监控任务。"""

import asyncio
import base64
import io
import json
import logging
import random
import re
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
import fetcher
import portfolio_fetcher as pf
import scorer
from config import (
    MIN_INTERVAL_SEC,
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
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: startup and shutdown."""
    await db.init_tables()
    logger.info("Database tables initialized")

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
    if candidate_refresh_task:
        candidate_refresh_task.cancel()
    if daily_task:
        daily_task.cancel()
    if scheduler_task:
        scheduler_task.cancel()
    await db.close_pool()
    logger.info("Server shutdown complete")


app = FastAPI(title="QDII Monitor", lifespan=lifespan)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """鉴权中间件：未登录用户只能访问登录页和登录接口。"""
    path = request.url.path

    # 放行登录相关路径
    if _is_auth_path(path):
        # 已登录用户访问根路径 → 跳转主页
        if path == "/" and await _check_session(request):
            return RedirectResponse(url="/static/index.html", status_code=302)
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
futures_kill_enabled: bool = False
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
market_cache: list[dict] = []        # 市场参考看板缓存（QQQ/SPY/SOXX/HSI/BZ/GC/DXY/US30Y/US10Y/VIX）
portfolio_task: Optional[asyncio.Task] = None
fund_nav_task: Optional[asyncio.Task] = None
market_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _broadcast(event: str, data: dict):
    """向所有 SSE 客户端广播事件。"""
    payload = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    disconnected = []
    for queue in clients:
        try:
            await queue.put(payload)
        except Exception:
            disconnected.append(queue)
    for q in disconnected:
        clients.remove(q)


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
    if not user or user["password"] != data.password:
        return JSONResponse(
            status_code=401,
            content={"ok": False, "message": "用户名或密码错误"},
        )
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
        "interval_sec": MIN_INTERVAL_SEC,
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
        "futures_kill_enabled": futures_kill_enabled,
        "proxy_count": len(proxy_list),
        "etf_count": len(etf_list),
        "interval_sec": MIN_INTERVAL_SEC,
        "overnight": overnight_data,
        "in_trading_hours": _is_trading_hours(),
    }


@app.post("/api/monitor/futures_kill")
async def futures_kill(enabled: bool = Query(True)):
    global futures_kill_enabled
    futures_kill_enabled = enabled
    return {"ok": True, "futures_kill_enabled": futures_kill_enabled}


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

@app.get("/api/config")
async def config_list():
    rows = await db.get_configs()
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()
    return rows


@app.put("/api/config/{key}")
async def config_set(key: str, data: ConfigInput):
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

async def _candidate_refresh_loop():
    """后台定时刷新候选池缓存。"""
    global candidate_cache, candidate_cache_time, global_index_cache
    logger.info("候选池刷新循环启动 (间隔 %.0fs)", CANDIDATE_REFRESH_SEC)

    while True:
        try:
            candidates = await db.get_candidate_list(enabled_only=True)
            if not candidates:
                candidate_cache = {}
                await asyncio.sleep(CANDIDATE_REFRESH_SEC)
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

            await asyncio.sleep(CANDIDATE_REFRESH_SEC)
        except asyncio.CancelledError:
            logger.info("候选池刷新循环已停止")
            break
        except Exception as e:
            logger.exception("候选池刷新错误: %s", e)
            await asyncio.sleep(CANDIDATE_REFRESH_SEC)


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
            futures_beta          = _safe_float(await db.get_config("futures_beta"), 1.15)
            scenario_a_fut_drop   = _safe_float(await db.get_config("scenario_a_fut_drop"), -0.6)
            scenario_b_ndx_drop   = _safe_float(await db.get_config("scenario_b_ndx_drop"), -0.5)
            scenario_b_fut_rise   = _safe_float(await db.get_config("scenario_b_fut_rise"), 0.3)
            scenario_a_overshot   = _safe_float(await db.get_config("scenario_a_overshot"), 1.0)
            scenario_b_lag        = _safe_float(await db.get_config("scenario_b_lag"), 0.3)
            buysell_ratio_thresh  = _safe_float(await db.get_config("buysell_ratio_threshold"), 2.0)

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
                    "interval_sec": MIN_INTERVAL_SEC,
                    "etf_count": 0,
                    "timestamp": datetime.now().isoformat(),
                })
                await asyncio.sleep(MIN_INTERVAL_SEC)
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

            # 7. Calculate sleep interval (总循环 >= MIN_INTERVAL_SEC，因为数据源本身 ~3s 更新一次)
            elapsed = time.monotonic() - loop_start
            interval = max(0, MIN_INTERVAL_SEC - elapsed)

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
            await asyncio.sleep(MIN_INTERVAL_SEC)

    logger.info("Monitor loop stopped")


# ---------------------------------------------------------------------------
# 持仓模块：行情采集循环 + 基金净值/汇率循环
# ---------------------------------------------------------------------------

async def _portfolio_loop():
    """持仓行情采集：盘中10s/扩展时段30s/休市600s，成功才写 tick。"""
    global portfolio_cache
    logger.info("持仓行情采集循环启动")
    while True:
        interval = 600.0
        try:
            session, et_date, et_now = pf.us_market_session()
            interval = {"regular": 10.0, "pre": 30.0, "post": 30.0}.get(session, 600.0)
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
                    await db.update_holding_names({s: q["name"] for s, q in quotes.items() if q.get("name")})
                except Exception:
                    pass
            positions, summary = pf.build_positions(holdings, quotes, fund_nav_cache, fx_state["rate"])
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
            portfolio_cache = {
                "ok": bool(quotes),
                "fetch_time": datetime.now().strftime("%H:%M:%S"),
                "session": session,
                "et_now": et_now,
                "trade_date": str(et_date),
                "fx_usdhkd": fx_state["rate"],
                "fx_fallback": fx_state["fallback"],
                "fx_time": fx_state["time"],
                "positions": positions,
                "summary": summary,
                "cash": cash_rows,
                "errors": [] if quotes else ["行情源暂不可用"],
            }
            # 休市不写 tick（避免日线聚合混入休市平线）
            if quotes and session != "closed":
                # tick 口径：总资产(含现金折算)，现金变动当日需保持曲线连续
                await db.save_portfolio_tick(
                    datetime.now(), et_date, summary["total_value"], summary["day_pnl"],
                    summary["total_pnl"], summary["day_pnl_pct"], summary["total_pnl_pct"])
        except asyncio.CancelledError:
            logger.info("持仓行情采集循环已停止")
            break
        except Exception as e:
            logger.exception("持仓采集循环错误: %s", e)
            # 故障时钳制最小间隔，避免 10s 高频刷异常日志
            interval = max(interval, 30.0)
        await asyncio.sleep(interval)


def _next_fund_nav_run(now: datetime) -> datetime:
    """计算基金净值任务下一次执行时刻（北京 14:00/20:00，过点顺延次日）。"""
    targets = [now.replace(hour=14, minute=0, second=0, microsecond=0),
               now.replace(hour=20, minute=0, second=0, microsecond=0)]
    return next((t for t in targets if t > now), targets[0] + timedelta(days=1))


async def _fund_nav_loop():
    """基金净值+汇率：启动立即执行，此后每天北京时间 14:00/20:00；失败 30 分钟后重试。"""
    global fund_nav_cache, fx_state
    logger.info("基金净值/汇率任务启动")
    while True:
        ok = False
        try:
            # 新浪汇率直连 403（云服务器 IP 被拉黑）时代理池重试；取法仿 _market_loop
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            rate, fallback = await pf.fetch_fx_usdhkd(proxy_urls=proxies)
            fx_state = {"rate": rate, "fallback": fallback,
                        "time": datetime.now().strftime("%m-%d %H:%M")}
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


async def _market_loop():
    """市场参考看板采集：60s 周期刷新，异常钳制 120s 避免刷日志。"""
    global market_cache
    logger.info("市场看板采集循环启动")
    while True:
        interval = 60.0
        try:
            # Yahoo 链路国内直连不稳：仿 _fund_nav_loop 从代理池取存活代理透传
            proxies = [r["url"] for r in await db.get_alive_proxies()]
            market_cache = await pf.fetch_market_panel(proxy_urls=proxies)
        except asyncio.CancelledError:
            logger.info("市场看板采集循环已停止")
            break
        except Exception as e:
            logger.exception("市场看板采集错误: %s", e)
            interval = 120.0
        await asyncio.sleep(interval)


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
    """持仓总览（读缓存，含汇总/持仓明细/时段/汇率/市场看板）。"""
    data = dict(portfolio_cache)
    data["fund"] = fund_nav_cache
    data["market"] = market_cache
    return data


@app.get("/api/portfolio/intraday")
async def portfolio_intraday(trade_date: str = Query(default=""),
                             hours: int = Query(default=0, ge=0, le=48)):
    """当日（或指定美东交易日）分时点；hours>0 时返回最近 N 小时窗口（跨日分时）。"""
    if hours > 0:
        rows = await db.get_portfolio_recent(hours)
        return {"ok": True, "window": f"{hours}h", "points": [
            {"ts": r["ts"].strftime("%m-%d %H:%M") if isinstance(r["ts"], datetime) else str(r["ts"]),
             "v": float(r["total_value_usd"]), "p": float(r["day_pnl_usd"])} for r in rows]}
    if not trade_date:
        trade_date = portfolio_cache.get("trade_date") or datetime.now().strftime("%Y-%m-%d")
    rows = await db.get_portfolio_intraday(trade_date)
    return {"ok": True, "window": "day", "trade_date": trade_date, "points": [
        {"ts": r["ts"].strftime("%H:%M:%S") if isinstance(r["ts"], datetime) else str(r["ts"]),
         "v": float(r["total_value_usd"]), "p": float(r["day_pnl_usd"])} for r in rows]}


@app.get("/api/portfolio/daily")
async def portfolio_daily(days_range: str = Query(default="30", alias="range")):
    """历史日线（每美东交易日最后一条采样）。range: 30/90/all。"""
    days = 3650 if days_range == "all" else (90 if days_range == "90" else 30)
    rows = await db.get_portfolio_daily(days)
    return {"ok": True, "days": [
        {"date": str(r["date"]), "v": float(r["total_value"]),
         "p": float(r["day_pnl"]), "pct": float(r["total_pnl_pct"])} for r in rows]}


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
# Static files & root redirect
# ---------------------------------------------------------------------------

@app.get("/")
async def root(request: Request):
    """根路径：已登录跳转主页，未登录跳转登录页。"""
    if _check_session(request):
        return RedirectResponse(url="/static/index.html")
    return RedirectResponse(url="/static/login.html")


# Mount static directory
app.mount("/static", StaticFiles(directory="static", html=True), name="static")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
