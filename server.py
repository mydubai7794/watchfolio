"""FastAPI 主服务：SSE 推送 + REST 管理 API + 后台监控任务。"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
import fetcher
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


# ---------------------------------------------------------------------------
# App & globals
# ---------------------------------------------------------------------------

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

    yield
    global monitor_running
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
    if candidate_refresh_task:
        candidate_refresh_task.cancel()
    if daily_task:
        daily_task.cancel()
    if scheduler_task:
        scheduler_task.cancel()
    await db.close_pool()
    logger.info("Server shutdown complete")


app = FastAPI(title="QDII Monitor", lifespan=lifespan)

# SSE client queues
clients: list[asyncio.Queue] = []

# Background monitor state
monitor_task: Optional[asyncio.Task] = None
daily_task: Optional[asyncio.Task] = None
scheduler_task: Optional[asyncio.Task] = None
monitor_running: bool = False
monitor_mode: str = "auto"  # "auto" = 交易时间自动启停, "manual" = 手动控制
futures_kill_enabled: bool = False
overnight_data: dict = {}

# ETF昨日涨跌幅缓存 {code: change_pct}，每天一次 _daily_init 更新
prev_day_changes_cache: dict[str, float] = {}
prev_day_changes_date: str = ""  # 缓存对应的日期

candidate_cache: dict = {}
candidate_cache_time: float = 0
candidate_refresh_task: Optional[asyncio.Task] = None


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
        "mode": monitor_mode,
        "in_trading_hours": _is_trading_hours(),
    })
    return True


@app.post("/api/monitor/start")
async def monitor_start():
    global monitor_mode
    monitor_mode = "manual"  # 手动操作切换为手动模式
    if await _do_monitor_start():
        return {"ok": True, "message": "Monitor started (manual mode)"}
    return {"ok": False, "message": "Monitor is already running"}


@app.post("/api/monitor/stop")
async def monitor_stop():
    global monitor_mode
    monitor_mode = "manual"  # 手动操作切换为手动模式
    if await _do_monitor_stop():
        return {"ok": True, "message": "Monitor stopped (manual mode)"}
    return {"ok": False, "message": "Monitor is not running"}


@app.post("/api/monitor/mode")
async def monitor_set_mode(mode: str = Query("auto")):
    """切换监控模式: auto=交易时间自动启停, manual=手动控制."""
    global monitor_mode
    if mode not in ("auto", "manual"):
        return JSONResponse(status_code=400, content={"ok": False, "message": "mode must be 'auto' or 'manual'"})
    monitor_mode = mode
    return {"ok": True, "mode": monitor_mode}


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
        "mode": monitor_mode,
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
            resp = await client.get("https://httpbin.org/ip")
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
    """返回候选池缓存的行情数据。"""
    if not candidate_cache:
        return {"items": [], "total": 0, "fetch_time": ""}

    if codes:
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        items = [candidate_cache[c] for c in code_list if c in candidate_cache]
    else:
        items = list(candidate_cache.values())

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
    global candidate_cache, candidate_cache_time
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

            proxy_rows = await db.get_alive_proxies()
            proxy_urls = [row["url"] for row in proxy_rows]
            fetcher.set_proxies(proxy_urls)

            result = await fetcher.fetch_candidates(tencent_codes, futures_codes or None)

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
    global overnight_data, prev_day_changes_cache, prev_day_changes_date, candidate_cache, candidate_cache_time

    logger.info("===== 每日静态数据初始化 开始 =====")

    try:
        # 1. 加载代理池
        proxy_rows = await db.get_alive_proxies()
        proxy_urls = [row["url"] for row in proxy_rows]
        fetcher.set_proxies(proxy_urls)
        logger.info("代理池: %d 个可用", len(proxy_urls))

        # 2. 隔夜美股收盘数据 — 每次都刷新
        overnight_data = await fetcher.fetch_overnight()
        logger.info("隔夜美股: %d 个指数", len(overnight_data))

        # 2.5. 首次填充候选池缓存
        if not candidate_cache:
            candidates = await db.get_candidate_list(enabled_only=True)
            tencent_codes = [c["code"] for c in candidates if c["source"] == "tencent"]
            futures_codes = [c["code"] for c in candidates if c["source"] in ("sina_futures", "eastmoney_futures")]
            if tencent_codes or futures_codes:
                cresult = await fetcher.fetch_candidates(tencent_codes, futures_codes or None)
                cache_init: dict = {}
                for q in cresult.get("quotes", []):
                    cache_init[q["code"]] = q
                for code, change_pct in cresult.get("futures", {}).items():
                    name = next((c["name"] for c in candidates if c["code"] == code), code)
                    cache_init[code] = {"code": code, "name": name, "type": "futures", "change_pct": change_pct, "price": 0, "pre_close": 0}
                for c in candidates:
                    if c["code"] not in cache_init:
                        cache_init[c["code"]] = {"code": c["code"], "name": c["name"], "type": c["type"], "price": 0, "pre_close": 0, "change_pct": 0}
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
    """后台定时任务：auto 模式下根据交易时间自动启停监控。

    每 30 秒检查一次：
    - 进入交易时间 且 auto 模式 且监控未运行 → 自动启动
    - 离开交易时间 且 auto 模式 且监控运行中   → 自动停止
    - manual 模式 → 不做任何操作
    """
    logger.info("交易时间调度器启动 (默认 auto 模式)")
    while True:
        try:
            if monitor_mode == "auto":
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
                "mode": monitor_mode,
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
# Static files & root redirect
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html")


# Mount static directory
app.mount("/static", StaticFiles(directory="static", html=True), name="static")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
