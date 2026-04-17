"""FastAPI 主服务：SSE 推送 + REST 管理 API + 后台监控任务。"""

import asyncio
import json
import logging
import time
from datetime import datetime
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
    DEFAULT_W_RATIO,
    DEFAULT_W_PREMIUM,
    DEFAULT_FUTURES_KILL_THRESHOLD,
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
    yield
    global monitor_running
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
    await db.close_pool()
    logger.info("Server shutdown complete")


app = FastAPI(title="QDII Monitor", lifespan=lifespan)

# SSE client queues
clients: list[asyncio.Queue] = []

# Background monitor state
monitor_task: Optional[asyncio.Task] = None
monitor_running: bool = False
futures_kill_enabled: bool = False


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

@app.post("/api/monitor/start")
async def monitor_start():
    global monitor_task, monitor_running
    if monitor_running:
        return {"ok": False, "message": "Monitor is already running"}
    monitor_running = True
    monitor_task = asyncio.create_task(_monitor_loop())
    return {"ok": True, "message": "Monitor started"}


@app.post("/api/monitor/stop")
async def monitor_stop():
    global monitor_running, monitor_task
    if not monitor_running:
        return {"ok": False, "message": "Monitor is not running"}
    monitor_running = False
    if monitor_task:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        monitor_task = None
    return {"ok": True, "message": "Monitor stopped"}


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
# Background monitor loop
# ---------------------------------------------------------------------------

async def _monitor_loop():
    """后台监控主循环。"""
    global monitor_running
    logger.info("Monitor loop started")

    while monitor_running:
        loop_start = time.monotonic()

        try:
            # 1. Load config from DB
            w_ratio_str = await db.get_config("w_ratio")
            w_premium_str = await db.get_config("w_premium")
            threshold_str = await db.get_config("futures_kill_threshold")

            w_ratio = _safe_float(w_ratio_str, DEFAULT_W_RATIO)
            w_premium = _safe_float(w_premium_str, DEFAULT_W_PREMIUM)
            futures_kill_threshold = _safe_float(threshold_str, DEFAULT_FUTURES_KILL_THRESHOLD)

            # 2. Load ETF list and proxy pool from DB
            etf_rows = await db.get_etf_list(enabled_only=True)
            proxy_rows = await db.get_alive_proxies()

            etf_codes = [row["code"] for row in etf_rows]
            proxy_urls = [row["url"] for row in proxy_rows]

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
            round_result = await fetcher.fetch_round(etf_codes, proxy_urls)

            # 4. Merge futures_change into each ETF based on its futures_index
            futures_data = round_result.get("futures_data", {})
            etf_data = round_result.get("etf_data", [])

            # Build a lookup: code -> futures_index
            code_to_futures = {row["code"]: row["futures_index"] for row in etf_rows}

            for etf in etf_data:
                fi = code_to_futures.get(etf["code"], "")
                etf["futures_change"] = futures_data.get(fi)

            # 5. Score and rank
            ranked = scorer.rank_etfs(
                etf_data,
                w_ratio=w_ratio,
                w_premium=w_premium,
                futures_kill_enabled=futures_kill_enabled,
                futures_kill_threshold=futures_kill_threshold,
            )

            # 6. Broadcast snapshot event
            snapshot_items = []
            for etf in ranked:
                snapshot_items.append({
                    "code": etf["code"],
                    "name": etf.get("name", ""),
                    "price": etf["price"],
                    "iopv": etf["iopv"],
                    "change_pct": etf["change_pct"],
                    "premium_pct": etf["premium_pct"],
                    "buy1_vol": etf["buy1_vol"],
                    "sell1_vol": etf["sell1_vol"],
                    "buy1_price": etf["buy1_price"],
                    "sell1_price": etf["sell1_price"],
                    "ratio": etf.get("ratio", 0),
                    "premium": etf.get("premium", 0),
                    "score": etf.get("score", 0),
                    "futures_index": code_to_futures.get(etf["code"], ""),
                    "futures_change": etf.get("futures_change"),
                    "iopv_source": etf.get("iopv_source", "N/A"),
                    "weibi": etf.get("weibi", 0),
                    "outer_vol": etf.get("outer_vol", 0),
                    "inner_vol": etf.get("inner_vol", 0),
                })

            await _broadcast("snapshot", {
                "items": snapshot_items,
                "futures": {k: v for k, v in futures_data.items()},
                "fetch_time": round_result.get("fetch_time", ""),
            })

            # 7. Calculate sleep interval
            elapsed = time.monotonic() - loop_start
            interval = max(0.5, MIN_INTERVAL_SEC - elapsed)

            # 8. Broadcast status event
            await _broadcast("status", {
                "running": True,
                "proxy_count": len(proxy_urls),
                "interval_sec": round(interval, 2),
                "etf_count": len(etf_codes),
                "timestamp": datetime.now().isoformat(),
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
