"""腾讯行情攒批发送路由（请求总线）。

原则（用户）：次数要少，尽量同一批把数据都查出来。

背景：qt.gtimg.cn 批量行情被三个循环各自独立请求——
  1. A股监控 _monitor_loop（fetch_round，ETF 一批，5s 节奏）
  2. 候选池 _candidate_refresh_loop（候选一批，~10s）
  3. 持仓 _portfolio_loop（美股+港股一批，盘中 10s/夜盘 60s）
A股开盘（9:30-15:00 CST）恰逢美股夜盘窗口（CST 08:00-16:00），三循环并发
请求同一域名（实测 ~19 次/min）。总线把三者合并为「最高频消费者节奏下的
单次批量请求」：TTL 缓存直接命中 + 在飞请求共享 + 待查集合聚合进下一批，
请求次数收敛为最高频循环的节奏（~12 次/min，-37%）。

用法：消费方把「直接 GET 批量 URL」换成 `await bus.get(codes)`，返回拼接的
原始响应文本（v_xxx=...; ...），现有解析函数（按 ';' 切分）无感接入。
codes 必须是带前缀的腾讯代码（如 usAMD/hk03152/sz159501）。
"""
import asyncio
import logging
import time

import httpx

from config import TENCENT_BUS_INTERVAL_SEC

logger = logging.getLogger("tencent_bus")

TENCENT_QUOTE_URL = "http://qt.gtimg.cn/q={symbols}"


class TencentBus:
    """腾讯行情微批合并路由（模块级单例 tencent_bus）。

    - TTL 内的 code 直接回缓存；
    - 距上次拉取 >= MIN_INTERVAL 的 get() 立即发起一次批量拉取（聚合全部
      待查 code，一个 URL）；
    - MIN_INTERVAL 内并发到达的 get() 共享同一次在飞请求（其未覆盖的 code
      记入 pending，下一批带上；等待窗口内轮询重查）；
    - 单次拉取失败清空在飞状态，消费方拿到缓存旧值（宁用旧数据不放大请求）。
    """

    def __init__(self, min_interval: float = 3.0, ttl: float = 4.0,
                 timeout: float = 8.0):
        self._min_interval = min_interval   # 两次真实拉取的最小间隔（s）
        self._ttl = ttl                     # 缓存新鲜期（s）
        self._timeout = timeout
        self._result: dict[str, str] = {}   # code -> "v_xxx=..." 原始片段
        self._ts: dict[str, float] = {}     # code -> 拉取时刻（monotonic）
        self._pending: set[str] = set()     # 待下一批聚合的 code
        self._last_fetch = 0.0
        self._inflight: asyncio.Task | None = None

    # ---- 对外接口 ----

    async def get(self, codes: list[str]) -> str:
        """按 codes 取行情，返回拼接原始文本（与直接 GET 批量 URL 的响应等价）。"""
        codes = [c for c in dict.fromkeys(codes) if c]
        if not codes:
            return ""
        deadline = time.monotonic() + self._min_interval + self._timeout
        while True:
            missing = self._missing(codes)
            if not missing:
                return self._join(codes)
            now = time.monotonic()
            if now > deadline:
                return self._join(codes)   # 超时兜底：拿现有缓存（可能缺 code）
            self._pending.update(missing)  # 待查集合先并入（发起与蹭车都依赖）
            if self._inflight is None and now - self._last_fetch >= self._min_interval:
                self._launch()
            if self._inflight is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._inflight), timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            else:
                await asyncio.sleep(0.3)

    # ---- 内部实现 ----

    def _missing(self, codes: list[str]) -> list[str]:
        now = time.monotonic()
        return [c for c in codes if now - self._ts.get(c, -1e9) >= self._ttl]

    def _join(self, codes: list[str]) -> str:
        return ";".join(s for s in (self._result.get(c) for c in codes) if s)

    def _launch(self) -> None:
        self._inflight = asyncio.create_task(self._fetch())

    async def _fetch(self) -> None:
        try:
            batch = sorted(self._pending)
            self._pending.clear()
            if not batch:
                return
            url = TENCENT_QUOTE_URL.format(symbols=",".join(batch))
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning("总线拉取 HTTP %d (%d 只)", resp.status_code, len(batch))
                    return
                raw = resp.content.decode("gbk", errors="replace")
            now = time.monotonic()
            self._last_fetch = now
            for c in batch:
                # 本批全部 code 记拉取时刻（含无结果项）：失败 code 不无限催批，
                # TTL 过期后随下一轮自然重查
                self._ts[c] = now
            for chunk in raw.split(";"):
                chunk = chunk.strip()
                if not chunk.startswith("v_") or "=" not in chunk:
                    continue
                code = chunk[2:chunk.index("=")]
                self._result[code] = chunk
                self._ts[code] = now
        except Exception as e:
            logger.warning("总线拉取失败: %s", e)
        finally:
            self._inflight = None


tencent_bus = TencentBus(min_interval=TENCENT_BUS_INTERVAL_SEC)
