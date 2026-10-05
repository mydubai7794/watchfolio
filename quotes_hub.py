"""内存实时真相源：quotes_live + tick 环形缓冲 + 1s 降采样攒批。

push 高频 tick 不逐条落库：同秒取末值、价变才写、批量 flush market_ticks
（uk_sym_ts 幂等，flush 时同 symbol 多条错开 ts 防 INSERT IGNORE 丢点）。

线程契约：push_tick 线程安全，可在长桥 SDK 回调线程直调——Sampler 内部
全程持 threading.Lock；quotes_live 单键赋值原子（CPython GIL）、ring 的
deque.append 线程安全，二者不加锁。snapshot / flush_once / flush_loop
仅限事件循环线程调用。@author yys
"""
import asyncio
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import database as db

logger = logging.getLogger("qdii.quotes_hub")

RING_MAX = 1800                     # 每 symbol 保留最近 30 分钟（1s 粒度）
quotes_live: dict[str, dict] = {}   # {symbol: {price, bid, ask, ts, source}}
ring: dict[str, deque] = {}


def _ring_push(symbol: str, price: float):
    d = ring.setdefault(symbol, deque(maxlen=RING_MAX))
    d.append(price)


def update(symbol: str, price: float, source: str = "", bid: float | None = None,
           ask: float | None = None, ts: datetime | None = None):
    """所有数据入口统一写内存（推送/轮询/24h数据源）。"""
    quotes_live[symbol] = {"price": price, "bid": bid, "ask": ask,
                           "ts": ts or datetime.now(timezone.utc).isoformat(),
                           "source": source}
    _ring_push(symbol, price)


def snapshot() -> list[dict]:
    """SSE quote 快照（浅拷贝防遍历中变更）。"""
    return [{"symbol": s, **v} for s, v in list(quotes_live.items())]


class Sampler:
    """1s 窗口降采样：同秒取末值、价变才保留；线程安全（全程持锁）。

    _win 值为 (sec, price, flushed)：flushed 标记该价已随 drain 出队落库——
    价格停滞后不再重复出队同价点（防 market_ticks 行数放大）；同秒来新价时
    重置 flushed（新价未落库，待下次 drain 带出，兜住「最后一个价变」）。

    锁的必要性：push 在 SDK 回调线程被直调，drain/requeue 在事件循环线程
    flush 中被调——不持锁时 drain 迭代 _win 期间 push 改 dict 会触发
    RuntimeError（整批丢失），drain 写回还会覆盖 push 刚写入的新价。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._win: dict[str, tuple[int, float, bool]] = {}   # sym -> (sec, price, flushed)
        self._out: list[tuple[str, float]] = []

    def push(self, symbol: str, price: float, now: float | None = None):
        sec = int(now if now is not None else time.time())
        with self._lock:
            prev = self._win.get(symbol)
            if prev and prev[0] == sec:
                self._win[symbol] = (sec, price, False)  # 同秒取末值：新价待落库
                return
            if prev and prev[1] != price and not prev[2]:
                self._out.append((symbol, prev[1]))      # 上一秒定格价未落库且已价变 → 出队
            # 换秒：价未变继承 flushed（同价不重复出队）；价变则新价待落库
            flushed = bool(prev and prev[1] == price and prev[2])
            self._win[symbol] = (sec, price, flushed)

    def drain(self) -> list[tuple[str, float]]:
        """已定格的价变点 + 未落库的当前窗口末值一并出队。"""
        with self._lock:
            out = self._out
            self._out = []
            for sym, (sec, price, flushed) in self._win.items():
                if not flushed:
                    out.append((sym, price))
                    self._win[sym] = (sec, price, True)
            return out

    def requeue(self, rows: list[tuple[str, float]], cap: int = 10000):
        """落库失败回灌：rows（drain 返回的 (symbol, price) 形态）插回队头原序。

        INSERT IGNORE 幂等保证下轮重放安全；回灌后超 cap 丢最旧，防 DB 长时间
        挂掉时内存无限涨（与 flush 截断语义一致）。
        """
        with self._lock:
            self._out = rows + self._out
            if len(self._out) > cap:
                dropped = len(self._out) - cap
                self._out = self._out[-cap:]
                logger.warning("回灌后队列超限丢弃最旧 %d 条", dropped)


_sampler = Sampler()


def push_tick(symbol: str, price: float, source: str = "",
              bid: float | None = None, ask: float | None = None,
              now: float | None = None):
    """push 回调统一入口：内存 + 环形缓冲 + 降采样（now 仅测试注入用）。"""
    update(symbol, price, source=source, bid=bid, ask=ask)
    _sampler.push(symbol, price, now=now)


async def flush_once(max_queue: int = 10000) -> int:
    """单轮攒批落库 market_ticks，返回写入条数。

    ts 统一 UTC naive；同 symbol 多条错秒（uk_sym_ts 唯一键下同 ts 会被
    INSERT IGNORE 静默去重，错秒保住每个价变点）；落库失败回灌队头待下轮
    重放（INSERT IGNORE 幂等 + executemany 单语句原子），回灌后向上抛。
    """
    rows = _sampler.drain()
    if not rows:
        return 0
    if len(rows) > max_queue:
        dropped = len(rows) - max_queue
        rows = rows[-max_queue:]
        logger.warning("降采样队列超限丢弃最旧 %d 条", dropped)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    seen: dict[str, int] = {}
    batch = []
    for sym, price in rows:
        n = seen.get(sym, 0)
        seen[sym] = n + 1
        batch.append((sym, now + timedelta(seconds=n), price))
    try:
        return await db.save_market_ticks_batch(batch)
    except asyncio.CancelledError:
        raise                      # 任务关闭不回灌，直接传播
    except Exception:
        _sampler.requeue(rows, cap=max_queue)   # 失败回灌，防整批永久丢失
        raise


async def flush_loop(interval: float = 3.0, max_queue: int = 10000):
    """攒批落库循环（失败回灌待下轮重放，循环不退出）。"""
    while True:
        await asyncio.sleep(interval)
        try:
            await flush_once(max_queue=max_queue)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("降采样 flush 失败: %s", e)
