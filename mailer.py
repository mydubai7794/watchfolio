"""提醒邮件通道：QQ SMTP + info 级 5 分钟聚合、critical 立即。

授权码存 sys_config（MAIL_ADDRESS/MAIL_AUTH_CODE），未配置时仅记日志。@author yys
"""
import asyncio
import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.header import Header

import database as db

logger = logging.getLogger("qdii.mailer")

AGG_WINDOW = 300.0            # info 聚合窗口 5 分钟

_cfg_cache: dict = {"at": 0.0, "addr": "", "code": ""}
_fail_streak = 0


async def _mail_config() -> tuple[str, str]:
    """读发件配置（10min 缓存）。"""
    global _cfg_cache
    if time.time() - _cfg_cache["at"] > 600:
        addr = await db.get_config("MAIL_ADDRESS") or ""
        code = await db.get_config("MAIL_AUTH_CODE") or ""
        _cfg_cache.update({"at": time.time(), "addr": addr, "code": code})
    return _cfg_cache["addr"], _cfg_cache["code"]


def _send_mail(subject: str, text: str) -> None:
    """同步发送（在线程池执行）。空配置时由 try_send 拦截。"""
    addr, code = _cfg_cache["addr"], _cfg_cache["code"]
    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = addr
    msg["To"] = addr
    with smtplib.SMTP_SSL("smtp.qq.com", 465, timeout=15) as smtp:
        smtp.login(addr, code)
        smtp.sendmail(addr, [addr], msg.as_string())


async def try_send(subject: str, text: str) -> bool:
    """带配置检查与失败记账的发送；未配置返回 False 仅记日志。

    配置读取与发送全程受保护：DB 抖动导致 get_config 异常时同样计入
    连败并记 warning，不向调用侧外泄（_emit 的后台任务无人接收异常）。
    """
    global _fail_streak
    try:
        addr, code = await _mail_config()
        if not addr or not code:
            logger.info("邮件未配置(sys_config MAIL_ADDRESS/MAIL_AUTH_CODE)，仅记录: %s",
                        subject)
            return False
        await asyncio.to_thread(_send_mail, subject, text)
        _fail_streak = 0
        return True
    except Exception as e:
        _fail_streak += 1
        logger.warning("邮件发送失败(%d 连败): %s", _fail_streak, e)
        return False


_bg_tasks: set = set()


def _emit(subject: str, text: str) -> None:
    """同步发送钩子：有事件循环时调度异步 try_send（配置检查+线程池+失败记账），
    无循环（测试同步调用）时直接走 _send_mail。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _send_mail(subject, text)          # 测试/无循环环境
        return
    t = loop.create_task(try_send(subject, text))
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)


class MailBox:
    """事件邮箱：info 进聚合缓冲、critical 立即出队。"""

    def __init__(self):
        self.pending: list[dict] = []
        self.window_start: float = 0.0

    def push(self, event: dict):
        self.pending.append(event)
        if not self.window_start:
            self.window_start = time.time()


def _render(events: list[dict]) -> tuple[str, str]:
    """聚合渲染：subject 带条数与首条标题，text 逐行 [级别] 标的 标题。

    字段全部防御取值，畸形事件不抛 KeyError（后台通道不容单体毒丸）。
    """
    lines = [f"[{str(e.get('level', 'info')).upper()}] "
             f"{e.get('symbol', '')} {e.get('title', '')}".strip()
             for e in events]
    subject = f"持仓提醒 x{len(events)}: {events[0].get('title', '')}"
    return subject, "\n".join(lines)


def flush(box: MailBox, force: bool = False, now: float | None = None) -> int:
    """聚合刷出（同步纯决策 + _emit 发送）；返回本轮尝试发出封数（发送为异步尽力语义）。

    - critical 立即发，从 pending 移除；
    - info 到期（force 或距窗口起点≥AGG_WINDOW）→ 发送并移除、窗口归零；
    - info 未到期 → 保留在 pending（window_start 不动，事件不丢）。
    """
    now = now if now is not None else time.time()
    if not box.pending:
        return 0
    critical = [e for e in box.pending if e.get("level") == "critical"]
    infos = [e for e in box.pending if e.get("level") != "critical"]
    sent = 0
    if critical:
        subj, text = _render(critical)
        _emit(subj, text)
        sent += 1
    due = force or (now - box.window_start >= AGG_WINDOW)
    if infos and due:
        subj, text = _render(infos)
        _emit(subj, text)
        sent += 1
        infos = []
    box.pending = infos
    if not box.pending:
        box.window_start = 0.0
    return sent


_queue: asyncio.Queue | None = None


async def mail_worker(interval: float = 30.0):
    """后台协程：消费全局队列入箱 → 按固定间隔聚合刷出。

    失败记账由 try_send 内 _fail_streak 承担，轮询间隔固定 interval。
    """
    global _queue
    _queue = asyncio.Queue()
    box = MailBox()

    async def _drain():
        while True:
            ev = await _queue.get()
            box.push(ev)

    drain = asyncio.create_task(_drain())
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                flush(box)
            except Exception as e:   # 单轮异常（如畸形事件）只跳过本轮，不杀通道
                logger.warning("邮件聚合刷出异常，本轮跳过: %s", e)
    finally:
        drain.cancel()
        _queue = None   # 停机后入队事件改走 enqueue 丢弃告警路径，防静默积压


def enqueue(event: dict):
    """提醒事件入队（须在事件循环线程内调用；asyncio.Queue 非线程安全，
    未初始化时直接丢弃并记日志）。"""
    if _queue is None:
        logger.warning("邮件队列未初始化，事件丢弃: %s", event.get("title"))
        return
    _queue.put_nowait(event)
