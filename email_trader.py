"""HSBC 成交邮件 → 解析 → 幂等落交易表 → 自动更新持仓与现金。

链路：QQ 邮箱 IMAP 拉取 HSBC 成交通知邮件（只拉最近 1 天）→ parse_hsbc_trade
解析关键字段（纯函数，繁体中文/全半角冒号/空格差异容错）→ apply_trade 幂等
编排：先插 trade_records（幂等锁）再更新 holdings 数量与 cash_accounts 余额。
"""
import asyncio
import email
import html as html_mod
import imaplib
import logging
import re
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime

import database as db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 邮箱配置（QQ 邮箱 IMAP，授权码非登录密码）
# ---------------------------------------------------------------------------
IMAP_HOST = "imap.qq.com"
IMAP_PORT = 993
IMAP_USER = "***REMOVED***"
IMAP_PASSWORD = "***REMOVED***"
HSBC_SENDER = "HSBC@notification.hsbc.com.hk"

POLL_INTERVAL_SEC = 1800  # 轮询间隔 30 分钟
FIRST_DELAY_SEC = 60      # 启动后首跑延迟

# IMAP SEARCH SINCE 日期的英文月份缩写（%b 受 locale 影响，手动映射保证英文）
_MONTHS_EN = ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

# ---------------------------------------------------------------------------
# HTML 剥标签（fetch 阶段兜底：multipart 无 text/plain 时用）
# ---------------------------------------------------------------------------

def _strip_html(html_text: str) -> str:
    """HTML 转纯文本：块级标签转换行防字段粘连，其余标签换空格，实体解码。"""
    text = re.sub(r"(?is)<(br|/p|/div|/tr|/li|/h[1-6])[^>]*>", "\n", html_text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()

# ---------------------------------------------------------------------------
# 邮件解析（纯函数，可离线测试）
# ---------------------------------------------------------------------------
# 字段行形如「• 交易編號: P741002」；标签与冒号间空格可缺失、冒号全/半角兼容、
# 繁简兼容（買入/买入、賣出/卖出）。数值允许千分位逗号。

_STATUS_RE = re.compile(r"(?:交易狀況|交易状况)\s*[:：]\s*(\S+)")
# HSBC 实测卖出表述为「沽出」（繁体賣出/简体卖出兼容）
_SIDE_RE = re.compile(r"(?:指示類別|指示类别)\s*[:：]\s*([買买]入|[賣卖]出|沽出)")
_TRADE_NO_RE = re.compile(r"(?:交易編號|交易编号)\s*[:：]\s*([A-Za-z0-9][A-Za-z0-9\-]*)")
_STOCK_RE = re.compile(r"股票名稱\s*/\s*股票編號\s*[:：]\s*(.*)")
_QTY_TOTAL_RE = re.compile(
    r"(?:共成交數量|共成交数量)\s*(?:[(（][^)）]*[)）])?\s*[:：]\s*([\d,]+(?:\.\d+)?)")
_QTY_FILLED_RE = re.compile(
    r"(?:已成交數量|已成交数量)\s*(?:[(（][^)）]*[)）])?\s*[:：]\s*([\d,]+(?:\.\d+)?)")
_PRICE_RE = re.compile(
    r"成交價\s*(?:[(（][^)）]*[)）])?\s*[:：]\s*([A-Z]{3})?\s*([\d,]+(?:\.\d+)?)")

# 市场对应币种（成交價缺前缀时兜底）
_MARKET_CURRENCY = {"HK": "HKD", "US": "USD"}


def parse_hsbc_trade(text: str) -> dict | None:
    """解析 HSBC 成交邮件正文 → 交易 dict；非「全部執行」或字段不全返回 None。

    返回 {trade_no, side(BUY/SELL), symbol, name, market, quantity, price, currency}。
    """
    if not text:
        return None
    text = text.replace("\u00a0", " ")

    # 交易狀況：只处理「全部執行」（部分執行等由后续邮件覆盖）
    sm = _STATUS_RE.search(text)
    if not sm:
        return None
    status = sm.group(1)
    if "全部執行" not in status and "全部执行" not in status:
        return None

    # 指示類別 → BUY / SELL
    bm = _SIDE_RE.search(text)
    if not bm:
        return None
    side = "SELL" if "出" in bm.group(1) else "BUY"

    tm = _TRADE_NO_RE.search(text)
    if not tm:
        return None

    # 股票名稱/股票編號:「NVIDIA CORP (NVDA)」→ name + symbol；港股「騰訊控股 (00700)」
    stm = _STOCK_RE.search(text)
    if not stm:
        return None
    line = stm.group(1).strip()
    nm = re.search(r"^(.*?)\s*[（(]\s*([A-Za-z0-9.\-]+)\s*[)）]", line)
    if not nm or not nm.group(1).strip():
        return None
    name = nm.group(1).strip()
    symbol = nm.group(2).strip()

    # 数量：共成交數量优先，缺失退化已成交數量（全部執行时两者相等）
    qm = _QTY_TOTAL_RE.search(text) or _QTY_FILLED_RE.search(text)
    if not qm:
        return None
    quantity = float(qm.group(1).replace(",", ""))
    if quantity <= 0:
        return None

    # 成交價：USD229.80 / HKD600.0 / 纯数字；货币缺省按市场兜底
    pm = _PRICE_RE.search(text)
    if not pm:
        return None
    currency = pm.group(1) or ""
    price = float(pm.group(2).replace(",", ""))
    if price <= 0:
        return None

    market = "HK" if symbol.isdigit() else "US"
    if not currency:
        currency = _MARKET_CURRENCY[market]

    return {
        "trade_no": tm.group(1),
        "side": side,
        "symbol": symbol,
        "name": name,
        "market": market,
        "quantity": quantity,
        "price": price,
        "currency": currency,
    }

# ---------------------------------------------------------------------------
# 邮件拉取（imaplib 标准库，同步；调用方用 asyncio.to_thread 包装）
# ---------------------------------------------------------------------------

def _parse_email_date(header: str) -> datetime | None:
    """Date 头 → naive 本地时间（MySQL DATETIME 直接可写）；解析失败返回 None。"""
    try:
        dt = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def _is_recent(dt: datetime | None, days: int) -> bool:
    """客户端日期过滤：Date 头落在最近 N 天内才保留。

    QQ 邮箱 IMAP 实测会忽略 SEARCH SINCE（返回全部历史邮件），若不过滤，
    首轮运行会把历史成交全部重复入账导致持仓/现金错乱，故必须客户端兜底。
    无 Date 头的邮件无法判定日期，宁可不处理（防错账）。
    """
    if dt is None:
        return False
    return dt.date() >= date.today() - timedelta(days=days)


def _extract_text(msg) -> str:
    """multipart 遍历取正文：text/plain 优先，无则 text/html 剥标签。"""
    plain, html_body = None, None
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype not in ("text/plain", "text/html"):
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        charset = part.get_content_charset() or "utf-8"
        try:
            decoded = payload.decode(charset, errors="replace")
        except (LookupError, UnicodeDecodeError):
            decoded = payload.decode("utf-8", errors="replace")
        if ctype == "text/plain":
            if plain is None:
                plain = decoded
        elif html_body is None:
            html_body = decoded
    if plain and plain.strip():
        return plain
    if html_body:
        return _strip_html(html_body)
    return ""


def fetch_hsbc_emails(days: int = 1) -> list[dict]:
    """拉取最近 N 天 HSBC 成交邮件 → [{message_id, date, text}]；异常返回 []。

    只读操作（SEARCH + FETCH），不做任何标记/删除。
    """
    out: list[dict] = []
    imap = None
    try:
        since = date.today() - timedelta(days=days)
        since_str = f"{since.day:02d}-{_MONTHS_EN[since.month]}-{since.year}"
        imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        imap.login(IMAP_USER, IMAP_PASSWORD)
        imap.select("INBOX")
        status, data = imap.search(
            None, f'(FROM "{HSBC_SENDER}" SINCE "{since_str}")')
        if status != "OK" or not data or not data[0]:
            return out
        for num in data[0].split():
            status, msg_data = imap.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data or msg_data[0] is None:
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            # 客户端日期过滤兜底（QQ IMAP 忽略 SEARCH SINCE，见 _is_recent 注释）
            dt = _parse_email_date(msg.get("Date", ""))
            if not _is_recent(dt, days):
                continue
            out.append({
                "message_id": (msg.get("Message-ID") or "").strip(),
                "date": dt,
                "text": _extract_text(msg),
            })
        return out
    except Exception as e:
        logger.error("拉取 HSBC 邮件失败: %s", e)
        return out
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# 交易应用编排（幂等：先插 trade_records 再动 holdings/cash）
# ---------------------------------------------------------------------------

async def apply_trade(trade: dict) -> dict:
    """应用一笔已解析交易：幂等检查 → 落交易表 → 更新持仓与现金。

    返回 {status: applied/duplicated/rejected/error, ...}：
    - duplicated: message_id 或 trade_no 已存在（防同邮件多收/重复投递）
    - rejected:   业务校验不过（卖出无持仓/持仓不足），不动表等人工核查
    - error:      交易表已落但持仓/现金更新失败（message_id 已在表不会重试，
                  logger.error 记录后人工修）
    """
    message_id = (trade.get("message_id") or "").strip()
    trade_no = (trade.get("trade_no") or "").strip()
    if not message_id:
        return {"status": "rejected", "reason": "missing_message_id", "trade": trade}

    # 幂等双查：message_id / trade_no 任一存在即跳过
    if await db.trade_message_exists(message_id):
        return {"status": "duplicated", "reason": "message_id_exists",
                "message_id": message_id}
    if trade_no and await db.trade_no_exists(trade_no):
        return {"status": "duplicated", "reason": "trade_no_exists",
                "trade_no": trade_no}

    side = trade["side"]
    symbol = trade["symbol"]
    qty = float(trade["quantity"])
    price = float(trade["price"])
    currency = trade["currency"]
    amount = round(qty * price, 2)

    holdings = await db.get_holdings()
    row = next((h for h in holdings if h["symbol"] == symbol), None)

    # SELL 业务校验放在落表前：拒绝的交易不应用也不登记，人工核查
    if side == "SELL":
        if row is None:
            logger.error("拒绝卖出: 无持仓 symbol=%s trade_no=%s", symbol, trade_no)
            return {"status": "rejected", "reason": "no_holding", "symbol": symbol}
        if float(row["quantity"]) - qty < 0:
            logger.error("拒绝卖出: 持仓不足 symbol=%s 持有=%s 卖出=%s trade_no=%s",
                         symbol, row["quantity"], qty, trade_no)
            return {"status": "rejected", "reason": "insufficient_holding",
                    "symbol": symbol}

    # 幂等锁：先插交易记录，成功后再动持仓/现金
    await db.insert_trade_record(
        message_id=message_id, trade_no=trade_no, symbol=symbol,
        name=trade.get("name", ""), market=trade.get("market", ""),
        side=side, quantity=qty, price=price, currency=currency,
        email_date=trade.get("email_date"))

    try:
        if side == "BUY":
            if row is None:
                # 新票建仓：本次成交价作为初始成本价
                await db.upsert_holding(symbol, trade.get("name", ""),
                                        trade.get("market", "US"), qty, price,
                                        currency)
                logger.info("买入建仓: %s(%s) %s股 @%s%s 现金扣减%s",
                            trade.get("name"), symbol, qty, currency, price, amount)
            else:
                # 已有票加仓：仅累加数量，名称/成本价/币种/启用/排序保留原值
                await db.update_holding(
                    row["id"], row["name"], row["market"],
                    float(row["quantity"]) + qty, row["cost_price"],
                    row["currency"], row["enabled"], row["sort_order"])
                logger.info("买入加仓: %s 持仓 %s→%s 现金扣减%s",
                            symbol, row["quantity"],
                            float(row["quantity"]) + qty, amount)
            balance = await db.deduct_cash(currency, amount)
        else:
            # 减到 0 保留行（enabled 照旧）方便后续再用
            await db.update_holding(
                row["id"], row["name"], row["market"],
                float(row["quantity"]) - qty, row["cost_price"],
                row["currency"], row["enabled"], row["sort_order"])
            balance = await db.deduct_cash(currency, -amount)
            logger.info("卖出减仓: %s 持仓 %s→%s 现金入账%s",
                        symbol, row["quantity"],
                        float(row["quantity"]) - qty, amount)
        return {"status": "applied", "side": side, "symbol": symbol,
                "quantity": qty, "amount": amount, "currency": currency,
                "balance": balance}
    except Exception as e:
        logger.error("交易应用失败(交易表已登记, 人工修复) message_id=%s: %s",
                     message_id, e)
        return {"status": "error", "reason": str(e), "side": side,
                "symbol": symbol}


async def poll_once(days: int = 1) -> list[dict]:
    """单轮处理：拉邮件 → 逐封解析 → 幂等应用，返回各封处理结果（供循环与测试）。"""
    mails = await asyncio.to_thread(fetch_hsbc_emails, days)
    results: list[dict] = []
    for mail in mails:
        trade = parse_hsbc_trade(mail.get("text", ""))
        if trade is None:
            logger.info("HSBC 邮件未识别为全部执行交易(忽略): %s",
                        mail.get("message_id", ""))
            continue
        trade["message_id"] = mail.get("message_id", "")
        trade["email_date"] = mail.get("date")
        result = await apply_trade(trade)
        logger.info("HSBC 交易处理: %s", result)
        results.append(result)
    return results
