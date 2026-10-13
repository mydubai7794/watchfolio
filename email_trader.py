"""券商成交邮件 → 解析 → 幂等落交易表 → 自动更新持仓与现金。

链路：QQ 邮箱 IMAP 拉取成交通知邮件（回溯窗口按上次成功轮询时刻自适应放大，
停机多久补多久，宁多勿漏）→ parse_hsbc_trade / parse_chief_trade / parse_za_trade
解析关键字段（纯函数，繁简/全半角冒号空格差异容错）→ apply_trade 幂等编排：先插
trade_records（幂等锁）再更新 holdings 数量与 cash_accounts 余额。

支持三类成交邮件：
- HSBC（HSBC@notification.hsbc.com.hk）：只处理「全部執行」，部分執行等终态邮件；
- 致富证券（donotreply@chiefgroup.com.hk，同发件人有营销邮件，靠解析门槛判别）：
  「订单成交及部份成交通知」，只处理待成交数量=0 的终态邮件，
  数量取该邮件已成交数量（订单累计终态，同 HSBC 口径）；
- 众安证券（notification@service.bank.za.group，同发件人还有 ZA Bank 银行通知
  邮件：换汇/定投/月结单等，靠解析门槛判别）：「完全成交」终态邮件，
  「已提交」中间态与「成交单已可供查阅」（无价格）版式自然挡；
  邮件无交易编号，幂等仅靠 message_id。
"""
import asyncio
import email
import html as html_mod
import imaplib
import logging
import os
import re
import unicodedata
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime

import config   # 触发 load_env_file：邮箱凭证环境变量在本模块常量求值前注入
import database as db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 邮箱配置（QQ 邮箱 IMAP，授权码非登录密码）
# ---------------------------------------------------------------------------
IMAP_HOST = "imap.qq.com"
IMAP_PORT = 993
# 邮箱账号与授权码属敏感凭据：全部经环境变量注入（QDII_IMAP_USER /
# QDII_IMAP_PASSWORD），任一缺失时轮询入口安全跳过（见 poll_once）
IMAP_USER = os.getenv("QDII_IMAP_USER", "")
IMAP_PASSWORD = os.getenv("QDII_IMAP_PASSWORD", "")
HSBC_SENDER = "HSBC@notification.hsbc.com.hk"
CHIEF_SENDER = "donotreply@chiefgroup.com.hk"
# 众安证券成交通知与 ZA Bank 银行通知同发件域（fetch_za_notice_emails 同源）
ZA_SENDER = "notification@service.bank.za.group"

POLL_INTERVAL_SEC = 1800  # 轮询间隔 30 分钟
FIRST_DELAY_SEC = 60      # 启动后首跑延迟

# 上次成功轮询时刻的持久化 key（database.sys_config），停机恢复后据此放大回溯窗口
LAST_POLL_KEY = "email_last_poll"

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
# 致富证券成交通知解析（纯函数）
# ---------------------------------------------------------------------------
# 版式（text/html 剥标签后，字段行无冒号）：「您卖出 #03152 (Ａ博时港元)的成交，
# 详情如下：」+ 字段表（参考编号/客户编号/货币/价格/已成交数量/待成交数量/
# 成交金额/最后更新时间）。同发件人还投递营销/研究邮件——以成交字段齐备为
# 门槛，营销邮件无这些字段自然被挡。

_CHIEF_SIDE_RE = re.compile(r"您\s*([買买]入|[賣卖]出|沽出)")
_CHIEF_SYM_RE = re.compile(
    r"([買买]入|[賣卖]出|沽出)\s*#?\s*([A-Za-z0-9.\-]+)\s*[（(]\s*([^()（）]+?)\s*[)）]")
_CHIEF_REF_RE = re.compile(r"参考编号\s*[:：]?\s*([A-Za-z0-9\-]+)")
_CHIEF_CUR_RE = re.compile(r"货币\s*[:：]?\s*([A-Z]{3})")
_CHIEF_PRICE_RE = re.compile(r"价格\s*[:：]?\s*([\d,]+(?:\.\d+)?)")
_CHIEF_FILLED_RE = re.compile(r"已成交数量\s*[:：]?\s*([\d,]+(?:\.\d+)?)")
_CHIEF_PENDING_RE = re.compile(r"待成交数量\s*[:：]?\s*([\d,]+(?:\.\d+)?)")
_CHIEF_AMOUNT_RE = re.compile(r"成交金额\s*[:：]?\s*([\d,]+(?:\.\d+)?)")


def parse_chief_trade(text: str) -> dict | None:
    """解析致富成交邮件正文 → 交易 dict；非终态成交或字段不全返回 None。

    终态口径（与 HSBC「全部執行」同款）：待成交数量=0 才应用，数量取该邮件
    已成交数量（订单累计终态）；部份成交邮件跳过等终态覆盖。
    全文先 NFKC 归一化：全角Ａ→A（与 holdings 名称一致）、全角数字/括号容错。

    返回 {trade_no, side(BUY/SELL), symbol, name, market, quantity, price,
    currency, amount(可为 None)}；amount 为邮件成交金额（致富现金动账口径，
    缺省退化 qty×price）。
    """
    if not text:
        return None
    text = unicodedata.normalize("NFKC", text).replace("\u00a0", " ")

    # 成交字段门槛：任一缺失视为非成交邮件（营销/研究邮件在此被挡）
    sm = _CHIEF_SIDE_RE.search(text)
    rm = _CHIEF_REF_RE.search(text)
    fm = _CHIEF_FILLED_RE.search(text)
    pm = _CHIEF_PENDING_RE.search(text)
    if not (sm and rm and fm and pm):
        return None

    # 终态门控：待成交>0 为部份成交中间态，跳过
    if float(pm.group(1).replace(",", "")) > 0:
        return None
    quantity = float(fm.group(1).replace(",", ""))
    if quantity <= 0:
        return None

    # 代码与名称：「#03152 (Ａ博时港元)」→ 03152 / A博时港元；美股无 # 前缀
    sym = _CHIEF_SYM_RE.search(text)
    if not sym or not sym.group(3).strip():
        return None
    symbol = sym.group(2).strip()
    name = sym.group(3).strip()

    prm = _CHIEF_PRICE_RE.search(text)
    if not prm:
        return None
    price = float(prm.group(1).replace(",", ""))
    if price <= 0:
        return None

    market = "HK" if symbol.isdigit() else "US"
    cm = _CHIEF_CUR_RE.search(text)
    currency = cm.group(1) if cm else _MARKET_CURRENCY[market]

    am = _CHIEF_AMOUNT_RE.search(text)
    amount = float(am.group(1).replace(",", "")) if am else None

    side = "SELL" if "出" in sm.group(1) else "BUY"
    return {
        "trade_no": rm.group(1),
        "side": side,
        "symbol": symbol,
        "name": name,
        "market": market,
        "quantity": quantity,
        "price": price,
        "currency": currency,
        "amount": amount,
    }

# ---------------------------------------------------------------------------
# 众安证券成交通知解析（纯函数）
# ---------------------------------------------------------------------------
# 版式（text/html 剥标签后，全角冒号、字段两侧带空格）：
# 「你的股票卖出订单已完全成交。/ 卖出股票： QQQ ( 纳指100 ETF ) /
# 委托数量： 1 股 / 成交详情： 1 股 @ USD 747.6700」。终态门控=「已完全成交」；
# 「已提交」版式无成交详情、买入「成交单已可供查阅」版式无委托数量与价格，
# 字段门槛自然挡；同发件人的 ZA Bank 银行通知（换汇/定投/月结单）无股票
# 字段同样被挡。邮件无交易编号 → trade_no 空串（幂等仅靠 message_id）。

_ZA_DONE_RE = re.compile(r"你的股票\s*([買买]入|[賣卖]出)\s*[订訂][单單]已完全成交")
_ZA_SYM_RE = re.compile(
    r"([買买]入|[賣卖]出)股票\s*[:：]\s*([A-Za-z0-9.\-]+)\s*[（(]\s*([^()（）]+?)\s*[)）]")
_ZA_QTY_RE = re.compile(r"委[托託][数數]量\s*[:：]\s*([\d,]+(?:\.\d+)?)\s*股")
_ZA_PRICE_RE = re.compile(r"股\s*@\s*([A-Z]{3})\s*([\d,]+(?:\.\d+)?)")


def parse_za_trade(text: str) -> dict | None:
    """解析众安证券成交邮件正文 → 交易 dict；非「已完全成交」终态或字段不全返回 None。

    终态门控（与 HSBC「全部執行」/致富「待成交=0」同款）：只处理
    「你的股票买入/卖出订单已完全成交」；「已提交」等中间态邮件跳过等终态
    邮件覆盖。全文先 NFKC 归一化（全角符号/数字容错）。

    返回 {trade_no(""), side(BUY/SELL), symbol, name, market, quantity,
    price, currency}；众安邮件无成交金额行，现金动账由 apply_trade 按
    qty×price 计算。
    """
    if not text:
        return None
    text = unicodedata.normalize("NFKC", text).replace("\u00a0", " ")

    # 终态门控：你的股票买入/卖出订单已完全成交（已提交等中间态在此被挡）
    dm = _ZA_DONE_RE.search(text)
    if not dm:
        return None
    side = "SELL" if "出" in dm.group(1) else "BUY"

    # 标的：「卖出股票： QQQ ( 纳指100 ETF )」→ symbol + name
    sm = _ZA_SYM_RE.search(text)
    if not sm or not sm.group(3).strip():
        return None
    symbol = sm.group(2).strip()
    name = sm.group(3).strip()

    qm = _ZA_QTY_RE.search(text)
    if not qm:
        return None
    quantity = float(qm.group(1).replace(",", ""))
    if quantity <= 0:
        return None

    # 成交价：「1 股 @ USD 747.6700」——「股 @」锚点避开邮件头部样式噪音
    pm = _ZA_PRICE_RE.search(text)
    if not pm:
        return None
    price = float(pm.group(2).replace(",", ""))
    if price <= 0:
        return None

    market = "HK" if symbol.isdigit() else "US"
    return {
        "trade_no": "",
        "side": side,
        "symbol": symbol,
        "name": name,
        "market": market,
        "quantity": quantity,
        "price": price,
        "currency": pm.group(1),
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


def _fetch_sender_emails(sender: str, days: int, label: str) -> list[dict]:
    """拉取最近 N 天指定发件人的邮件 → [{message_id, date, text}]。

    只读操作（SEARCH + FETCH），不做任何标记/删除。
    拉取失败（登录/网络/协议异常）抛出异常而非返回 []——调用方需区分
    「无新邮件」与「拉取失败」：前者才允许推进轮询水位，后者保持水位下轮重试。
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
            None, f'(FROM "{sender}" SINCE "{since_str}")')
        if status != "OK" or not data or not data[0]:
            return out
        for num in data[0].split():
            status, msg_data = imap.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data or msg_data[0] is None:
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            # 客户端日期过滤兜底（QQ IMAP 忽略 SEARCH SINCE，见 _is_recent 注释）；
            # 窗口与 SEARCH SINCE 同用 days 参数，停机恢复放大时两处同步放宽
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
        logger.error("拉取 %s 邮件失败: %s", label, e)
        raise
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass


def fetch_hsbc_emails(days: int = 1) -> list[dict]:
    """拉取最近 N 天 HSBC 成交邮件 → [{message_id, date, text}]。"""
    return _fetch_sender_emails(HSBC_SENDER, days, "HSBC")


def fetch_chief_emails(days: int = 1) -> list[dict]:
    """拉取最近 N 天致富证券邮件 → [{message_id, date, text}]。

    同发件人包含营销/研究邮件，多拉的由 parse_chief_trade 解析门槛挡掉。
    """
    return _fetch_sender_emails(CHIEF_SENDER, days, "致富")


def fetch_za_emails(days: int = 1) -> list[dict]:
    """拉取最近 N 天众安证券邮件 → [{message_id, date, text}]。

    同发件人包含 ZA Bank 银行通知邮件（换汇/定投/月结单等），多拉的由
    parse_za_trade 解析门槛挡掉（无终态句/无成交价字段）。
    """
    return _fetch_sender_emails(ZA_SENDER, days, "众安")


def fetch_za_notice_emails(days: int = 30) -> list[dict]:
    """拉取最近 N 天 ZA Bank 通知邮件（换汇/转出/派息/入金版式）→ [{message_id, date, subject, text}]。

    记账模块（ledger）录入辅助通道：仅供人工/AI 对账采样，不进生产轮询循环。
    只读操作，拉取失败抛异常。
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
            None, f'(FROM "notification@service.bank.za.group" SINCE "{since_str}")')
        if status != "OK" or not data or not data[0]:
            return out
        for num in data[0].split():
            status, msg_data = imap.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data or msg_data[0] is None:
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            dt = _parse_email_date(msg.get("Date", ""))
            if not _is_recent(dt, days):
                continue
            subject = msg.get("Subject") or ""
            try:
                parts = email.header.decode_header(subject)
                subject = "".join(
                    p[0].decode(p[1] or "utf-8", "replace") if isinstance(p[0], bytes) else p[0]
                    for p in parts)
            except Exception:
                pass
            out.append({
                "message_id": (msg.get("Message-ID") or "").strip(),
                "date": dt,
                "subject": subject,
                "text": _extract_text(msg),
            })
        return out
    except Exception as e:
        logger.error("拉取 ZA Bank 通知邮件失败: %s", e)
        raise
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
    # 现金动账金额：致富取邮件成交金额（含券商口径的整笔金额），缺省 qty×price（HSBC）
    amount = round(float(trade.get("amount") or qty * price), 2)

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
                # 已有票加仓：数量累加，成本价按加权平均摊薄（原成本缺失/为0时
                # 以本次成交价充当）；名称/币种/启用/排序保留原值
                old_qty = float(row["quantity"])
                old_cost = float(row["cost_price"] or 0)
                new_qty = old_qty + qty
                avg_cost = round(price if old_cost <= 0 else
                                 (old_qty * old_cost + qty * price) / new_qty, 4)
                await db.update_holding(
                    row["id"], row["name"], row["market"],
                    new_qty, avg_cost,
                    row["currency"], row["enabled"], row["sort_order"])
                logger.info("买入加仓: %s 持仓 %s→%s 成本摊薄 %s→%s 现金扣减%s",
                            symbol, row["quantity"], new_qty,
                            row["cost_price"], avg_cost, amount)
            balance = await db.deduct_cash(currency, amount)
        else:
            # 减仓：成本价按摊薄口径重算（已实现盈亏摊入剩余持仓）：
            # (原数量×原成本 − 卖出数量×卖价) / 剩余数量——亏损减仓拉高成本、
            # 盈利减仓压低成本（大额盈利减仓后可为负，DECIMAL 有符号可存）；
            # 减到 0 保留行（enabled 照旧）成本保持原值，原成本未知(0)不摊薄
            old_qty = float(row["quantity"])
            old_cost = float(row["cost_price"] or 0)
            new_qty = old_qty - qty
            if new_qty > 0 and old_cost != 0:
                avg_cost = round((old_qty * old_cost - qty * price) / new_qty, 4)
            else:
                avg_cost = row["cost_price"]
            await db.update_holding(
                row["id"], row["name"], row["market"],
                new_qty, avg_cost,
                row["currency"], row["enabled"], row["sort_order"])
            balance = await db.deduct_cash(currency, -amount)
            logger.info("卖出减仓: %s 持仓 %s→%s 成本摊薄 %s→%s 现金入账%s",
                        symbol, row["quantity"], new_qty,
                        row["cost_price"], avg_cost, amount)
        return {"status": "applied", "side": side, "symbol": symbol,
                "quantity": qty, "amount": amount, "currency": currency,
                "balance": balance}
    except Exception as e:
        logger.error("交易应用失败(交易表已登记, 人工修复) message_id=%s: %s",
                     message_id, e)
        return {"status": "error", "reason": str(e), "side": side,
                "symbol": symbol}


def _compute_backtrack_days(last_poll: str | None, default_days: int = 1) -> int:
    """按上次成功轮询时刻计算本次回溯天数：宁多勿漏。

    停机超 1 天后恢复时（如停 3 天）回溯 gap_days+1 天（=4），多拉的旧邮件
    由 apply_trade 幂等（message_id/trade_no 去重）兜住，不怕重复处理。
    last_poll 缺失或格式异常时回退 default_days（首跑/配置损坏的保守窗口）。
    """
    if not last_poll:
        return default_days
    try:
        last = datetime.fromisoformat(last_poll)
    except (TypeError, ValueError):
        logger.warning("last_poll 配置值无法解析: %r，回退 %d 天窗口", last_poll, default_days)
        return default_days
    gap_days = (datetime.now() - last).days
    return max(default_days, gap_days + 1)


async def poll_once(days: int = 1) -> list[dict]:
    """单轮处理（HSBC + 致富 + 众安三源）：自适应回溯拉邮件 → 逐封解析 → 幂等应用。

    回溯天数 = max(days, (now - 上次成功轮询).days + 1)，修复停机超 1 天
    成交邮件永久漏账的问题。仅当全部源整轮无异常完成（即使 0 封新邮件）才把
    last_poll 推进到当前时刻；任一源拉取异常时保持旧水位，下轮放大窗口重试
    （幂等去重不怕重复；该轮已成功应用的交易不受影响）。
    """
    # 凭证未配置：安全跳过本轮（不抛异常、不推进水位，配置好后自然恢复轮询）
    if not IMAP_USER or not IMAP_PASSWORD:
        logger.warning("邮箱凭证未配置(QDII_IMAP_USER/QDII_IMAP_PASSWORD)，"
                       "本轮成交邮件轮询跳过")
        return []
    # 读取上次成功轮询时刻（读取失败不阻断本轮，回退保守窗口）
    try:
        last_poll = await db.get_config(LAST_POLL_KEY)
    except Exception as e:
        logger.warning("读取 %s 失败，回退 %d 天窗口: %s", LAST_POLL_KEY, days, e)
        last_poll = None
    backtrack_days = _compute_backtrack_days(last_poll, days)

    # 三源拉取：任一失败即标记，整轮结束不推进水位（防漏账放大）
    fetch_failed = False
    hsbc_mails: list[dict] = []
    chief_mails: list[dict] = []
    za_mails: list[dict] = []
    try:
        hsbc_mails = await asyncio.to_thread(fetch_hsbc_emails, backtrack_days)
    except Exception as e:
        fetch_failed = True
        logger.error("本轮 HSBC 邮件拉取失败，保持轮询水位不推进: %s", e)
    try:
        chief_mails = await asyncio.to_thread(fetch_chief_emails, backtrack_days)
    except Exception as e:
        fetch_failed = True
        logger.error("本轮致富邮件拉取失败，保持轮询水位不推进: %s", e)
    try:
        za_mails = await asyncio.to_thread(fetch_za_emails, backtrack_days)
    except Exception as e:
        fetch_failed = True
        logger.error("本轮众安邮件拉取失败，保持轮询水位不推进: %s", e)

    results: list[dict] = []
    for mail in hsbc_mails:
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

    for mail in chief_mails:
        trade = parse_chief_trade(mail.get("text", ""))
        if trade is None:
            logger.info("致富邮件未识别为终态成交(忽略): %s",
                        mail.get("message_id", ""))
            continue
        trade["message_id"] = mail.get("message_id", "")
        trade["email_date"] = mail.get("date")
        result = await apply_trade(trade)
        logger.info("致富交易处理: %s", result)
        results.append(result)

    for mail in za_mails:
        trade = parse_za_trade(mail.get("text", ""))
        if trade is None:
            logger.info("众安邮件未识别为完全成交交易(忽略): %s",
                        mail.get("message_id", ""))
            continue
        trade["message_id"] = mail.get("message_id", "")
        trade["email_date"] = mail.get("date")
        result = await apply_trade(trade)
        logger.info("众安交易处理: %s", result)
        results.append(result)

    # 整轮无异常完成才推进水位（回溯窗口随时间自动收敛回 1 天）
    if fetch_failed:
        return results
    await db.set_config(LAST_POLL_KEY, datetime.now().isoformat(timespec="seconds"),
                        remark="成交邮件上次成功轮询时刻")
    return results
