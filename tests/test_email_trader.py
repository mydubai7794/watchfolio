"""HSBC 成交邮件模块离线单元测试：解析样例全覆盖 + apply_trade 编排（db 全 mock，无网络）。"""
import asyncio
from datetime import datetime

import pytest

import email_trader as et

# ---------------------------------------------------------------------------
# 邮件正文样例（繁体中文，任务实测格式）
# ---------------------------------------------------------------------------

SAMPLE_BUY = """
全部執行: 買入 NVDA: NVIDIA CORP 的股/單位(交易編號: P741002)

• 交易編號: P741002
• 交易狀況: 全部執行
• 指示類別: 買入
• 股票名稱/ 股票編號: NVIDIA CORP (NVDA)
• 已成交數量(股/單位): 1
• 成交價: USD229.80
• 共成交數量(股/單位): 1
• 餘下數量(股/單位): 0
"""

SAMPLE_SELL_HK = """
全部執行: 賣出 0700: 騰訊控股 的股/單位(交易編號: P741100)

• 交易編號: P741100
• 交易狀況: 全部執行
• 指示類別: 賣出
• 股票名稱/ 股票編號: 騰訊控股 (00700)
• 已成交數量(股/單位): 100
• 成交價: HKD600.00
• 共成交數量(股/單位): 100
• 餘下數量(股/單位): 0
"""

HTML_BUY = """<html><body style="font-family:Arial">
<p>全部執行: 買入 NVDA: NVIDIA CORP 的股/單位(交易編號: P741002)</p>
<ul>
<li>交易編號: P741002</li>
<li>交易狀況: 全部執行</li>
<li>指示類別: 買入</li>
<li>股票名稱/ 股票編號: NVIDIA CORP (NVDA)</li>
<li>已成交數量(股/單位): 1</li>
<li>成交價: USD229.80</li>
<li>共成交數量(股/單位): 1</li>
<li>餘下數量(股/單位): 0</li>
</ul></body></html>"""


class TestParseHsbcTrade:
    def test_full_sample_buy(self):
        """任务实测样例：买入 NVDA 1 股 @USD229.80。"""
        t = et.parse_hsbc_trade(SAMPLE_BUY)
        assert t == {
            "trade_no": "P741002", "side": "BUY", "symbol": "NVDA",
            "name": "NVIDIA CORP", "market": "US", "quantity": 1.0,
            "price": 229.80, "currency": "USD",
        }

    def test_title_no_space(self):
        """標題「買入NVDA」无空格变体：字段行解析不受标题影响。"""
        t = et.parse_hsbc_trade(SAMPLE_BUY.replace("買入 NVDA", "買入NVDA"))
        assert t is not None and t["symbol"] == "NVDA" and t["side"] == "BUY"

    def test_sell_hk_stock(self):
        """卖出 + 港股编号格式：00700 → market HK / currency HKD。"""
        t = et.parse_hsbc_trade(SAMPLE_SELL_HK)
        assert t == {
            "trade_no": "P741100", "side": "SELL", "symbol": "00700",
            "name": "騰訊控股", "market": "HK", "quantity": 100.0,
            "price": 600.0, "currency": "HKD",
        }

    def test_sell_gu_chu_wording(self):
        """HSBC 真实邮件卖出表述为「沽出」（线上实测），须识别为 SELL。"""
        text = SAMPLE_SELL_HK.replace("指示類別: 賣出", "指示類別: 沽出")
        t = et.parse_hsbc_trade(text)
        assert t is not None and t["side"] == "SELL" and t["symbol"] == "00700"

    def test_partial_exec_rejected(self):
        """非「全部執行」（部分執行）→ None（等后续邮件）。"""
        text = SAMPLE_BUY.replace("全部執行", "部分執行")
        assert et.parse_hsbc_trade(text) is None

    def test_html_stripped(self):
        """HTML 正文经 _strip_html 剥标签后可解析（字段不粘连）。"""
        t = et.parse_hsbc_trade(et._strip_html(HTML_BUY))
        assert t is not None
        assert t["trade_no"] == "P741002" and t["symbol"] == "NVDA"
        assert t["quantity"] == 1.0 and t["price"] == 229.80

    def test_fullwidth_colon(self):
        """全角冒号「：」兼容。"""
        t = et.parse_hsbc_trade(SAMPLE_BUY.replace("交易編號: P741002", "交易編號：P741002")
                                .replace("成交價: USD229.80", "成交價：USD229.80"))
        assert t is not None and t["trade_no"] == "P741002" and t["price"] == 229.80

    def test_thousand_separator(self):
        """千分位数量/价格：1,000 股 @USD1,229.80。"""
        text = SAMPLE_BUY.replace("已成交數量(股/單位): 1", "已成交數量(股/單位): 1,000") \
                         .replace("共成交數量(股/單位): 1\n", "共成交數量(股/單位): 1,000\n") \
                         .replace("USD229.80", "USD1,229.80")
        t = et.parse_hsbc_trade(text)
        assert t is not None
        assert t["quantity"] == 1000.0 and t["price"] == 1229.80

    def test_price_without_prefix_falls_back_by_market(self):
        """成交價无货币前缀：US → USD 兜底。"""
        t = et.parse_hsbc_trade(SAMPLE_BUY.replace("USD229.80", "229.80"))
        assert t is not None and t["currency"] == "USD"

    def test_missing_price_rejected(self):
        """字段不全（缺成交價）→ None。"""
        text = SAMPLE_BUY.replace("• 成交價: USD229.80\n", "")
        assert et.parse_hsbc_trade(text) is None

    def test_missing_stock_rejected(self):
        """字段不全（缺股票名稱/股票編號行）→ None。"""
        text = SAMPLE_BUY.replace("• 股票名稱/ 股票編號: NVIDIA CORP (NVDA)\n", "")
        assert et.parse_hsbc_trade(text) is None

    def test_empty_text(self):
        assert et.parse_hsbc_trade("") is None
        assert et.parse_hsbc_trade(None) is None

    def test_unrelated_email(self):
        """非成交邮件（无关键字段）→ None。"""
        assert et.parse_hsbc_trade("您的賬戶結單已發出，請查收。") is None


# ---------------------------------------------------------------------------
# apply_trade 编排（db 层全 mock）
# ---------------------------------------------------------------------------

def _holding(**kw):
    h = {"id": 5, "symbol": "NVDA", "name": "NVIDIA CORP", "market": "US",
         "quantity": 10.0, "cost_price": 900.0, "currency": "USD",
         "enabled": 1, "sort_order": 3}
    h.update(kw)
    return h


def _trade(**kw):
    t = {"trade_no": "P741002", "side": "BUY", "symbol": "NVDA",
         "name": "NVIDIA CORP", "market": "US", "quantity": 1.0,
         "price": 229.80, "currency": "USD",
         "message_id": "<m1@x>", "email_date": None}
    t.update(kw)
    return t


def _patch_db(monkeypatch, holdings=None, message_exists=False, no_exists=False):
    """mock database 模块全部依赖函数，记录调用供断言。"""
    calls = {"insert": [], "upsert": [], "update": [], "deduct": []}

    async def fake_message_exists(mid):
        return message_exists

    async def fake_no_exists(tn):
        return no_exists

    async def fake_insert(**kw):
        calls["insert"].append(kw)
        return 1

    async def fake_get_holdings(enabled_only=False):
        return list(holdings or [])

    async def fake_upsert(symbol, name, market, quantity, cost_price, currency):
        calls["upsert"].append((symbol, name, market, quantity, cost_price, currency))
        return 1

    async def fake_update(hid, name, market, quantity, cost_price, currency,
                          enabled, sort_order):
        calls["update"].append((hid, name, market, quantity, cost_price,
                                currency, enabled, sort_order))
        return 1

    async def fake_deduct(currency, amount):
        calls["deduct"].append((currency, amount))
        return 1000.0 - amount

    monkeypatch.setattr(et.db, "trade_message_exists", fake_message_exists)
    monkeypatch.setattr(et.db, "trade_no_exists", fake_no_exists)
    monkeypatch.setattr(et.db, "insert_trade_record", fake_insert)
    monkeypatch.setattr(et.db, "get_holdings", fake_get_holdings)
    monkeypatch.setattr(et.db, "upsert_holding", fake_upsert)
    monkeypatch.setattr(et.db, "update_holding", fake_update)
    monkeypatch.setattr(et.db, "deduct_cash", fake_deduct)
    return calls


class TestApplyTrade:
    def test_duplicate_message_id(self, monkeypatch):
        """message_id 已存在 → duplicated 跳过，不插表不动持仓。"""
        calls = _patch_db(monkeypatch, message_exists=True)
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "duplicated"
        assert calls["insert"] == [] and calls["upsert"] == [] and calls["deduct"] == []

    def test_duplicate_trade_no(self, monkeypatch):
        """trade_no 已存在（同邮件多收）→ duplicated 跳过。"""
        calls = _patch_db(monkeypatch, no_exists=True)
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "duplicated" and r["reason"] == "trade_no_exists"
        assert calls["insert"] == []

    def test_buy_new_holding(self, monkeypatch):
        """BUY 无持仓 → upsert 建仓（成交价即成本）+ 扣现金。"""
        calls = _patch_db(monkeypatch, holdings=[])
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "applied"
        assert calls["upsert"] == [("NVDA", "NVIDIA CORP", "US", 1.0, 229.80, "USD")]
        assert calls["update"] == []
        assert calls["deduct"] == [("USD", 229.80)]
        assert calls["insert"][0]["message_id"] == "<m1@x>"
        assert calls["insert"][0]["trade_no"] == "P741002"

    def test_buy_add_existing(self, monkeypatch):
        """BUY 已有持仓 → update 仅累加数量，成本价/名称/排序保留原值。"""
        calls = _patch_db(monkeypatch, holdings=[_holding()])
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "applied"
        assert calls["upsert"] == []
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 11.0, 900.0, "USD", 1, 3)]
        assert calls["deduct"] == [("USD", 229.80)]

    def test_sell_reduce_and_add_cash(self, monkeypatch):
        """SELL → update 减数量（10→9）+ 负数扣现金即入账。"""
        calls = _patch_db(monkeypatch, holdings=[_holding()])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", trade_no="P741100", message_id="<m2@x>")))
        assert r["status"] == "applied"
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 9.0, 900.0, "USD", 1, 3)]
        assert calls["deduct"] == [("USD", -229.80)]

    def test_sell_to_zero_keeps_row(self, monkeypatch):
        """SELL 减到 0：保留行（enabled 照旧），数量归零。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(quantity=1.0)])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", message_id="<m3@x>")))
        assert r["status"] == "applied"
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 0.0, 900.0, "USD", 1, 3)]

    def test_sell_insufficient_rejected(self, monkeypatch):
        """SELL 减为负数 → rejected，不落交易表不动持仓。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(quantity=0.5)])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", quantity=1.0, message_id="<m4@x>")))
        assert r["status"] == "rejected" and r["reason"] == "insufficient_holding"
        assert calls["insert"] == [] and calls["update"] == [] and calls["deduct"] == []

    def test_sell_no_holding_rejected(self, monkeypatch):
        """SELL 无该 symbol 持仓 → rejected。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(symbol="MU")])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", message_id="<m5@x>")))
        assert r["status"] == "rejected" and r["reason"] == "no_holding"
        assert calls["insert"] == []

    def test_missing_message_id_rejected(self, monkeypatch):
        """无 message_id → rejected（无法幂等，不入账）。"""
        calls = _patch_db(monkeypatch, holdings=[])
        r = asyncio.run(et.apply_trade(_trade(message_id="")))
        assert r["status"] == "rejected" and r["reason"] == "missing_message_id"
        assert calls["insert"] == []

    def test_holding_update_failure_reported(self, monkeypatch):
        """持仓更新抛异常 → status=error（交易表已登记，人工修）。"""
        calls = _patch_db(monkeypatch, holdings=[_holding()])

        async def boom(*args):
            raise RuntimeError("db down")

        monkeypatch.setattr(et.db, "update_holding", boom)
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "error"
        assert len(calls["insert"]) == 1   # 幂等锁已落


class TestPollOnce:
    def test_poll_once_end_to_end(self, monkeypatch):
        """单轮链路：拉邮件(mock) → 解析 → 幂等应用。"""
        calls = _patch_db(monkeypatch, holdings=[])

        def fake_fetch(days=1):
            assert days == 1
            return [{"message_id": "<mail1@hsbc>", "date": None, "text": SAMPLE_BUY}]

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        results = asyncio.run(et.poll_once(days=1))
        assert len(results) == 1 and results[0]["status"] == "applied"
        assert calls["insert"][0]["message_id"] == "<mail1@hsbc>"
        assert calls["deduct"] == [("USD", 229.80)]

    def test_poll_once_skips_unparsed(self, monkeypatch):
        """非全部执行邮件：解析为 None 直接跳过，不触库。"""
        calls = _patch_db(monkeypatch, holdings=[])

        def fake_fetch(days=1):
            return [{"message_id": "<mail2@hsbc>", "date": None,
                     "text": SAMPLE_BUY.replace("全部執行", "部分執行")}]

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["insert"] == []


class TestStripHtml:
    def test_block_tags_to_newline(self):
        """块级标签转换行，字段各占一行防粘连。"""
        text = et._strip_html("<p>交易狀況: 全部執行</p><p>指示類別: 買入</p>")
        assert "交易狀況: 全部執行" in text and "指示類別: 買入" in text

    def test_entity_decode(self):
        assert et._strip_html("<p>NVIDIA&nbsp;CORP</p>") == "NVIDIA CORP"

    def test_empty(self):
        assert et._strip_html("") == ""


class TestParseEmailDate:
    def test_rfc2822_with_tz(self):
        dt = et._parse_email_date("Thu, 01 Oct 2026 21:30:00 +0800")
        assert dt is not None and dt.tzinfo is None  # 转本地 naive

    def test_bad_header(self):
        assert et._parse_email_date("garbage") is None
        assert et._parse_email_date("") is None


class TestIsRecent:
    """客户端日期过滤（QQ IMAP 忽略 SEARCH SINCE，历史邮件会全量返回）：
    Date 头落在最近 N 天内保留；无日期宁可不处理（防历史交易重复入账）。"""

    def test_today_kept(self):
        assert et._is_recent(datetime(2026, 10, 2, 12, 0), 1) is True

    def test_yesterday_kept(self):
        # days=1 → 窗口含昨天整天（since = today-1 的 00:00 起）
        assert et._is_recent(datetime(2026, 10, 1, 0, 30), 1) is True

    def test_older_dropped(self):
        assert et._is_recent(datetime(2026, 9, 30, 23, 59), 1) is False

    def test_no_date_dropped(self):
        assert et._is_recent(None, 1) is False
