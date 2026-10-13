"""HSBC 成交邮件模块离线单元测试：解析样例全覆盖 + apply_trade 编排（db 全 mock，无网络）。"""
import asyncio
from datetime import datetime, timedelta

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

# ---------------------------------------------------------------------------
# 致富证券成交通知样例（2026-10-07 线上实测邮件正文，字段行无冒号、全角Ａ）
# ---------------------------------------------------------------------------

CHIEF_SELL = """
订单成交及部份成交通知

亲爱的客户：
您卖出 #03152 (Ａ博时港元)的成交，详情如下：
参考编号 98438804
客户编号 M****27
货币 HKD
价格 1135.100
已成交数量 1
待成交数量 0
成交金额 1135.100
最后更新时间 2026-10-07 10:57:44
"""

CHIEF_BUY_HTML = """<html><body>
<p>亲爱的客户：</p>
<p>您买入 #03152 (Ａ博时港元)的成交，详情如下：</p>
<table>
<tr><td>参考编号</td><td>98438805</td></tr>
<tr><td>客户编号</td><td>M****27</td></tr>
<tr><td>货币</td><td>HKD</td></tr>
<tr><td>价格</td><td>1135.100</td></tr>
<tr><td>已成交数量</td><td>1</td></tr>
<tr><td>待成交数量</td><td>0</td></tr>
<tr><td>成交金额</td><td>1135.100</td></tr>
<tr><td>最后更新时间</td><td>2026-10-07 11:00:00</td></tr>
</table></body></html>"""

CHIEF_MARKETING = """
致富前瞻｜美欧央行纪会将揭晓，AI产业迎来哪些新看点？
亲爱的客户：
美光公布2026财年第四季度业绩：收入542.29亿美元，同比增长379.3%。
致富证券研究部温馨提示：投资涉及风险，证券价格可升亦可跌。
"""

# ---------------------------------------------------------------------------
# 众安证券成交通知样例（2026-10-12 线上实测邮件正文，_strip_html 提取形态）
# ---------------------------------------------------------------------------

ZA_SELL_FULL = """
股票卖出成交
 你好 dubai ：
 你的股票卖出订单已完全成交。
 卖出股票： QQQ ( 纳指100 ETF )
 委托数量： 1 股
 成交详情：
 1 股 @ USD 747.6700
 请登入 ZA Bank App 查阅订单详情。
"""

ZA_SELL_FRACTION = """
股票卖出成交
 你好 dubai ：
 你的股票卖出订单已完全成交。
 卖出股票： NVDA ( 英伟达 )
 委托数量： 0.2706 股
 成交详情：
 0.2706 股 @ USD 227.2700
 请登入 ZA Bank App 查阅订单详情。
"""

ZA_SELL_SUBMITTED = """
你好 dubai：
你的股票卖出订单已提交。
卖出股票： NVDA (英伟达)
委托数量：0.2706 股
委托状态：已提交
委托时间：2026-10-12 21:54:38
你可于 ZA Bank App 查看最新的订单详情。
"""

ZA_BUY_NOTE_NO_PRICE = """
发出成交单
 你好 dubai ：
 你的买入成交单已发出。
 买入股票： NVDA ( 英伟达 )
 成交数量： 0.0012 股
 请登入众安银行 App 查阅及下载成交单。
"""

ZA_FUND_SUBSCRIBE = """
认购成功通知
 你好 dubai ：
 你已于 2026-10-12 18:01:21 成功 定投 认购 HKD 1,100.00 邓普顿智选亚太股票入息基金 HKD (A Dis) 。
 认购金额：
 HKD 1,100.00
"""


@pytest.fixture(autouse=True)
def _stub_chief_fetch(monkeypatch):
    """默认桩掉致富邮件拉取，保证全部用例离线（不触 IMAP）。

    需要致富邮件的用例在测试体内再次 monkeypatch.setattr 覆盖即可
    （后设者优先）。"""
    monkeypatch.setattr(et, "fetch_chief_emails", lambda days=1: [])


@pytest.fixture(autouse=True)
def _stub_za_fetch(monkeypatch):
    """默认桩掉众安邮件拉取（poll_once 三源之一），保证全部用例离线。

    需要众安邮件的用例在测试体内再次 monkeypatch.setattr 覆盖即可
    （后设者优先）。"""
    monkeypatch.setattr(et, "fetch_za_emails", lambda days=1: [])


@pytest.fixture(autouse=True)
def _fake_imap_creds(monkeypatch):
    """注入明显假凭证，解耦真实环境：poll_once 入口在 IMAP_USER/PASSWORD
    任一为空时安全短路返回 []——干净环境（无 qdii.env、无 QDII_IMAP_* 环境变量）
    下模块常量为空串，TestPollOnce/TestPollOnceChief 会因此全红。
    假凭证不会真连 IMAP（拉取函数全部被 mock）。"""
    monkeypatch.setattr(et, "IMAP_USER", "u@test")
    monkeypatch.setattr(et, "IMAP_PASSWORD", "p@test")


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


def _patch_db(monkeypatch, holdings=None, message_exists=False, no_exists=False,
              last_poll=None):
    """mock database 模块全部依赖函数，记录调用供断言。"""
    calls = {"insert": [], "upsert": [], "update": [], "deduct": [], "config": []}

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

    async def fake_get_config(key):
        return last_poll

    async def fake_set_config(key, value, remark=""):
        calls["config"].append((key, value))
        return 1

    monkeypatch.setattr(et.db, "trade_message_exists", fake_message_exists)
    monkeypatch.setattr(et.db, "trade_no_exists", fake_no_exists)
    monkeypatch.setattr(et.db, "insert_trade_record", fake_insert)
    monkeypatch.setattr(et.db, "get_holdings", fake_get_holdings)
    monkeypatch.setattr(et.db, "upsert_holding", fake_upsert)
    monkeypatch.setattr(et.db, "update_holding", fake_update)
    monkeypatch.setattr(et.db, "deduct_cash", fake_deduct)
    monkeypatch.setattr(et.db, "get_config", fake_get_config)
    monkeypatch.setattr(et.db, "set_config", fake_set_config)
    return calls


class TestParseChiefTrade:
    """致富证券「订单成交及部份成交通知」解析：终态门控 + 全角/无冒号容错。"""

    def test_real_sample_sell(self):
        """线上实测样例：卖出 03152 1股 @HKD1135.10，Ａ全角归一化为 A。"""
        t = et.parse_chief_trade(CHIEF_SELL)
        assert t == {
            "trade_no": "98438804", "side": "SELL", "symbol": "03152",
            "name": "A博时港元", "market": "HK", "quantity": 1.0,
            "price": 1135.10, "currency": "HKD", "amount": 1135.10,
        }

    def test_html_table_form(self):
        """HTML 表格版式（text/html 剥标签后字段无冒号、各占一行）。"""
        t = et.parse_chief_trade(et._strip_html(CHIEF_BUY_HTML))
        assert t is not None
        assert t["side"] == "BUY" and t["trade_no"] == "98438805"
        assert t["symbol"] == "03152" and t["name"] == "A博时港元"
        assert t["quantity"] == 1.0 and t["price"] == 1135.10

    def test_partial_fill_skipped(self):
        """待成交数量>0（部份成交非终态）→ None，等终态邮件。"""
        text = CHIEF_SELL.replace("待成交数量 0", "待成交数量 3")
        assert et.parse_chief_trade(text) is None

    def test_zero_filled_skipped(self):
        """已成交 0 / 待成交 0（订单未有任何成交）→ None。"""
        text = CHIEF_SELL.replace("已成交数量 1", "已成交数量 0") \
                         .replace("成交金额 1135.100", "成交金额 0.000")
        assert et.parse_chief_trade(text) is None

    def test_marketing_mail_rejected(self):
        """同发件人的营销/研究邮件无成交字段 → None。"""
        assert et.parse_chief_trade(CHIEF_MARKETING) is None

    def test_us_symbol_buy(self):
        """美股代码（无 # 前缀、字母）→ market US。"""
        text = ("您买入 NVDA (NVIDIA CORP)的成交，详情如下：\n"
                "参考编号 98438806\n货币 USD\n价格 1,229.80\n"
                "已成交数量 1\n待成交数量 0\n成交金额 1,229.80\n")
        t = et.parse_chief_trade(text)
        assert t is not None
        assert t["market"] == "US" and t["currency"] == "USD"
        assert t["symbol"] == "NVDA" and t["side"] == "BUY"

    def test_missing_currency_falls_back_by_market(self):
        """货币行缺失 → 按市场兜底 HK。"""
        text = CHIEF_SELL.replace("货币 HKD\n", "")
        t = et.parse_chief_trade(text)
        assert t is not None and t["currency"] == "HKD"

    def test_missing_amount_degrades(self):
        """成交金额行缺失 → amount None（应用时退化 qty×price）。"""
        text = CHIEF_SELL.replace("成交金额 1135.100\n", "")
        t = et.parse_chief_trade(text)
        assert t is not None and t["amount"] is None
        assert t["quantity"] == 1.0 and t["price"] == 1135.10

    def test_thousand_separator(self):
        """千分位数量/价格/金额。"""
        text = ("您卖出 #03152 (Ａ博时港元)的成交，详情如下：\n"
                "参考编号 98438807\n货币 HKD\n价格 1,135.100\n"
                "已成交数量 1,000\n待成交数量 0\n成交金额 1,135,100.000\n")
        t = et.parse_chief_trade(text)
        assert t is not None
        assert t["quantity"] == 1000.0 and t["price"] == 1135.10
        assert t["amount"] == 1135100.0

    def test_traditional_chinese_side(self):
        """繁体買入/賣出句式兼容。"""
        text = CHIEF_SELL.replace("您卖出", "您賣出")
        t = et.parse_chief_trade(text)
        assert t is not None and t["side"] == "SELL"
        text2 = ("您買入 #03152 (Ａ博时港元)的成交，详情如下：\n"
                 "参考编号 98438808\n货币 HKD\n价格 1135.100\n"
                 "已成交数量 1\n待成交数量 0\n成交金额 1135.100\n")
        assert et.parse_chief_trade(text2)["side"] == "BUY"

    def test_field_incomplete_rejected(self):
        """缺参考编号/缺待成交数量 → None（防营销邮件误伤 + 无法判定终态）。"""
        assert et.parse_chief_trade(CHIEF_SELL.replace("参考编号 98438804\n", "")) is None
        assert et.parse_chief_trade(CHIEF_SELL.replace("待成交数量 0\n", "")) is None

    def test_empty_text(self):
        assert et.parse_chief_trade("") is None
        assert et.parse_chief_trade(None) is None


class TestParseZaTrade:
    """众安证券「完全成交」终态邮件解析：已提交/无价格版式自然挡。"""

    def test_real_sample_sell(self):
        """线上实测样例：卖出 QQQ 1股 @USD747.67，trade_no 空（邮件无编号）。"""
        t = et.parse_za_trade(ZA_SELL_FULL)
        assert t == {
            "trade_no": "", "side": "SELL", "symbol": "QQQ",
            "name": "纳指100 ETF", "market": "US", "quantity": 1.0,
            "price": 747.67, "currency": "USD",
        }

    def test_fractional_shares(self):
        """碎股卖出：0.2706 股 @USD227.27。"""
        t = et.parse_za_trade(ZA_SELL_FRACTION)
        assert t is not None
        assert t["symbol"] == "NVDA" and t["side"] == "SELL"
        assert t["quantity"] == 0.2706 and t["price"] == 227.27

    def test_submitted_not_final_skipped(self):
        """「已提交」中间态邮件（无终态句无成交价）→ None，等终态覆盖。"""
        assert et.parse_za_trade(ZA_SELL_SUBMITTED) is None

    def test_buy_note_without_price_skipped(self):
        """买入「成交单已可供查阅」版式（无委托数量/无价格）→ None。"""
        assert et.parse_za_trade(ZA_BUY_NOTE_NO_PRICE) is None

    def test_bank_notice_skipped(self):
        """同发件人 ZA Bank 银行通知（基金定投认购）→ None。"""
        assert et.parse_za_trade(ZA_FUND_SUBSCRIBE) is None

    def test_traditional_chinese_side(self):
        """繁体買入/賣出句式兼容。"""
        text = ZA_SELL_FULL.replace("卖出订单", "賣出訂單").replace("卖出股票", "賣出股票")
        t = et.parse_za_trade(text)
        assert t is not None and t["side"] == "SELL" and t["symbol"] == "QQQ"

    def test_thousand_separator(self):
        """千分位数量/价格：1,000 股 @ USD 7,476.70。"""
        text = (ZA_SELL_FULL
                .replace("委托数量： 1 股", "委托数量： 1,000 股")
                .replace("1 股 @ USD 747.6700", "1,000 股 @ USD 7,476.70"))
        t = et.parse_za_trade(text)
        assert t is not None
        assert t["quantity"] == 1000.0 and t["price"] == 7476.70

    def test_empty_text(self):
        assert et.parse_za_trade("") is None
        assert et.parse_za_trade(None) is None


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
        """BUY 已有持仓 → update 累加数量，成本价加权平均摊薄 (10×900+1×229.8)/11=839.0727。"""
        calls = _patch_db(monkeypatch, holdings=[_holding()])
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "applied"
        assert calls["upsert"] == []
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 11.0, 839.0727, "USD", 1, 3)]
        assert calls["deduct"] == [("USD", 229.80)]

    def test_buy_add_existing_zero_cost(self, monkeypatch):
        """BUY 加仓但原成本为 0 → 以本次成交价充当新成本。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(cost_price=0)])
        r = asyncio.run(et.apply_trade(_trade()))
        assert r["status"] == "applied"
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 11.0, 229.80, "USD", 1, 3)]

    def test_sell_reduce_and_add_cash(self, monkeypatch):
        """SELL → update 减数量（10→9）+ 负数扣现金即入账；亏损减仓摊薄拉高成本 (9000−229.8)/9=974.4667。"""
        calls = _patch_db(monkeypatch, holdings=[_holding()])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", trade_no="P741100", message_id="<m2@x>")))
        assert r["status"] == "applied"
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 9.0, 974.4667, "USD", 1, 3)]
        assert calls["deduct"] == [("USD", -229.80)]

    def test_sell_profit_lowers_cost(self, monkeypatch):
        """盈利减仓摊薄压低成本：10 股@100 卖 1 股@200 → (1000−200)/9=88.8889。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(cost_price=100.0)])
        r = asyncio.run(et.apply_trade(
            _trade(side="SELL", quantity=1.0, price=200.0, message_id="<m6@x>")))
        assert r["status"] == "applied"
        assert calls["update"] == [(5, "NVIDIA CORP", "US", 9.0, 88.8889, "USD", 1, 3)]

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

    def test_amount_field_overrides_qty_times_price(self, monkeypatch):
        """trade 带显式 amount（致富成交金额口径）→ 扣现金用 amount 而非 qty×price。"""
        calls = _patch_db(monkeypatch, holdings=[])
        r = asyncio.run(et.apply_trade(_trade(amount=100.01)))
        assert r["status"] == "applied"
        assert calls["deduct"] == [("USD", 100.01)]
        assert r["amount"] == 100.01

    def test_amount_none_falls_back_to_qty_times_price(self, monkeypatch):
        """无 amount（HSBC 路径）→ 维持 qty×price 原行为。"""
        calls = _patch_db(monkeypatch, holdings=[])
        asyncio.run(et.apply_trade(_trade(amount=None)))
        assert calls["deduct"] == [("USD", 229.80)]


class TestPollOnce:
    def test_poll_once_end_to_end(self, monkeypatch):
        """单轮链路：拉邮件(mock) → 解析 → 幂等应用。"""
        calls = _patch_db(monkeypatch, holdings=[])

        def fake_fetch(days=1):
            assert days == 1  # 无 last_poll（首跑）→ 默认 1 天窗口
            return [{"message_id": "<mail1@hsbc>", "date": None, "text": SAMPLE_BUY}]

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        results = asyncio.run(et.poll_once(days=1))
        assert len(results) == 1 and results[0]["status"] == "applied"
        assert calls["insert"][0]["message_id"] == "<mail1@hsbc>"
        assert calls["deduct"] == [("USD", 229.80)]

    def test_poll_once_no_creds_short_circuit(self, monkeypatch):
        """凭证缺失：WARNING 短路返回 []，不发起拉取不触库（水位不动）。"""
        monkeypatch.setattr(et, "IMAP_USER", "")
        called = []
        monkeypatch.setattr(et, "fetch_hsbc_emails",
                            lambda days=1: called.append(days) or [])
        assert asyncio.run(et.poll_once(days=1)) == []
        assert called == []

    def test_poll_once_skips_unparsed(self, monkeypatch):
        """非全部执行邮件：解析为 None 直接跳过，不触库。"""
        calls = _patch_db(monkeypatch, holdings=[])

        def fake_fetch(days=1):
            return [{"message_id": "<mail2@hsbc>", "date": None,
                     "text": SAMPLE_BUY.replace("全部執行", "部分執行")}]

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["insert"] == []

    def test_poll_once_enlarged_window_on_downtime(self, monkeypatch):
        """停机 3 天恢复：回溯窗口放大为 4 天（宁多勿漏，幂等去重兜底）。"""
        calls = _patch_db(monkeypatch, holdings=[],
                          last_poll=(datetime.now() - timedelta(days=3))
                          .isoformat(timespec="seconds"))  # 距今 3 天（动态构造防时间炸弹）
        seen_days = []

        def fake_fetch(days=1):
            seen_days.append(days)
            return []

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert seen_days == [4]  # gap_days(3) + 1

    def test_poll_once_persists_last_poll_on_success(self, monkeypatch):
        """整轮成功（即使 0 封新邮件）→ last_poll 推进落库。"""
        calls = _patch_db(monkeypatch, holdings=[])

        async def fake_get_config(key):
            return None

        def fake_fetch(days=1):
            return []

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        asyncio.run(et.poll_once(days=1))
        assert len(calls["config"]) == 1
        key, value = calls["config"][0]
        assert key == et.LAST_POLL_KEY
        datetime.fromisoformat(value)  # 落库值须为可解析的 isoformat 时刻

    def test_poll_once_zero_mails_still_advances(self, monkeypatch):
        """0 封新邮件属正常完成 → 水位照常推进，窗口下轮收敛回 1 天。"""
        calls = _patch_db(monkeypatch, holdings=[],
                          last_poll=datetime.now().isoformat(timespec="seconds"))

        def fake_fetch(days=1):
            assert days == 1  # 刚轮询过 → 窗口应已收敛
            return []

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert len(calls["config"]) == 1

    def test_poll_once_fetch_failure_keeps_waterline(self, monkeypatch):
        """拉取失败：不推进水位（下轮以放大窗口重拉），返回空结果。"""
        calls = _patch_db(monkeypatch, holdings=[])

        def fake_fetch(days=1):
            raise RuntimeError("imap down")

        monkeypatch.setattr(et, "fetch_hsbc_emails", fake_fetch)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["config"] == []  # 水位未推进


class TestPollOnceChief:
    """poll_once 双源（HSBC + 致富）拉取/失败语义。"""

    def test_chief_end_to_end(self, monkeypatch):
        """致富终态邮件 → 解析 → 应用：落表 trade_no=98438804、HKD 现金入账 1135.10。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(
            symbol="03152", name="A博时港元", market="HK", quantity=1.0,
            cost_price=1135.10, currency="HKD")])

        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_chief_emails",
                            lambda days=1: [{"message_id": "<m1@chief>",
                                             "date": None, "text": CHIEF_SELL}])
        results = asyncio.run(et.poll_once(days=1))
        assert len(results) == 1 and results[0]["status"] == "applied"
        assert calls["insert"][0]["trade_no"] == "98438804"
        assert calls["insert"][0]["symbol"] == "03152"
        assert calls["update"][0][3] == 0.0          # 持仓 1 → 0
        assert calls["deduct"] == [("HKD", -1135.10)]  # 卖出 → 负扣即入账
        assert len(calls["config"]) == 1             # 双源成功 → 水位推进

    def test_chief_partial_skipped(self, monkeypatch):
        """致富部份成交邮件 → 跳过不触库；水位照常推进。"""
        calls = _patch_db(monkeypatch, holdings=[])
        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_chief_emails",
                            lambda days=1: [{"message_id": "<m2@chief>", "date": None,
                                             "text": CHIEF_SELL.replace(
                                                 "待成交数量 0", "待成交数量 3")}])
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["insert"] == []
        assert len(calls["config"]) == 1

    def test_chief_fetch_failure_keeps_waterline(self, monkeypatch):
        """致富拉取失败（HSBC 正常）→ 水位不推进，返回空结果。"""
        calls = _patch_db(monkeypatch, holdings=[])
        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])

        def boom(days=1):
            raise RuntimeError("imap down")

        monkeypatch.setattr(et, "fetch_chief_emails", boom)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["config"] == []

    def test_chief_fetch_failure_blocks_hsbc_waterline(self, monkeypatch):
        """致富拉取失败时，即便 HSBC 有可应用邮件也不推进水位（防致富漏账放大）。"""
        calls = _patch_db(monkeypatch, holdings=[])
        monkeypatch.setattr(et, "fetch_hsbc_emails",
                            lambda days=1: [{"message_id": "<mail1@hsbc>",
                                             "date": None, "text": SAMPLE_BUY}])

        def boom(days=1):
            raise RuntimeError("imap down")

        monkeypatch.setattr(et, "fetch_chief_emails", boom)
        results = asyncio.run(et.poll_once(days=1))
        assert len(results) == 1 and results[0]["status"] == "applied"  # HSBC 照常应用
        assert calls["config"] == []                                    # 但水位不推进


class TestPollOnceZa:
    """poll_once 三源（HSBC + 致富 + 众安）拉取/失败语义。"""

    def test_za_end_to_end(self, monkeypatch):
        """众安完全成交邮件 → 解析 → 应用：trade_no 空、USD 现金入账 747.67。"""
        calls = _patch_db(monkeypatch, holdings=[_holding(
            symbol="QQQ", name="纳指100 ETF", market="US", quantity=1.0,
            cost_price=719.88, currency="USD")])
        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_chief_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_za_emails",
                            lambda days=1: [{"message_id": "<m1@za>",
                                             "date": None, "text": ZA_SELL_FULL}])
        results = asyncio.run(et.poll_once(days=1))
        assert len(results) == 1 and results[0]["status"] == "applied"
        assert calls["insert"][0]["symbol"] == "QQQ"
        assert calls["insert"][0]["trade_no"] == ""       # 众安邮件无交易编号
        assert calls["update"][0][3] == 0.0               # 持仓 1 → 0
        assert calls["deduct"] == [("USD", -747.67)]      # 卖出 → 现金入账
        assert len(calls["config"]) == 1                  # 三源成功 → 水位推进

    def test_za_submitted_skipped(self, monkeypatch):
        """众安「已提交」非终态邮件 → 跳过不触库；水位照常推进。"""
        calls = _patch_db(monkeypatch, holdings=[])
        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_chief_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_za_emails",
                            lambda days=1: [{"message_id": "<m2@za>", "date": None,
                                             "text": ZA_SELL_SUBMITTED}])
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["insert"] == []
        assert len(calls["config"]) == 1

    def test_za_fetch_failure_keeps_waterline(self, monkeypatch):
        """众安拉取失败（其余源正常）→ 水位不推进（防众安漏账放大）。"""
        calls = _patch_db(monkeypatch, holdings=[])
        monkeypatch.setattr(et, "fetch_hsbc_emails", lambda days=1: [])
        monkeypatch.setattr(et, "fetch_chief_emails", lambda days=1: [])

        def boom(days=1):
            raise RuntimeError("imap down")

        monkeypatch.setattr(et, "fetch_za_emails", boom)
        assert asyncio.run(et.poll_once(days=1)) == []
        assert calls["config"] == []


class TestComputeBacktrackDays:
    """回溯窗口计算：停机越久回溯越久，宁多勿漏；异常输入回退保守窗口。"""

    def test_no_last_poll_defaults(self):
        assert et._compute_backtrack_days(None, 1) == 1
        assert et._compute_backtrack_days("", 1) == 1

    def test_recent_poll_keeps_min_window(self):
        # 30 分钟前刚轮询过 → gap_days=0 → max(1, 1) = 1
        recent = (datetime.now() - timedelta(minutes=30)).isoformat(timespec="seconds")
        assert et._compute_backtrack_days(recent, 1) == 1

    def test_downtime_enlarges_window(self):
        # 停机 3 天 → 回溯 4 天（+1 天余量覆盖边界，宁多勿漏）
        stale = (datetime.now() - timedelta(days=3)).isoformat(timespec="seconds")
        assert et._compute_backtrack_days(stale, 1) == 4

    def test_bad_value_falls_back(self):
        assert et._compute_backtrack_days("not-a-date", 1) == 1
        assert et._compute_backtrack_days("12345", 2) == 2


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
        assert et._is_recent(datetime.now(), 1) is True

    def test_yesterday_kept(self):
        # days=1 → 窗口含昨天整天（since = today-1 的 00:00 起）
        assert et._is_recent(datetime.now() - timedelta(days=1), 1) is True

    def test_older_dropped(self):
        assert et._is_recent(datetime.now() - timedelta(days=3), 1) is False

    def test_no_date_dropped(self):
        assert et._is_recent(None, 1) is False
