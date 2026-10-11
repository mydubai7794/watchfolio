"""记账模块：报表纯计算（无 DB 依赖）+ ECB 参考价拉取。

口径：
- 总投入 = Σ inflow.cny_amount；
- HKD→USD 静态折算 7.846；点差由事件自带 mid_rate+双边金额实时推导；
- 恒等式（净投入口径）：净投入 = 总投入 − 消耗折CNY；net_cny = 总资产×USDCNH − 净投入 = 增值 + 汇损（±1 舍入容差）；
- 保活转出（transfer→external）面值单列，不进恒等式不产生损益。
"""
import logging
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

HKD_USD_STATIC = 7.846
ECB_BASE_URL = "https://api.frankfurter.dev/v1"
LEDGER_FX_START = "2026-06-18"          # 首笔购汇日，回填起点


# ---------------------------------------------------------------------------
# 报表纯计算
# ---------------------------------------------------------------------------

def _f(v) -> float | None:
    return None if v is None else float(v)


def compute_fx_detail(events: list[dict]) -> list[dict]:
    """推导每笔换汇事件的 理论应得/点差%/损失USD（单一事实源：mid_rate+双边金额）。"""
    rows = []
    for e in events:
        if e["event_type"] != "fx" or not e.get("mid_rate"):
            continue
        amount, mid = float(e["amount"]), float(e["mid_rate"])
        actual = _f(e.get("recv_amount"))
        theoretical = amount * mid
        if actual is None or theoretical <= 0:
            continue
        diff = theoretical - actual                     # 收币单位（正=少得）
        recv_is_hkd = (e.get("recv_ccy") or "") == "HKD"
        loss_usd = diff / HKD_USD_STATIC if recv_is_hkd else diff
        rows.append({"ref_no": e["ref_no"], "event_date": e["event_date"],
                     "channel": e.get("channel") or "", "pay_ccy": e["amount_ccy"],
                     "pay_amount": amount, "recv_ccy": e.get("recv_ccy") or "",
                     "recv_amount": actual, "theoretical": round(theoretical, 2),
                     "spread_pct": round(diff / theoretical * 100, 4),
                     "loss_usd": round(loss_usd, 4)})
    return rows


def build_report(events: list[dict], *, total_assets_usd: float | None,
                 usdcnh: float | None, cash_accounts: list[dict] | None = None) -> dict:
    """由事件表实时计算全套报表（口径见模块 docstring）。"""
    inflow = [e for e in events if e["event_type"] == "inflow"]
    fee = [e for e in events if e["event_type"] == "fee"]
    reward = [e for e in events if e["event_type"] == "reward"]
    realized = [e for e in events if e["event_type"] == "realized"]
    expense = [e for e in events if e["event_type"] == "expense"]
    transfer_ext = [e for e in events if e["event_type"] == "transfer"
                    and e.get("account") == "external"
                    and e.get("usd_equivalent") is not None]

    total_cny = sum(float(e["cny_amount"] or 0) for e in inflow)
    usd_in = sum(_f(e.get("usd_equivalent")) or 0 for e in inflow)
    avg_cost = total_cny / usd_in if usd_in else None
    fx_detail = compute_fx_detail(events)
    fx_loss_usd = sum(r["loss_usd"] for r in fx_detail)
    fee_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in fee)
    reward_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in reward)
    realized_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in realized)
    expense_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in expense)
    ext_usd = sum(_f(e.get("usd_equivalent")) or 0 for e in transfer_ext)

    # 通道加权平均点差（权重=理论应得，收币统一 USD 化）
    chan: dict[str, dict] = {}
    for r in fx_detail:
        theo_usd = r["theoretical"] / HKD_USD_STATIC if r["recv_ccy"] == "HKD" else r["theoretical"]
        c = chan.setdefault(r["channel"], {"n": 0, "wsum": 0.0, "loss": 0.0})
        c["n"] += 1
        c["wsum"] += theo_usd
        c["loss"] += r["loss_usd"]
    channels = [{"channel": k, "n": v["n"],
                 "wavg_spread_pct": round(v["loss"] / v["wsum"] * 100, 4),
                 "loss_usd": round(v["loss"], 2)}
                for k, v in chan.items()]
    channels.sort(key=lambda c: -c["wavg_spread_pct"])

    identity: dict = {"usdcnh": usdcnh, "total_assets_usd": total_assets_usd,
                      "external_transfer_usd": round(ext_usd, 2),
                      "gain_usd": None, "gain_pct": None, "gain_cny": None,
                      "fx_loss_cny": None, "expense_cny": None, "invest_net_cny": None,
                      "net_cny": None, "net_pct": None, "identity_check_cny": None}
    if usdcnh and total_assets_usd and avg_cost:
        # 净投入口径：生活消耗不属于投资本金，
        # 按当前汇率折 CNY 从总投入中扣除 → 恒等式简化为两项：
        # net = 增值 + 汇损（增值端含消耗回流，使分解与净投入基数匹配）
        invest_base = usd_in - expense_usd
        expense_cny = expense_usd * usdcnh
        invest_net_cny = total_cny - expense_cny
        gain_usd = total_assets_usd + expense_usd - usd_in
        fx_loss_cny = usd_in * (usdcnh - avg_cost)
        net_cny = total_assets_usd * usdcnh - invest_net_cny
        identity.update({
            "gain_usd": round(gain_usd, 2),
            "gain_pct": round(gain_usd / invest_base * 100, 2) if invest_base else None,
            "gain_cny": round(gain_usd * usdcnh, 2),
            "fx_loss_cny": round(fx_loss_cny, 2),
            "expense_cny": round(expense_cny, 2),
            "invest_net_cny": round(invest_net_cny, 2),
            "net_cny": round(net_cny, 2),
            "net_pct": round(net_cny / invest_net_cny * 100, 2) if invest_net_cny else None,
            "identity_check_cny": round(gain_usd * usdcnh + fx_loss_cny, 2),
        })

    # 对账位：每「账户×币种」取最新余额快照，按币种合计 vs cash_accounts 系统值
    balances: dict = {"ledger": {}, "system": {}, "diff": {}}
    latest: dict[tuple, dict] = {}
    for e in events:
        if e["event_type"] == "balance":
            key = (e.get("account"), e["amount_ccy"])
            if key not in latest or e["event_date"] >= latest[key]["event_date"]:
                latest[key] = e
    for (account, ccy), e in latest.items():
        balances["ledger"][ccy] = round(balances["ledger"].get(ccy, 0) + float(e["amount"]), 2)
    if cash_accounts:
        for row in cash_accounts:
            balances["system"][row["currency"]] = row["amount"]
    for ccy in set(balances["ledger"]) | set(balances["system"]):
        lv, sv = balances["ledger"].get(ccy), balances["system"].get(ccy)
        balances["diff"][ccy] = None if (lv is None or sv is None) else round(sv - lv, 2)

    return {
        "summary": {
            "total_cny_invested": round(total_cny, 2),
            "usd_in": round(usd_in, 2),
            "avg_cost": round(avg_cost, 4) if avg_cost else None,
            "fx_loss_usd": round(fx_loss_usd, 2),
            "fee_usd": round(fee_usd, 2),
            "reward_usd": round(reward_usd, 2),
            "realized_usd": round(realized_usd, 2),
            "expense_usd": round(expense_usd, 2),
            "external_transfer_usd": round(ext_usd, 2),
            "pending_count": sum(1 for e in inflow if e.get("cny_amount") is None),
        },
        "identity": identity,
        "channels": channels,
        "fx_detail": fx_detail,
        "balances": balances,
    }


# ---------------------------------------------------------------------------
# 单位净值（TWR）纯计算
# ---------------------------------------------------------------------------

# 现金流事件 → 份额调整方向（金额一律取 usd_equivalent）：
# fx 为组合内部币种结构变化（inflow 已是换汇后到账值）、reward 为无本金
# 收益（推高净值）、realized/balance 为备忘对账类——均不调份额；
# transfer 仅计 account=external（转出组合，与 build_report 口径一致），
# 账户间内部划转即使误填 usd_equivalent 也不调份额（防 NAV 虚增）
NAV_CF_SIGNS = {"inflow": 1.0, "expense": -1.0, "fee": -1.0}


def _nav_cf_sign(e: dict) -> float | None:
    """单事件的现金流方向：非现金流类型/内部划转 → None（不调份额）。"""
    etype = e.get("event_type") or ""
    if etype == "transfer":
        return -1.0 if e.get("account") == "external" else None
    return NAV_CF_SIGNS.get(etype)


def build_nav_series(daily_rows: list[dict], events: list[dict],
                     latest_total_value: float | None = None) -> dict | None:
    """单位净值（TWR 时间加权收益率）纯计算：日线市值 + 现金流事件流重放。

    口径（specs/2026-10-09-nav-twr-design.md）：
    - 基期 = 首笔现金流日；首个市值观测点（≥基期的第一条日线）之前（含当日）
      的全部现金流视同期初投入（按 NAV=1.0 折入份额₀，叠加基期前遗留资产），
      首观测点 NAV = 当日市值 / 份额₀ 并归一为 1.0——不可观测区间的收益
      不可观测，TWR 自首个可观测市值日起算；
    - 此后同日先按昨日 NAV 调份额、再取当日末 NAV（现金流发生前估值，
      入金/转出不虚增净值）；现金流按区间归并 (上一观测日, 当日] 折算，
      落在两日线之间空洞日的事件按前一观测日 NAV 折份额，不会漏折；
      全序列 NAV 以首观测点归一（首点 NAV=1.0，份额折算仍用原始 NAV）；
    - latest_total_value 提供时以实时总资产覆盖末点（实时 TWR）；晚于最后
      日线的尾部现金流先按末点原始 NAV 折份额再覆盖（days 基准亦含尾部日）。
    无现金流事件、或份额退化非正 → None（宁缺毋错）。
    """
    cf_by_date: dict[str, float] = {}
    for e in events:
        sign = _nav_cf_sign(e)
        usd = _f(e.get("usd_equivalent"))
        if sign is None or usd is None:
            continue
        d = str(e["event_date"])
        cf_by_date[d] = cf_by_date.get(d, 0.0) + sign * usd
    if not cf_by_date:
        return None
    base_date = min(cf_by_date)
    # 基期前遗留资产：基期日前最近一条日线市值（daily_rows 按日期升序）
    legacy = 0.0
    for r in daily_rows:
        d = str(r["date"])
        if d < base_date:
            legacy = float(r["total_value"])
        else:
            break
    shares = legacy
    series: list[dict] = []
    nav = 1.0
    base_nav = 0.0
    prev_d = base_date
    for r in daily_rows:
        d = str(r["date"])
        if d < base_date:
            continue
        if base_nav <= 0:
            # 首个市值观测点：其之前（含当日）的全部现金流视同期初投入
            # （不可观测区间无市值数据，收益不可观测，按 NAV=1.0 折入份额₀）
            for d0 in cf_by_date:
                if d0 <= d:
                    shares += cf_by_date[d0]
            if shares <= 0:
                return None
            nav = float(r["total_value"]) / shares
            if nav <= 0:
                return None
            base_nav = nav
            prev_d = d
        else:
            # 区间归并（prev_d, d]：落在两日线之间空洞日的现金流按昨日
            # 原始 NAV 折份额——下一日线市值已含它，漏折会把入金计成收益
            for d_cf in cf_by_date:
                if prev_d < d_cf <= d and nav > 0:
                    shares += cf_by_date[d_cf] / nav
            prev_d = d
            nav = float(r["total_value"]) / shares if shares > 0 else 0.0
        if nav > 0 and base_nav > 0:
            series.append({"date": d, "nav": round(nav / base_nav, 6)})  # 首观测点归一为 1.0
    if not series or shares <= 0:
        return None
    # 尾部现金流：晚于最后日线日期的事件按末点原始 NAV 折入份额
    # （latest_total_value 已含这些现金流，不折算会把入金计成收益）
    for d2 in sorted(cf_by_date):
        if d2 > series[-1]["date"] and nav > 0:
            shares += cf_by_date[d2] / nav
    if shares <= 0:
        return None
    if latest_total_value:
        nav_now = float(latest_total_value) / shares
        if nav_now > 0:
            series[-1] = dict(series[-1], nav=round(nav_now / base_nav, 6))
    twr = series[-1]["nav"] - 1.0
    from datetime import datetime as _dt
    end_date = max(series[-1]["date"], max(cf_by_date))  # 末点被覆盖时 days 仍含尾部现金流日
    days = (_dt.strptime(end_date, "%Y-%m-%d")
            - _dt.strptime(series[0]["date"], "%Y-%m-%d")).days + 1
    ann = ((1.0 + twr) ** (365.0 / days) - 1.0) * 100.0 if days >= 30 else None
    return {"series": series, "twr_pct": round(twr * 100.0, 2),
            "ann_pct": None if ann is None else round(ann, 2), "days": days}


# ---------------------------------------------------------------------------
# ECB 参考价拉取（frankfurter.dev，免费无 key；每日一次，严禁短周期重试放大）
# ---------------------------------------------------------------------------

def parse_ecb_payload(data: dict) -> list[tuple]:
    """解析 frankfurter rates 载荷 → [(date, usd_cny, usd_hkd), ...]（按日期升序）。"""
    rates = (data or {}).get("rates") or {}
    rows = []
    for d in sorted(rates):
        item = rates[d] or {}
        cny, hkd = item.get("CNY"), item.get("HKD")
        if cny and hkd:
            rows.append((d, float(cny), float(hkd)))
    return rows


async def fetch_ecb_rates(start: str, end: str,
                          proxy_urls: list[str] | None = None) -> list[tuple]:
    """区间拉取 ECB USD→CNY/HKD 参考价（一次请求；直连→代理容错）。"""
    url = f"{ECB_BASE_URL}/{start}..{end}?base=USD&symbols=CNY,HKD"
    candidates = [None] + [p if "://" in p else f"http://{p}" for p in (proxy_urls or [])[:2]]
    last_err = ""
    for proxy in candidates:
        try:
            async with httpx.AsyncClient(timeout=15, proxy=proxy) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return parse_ecb_payload(resp.json())
                last_err = f"HTTP {resp.status_code}"
        except Exception as e:
            last_err = str(e)
    logger.warning("ECB 参考价拉取失败(%s..%s): %s", start, end, last_err)
    return []


# ---------------------------------------------------------------------------
# 已清仓周期推导（trade_records 回放，纯计算无 IO）
# ---------------------------------------------------------------------------

def _dt_or_none(v):
    """宽容取 datetime：datetime 原样 / date 补零点 / ISO 字符串截前 10 解析。"""
    if v is None or isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    try:
        return datetime.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _replay_closed_cycles(sym: str, meta: dict, cur_qty: float,
                          recs: list[dict]) -> list[dict]:
    """单 symbol 数量轨迹回放（build_closed_positions 内部辅助）。

    锚点反推期初：q0 = cur_qty − Σ(BUY) + Σ(SELL)；q0>0 = email 记录前的
    手工录入期初（期初成本 q0×cost_price 近似，首周期 approx）；q0<0（手工
    改量/删行失真）钳 0 且首周期打 approx。数量每次归零 → 周期闭合：
    realized = Σ卖额 − (Σ买额 + 期初成本)。@author yys
    """
    net = sum(float(t["quantity"]) * (1 if t["side"] == "BUY" else -1)
              for t in recs)
    q0 = cur_qty - net
    anchor_approx = q0 > 1e-9
    if q0 < -1e-9:
        q0 = 0.0
        anchor_approx = True
    qty = q0
    first_cycle = q0 <= 0            # q0>0 时首个周期已由期初开启
    basis = q0 * float(meta.get("cost_price") or 0) if q0 > 0 else 0.0
    start = _dt_or_none(recs[0].get("email_date") or recs[0].get("created_at")) \
        if q0 > 0 else None
    sell_amt = sell_qty = 0.0
    cyc_approx = anchor_approx
    out: list[dict] = []
    for t in recs:
        q, p = float(t["quantity"]), float(t["price"])
        dt = _dt_or_none(t.get("email_date") or t.get("created_at"))
        if t["side"] == "BUY":
            if start is None:        # 新周期首笔（上一周期已闭合）
                cyc_approx = anchor_approx if first_cycle else False
                first_cycle = False
                start, basis, sell_amt, sell_qty = dt, 0.0, 0.0, 0.0
            qty += q
            basis += q * p
        else:                        # SELL
            qty -= q
            sell_amt += q * p
            sell_qty += q
            if qty < -1e-9:          # 轨迹失真防御：钳制归零
                qty = 0.0
                cyc_approx = True
            if abs(qty) <= 1e-9:
                qty = 0.0
                realized = sell_amt - basis
                exit_avg = sell_amt / sell_qty if sell_qty > 0 else None
                out.append({
                    "symbol": sym,
                    "name": meta.get("name") or "",
                    "market": meta.get("market") or "",
                    "exit_date": dt.date().isoformat() if dt else None,
                    "hold_days": (dt.date() - start.date()).days
                                 if dt and start else None,
                    "exit_qty": round(sell_qty, 4),
                    "exit_avg": round(exit_avg, 4) if exit_avg else None,
                    "realized_pnl": round(realized, 2),
                    "realized_pct": round(realized / basis * 100, 2)
                                    if basis > 0 else None,
                    "approx": bool(cyc_approx),
                })
                # 周期状态复位（first_cycle 消耗语义：期初已开启则此处不再置位）
                start, basis, sell_amt, sell_qty = None, 0.0, 0.0, 0.0
                cyc_approx = False
    return out


def build_closed_positions(trades: list[dict], holdings: list[dict]) -> list[dict]:
    """由 trade_records 回放推导已清仓周期列表（round trip）+ 无成交占位行。

    口径（spec 2026-10-11 §3）：锚点反推期初数量、数量归零切周期、
    realized = Σ卖额 − Σ买额 − 期初成本（期初成本 = q0 × 当前 cost_price 近似）。
    trades 须按时间升序（get_all_trades 的 COALESCE 排序已保证）。
    返回：周期行按 exit_date 倒序在前；qty==0 且无周期的 holdings 行输出
    占位行（no_records=True、盈亏字段 None）沉底。@author yys
    """
    by_symbol: dict[str, list[dict]] = {}
    for t in trades:
        by_symbol.setdefault(t["symbol"], []).append(t)

    rows: list[dict] = []
    placeholders: list[dict] = []
    for h in holdings:
        sym = h["symbol"]
        recs = by_symbol.pop(sym, [])
        cur_qty = float(h["quantity"] or 0)
        cycles = _replay_closed_cycles(sym, h, cur_qty, recs)
        if cycles:
            rows.extend(cycles)
        elif cur_qty == 0:
            placeholders.append({
                "symbol": sym, "name": h.get("name") or "",
                "market": h.get("market") or "",
                "exit_date": None, "hold_days": None, "exit_qty": None,
                "exit_avg": None, "realized_pnl": None, "realized_pct": None,
                "approx": False, "no_records": True})

    # holdings 行已删除（历史手动清理）但有成交的 symbol：按 q0=0 回放，
    # 名称/市场取末条记录
    for sym, recs in by_symbol.items():
        if not recs:
            continue
        last = recs[-1]
        meta = {"name": last.get("name") or "", "market": last.get("market") or "",
                "cost_price": 0}
        rows.extend(_replay_closed_cycles(sym, meta, 0.0, recs))

    rows.sort(key=lambda r: r["exit_date"] or "", reverse=True)
    return rows + placeholders
