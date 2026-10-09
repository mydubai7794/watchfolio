"""记账模块：报表纯计算（无 DB 依赖）+ ECB 参考价拉取。

口径对齐《账户成本盈亏盘点_20261003.md》(v3) 及 2026-10-03 勘误：
- 总投入 = Σ inflow.cny_amount = 84,850（v3 原写 84,270 系电费双重扣减错误）；
- HKD→USD 静态折算 7.846；点差由事件自带 mid_rate+双边金额实时推导；
- 恒等式（净投入口径）：净投入 = 总投入 − 消耗折CNY；net_cny = 总资产×USDCNH − 净投入 = 增值 + 汇损（±1 舍入容差）；
- 保活转出（transfer→external）面值单列，不进恒等式不产生损益。
"""
import logging
from datetime import date

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
        # 净投入口径（2026-10-03 用户定版）：生活消耗（电费）不属于投资本金，
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
# 收益（推高净值）、realized/balance 为备忘对账类——均不调份额
NAV_CF_SIGNS = {"inflow": 1.0, "transfer": -1.0, "expense": -1.0, "fee": -1.0}


def build_nav_series(daily_rows: list[dict], events: list[dict],
                     latest_total_value: float | None = None) -> dict | None:
    """单位净值（TWR 时间加权收益率）纯计算：日线市值 + 现金流事件流重放。

    口径（specs/2026-10-09-nav-twr-design.md）：
    - 基期 = 首笔现金流日；首个市值观测点（≥基期的第一条日线）之前（含当日）
      的全部现金流视同期初投入（按 NAV=1.0 折入份额₀，叠加基期前遗留资产），
      首观测点 NAV = 当日市值 / 份额₀ 并归一为 1.0——不可观测区间的收益
      不可观测，TWR 自首个可观测市值日起算；
    - 此后同日先按昨日 NAV 调份额、再取当日末 NAV（现金流发生前估值，
      入金/转出不虚增净值）；全序列 NAV 以首观测点归一（首点 NAV=1.0，
      份额折算仍用原始 NAV）；
    - latest_total_value 提供时以实时总资产覆盖末点（实时 TWR）；晚于最后
      日线的尾部现金流先按末点原始 NAV 折份额再覆盖（days 基准亦含尾部日）。
    无现金流事件、或份额退化非正 → None（宁缺毋错）。
    """
    cf_by_date: dict[str, float] = {}
    for e in events:
        sign = NAV_CF_SIGNS.get(e.get("event_type") or "")
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
        else:
            if d in cf_by_date and nav > 0:
                shares += cf_by_date[d] / nav        # 现金流按昨日 NAV（原始值）折份额
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
