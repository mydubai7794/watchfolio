"""记账模块：报表纯计算（无 DB 依赖）+ ECB 参考价拉取。

口径对齐《账户成本盈亏盘点_20261003.md》(v3) 及 2026-10-03 勘误：
- 总投入 = Σ inflow.cny_amount = 84,850（v3 原写 84,270 系电费双重扣减错误）；
- HKD→USD 静态折算 7.846；点差由事件自带 mid_rate+双边金额实时推导；
- 恒等式：net_cny = 总资产×USDCNH − 总投入；分解 = 增值 + 汇损 − 消耗（±1 CNY 舍入容差）；
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
                      "gain_usd": None, "gain_pct": None, "fx_loss_cny": None,
                      "expense_cny": None, "net_cny": None, "net_pct": None,
                      "identity_check_cny": None}
    if usdcnh and total_assets_usd and avg_cost:
        invest_base = usd_in - expense_usd
        gain_usd = total_assets_usd - invest_base
        fx_loss_cny = usd_in * (usdcnh - avg_cost)
        expense_cny = expense_usd * usdcnh
        net_cny = total_assets_usd * usdcnh - total_cny
        identity.update({
            "gain_usd": round(gain_usd, 2),
            "gain_pct": round(gain_usd / invest_base * 100, 2) if invest_base else None,
            "fx_loss_cny": round(fx_loss_cny, 2),
            "expense_cny": round(expense_cny, 2),
            "net_cny": round(net_cny, 2),
            "net_pct": round(net_cny / total_cny * 100, 2) if total_cny else None,
            "identity_check_cny": round(gain_usd * usdcnh + fx_loss_cny - expense_cny, 2),
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
