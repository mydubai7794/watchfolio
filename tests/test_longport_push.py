"""长桥推送 shadow/live 门控与回调路由测试。

SDK 全打桩（fake ctx/callbacks）：零连库零外网，真连接留给线上 shadow 观测
（计划 Task 13）。覆盖：shadow 不喂/live 喂入、.US 剥离、模式热切换、
断连重连退避、Depth 无权限降级、ctx 重建重挂、回调异常不炸、Decimal 归一。
@author yys
"""
import asyncio
import time
from decimal import Decimal

import pytest

import database as db
import longport_fetcher as lp
import longport_push as lpp


class FakeSub:
    """SubType 打桩（SDK 未装环境同样可测订阅路由）。"""

    Quote = "quote"
    Depth = "depth"


class FakeCtx:
    """QuoteContext 打桩：记录回调注册/订阅/退订调用。

    fail_depth 模拟无 Depth 权限（订阅降级路径）；
    fail_unsubscribe 模拟退订失败（差集重试路径）。
    """

    def __init__(self, fail_depth: bool = False, fail_unsubscribe: bool = False):
        self.quotes_cb = None
        self.depth_cb = None
        self.sub_calls: list[tuple[list, list]] = []
        self.unsub_calls: list[tuple[list, list]] = []
        self.fail_depth = fail_depth
        self.fail_unsubscribe = fail_unsubscribe

    def set_on_quote(self, cb):
        self.quotes_cb = cb

    def set_on_depth(self, cb):
        self.depth_cb = cb

    def subscribe(self, symbols, sub_types):
        if self.fail_depth and any(t == FakeSub.Depth for t in sub_types):
            raise RuntimeError("301604 no quote access")
        self.sub_calls.append((list(symbols), list(sub_types)))

    def unsubscribe(self, symbols, sub_types):
        self.unsub_calls.append((list(symbols), list(sub_types)))
        if self.fail_unsubscribe:
            raise RuntimeError("unsubscribe fail")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """模块级状态跨用例隔离；缓存时间戳冻结 → 路径零连库。"""
    lpp._pending_eval.clear()
    lpp._stats.update(connected_at=0.0, quotes=0, depths=0, reconnects=0)
    lpp._mode_cache.update(v="shadow", at=time.monotonic() + 999.0)
    monkeypatch.setattr(lpp, "_armed_ctx", None)
    monkeypatch.setattr(lpp, "_armed_symbols", [])
    monkeypatch.setattr(lpp, "_drift_check_at", time.monotonic() + 999.0)
    monkeypatch.setattr(lpp, "_SubType", FakeSub)
    monkeypatch.setattr(lpp, "on_events", None)   # 钩子跨用例隔离（测试内可覆盖）
    yield


def test_shadow_mode_routes_to_logger_only(monkeypatch):
    """shadow 只计数不喂 quotes_hub、不排队提醒评估（计划 Step1 用例 1）。"""
    calls = []
    monkeypatch.setattr(lpp.quotes_hub, "push_tick",
                        lambda s, p, **kw: calls.append(("tick", s, p)))
    n0 = lpp._stats["quotes"]
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    assert calls == []                      # shadow 不写内存/不触发评估
    assert lpp._stats["quotes"] == n0 + 1   # 仅计数（统计日志采样观测）
    assert lpp._pending_eval == {}


def test_live_mode_updates_hub(monkeypatch):
    """live 喂入 quotes_hub 且剥离 .US 后缀（计划 Step1 用例 2）。"""
    calls = []
    monkeypatch.setattr(lpp, "_mode", lambda: "live")

    def fake_push(s, p, source="", bid=None, ask=None, now=None):
        calls.append((s, p, source, bid, ask))

    monkeypatch.setattr(lpp.quotes_hub, "push_tick", fake_push)
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    assert calls == [("QQQ", 100.0, "lp_push", 99.9, 100.1)]
    assert lpp._pending_eval["QQQ"] == 100.0


def test_mode_hot_switch(monkeypatch):
    """sys_config.LP_PUSH_MODE 变更在下一个检查点热生效（不必断连重连）。"""
    values = {"v": "live"}

    async def fake_cfg(key):
        return values["v"]

    monkeypatch.setattr(db, "get_config", fake_cfg)
    lpp._mode_cache.update(v="shadow", at=0.0)   # 缓存过期 → 强制重读
    asyncio.run(lpp._refresh_mode())
    assert lpp._mode() == "live"
    values["v"] = "shadow"
    lpp._mode_cache.update(at=0.0)
    asyncio.run(lpp._refresh_mode())
    assert lpp._mode() == "shadow"


def test_live_mode_evaluates_alerts(monkeypatch):
    """live 模式 eval_drain 批量交提醒引擎、消费即清（计划 Step1 用例 3 稳定版）。"""
    events = []

    async def fake_round(prices):
        events.append(dict(prices))
        return [{"symbol": s} for s in prices]

    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    monkeypatch.setattr(lpp.alert_engine, "evaluate_round", fake_round)
    lpp.on_price("QQQ.US", 100.0, bid=99.9, ask=100.1)
    assert lpp._pending_eval["QQQ"] == 100.0     # 已排队待评估
    out = asyncio.run(lpp.eval_drain())
    assert out == [{"symbol": "QQQ"}]            # 引擎事件原样透传
    assert events == [{"QQQ": 100.0}]
    assert lpp._pending_eval == {}               # 消费即清


def test_shadow_drain_clears_stale(monkeypatch):
    """shadow 下 drain 清空残留待评价（先 live 后切 shadow 不泄漏陈旧价）。"""
    called = []

    async def fake_round(prices):
        called.append(prices)
        return []

    monkeypatch.setattr(lpp, "_mode", lambda: "shadow")
    monkeypatch.setattr(lpp.alert_engine, "evaluate_round", fake_round)
    lpp._pending_eval["QQQ"] = 100.0
    out = asyncio.run(lpp.eval_drain())
    assert out == [] and called == []
    assert lpp._pending_eval == {}


def test_bad_price_ignored(monkeypatch):
    """零价/空价不喂 hub（防零价污染内存真相源）。"""
    calls = []
    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    monkeypatch.setattr(lpp.quotes_hub, "push_tick",
                        lambda s, p, **kw: calls.append((s, p)))
    lpp.on_price("QQQ.US", 0.0)
    lpp.on_price("QQQ.US", None)
    assert calls == [] and lpp._pending_eval == {}


def test_callback_decimal_and_exception_safe(monkeypatch):
    """SDK 回调：Decimal 归一 float、bid/ask 正确接线；push_tick 抛异常不上抛。"""
    seen = []
    monkeypatch.setattr(lpp, "_mode", lambda: "live")

    def boom(s, p, source="", bid=None, ask=None, now=None):
        seen.append((s, p, bid, ask))
        raise RuntimeError("hub down")

    monkeypatch.setattr(lpp.quotes_hub, "push_tick", boom)

    class Ev:   # PushQuote 打桩：Decimal 字段是 SDK 常态
        symbol = "MU.US"
        last_done = Decimal("101.5")
        bid = Decimal("101.4")
        ask = Decimal("101.6")

    lpp._handle_quote(None, Ev())        # 异常被回调壳捕获，不炸 SDK 线程
    assert seen == [("MU", 101.5, 101.4, 101.6)]


def test_callback_depth_counts_only():
    """depth 回调只计数（LV1 深度不喂价，价格真相源仅 quote）。"""
    class Ev:
        symbol = "QQQ.US"

    n0 = lpp._stats["depths"]
    lpp._handle_depth(None, Ev())
    assert lpp._stats["depths"] == n0 + 1


def test_arm_strips_us_suffix_and_registers_callbacks():
    """订阅符号带 .US 后缀（长桥铁律）；quote/depth 回调均注册。"""
    ctx = FakeCtx()
    asyncio.run(lpp._arm(ctx, ["QQQ", "MU"]))
    assert ctx.quotes_cb is not None and ctx.depth_cb is not None
    syms, types = ctx.sub_calls[0]
    assert syms == ["QQQ.US", "MU.US"]
    assert types == [FakeSub.Quote, FakeSub.Depth]


def test_arm_depth_permission_degrade():
    """Depth 无权限（301604 同类）降级仅 Quote 订阅，不炸主循环。"""
    ctx = FakeCtx(fail_depth=True)
    asyncio.run(lpp._arm(ctx, ["QQQ"]))
    assert len(ctx.sub_calls) == 1
    syms, types = ctx.sub_calls[0]
    assert syms == ["QQQ.US"]
    assert types == [FakeSub.Quote]


def test_arm_same_ctx_idempotent():
    """同 ctx 同标的集重复 _arm 幂等：不重复订阅、不退订。"""
    ctx = FakeCtx()
    asyncio.run(lpp._arm(ctx, ["QQQ", "MU"]))
    asyncio.run(lpp._arm(ctx, ["MU", "QQQ"]))    # 顺序无关（内部 sorted 比较）
    assert len(ctx.sub_calls) == 1
    assert ctx.unsub_calls == []


def test_arm_drift_expand():
    """同 ctx 标的集扩张 {QQQ} → {QQQ,MU}：全量订阅新集合、无退订。"""
    ctx = FakeCtx()
    asyncio.run(lpp._arm(ctx, ["QQQ"]))
    asyncio.run(lpp._arm(ctx, ["QQQ", "MU"]))
    assert len(ctx.sub_calls) == 2
    assert ctx.sub_calls[-1] == (["QQQ.US", "MU.US"],
                                 [FakeSub.Quote, FakeSub.Depth])
    assert ctx.unsub_calls == []                 # 扩张无差集
    assert sorted(lpp._armed_symbols) == ["MU", "QQQ"]
    assert lpp._armed_ctx is ctx


def test_arm_drift_shrink():
    """同 ctx 标的集收缩 {QQQ,MU} → {QQQ}：差集退订旧标的 + 全量订阅。"""
    ctx = FakeCtx()
    asyncio.run(lpp._arm(ctx, ["QQQ", "MU"]))
    asyncio.run(lpp._arm(ctx, ["QQQ"]))
    assert ctx.unsub_calls == [(["MU.US"], [FakeSub.Quote, FakeSub.Depth])]
    assert ctx.sub_calls[-1][0] == ["QQQ.US"]
    assert sorted(lpp._armed_symbols) == ["QQQ"]


def test_arm_unsubscribe_fail_retries():
    """退订失败不阻断：gone 保留在已订阅集，下轮差集非空自动重试。"""
    ctx = FakeCtx(fail_unsubscribe=True)
    asyncio.run(lpp._arm(ctx, ["QQQ", "MU"]))
    asyncio.run(lpp._arm(ctx, ["QQQ"]))          # 退订失败 → 残留
    assert len(ctx.unsub_calls) == 1             # 首次尝试
    assert sorted(lpp._armed_symbols) == ["MU", "QQQ"]   # gone 未从集内剔除
    asyncio.run(lpp._arm(ctx, ["QQQ"]))          # 下轮同集合：差集仍在 → 重试
    assert len(ctx.unsub_calls) == 2             # 二次重试发生


def test_push_loop_reconnect_backoff(monkeypatch):
    """断连指数退避 1s→2s→…上限 60s；成功后计数并归一退避。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    sleeps = []

    async def fake_sleep(sec):
        sleeps.append(sec)

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    attempts = {"n": 0}

    async def fake_ctx():
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise RuntimeError("conn fail")
        c = FakeCtx()
        monkeypatch.setattr(lp, "_ctx", c)
        return c

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=3))
    assert sleeps == [1.0, 2.0]              # 两次失败退避递增
    assert attempts["n"] == 3
    assert lpp._stats["reconnects"] == 1
    assert lpp._armed_ctx is not None


def test_push_loop_rearm_on_ctx_rebuild(monkeypatch):
    """拉取侧超时废弃/token 重建换 ctx 实例 → 守护循环检测身份变化自动重挂。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    ctx_a, ctx_b = FakeCtx(), FakeCtx()
    calls = {"n": 0}

    async def fake_ctx():
        calls["n"] += 1
        c = ctx_a if calls["n"] == 1 else ctx_b
        monkeypatch.setattr(lp, "_ctx", c)
        return c

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)
    inner = {"n": 0}

    async def fake_sleep(_sec):
        inner["n"] += 1
        if inner["n"] == 1:
            monkeypatch.setattr(lp, "_ctx", ctx_b)   # 内层期间连接被外部重建

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=4))
    assert len(ctx_a.sub_calls) == 1
    assert len(ctx_b.sub_calls) == 1     # ctx_b 重新挂回调+订阅
    assert lpp._armed_ctx is ctx_b
    assert lpp._stats["reconnects"] == 2


def test_push_loop_no_sdk_exits(monkeypatch):
    """SDK 不可用（本地未装/导入失败）push 循环直接退出不炸。"""
    monkeypatch.setattr(lp, "_SDK_OK", False)
    assert asyncio.run(lpp.push_loop(us_symbols=["QQQ"])) is None


def test_push_loop_empty_symbols_waits(monkeypatch):
    """标的为空（无美股持仓）等待重试而非空订阅。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    sleeps = []

    async def fake_sleep(sec):
        sleeps.append(sec)

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    asyncio.run(lpp.push_loop(us_symbols=[], _iterations=1))
    assert sleeps == [300.0]


def test_push_loop_token_invalid_waits(monkeypatch):
    """token 失效（401004）不反复订阅撞墙：30s 间隔等待换 token/自动续期自愈。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    monkeypatch.setattr(lp, "token_invalid", True)
    sleeps = []

    async def fake_sleep(sec):
        sleeps.append(sec)

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=1))
    assert sleeps == [30.0]


def test_status_shape():
    """status() 观测出口：shadow 观测期/排障依据。"""
    st = lpp.status()
    assert st["mode"] == "shadow"
    assert {"mode", "armed", "symbols", "sdk", "quotes", "depths",
            "reconnects", "connected_at"} <= set(st)


# ---------------------------------------------------------------------------
# 结转修复项 1：drift 比对基准改 _armed_symbols（退订失败残留可触发重试）
# ---------------------------------------------------------------------------

def test_symbols_drifted_uses_armed_as_truth(monkeypatch):
    """drift 基准是 _armed_symbols（含退订失败残留）而非局部盘点值。

    残留场景：armed={QQQ,MU}（MU 为退订失败残留）、持仓真实集={QQQ}
    （与外层局部盘点值一致）——旧实现以局部值为基准恒等不判 drift，
    重试死路；新实现以 armed 为真相，差集非空必须判 drift。
    """
    monkeypatch.setattr(lpp, "_armed_symbols", ["MU", "QQQ"])

    async def fake_load():
        return ["QQQ"]

    monkeypatch.setattr(lpp, "_load_us_symbols", fake_load)
    monkeypatch.setattr(lpp, "_drift_check_at", 0.0)   # 节流窗口过期
    assert asyncio.run(lpp._symbols_drifted()) is True


def test_symbols_drifted_false_when_latest_matches_armed(monkeypatch):
    """latest == armed（无残留无漂移）→ False，不空转重订阅。"""
    monkeypatch.setattr(lpp, "_armed_symbols", ["QQQ"])

    async def fake_load():
        return ["QQQ"]

    monkeypatch.setattr(lpp, "_load_us_symbols", fake_load)
    monkeypatch.setattr(lpp, "_drift_check_at", 0.0)
    assert asyncio.run(lpp._symbols_drifted()) is False


def test_symbols_drifted_throttled(monkeypatch):
    """300s 节流：窗口内不复核不连库（_load 不被调用）。"""
    called = []

    async def fake_load():
        called.append(1)
        return []

    monkeypatch.setattr(lpp, "_load_us_symbols", fake_load)
    monkeypatch.setattr(lpp, "_drift_check_at", time.monotonic() + 999.0)
    assert asyncio.run(lpp._symbols_drifted()) is False
    assert called == []


def test_push_loop_drift_retry_via_armed_residue(monkeypatch):
    """结转项闭环（push_loop 级集成）：退订失败残留 → drift（armed 基准）
    → break 外层重盘 → _arm 差集重试退订成功。

    时序：首轮订阅 {QQQ,MU} → 持仓收缩为 {QQQ} → 首次退订 MU 失败
    （残留 armed={QQQ,MU}）→ 下一复核周期以 armed 为真相判 drift →
    重盘退订 MU 成功。旧实现（局部盘点值基准）在残留场景恒等不判
    drift，第二次退订永不发生。
    """
    monkeypatch.setattr(lp, "_SDK_OK", True)
    ctx = FakeCtx(fail_unsubscribe=True)
    sleeps = []

    async def fake_sleep(sec):
        sleeps.append(sec)
        # 首次节拍：仅解冻漂移复核（退订仍失败 → 残留）
        # 第二次节拍：放开退订失败 + 再解冻 → drift（残留差集）→ 重盘重试
        monkeypatch.setattr(lpp, "_drift_check_at", 0.0)
        if len(sleeps) >= 2:
            ctx.fail_unsubscribe = False

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)

    async def fake_ctx():
        monkeypatch.setattr(lp, "_ctx", ctx)
        return ctx

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)

    async def fake_load():
        return ["QQQ"]     # 持仓真实集已收缩

    monkeypatch.setattr(lpp, "_load_us_symbols", fake_load)

    asyncio.run(lpp.push_loop(us_symbols=["QQQ", "MU"], _iterations=5))
    assert len(ctx.unsub_calls) == 2              # 首次失败 + 重盘后重试成功
    assert ctx.unsub_calls[1][0] == ["MU.US"]
    assert sorted(lpp._armed_symbols) == ["QQQ"]  # 残留清除
    assert sleeps == [1.0, 1.0]                   # 两次内层节拍，无异常退避


# ---------------------------------------------------------------------------
# 结转修复项 2：live push 事件接线（on_events 钩子）
# ---------------------------------------------------------------------------

def test_push_loop_dispatches_events_via_hook(monkeypatch):
    """live 窗口 eval_drain 返回的提醒事件经 on_events 钩子分发。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    ctx = FakeCtx()

    async def fake_ctx():
        monkeypatch.setattr(lp, "_ctx", ctx)
        return ctx

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)

    async def fake_sleep(_sec):
        pass

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    monkeypatch.setattr(lpp.quotes_hub, "push_tick", lambda s, p, **kw: None)

    async def fake_round(prices):
        return [{"id": 7, "symbol": list(prices)[0], "rule": "atr_stop",
                 "level": "critical", "title": "ATR 止损触发", "body": {"x": 1}}]

    monkeypatch.setattr(lpp.alert_engine, "evaluate_round", fake_round)

    got = []

    async def hook(events):
        got.append(events)

    monkeypatch.setattr(lpp, "on_events", hook)
    lpp.on_price("QQQ.US", 100.0)                 # 排队待评估
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=2))
    assert len(got) == 1
    assert got[0][0]["symbol"] == "QQQ"
    assert got[0][0]["id"] == 7 and "body" in got[0][0]   # 完整事件透传


def test_push_loop_hook_exception_swallowed(monkeypatch):
    """钩子异常全捕获：通知通道故障不炸推送循环（事件已落库，丢通知不丢状态）。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    ctx = FakeCtx()

    async def fake_ctx():
        monkeypatch.setattr(lp, "_ctx", ctx)
        return ctx

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)

    async def fake_sleep(_sec):
        pass

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    monkeypatch.setattr(lpp.quotes_hub, "push_tick", lambda s, p, **kw: None)

    async def fake_round(prices):
        return [{"id": 8, "symbol": list(prices)[0], "rule": "atr_stop",
                 "level": "critical", "title": "t"}]

    monkeypatch.setattr(lpp.alert_engine, "evaluate_round", fake_round)

    async def bad_hook(events):
        raise RuntimeError("通知通道故障")

    monkeypatch.setattr(lpp, "on_events", bad_hook)
    lpp.on_price("QQQ.US", 100.0)
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=2))
    assert lpp._armed_ctx is ctx                  # 循环正常武装退出 = 异常未炸


def test_push_loop_no_hook_silent_skip(monkeypatch):
    """未注册钩子（on_events=None）：事件仅落库不通知，循环照常。"""
    monkeypatch.setattr(lp, "_SDK_OK", True)
    ctx = FakeCtx()

    async def fake_ctx():
        monkeypatch.setattr(lp, "_ctx", ctx)
        return ctx

    monkeypatch.setattr(lp, "_get_ctx", fake_ctx)

    async def fake_sleep(_sec):
        pass

    monkeypatch.setattr(lpp, "_sleep", fake_sleep)
    monkeypatch.setattr(lpp, "_mode", lambda: "live")
    monkeypatch.setattr(lpp.quotes_hub, "push_tick", lambda s, p, **kw: None)

    async def fake_round(prices):
        return [{"id": 9, "symbol": list(prices)[0], "rule": "atr_stop",
                 "level": "critical", "title": "t"}]

    monkeypatch.setattr(lpp.alert_engine, "evaluate_round", fake_round)
    assert lpp.on_events is None                  # fixture 隔离保证
    lpp.on_price("QQQ.US", 100.0)
    asyncio.run(lpp.push_loop(us_symbols=["QQQ"], _iterations=2))
    assert lpp._armed_ctx is ctx                  # 无钩子不炸不阻断
