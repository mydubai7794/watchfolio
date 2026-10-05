# 数据源降频与新浪主源撤除 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** twelvedata USDCNH 降频 30min（根除 429 配额耗尽）；USD/HKD 主源切腾讯总线（零新增请求）；DXY 主价并入 Yahoo backfill 组（与 US30Y/US10Y 同轮 4h）；新浪看板批降级纯兜底（15min）。

**Architecture:** 全部为现有链路的参数/主备切换：tencent_bus 攒批合并（USD/HKD 与股票/候选池同请求节奏）；yahoo_refresh_plan 双轨节流复用（DXY 常态 4h 一次、冷启动即拉一次、失败 5min 重试）；新浪保留兜底角色（BZ/GC 兜底、USDCNH 冷备、DXY 回落）。

**Tech Stack:** Python 3 / FastAPI / pytest。

**用户确认的方案（2026-10-05）：** A. USDCNH 30min；B. USDHKD 切腾讯同总线；C. DXY 切 Yahoo 与 US30Y/US10Y 同组 4h；D. 新浪批纯兜底。

**实测依据（服务器验证）：**
- 腾讯 `whUSDHKD` 实时可用：`v_whUSDHKD="310~美元港元~USDHKD~7.8454~0~20261005124608~..."`（f[3]=现价，波浪线布局仅 ~21 字段，**不能**走 parse_tencent_quotes 的 us/hk 分支——守卫 len<38 会拒绝）
- 腾讯 whDINIW 停更（数据停 2026-08-10）、长桥 .DXY.US/.DX.US/.DINIW.US 空返回、twelvedata 无 DXY 符号 → DXY 唯一免费源 Yahoo DX-Y.NYB（YAHOO_DAY_SYMBOLS 已有映射）
- twelvedata USD/CNH 正常；降频后 48 credits/天（6% 配额）

**基线命令：**
```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py -q
```

**规范：** 全程中文；新增/重写函数 docstring 尾部 `@author yys`；禁止 TODO/伪代码；数据源只用系统内名称（腾讯/新浪/Yahoo/twelvedata/长桥/24h数据源）。

---

### Task 1: portfolio_fetcher.py 数据源切换（含测试改写，TDD）

**Files:**
- Modify: `portfolio_fetcher.py`
- Test: `tests/test_portfolio.py`、`tests/test_longport.py`

- [ ] **Step 1: 写失败测试**

1a. `tests/test_portfolio.py` 中 `parse_fx_usdhkd` 的 3 个断言用例（约 118-127 行，类名以实际为准，参考 `pf.parse_fx_usdhkd(raw) == 7.8466` 等断言定位）整体替换为：

```python
class TestParseTencentWhFx:
    """腾讯外汇行 v_whUSDHKD 解析：f[3] 现价 + 7.5~8.5 区间校验。"""

    WH_OK_RAW = 'v_whDINIW="310~美元指数~DINIW~99.6400~0~x";v_whUSDHKD="310~美元港元~USDHKD~7.8454~0~20261005124608~7.8470"'

    def test_ok(self):
        assert pf.parse_tencent_wh_fx(self.WH_OK_RAW) == 7.8454

    def test_out_of_band(self):
        raw = 'v_whUSDHKD="310~美元港元~USDHKD~9.9000~0~x"'
        assert pf.parse_tencent_wh_fx(raw) == 0.0   # 越界视为失败

    def test_missing_or_empty(self):
        assert pf.parse_tencent_wh_fx("") == 0.0
        assert pf.parse_tencent_wh_fx('v_whXXXX="1~2~3"') == 0.0
        assert pf.parse_tencent_wh_fx('v_whUSDHKD="310~a~b"') == 0.0   # 字段不足
```

1b. `TestFetchFxUsesProxies` 类（约 400-447 行）整体替换为：

```python
class TestFetchFxTencentPrimary:
    """fetch_fx_usdhkd：主源腾讯 whUSDHKD（总线攒批，零额外请求）→ TD 冷备 → 静态兜底。"""

    def test_tencent_primary_no_td(self, monkeypatch):
        """腾讯主源成功：零 TD 请求。"""
        called = {"td": 0, "bus_codes": None}

        async def td_spy(pair):
            called["td"] += 1
            return 7.8470

        async def fake_bus_get(codes):
            called["bus_codes"] = codes
            return 'v_whUSDHKD="310~美元港元~USDHKD~7.8466~0~x~7.8470"'

        monkeypatch.setattr(pf, "fetch_td_fx", td_spy)
        monkeypatch.setattr(pf.tencent_bus, "get", fake_bus_get)
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd())
        assert rate == 7.8466 and fallback is False
        assert called["td"] == 0
        assert called["bus_codes"] == ["whUSDHKD"]   # 与股票/候选池同总线的攒批代码

    def test_tencent_fail_td_cold_backup(self, monkeypatch):
        """腾讯失败（总线空返回）→ 回切 TD 冷备。"""

        async def fake_bus_get(codes):
            return ""

        async def td_ok(pair):
            assert pair == "USD/HKD"
            return 7.8470

        monkeypatch.setattr(pf, "fetch_td_fx", td_ok)
        monkeypatch.setattr(pf.tencent_bus, "get", fake_bus_get)
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd())
        assert rate == 7.8470 and fallback is True

    def test_all_fail_static_fallback(self, monkeypatch):
        """腾讯与 TD 均失败 → 7.80 静态兜底。"""

        async def fake_bus_get(codes):
            return None

        async def no_td(pair):
            return 0.0

        monkeypatch.setattr(pf, "fetch_td_fx", no_td)
        monkeypatch.setattr(pf.tencent_bus, "get", fake_bus_get)
        rate, fallback = asyncio.run(pf.fetch_fx_usdhkd())
        assert rate == pf.FX_FALLBACK and fallback is True
```

（若原文件顶部已导入 asyncio 则复用；`FX_OK_RAW` 常量如仅被删除的用例引用则一并删除。）

1c. `tests/test_longport.py` 的 `TestPanelUsdcnhInjection` 类内追加 2 个用例（保留既有 test_td_primary / test_sina_fallback 不动——后者断言 DXY 回落 sina 104.3 在新逻辑下仍成立）：

```python
    def test_dxy_yahoo_primary(self, monkeypatch):
        """DXY 主价迁 Yahoo：yahoo_day 注入优先于新浪批冷备价。"""
        by_key = self._run(monkeypatch,
                           {"dxy": {"price": 104.3}, "cnh": {"price": 7.1079},
                            "hf": {}}, td_usdcnh=None)
        # _run 固定传 yahoo_day={}，先验证回落路径不受影响
        assert by_key["DXY"]["price"] == 104.3

        async def fake_sina(proxy_urls=None):
            return {"dxy": {"price": 104.3}, "cnh": {"price": 7.1079}, "hf": {}}
        monkeypatch.setattr(pf, "fetch_sina_panel", fake_sina)
        panel = asyncio.run(pf.fetch_market_panel(
            proxy_urls=[], yahoo_day={"DXY": {"price": 105.1, "change_pct": 0.5}},
            spark_map={}, fetch_missing=False,
            sina_panel={"dxy": {"price": 104.3}}, td_usdcnh=None))
        by_key2 = {p["key"]: p for p in panel}
        assert by_key2["DXY"]["price"] == 105.1   # Yahoo 主价优先
```

- [ ] **Step 2: 跑测试确认失败**

```bash
python3 -m pytest tests/test_portfolio.py -k "TencentWhFx or TencentPrimary" -v
python3 -m pytest tests/test_longport.py::TestPanelUsdcnhInjection -v
```
预期：portfolio 新用例 FAIL（`parse_tencent_wh_fx`/新主源逻辑不存在）；longport 新用例 FAIL（DXY 仍取 sina_dxy，yahoo_day 优先逻辑未实现）。

- [ ] **Step 3: 实现 portfolio_fetcher.py**

3a. 新增解析函数（放 `parse_fx_usdhkd` 原 291 行附近，同时**删除** `parse_fx_usdhkd` 函数与第 43 行常量 `SINA_FX_URL = "https://hq.sinajs.cn/list=fx_susdhkd"`）：

```python
def parse_tencent_wh_fx(raw: str) -> float:
    """解析腾讯外汇行 v_whUSDHKD（波浪线布局 f[3] 现价），失败/越界返回 0.0。

    wh 行仅 ~21 字段且布局与 us 股票不同（f[4] 恒 0），不能复用
    parse_tencent_quotes 的 us/hk 分支；汇率场景只需现价。@author yys
    """
    m = re.search(r'v_whUSDHKD="([^"]*)"', raw or "")
    if not m:
        return 0.0
    f = m.group(1).split("~")
    v = _f(f[3]) if len(f) > 3 else 0.0
    return v if 7.5 <= v <= 8.5 else 0.0
```

3b. `fetch_fx_usdhkd`（1078-1091 行）整体替换（`proxy_urls` 参数保留以兼容调用方签名）：

```python
async def fetch_fx_usdhkd(proxy_urls: list[str] | None = None) -> tuple[float, bool]:
    """获取 USD/HKD 汇率。返回 (汇率, 是否兜底)。

    主源腾讯 whUSDHKD 走 tencent_bus 攒批总线（与股票/候选池同请求节奏，
    零额外请求；新浪易风控已退役），失败回切 twelvedata 冷备
    （TD 免费额度让位锚点恢复与 USDCNH 主价），再失败置 7.80 静态兜底。@author yys
    """
    raw = await tencent_bus.get(["whUSDHKD"])
    v = parse_tencent_wh_fx(raw or "")
    if v > 0:
        return v, False
    v = await fetch_td_fx("USD/HKD")
    if 7.5 <= v <= 8.5:
        return v, True
    return FX_FALLBACK, True
```

（确认文件已 `import tencent_bus`——fetch_market_panel 已用，无需新增 import。）

3c. `fetch_market_panel` 两处改动：

- 1263 行 `need_keys` 元组加 "DXY"：

```python
    need_keys = [k for k in ("US30Y", "US10Y", "VIX", "DJI", "NDX", "SPX", "DXY")
                 if not (yahoo_day.get(k) or {}).get("price")] if fetch_missing else []
```

- 1281-1283 行 DXY 独立分支删除，1292 行元组加 "DXY" 并带回落：

```python
        elif key in ("US30Y", "US10Y", "VIX", "DJI", "NDX", "SPX", "DXY"):
            # 主价+分时同源 Yahoo 当日分时缓存；VIX/DJI/NDX/SPX 常态由长桥
            # 实时价填充 yahoo_day（server 注入），Yahoo 仅长桥不可用时兜底；
            # DXY 唯一免费源 Yahoo（server 4h backfill 节流），失败窗口回落
            # 新浪批冷备价；收益率类 price 保留 3 位小数（前端再 fmt）
            q = yahoo_day.get(key) or {}
            if not q.get("price") and key == "DXY":
                q = sina_dxy
            if q.get("price") and key in ("US30Y", "US10Y"):
                q = {**q, "price": round(q["price"], 3)}
```

3d. 注释同步（仅文字，不改逻辑）：
- 模块头（第 8 行）：`- 新浪 fx_susdhkd：USD/HKD 汇率（失败兜底 7.80）` → `- 腾讯 whUSDHKD（总线攒批）：USD/HKD 汇率（失败兜底 7.80；新浪已退役）`；第 9 行 USDCNH 描述补"twelvedata 主价 30min"
- `fetch_td_fx` docstring（1059 行附近）"USD/CNH 由 _market_loop 60s 节流调用" → "30min 节流调用"
- `fetch_sina_panel` docstring（1204-1207 行）：更新为"本批由 server _market_loop 以 15min 节流低频调用（纯兜底批：BZ/GC 兜底 + USDCNH 冷备 + DXY 回落冷备；主价职责已全部分离：DXY→Yahoo backfill、USDCNH→twelvedata）"
- `fetch_market_panel` docstring 数据分工段（1230-1231 行）："DXY 新浪 DINIW（5min 鲜度）；USDCNH twelvedata 主价（60s）缺失回落新浪批" → "DXY Yahoo DX-Y.NYB backfill（4h，失败回落新浪批冷备）；USDCNH twelvedata 主价（30min）缺失回落新浪批"
- `MARKET_PANEL` 表 DXY 行注释（1133 行）：`主价新浪 DINIW` → `主价 Yahoo backfill(4h,新浪批冷备)`；USDCNH 行（1137 行）同步

- [ ] **Step 4: 跑 Task 1 测试确认通过**

```bash
python3 -m pytest tests/test_portfolio.py tests/test_longport.py -q
```
预期：全绿（存量 + 新增）。

- [ ] **Step 5: Commit**

```bash
git add portfolio_fetcher.py tests/test_portfolio.py tests/test_longport.py
git commit -m "refactor(datasource): USDHKD主源切腾讯whUSDHKD总线(DXY并入Yahoo主价组,新浪退役USDHKD主源)"
```

---

### Task 2: server.py 节流参数调整（含测试确认）

**Files:**
- Modify: `server.py`（`_market_loop` 内 4 处）

- [ ] **Step 1: 实现（参数改动无独立单测，由既有 `_market_loop` 相关纯函数测试与基线守护）**

2a. `sina_panel_interval` 初始值（约 1908 行）与成功重置值（约 1933 行）`300.0` → `900.0`；行注释 `成功固定 5min；失败 min(600*2^n, 1800)` → `成功固定 15min（纯兜底批）；失败 min(600*2^n, 1800)`。

2b. twelvedata USDCNH 节流（约 1909-1912 行注释块 + 1942 行判定 + 1947 行回落阈值）：

- 注释块改为：

```python
    # twelvedata USDCNH 主价状态（30min 节流 48 credits/天，根除免费配额 429；
    # 连续无价回落新浪冷备）
    td_usdcnh: float | None = None
    td_fx_at = 0.0        # 上次尝试时刻（30min 节流）
    td_fx_ok_at = 0.0     # 上次成功时刻（>90min 无有效价回落新浪）
```

- 1942 行 `if now_mono - td_fx_at >= 60.0:` → `>= 1800.0`
- 1947 行 `elif td_usdcnh is not None and now_mono - td_fx_ok_at < 600.0:` → `< 5400.0`（3 个周期无价才回落）
- 1940-1941 行分支注释 `60s 节流，失败沿用上次值，>10min 无有效价` → `30min 节流，失败沿用上次值，>90min 无有效价`

2c. `yahoo_keys`（约 1987-1988 行）加 "DXY"：

```python
            yahoo_keys = ("US30Y", "US10Y", "DXY") + (
                () if lp_idx_ok else ("VIX", "DJI", "NDX", "SPX"))
```

2d. `_market_loop` docstring（1893-1895 行）同步：
`USDCNH 主价源为 twelvedata（60s 节流 1 credit/min）` → `USDCNH 主价源为 twelvedata（30min 节流 48 credits/天）`；`新浪批（DXY 主价/BZ、GC 兜底）降频 5min 节流` → `DXY 主价并入 Yahoo backfill 组（与 US30Y/US10Y 同轮，4h 冷却）；新浪批降级纯兜底（BZ/GC 兜底/USDCNH 与 DXY 冷备，15min 节流）`。

- [ ] **Step 2: 基线回归**

```bash
python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py tests/test_alert_engine.py -q && python3 -m py_compile server.py portfolio_fetcher.py
```
预期：全绿、编译通过。

- [ ] **Step 3: Commit**

```bash
git add server.py
git commit -m "refactor(datasource): USDCNH降频30min根除td配额429(DXY并入Yahoo4h组,新浪批纯兜底15min)"
```

---

### Task 3: 部署与线上验证（主 Agent 执行）

- [ ] rsync 发布 + `systemctl restart qdii`
- [ ] 验证：`systemctl is-active qdii`；journalctl 10min 观察：无 twelvedata 429、新浪批按 15min 节奏、腾讯 whUSDHKD 生效（fx_state 非兜底）、DXY 出现 Yahoo backfill 刷新日志
- [ ] 线上 MySQL 抽查 `market_ticks` DXY/USDCNH 新 tick 写入
