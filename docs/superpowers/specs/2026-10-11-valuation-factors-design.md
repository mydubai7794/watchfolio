# 估值因子层（P/E、P/B、PEG、股息率）设计规格

> 2026-10-11。决策上下文层（agent-decision-context）的估值面扩展。
> 全链数据形状已服务器实测锚定（见 §7），实现时不得偏离实测事实。

## 1. 目标

为外部 Agent 与日K弹层提供估值因子：**P/E(TTM)、P/B、PEG、股息率**。
数据链：长桥 `static_info` 日更（券商已换算口径）+ EDGAR companyconcept 周更（EPS 增速）。
零新增独立循环，挂 `edgar_daily_loop` 尾部。

## 2. 数据源与请求预算

| 源 | 用途 | 频次 | 请求量 |
|----|------|------|--------|
| 长桥 `ctx.static_info(symbols)` | eps_ttm/bps/dividend_yield（USD/ADS 已换算） | 日更 | 1 次批量/天（≤500 只/次限制内，美股持仓 ~20 只一批） |
| EDGAR companyconcept `/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/EarningsPerShareDiluted.json` | 最近两个完整财年 EPS 同比（增速） | 周更（周一或距上次成功 >6 天） | ~10 次/周，每只 10-45KB |

复用：`longport_fetcher._get_ctx/_sdk_call/_mark_failure/_mark_success`（长桥限流契约、
静默空防御）；`edgar_fetcher._edgar_get`（SEC 2s 节流/UA/`_EDGAR_LOCK`/退避）。

## 3. 口径契约（实测锚定）

### 3.1 长桥 static_info（日更）

- 字段：`eps_ttm: Decimal`、`bps: Decimal`、`dividend_yield: Decimal`（SDK 对象
  `SecurityStaticInfo`，decimal 需转 float；负值/零语义见 §3.3）。
- **BABA 实测**：eps_ttm=4.39、bps=64.37（USD/ADS 口径，券商已完成 CNY→USD、
  普通股→ADS 换算）——P/E、P/B 直接 `price/eps_ttm`、`price/bps`，无需任何换算。
- **ETF 字段是噪声，必须门控**：实测 QQQ eps_ttm=-88.5（UIT 会计值）、SMH 全 0。
  门控规则（优先级从上到下，注入层 reason 推导口径，批2审查后与实现一致）：
  1. `asset_type == 'etf'` → `reason='fund'`（落库侧 ETF 行置 None 覆盖脏值）；
  2. `eps_ttm <= 0 or bps <= 0`（含 None/缺标的）→ `reason='no_data'`
     （保护 asset_type 仍为 NULL 的个股如 CCJ 正常通过，拦截长桥 0 值噪声）；
  3. `growth_pct is None or <= 0` → `reason='no_growth'`。
- 静默空陷阱（踩坑记录 §2.5）：返回集合缺 symbol 不报错——按返回集合 join，
  缺失标的自然落入 no_data。

### 3.2 EDGAR companyconcept（周更增速）

- CIK 来源：EDGAR ticker map 缓存 + `_CIK_OVERRIDES`（QQQ 等）；排除
  `_EDGAR_EXCLUDE`（F 同名错配红线）。仅 `asset_type in ('stock','fpi')` 且
  `eps_ttm/bps` 有效（reason 非 fund/no_data）的标的才需要增速——其余 `no_cik`
  或不需要。
- 解析规则 `parse_eps_growth(text) -> float | None`（同比 %）：
  1. 取 units 下**数据行数最多的 unit**（= 报表主币种；BABA 实测 CNY 47 行 >
     USD 16 行，取 CNY——增速是比值，货币无关，原币口径避开汇率噪声）；
  2. 行过滤：`form in ('10-K','20-F','40-F')` 且 `fp=='FY'` 且
     `start[:4] != end[:4]`（跨年，防季度值混入）；
  3. 按 `(start, end)` 去重（实测 NVDA 同财年多行重复申报），同键取 `end` 最新
     行的 val；按 end 升序排序取**最后两个完整财年**，`growth = (v2/v1 - 1) * 100`；
  4. 财年不足两个 / v1 == 0 / 解析失败 → None；`growth <= 0` → 保留值由上层
     标 `no_growth`（PEG null）。
- 404/非 200 容错：`_edgar_get` 返回 None → growth=None（SKHY 等 404 属预期）。

### 3.3 PEG 与计算时点（关键决策）

- **落库只存慢变原始值**（eps_ttm/bps/growth_pct/dividend_yield），
  **不落库 pe/pb/peg 与 reason**——现价每轮变，落库即过时；reason 由注入层
  根据落库数据 + asset_type 实时推导（2026-10-11 实施修订：表无 reason 列）。
- `_build_agent_context` 注入时实时算：
  `pe = price / eps_ttm`、`pb = price / bps`、`peg = pe / (growth_pct / 100)`
  （price 取 market_cache 现价；eps_ttm>0、bps>0、growth_pct>0 各自防御，
  缺哪个哪个为 null）。
- PEG 口径：trailing（TTM P/E ÷ 最近完整财年 EPS 同比），`reason` 无需额外
  标注口径——meta 已含数据源信息。

## 4. 存储（database.py）

```sql
CREATE TABLE IF NOT EXISTS holding_valuations (
  symbol         VARCHAR(16)  NOT NULL,
  eps_ttm        DECIMAL(16,6) NULL,
  bps            DECIMAL(16,6) NULL,
  growth_pct     DECIMAL(10,4) NULL COMMENT '最近两个完整财年EPS同比%（原币）',
  dividend_yield DECIMAL(8,4)  NULL,
  updated_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY(symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='估值因子原始值（长桥日更+EDGAR周更）';
```

CRUD：`upsert_holding_valuation(row: dict)`（ON DUPLICATE KEY UPDATE 全字段）、
`get_valuations() -> dict[symbol, dict]`（全表，估值层 ≤20 行无过滤必要）。
init_tables 注册（幂等建表，与 insider_tx 同模式）。

## 5. 模块与挂载

- **valuation.py**（新文件）：
  - `normalize_static_info(infos, holdings) -> dict[sym, {eps_ttm, bps, dividend_yield}]`
    （SDK 对象 → float 归一 + Decimal/None 防御）
  - `parse_eps_growth(text) -> float | None`（§3.2）
  - `run_valuation_round() -> dict`（stats：updated/fund/no_data/growth_ok 等；
    长桥批量 → EDGAR 逐只增速（周更门控）→ upsert 全量持仓行）
  - 周更门控：`sys_config.VALUATION_GROWTH_TS`（上次增速成功时间），
    `weekday()==0` 或距今 >6 天或无值 → 跑增速；成功后写 TS。
- **longport_fetcher.py**：`fetch_static_info(symbols) -> list`（薄封装：
  `_sdk_call(ctx.static_info, ...)`，失败 `_mark_failure` 返回 []，复用 backoff）。
- **edgar_fetcher.py**：`fetch_companyconcept(cik, concept="EarningsPerShareDiluted")
  -> str | None`（`_edgar_get` 新 URL 常量 `COMPANYCONCEPT_URL`，CIK 补零 10 位）。
  `edgar_daily_loop`：`run_edgar_round()` 完成后 try/except 调
  `valuation.run_valuation_round()`（异常仅 log，不影响 EDGAR 主轮；import 放
  函数内避免模块级循环依赖）。

## 6. API 与前端

### 6.1 API 注入（server.py `_build_agent_context`）

`factors[symbol].valuation = {pe_ttm, pb, peg, eps_ttm, bps, growth_pct,
dividend_yield, reason, updated_at}`——price 实时算（§3.3），全字段 null 安全。
两个端点（`/api/agent/portfolio`、`/api/portfolio/overview`）随 factors 同源下发。

### 6.2 前端（static/portfolio.html）

1. **撤持仓表三列**：删除表头 `RSI14`/`vs SPY(20d)`/`内部人90d` 三 `<th>` 与
   对应渲染分支（现 :485-487、:952-961 区域），colspan 12→9（已清仓分区等
   所有 colspan 同步）。
2. **日K弹层加「决策因子」区块**：RSI14、vs SPY(20d)、内部人90d（净额+笔数）、
   P/E(TTM)、P/B、PEG、股息率七项；数据取 `lastOverview.context.factors[symbol]`
   （SSE 保活合并已有，零额外请求）；样式参考弹层现有信息行（MA50/MA200/RSI14/
   吊灯止损模式，:2371-2377）；缺失项显示 `--`，fund/no_data 显示 reason 中文
   （基金/暂无数据）。

## 7. 实测锚点（fixture 素材，禁止凭空编造 schema）

- NVDA static_info：eps_ttm=8.0033、bps=9.5014、eps=4.9820、dy=0.52、USD、
  总股本 24.1B。
- BABA static_info：eps_ttm=4.3942、bps=64.3653（USD/ADS）、dy=1.03。
- QQQ static_info：eps_ttm=-88.4985、bps=267.21（噪声→fund 门控）；
  SMH：全 0（→no_data）。
- NVDA companyconcept（44KB）：units `USD/shares` 309 行；年度序列尾部
  `(2024,2025,2.94)`×2 重复 + `(2025,2026,4.90)` → growth=66.67%。
- BABA companyconcept（9.7KB）：units `CNY/shares` 47 行 + `USD/shares` 16 行
  → 主币种 CNY；年度尾部 `(2024,2025,6.7)`、`(2025,2026,5.5)` → growth=-17.91%。
- companyfacts/companyconcept URL **CIK 必须 10 位补零**（CIK1045810 → 404，
  CIK0001045810 → 200）。

## 8. 测试策略

- tests/test_valuation.py（新）：normalize_static_info（Decimal/BABA 值/QQQ 负值/
  缺标的）、parse_eps_growth（NVDA 重复行、BABA 双 unit 主币种、40-F、季度混入
  防御、单财年、v1=0、坏 JSON）、run_valuation_round（打桩长桥+EDGAR+db：
  fund 门控/no_data/no_cik/周更门控 TS 写入/异常不冒泡）、db CRUD。
- test_longport.py 扩展：fetch_static_info 成功/失败打桩。
- test_edgar_fetcher.py 扩展：fetch_companyconcept URL 补零/非 200 None。
- test_agent_context.py 扩展：valuation 注入（price 实时算 peg 链路、reason
  透传、null 安全）。
- 基线 15 文件 495 passed 不得回归（新测试叠加）。

## 9. 边界与不做

- ETF/fund 永不估值（reason='fund'）；无 CIK 标的 growth 缺（'no_cik'）；
  增长≤0 PEG null（'no_growth'，PE/PB 正常）。
- 不做分析师预期 PEG（forward）——无数据源，trailing 口径够用。
- companyfacts 全量端点不使用（4MB/只，companyconcept 44KB 已覆盖增速需求）。
- 长桥 static_info 的 name/lot_size 等其余字段不消费、不落库。
