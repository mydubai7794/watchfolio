# 外部 Agent 持仓接口

`GET /api/agent/portfolio` — 供外部 agent（持仓鉴定/再平衡分析）拉取当前持仓、收益、现金、盈亏快照 + 决策上下文（市场基准/宏观/广度/个股技术与估值因子）。数据与持仓页同源（读服务端内存缓存与数据库，零额外行情请求），只读。

- 服务地址：`http://<your-domain>`
- 请求方法：`GET`
- 无需浏览器登录态（middleware 已放行 `/api/agent/` 前缀），凭 `X-API-Key` 请求头自鉴权

## 鉴权

- 请求头 `X-API-Key: <AGENT_API_KEY>`；Key 存于服务端 `sys_config` 表 `AGENT_API_KEY` 项
- 鉴权在 `/api/agent` 路由域以 FastAPI router 依赖统一收口：该域下所有端点挂同一 `APIRouter` 即自动受保护，无需各端点手写
- 未配置 → 404（视为禁用，不暴露端点存在性）；不匹配/缺失头 → 401（常量时间比较，防时序侧信道）；Key 配置读取异常 fail-closed 同 404 并留服务端日志
- 更换 Key = 更新配置即时生效（无缓存，每次请求实时读配置）
- 配置方法（在服务器上执行 MySQL）：

```sql
INSERT INTO sys_config (`key`, value, remark)
VALUES ('AGENT_API_KEY', '<32位随机串>', '外部agent持仓接口密钥')
ON DUPLICATE KEY UPDATE value=VALUES(value);
```

> 随机串生成参考：`python3 -c "import secrets; print(secrets.token_urlsafe(24))"`

## 请求示例

```bash
curl -H "X-API-Key: <key>" http://<your-domain>/api/agent/portfolio
```

错误响应格式（FastAPI 原生形态，统一为 `{"detail": "..."}`）：

```json
{"detail": "无效的 API Key"}
```

| 状态码 | 含义 | 响应 body |
|--------|------|-----------|
| 200 | 成功返回快照 | 见「响应结构」 |
| 401 | Key 不匹配 / 未携带 `X-API-Key` 头 | `{"detail": "无效的 API Key"}` |
| 404 | Key 未配置（端点视为禁用）→ `{"detail": "接口未启用"}`；路径不存在 → FastAPI 默认 `{"detail": "Not Found"}` | `{"detail": "..."}` |

## 响应结构

完整 JSON 示例（与端点实际返回字段一一对应）：

```json
{
  "ok": true,
  "ready": true,
  "generated_at": "2026-10-05 22:35:18",
  "session": "regular",
  "trade_date": "2026-10-02",
  "summary": {
    "total_value": 12345.67,
    "holdings_value": 11000.0,
    "cash_usd": 1345.67,
    "cost_basis": 9800.0,
    "day_pnl": 120.5,
    "day_pnl_pct": 0.99,
    "close_pnl": 80.0,
    "overnight_pnl": 40.5,
    "total_pnl": 2545.67,
    "total_pnl_pct": 25.98
  },
  "holdings": [
    {
      "symbol": "NVDA",
      "name": "英伟达",
      "market": "US",
      "currency": "USD",
      "quantity": 10.0,
      "cost_price": 120.0,
      "price": 135.5,
      "value": 1355.0,
      "today_pnl": 25.0,
      "position_pnl": 155.0,
      "position_pnl_pct": 12.92,
      "cost_usd": 1200.0,
      "stale": false,
      "ext": "regular"
    }
  ],
  "cash": [
    {"currency": "USD", "amount": 1200.0},
    {"currency": "HKD", "amount": 1137.0}
  ],
  "fx": {
    "usdhkd": 7.8,
    "usdcnh": 7.12
  },
  "context": {
    "spy": {"close": 600.5, "asof": "2026-10-10", "chg_pct_1d": 0.8},
    "us10y": {"price": 4.04, "dgs10_fred": 4.05, "asof": "2026-10-10"},
    "credit_spread_hy": {"value": 3.18, "asof": "2026-10-10"},
    "cpi_yoy": {"value": 2.9, "asof": "2026-09-30"},
    "breadth": {"rsp_spy_ratio": 0.2412, "chg_pct_5d": -0.5, "chg_pct_20d": 1.2},
    "factors": {
      "NVDA": {
        "rsi14": 53.0,
        "spy_rel_20d_pct": 2.91,
        "asset_type": "stock",
        "insider_90d": {"net_shares": -4332118, "net_value_usd": -967599313.47,
                         "buys": 0, "sells": 25, "tx_count": 37,
                         "since": "2026-07-13"},
        "valuation": {
          "pe_ttm": 28.84, "pb": 24.29, "peg": 0.43,
          "eps_ttm": 8.00332, "bps": 9.501411,
          "growth_pct": 66.6667, "dividend_yield": 0.23,
          "reason": null, "updated_at": "2026-10-11 17:33:58"
        }
      },
      "QQQ": {
        "rsi14": 55.2, "spy_rel_20d_pct": 0.3, "asset_type": "etf",
        "insider_90d": null,
        "valuation": {"pe_ttm": null, "pb": null, "peg": null,
                      "eps_ttm": null, "bps": null, "growth_pct": null,
                      "dividend_yield": null, "reason": "fund",
                      "updated_at": "2026-10-11 17:33:48"}
      },
      "BABA": {
        "rsi14": 48.1, "spy_rel_20d_pct": -1.5, "asset_type": "fpi",
        "insider_90d": null, "insider_note": "fpi_no_form4",
        "valuation": {"pe_ttm": 25.24, "pb": 1.72, "peg": null,
                      "eps_ttm": 4.394193, "bps": 64.365309,
                      "growth_pct": -17.9104, "dividend_yield": 0.93,
                      "reason": "no_growth", "updated_at": "2026-10-11 17:33:50"}
      }
    },
    "meta": {"edgar_ok": true, "edgar_last_ok": "2026-10-11 15:51:21",
             "generated_at": "2026-10-11 18:05:00"}
  },
  "cny": {
    "rate": 7.12,
    "invested": 14000.0,
    "invested_gross": 14000.0,
    "expense": 0.0,
    "value": 14596.0,
    "pnl": 596.0,
    "pnl_pct": 4.26
  }
}
```

## 字段说明

### 顶层

| 字段 | 类型 | 说明 |
|------|------|------|
| `ok` | bool | 恒为 `true`（鉴权通过即成功） |
| `ready` | bool | 行情快照是否就绪：`true` 正常 / `false` 服务冷启动中（此时 `summary` 为空对象、`holdings` 为 `[]`、`cny` 为 `null`，agent 应稍后重试而非按空仓处理） |
| `generated_at` | string | 快照生成时间，北京时间 `YYYY-MM-DD HH:MM:SS` |
| `session` | string\|null | 美股当前时段：`regular` 盘中 / `pre` 盘前 / `post` 盘后 / `closed` 闭市（含周末假日）；服务冷启动未就绪时为 `null` |
| `trade_date` | string\|null | 当前美东交易日 `YYYY-MM-DD` |
| `summary` | object | 组合级汇总（USD 口径），见下表 |
| `holdings` | array | 持仓明细列表，见下表；空仓时为 `[]` |
| `cash` | array | 现金列表（账户币种维度）：`currency`（USD/HKD）+ `amount` |
| `fx` | object | 汇率快照：见「fx / cny 说明」 |
| `context` | object | 决策上下文（市场基准/宏观/广度/个股因子，60s 服务端缓存），见下节 |
| `cny` | object\|null | 人民币累计口径看板；输入缺失（本金未配置/看板无 USDCNH）时为 `null` |

### summary（USD 口径）

总资产含现金：`total_value = holdings_value + cash_usd`；一场口径恒等式：`day_pnl = close_pnl + overnight_pnl`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `total_value` | number | 总资产（USD，含现金） |
| `holdings_value` | number | 持仓市值合计（USD；港股/基金持仓已按 fx.usdhkd 折算为 USD） |
| `cash_usd` | number | 现金合计（USD 口径，HKD 现金按 fx.usdhkd 折算） |
| `cost_basis` | number | 持仓成本合计（USD 口径） |
| `day_pnl` | number | 今日盈亏（实时累计，USD） |
| `day_pnl_pct` | number | 今日盈亏率（%） |
| `close_pnl` | number | 收盘定格盈亏（常规时段 16:00 定格价口径，USD） |
| `overnight_pnl` | number | 夜盘增量（`day_pnl − close_pnl`，USD） |
| `total_pnl` | number | 累计盈亏（现值 − 成本，USD） |
| `total_pnl_pct` | number | 累计盈亏率（%，相对 cost_basis） |

### holdings[]

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | string | 标的代码（美股/港股/基金） |
| `name` | string | 标的名称 |
| `market` | string | 市场：`US` 美股 / `HK` 港股 / `HKFUND` 香港基金 |
| `currency` | string | 计价币种：`USD` / `HKD` |
| `quantity` | number | 持有数量（份额/股数） |
| `cost_price` | number | 单位成本（原币种） |
| `price` | number | 当前价（原币种）；行情缺失时为 `0` |
| `value` | number | 持仓市值（原币种）；行情缺失时为 `0` |
| `today_pnl` | number\|null | 今日盈亏（原币种）；行情缺失时为 `null` |
| `position_pnl` | number\|null | 持仓盈亏（原币种，现值−成本）；行情缺失时为 `null` |
| `position_pnl_pct` | number\|null | 持仓盈亏率（%，相对该仓 cost_usd）；行情缺失时为 `null` |
| `cost_usd` | number | 该仓成本折 USD 合计 |
| `stale` | bool | 行情陈旧标记：`true` 表示行情缺失/过期（此时 `value=0`、盈亏为 `null`，agent 应跳过该行计算或提示数据不可用） |
| `ext` | string | 数据所处时段：`regular` 盘中 / `pre` 盘前 / `post` 盘后 / `closed` 闭市 / `fund` 基金 NAV 口径 |

## context 说明（决策上下文）

facts only：全部来自服务端数据库（日K/宏观表/EDGAR/长桥估值原始值），构建零额外行情请求；60s 服务端缓存；任一块数据缺失时该块为 `null` 降级、不影响其余字段；`factors` 只含美股持仓，单标的坏数据仅丢弃该标的自身。

### 市场基准与宏观（顶层块）

| 字段 | 类型 | 说明 |
|------|------|------|
| `spy.close` / `spy.asof` / `spy.chg_pct_1d` | number/string/number | SPY 最新收盘、日期（`YYYY-MM-DD`）、日涨跌%（服务端日K库） |
| `us10y.price` | number\|null | 美 10 年期国债收益率（Yahoo 主价快照，可能 null） |
| `us10y.dgs10_fred` | number\|null | FRED DGS10 日线（双源对照） |
| `credit_spread_hy` | {value, asof}\|null | 高收益债信用利差 OAS（FRED，日更） |
| `cpi_yoy` | {value, asof}\|null | 美国 CPI 同比%（宏观日更） |
| `breadth.rsp_spy_ratio` | number | RSP/SPY 收盘比值（等权/市值权风格轮动指标：走高=广度改善） |
| `breadth.chg_pct_5d` / `chg_pct_20d` | number | 该比值 5/20 日变化% |
| `meta.edgar_ok` | bool | EDGAR 内部人数据是否新鲜（最近一轮成功距今 < 阈值） |
| `meta.edgar_last_ok` | string\|null | EDGAR 最近一次成功时间 |
| `meta.generated_at` | string | 上下文构建时间 |

### factors[symbol]（个股决策因子）

| 字段 | 类型 | 说明 |
|------|------|------|
| `rsi14` | number\|null | 14 日 RSI（日K收盘口径，[0,100]） |
| `spy_rel_20d_pct` | number\|null | 近 20 日相对 SPY 超额收益（百分点：个股20日% − SPY20日%） |
| `asset_type` | string\|null | `stock` 美股个股 / `etf` ETF / `fpi` 外国私募发行人（20-F 申报，无 Form 4）/ `none` 人工排除（SEC 同名实体错配红线，如 `F`，无 EDGAR 数据）/ `null` 未分类（EDGAR 探测中） |
| `insider_90d` | object\|null | 近 90 天内部人交易聚合（SEC Form 4，EDGAR 官方源），**只计公开市场交易**：买入=P 码、卖出=S 码；RSU 授予(A)/行权(M)/赠与(G)/税务代扣(F) 等非主动交易**不计入** `buys`/`sells`/`net_*`，仅计入 `tx_count`。`net_value_usd<0` = 净卖出。`fpi` 标的恒为 `null` |
| `insider_note` | string | 仅 `fpi`：`"fpi_no_form4"`（外企无 Form 4 申报义务） |
| `valuation` | object | 估值因子块，见下表 |

### factors[symbol].valuation（估值因子）

估值倍数**不落库、注入时用最新现价实时算**（每轮都是最新价）；原始值日更（长桥）+ 周更（EDGAR）。ETF/FPI 与美股的 eps/bps 均为 **USD/股（ADS 口径）**，无需再换算。

| 字段 | 类型 | 说明 |
|------|------|------|
| `pe_ttm` | number\|null | P/E (TTM) = 现价 ÷ `eps_ttm`；现价或 eps 缺失/≤0 → null |
| `pb` | number\|null | P/B = 现价 ÷ `bps`；同上防御 |
| `peg` | number\|null | PEG = P/E ÷ `growth_pct`（growth 为**百分数数值**：P/E=20、增速 20% → PEG=1.0）。**解读：<1 相对增长便宜、>1 偏贵**；`growth_pct` 缺失或 ≤0 → null。注意增速为 trailing 口径（最近两个完整财年），与部分行情源的 forward 预期口径数值不同 |
| `eps_ttm` | number\|null | TTM 每股收益（USD，长桥日更；亏损股为负值） |
| `bps` | number\|null | 每股净资产（USD，长桥日更） |
| `growth_pct` | number\|null | 最近两个完整财年 EPS 同比%（原币种比值、与汇率无关；EDGAR 10-K/20-F/40-F 年报，周更） |
| `dividend_yield` | number\|null | 真实股息率 % = 每股 TTM 股息(USD) ÷ 现价 × 100；现价缺失或无股息 → null。**注意落库原始字段为每股股息金额（如 NVDA $0.52/股），本字段已完成换算，勿再除价** |
| `reason` | string\|null | 估值缺失原因：`"fund"` ETF 不适用估值（QQQ 等 UIT 会计值为噪声）/ `"no_data"` 长桥无数据或 eps/bps ≤0（亏损股）/ `"no_growth"` 增速缺失或 ≤0（PEG 不可算）/ `null` 全量有效 |
| `updated_at` | string | 估值原始值最近落库时间（长桥日更约每日一次，growth 周更） |

**消费建议**：`reason != null` 时跳过对应解读（fund=不适用、no_data=数据不可用、no_growth=仅 PEG 缺）；`insider_90d.sells` 大而 `buys=0` 是常态（内部人以期权授予+卖出变现为主，零买入≠利空信号本身，关注`net_value_usd` 与历史分位）；`peg` 依赖年报 trailing 增速，对处于盈利拐点的公司（如周期反转初期）会滞后，可与 `growth_pct` 原值同看。

## fx / cny 说明

- `fx.usdhkd`：USD→HKD 静态折算率（合法区间 7.5–8.5，超出视为异常回退默认值）；用于港股/基金及 HKD 现金折 USD
- `fx.usdcnh`：USD/CNH 离岸汇率快照（来自市场看板缓存）；看板未就绪时为 `null`
- `cny`（人民币累计口径，净投入）：
  - `value = total_value × usdcnh`（总资产折人民币）
  - `pnl = value − (invested_gross − expense)`（赚了多少人民币）
  - `invested_gross` 来自 `sys_config.cny_invested`，`expense` 来自 `sys_config.expense_cny`
  - 任一输入缺失（本金未配置/无 USDCNH/总资产为 0）→ `cny = null`
  - 与持仓页 overview 接口同逻辑：用最新看板 USDCNH 实时重算，不受慢轮快照拖累

## 关联配置表（sys_config）

| key | 说明 |
|-----|------|
| `AGENT_API_KEY` | 本接口密钥（未配置 = 端点禁用 404） |
| `ALERT_MIN_MARKET_VALUE` | 提醒市值阈值（USD，默认 500，0 = 不过滤）——与本接口无关，供 agent 了解提醒口径 |
| `cny_invested` | 人民币累计投入本金（CNY，cny 口径输入） |
| `expense_cny` | 个人消费折 CNY（cny 净投入扣减项） |
| `EDGAR_LAST_OK` | EDGAR 内部人数据最近成功时间（`meta.edgar_ok` 的判定源） |
| `VALUATION_GROWTH_TS` | 估值增速（growth_pct）周更水位：周一或距今 >6 天才拉 EDGAR，其余轮次仅长桥日更 |
