# 外部 Agent 持仓接口

`GET /api/agent/portfolio` — 供外部 agent（持仓鉴定/再平衡分析）拉取当前持仓、收益、现金、盈亏快照。数据与持仓页同源（读服务端内存缓存，零额外行情请求），只读。

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
  "cny": {
    "rate": 7.12,
    "invested": 84850.0,
    "invested_gross": 84850.0,
    "expense": 0.0,
    "value": 87901.17,
    "pnl": 3051.17,
    "pnl_pct": 3.6
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
