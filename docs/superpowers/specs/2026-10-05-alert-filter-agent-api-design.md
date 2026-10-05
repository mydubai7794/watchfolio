# 策略提醒市值过滤 + 外部 Agent 持仓接口 设计文档

日期：2026-10-05
状态：已确认（方案 1A + 2A）

## 背景与目标

1. **策略提醒过滤**：提醒引擎当前对全部美股持仓每轮全量求值，小仓位也会触发
   邮件/SSE 提醒。需求：只提醒"值得关注"的股票，首版口径为持仓市值 ≥ 阈值
   （默认 500 USD，可配置，阈值用于预热开关，后续可换其他关注定义）。
2. **外部 Agent 持仓接口**：持仓/收益/现金/盈亏数据目前只有浏览器
   session cookie 鉴权的 `/api/portfolio/overview`，外部 agent 无法调用。
   需求：一个静态 API Key 鉴权的只读 HTTP 接口，供其他 agent 做持仓鉴定
   与再平衡分析。

## 一、策略提醒市值过滤（改动文件：alert_engine.py）

### 现状

- `_portfolio_loop`（server.py）每轮将全部美股持仓最新价喂给
  `alert_engine.evaluate_round`（alert_engine.py:286）；长桥 live 推送
  入口（server.py on_events 钩子）同样收口到 `evaluate_round`。
- `_load_costs`（alert_engine.py:50）从 `db.get_holdings` 加载
  `symbol → cost_price`，60s 缓存，仅美股（market == "US"）。

### 设计

1. **数据结构**：`_costs: dict[str, float]` 改为
   `_positions: dict[str, dict]`，值为 `{"cost": float, "qty": float}`。
   `_load_costs` 更名为 `_load_positions`，一次加载成本+数量，60s 缓存
   节奏不变。文件内引用点（eval_price_plan / 另一规则取成本处 /
   evaluate_round）同步改写为 `_positions.get(symbol, {}).get("cost")`。
2. **过滤逻辑**（evaluate_round 求值循环内）：

   ```
   threshold = float(await db.get_config("ALERT_MIN_MARKET_VALUE") or 500)
   ```

   - 阈值随同位置读取（与 ALERT_COOLDOWN_HOURS 同模式，每轮一次轻量
     DB 查询；读取异常时沿用默认 500 并记 warning）。
   - 求值循环内：`if threshold > 0 and price * qty < threshold: continue`
     —— 小仓位不评估、不提醒、不落库（用户确认：完全不触发）。
   - `ALERT_MIN_MARKET_VALUE` 设 0 或负值 = 不过滤（全部提醒）。
   - 市值口径：当前轮实时价 × 持仓数量（数量为 0 的退化持仓自然被过滤）。
3. **配置管理**：复用现有 `sys_config` 体系（`PUT /api/config/{key}` 或
   直接 SQL），前端无配置页，不新增 UI。

### 边界

- 港股/基金不在提醒引擎范围内（引擎只评估美股），过滤不影响其它市场。
- 长桥 push 入口传入的价格同样按市值过滤（同一函数收口，天然覆盖）。

## 二、外部 Agent 持仓接口（改动文件：server.py、新增 docs/api-agent-portfolio.md）

### 端点与鉴权

`GET /api/agent/portfolio`，请求头 `X-API-Key: <key>`：

| 情况 | 状态码 | 说明 |
|------|--------|------|
| sys_config `AGENT_API_KEY` 未配置/为空 | 404 | 视为禁用，不暴露端点存在性 |
| key 不匹配 | 401 | |
| key 匹配 | 200 | `hmac.compare_digest` 常量时间比较 |

### 数据来源与组装

- 与 `/api/portfolio/overview` 同源：读 `portfolio_cache`（零额外行情
  请求，不放大对外请求）。
- `cny` 与 overview 同逻辑：端点内用最新 market_cache USDCNH +
  `_cny_cfg` 实时重算（复用 build_cny_summary），不受 600s 慢轮快照拖累。
- `fx.usdcnh` 取 market_cache 中 key == "USDCNH" 的最新价；缺失为 null。

### 返回结构（agent 友好精简，无前端专用噪音字段）

```json
{
  "ok": true,
  "generated_at": "2026-10-05 09:30:12",
  "session": "regular",
  "trade_date": "2026-10-02",
  "summary": {
    "total_value": 12345.67,
    "holdings_value": 11000.00,
    "cash_usd": 1345.67,
    "cost_basis": 9800.00,
    "day_pnl": 120.5,
    "day_pnl_pct": 0.98,
    "close_pnl": 80.0,
    "overnight_pnl": 40.5,
    "total_pnl": 2545.67,
    "total_pnl_pct": 26.0
  },
  "holdings": [
    {
      "symbol": "NVDA",
      "name": "英伟达",
      "market": "US",
      "quantity": 10.0,
      "cost_price": 90.0,
      "price": 105.0,
      "value": 1050.0,
      "today_pnl": 30.0,
      "pnl": 150.0,
      "cost_usd": 900.0,
      "stale": false,
      "ext": "regular"
    }
  ],
  "cash": [{"currency": "USD", "amount": 1000.0}],
  "fx": {"usdhkd": 7.8, "usdcnh": 7.12},
  "cny": {"invested": 84850.0, "invested_gross": 84850.0, "expense": 0.0,
          "value": 87912.0, "pnl": 3062.0, "pnl_pct": 3.6, "rate": 7.12}
}
```

字段口径：
- `summary.total_value` 含现金（= holdings_value + cash_usd）；
- `day_pnl = close_pnl + overnight_pnl`（一场口径恒等式）；
- `holdings[].value/cost_usd/pnl` 均为 USD 口径（港股/基金按 USDHKD 折算）；
- `holdings[].stale=true` 表示该持仓行情缺失，value 为 0、pnl 为 null；
- `cny` 为人民币累计口径（净投入 = invested_gross − expense），任一输入
  缺失时为 null。

### 接口文档

新增 `docs/api-agent-portfolio.md`：鉴权说明、curl 示例、完整字段表、
口径说明（一场恒等式/含现金口径/汇率折算）、`AGENT_API_KEY` 与
`ALERT_MIN_MARKET_VALUE` 配置方法。

## 三、测试

- 提醒过滤：新增/扩展 `tests/test_alert_engine.py`——阈值过滤正常/0
  （不过滤）/配置缺失（默认 500）/恰好等于阈值（≥ 触发，保留）。
- Agent 接口：扩展 `tests/test_server_auth.py`——未配置 key 404 / 错 key
  401 / 对 key 200 且响应结构完整（summary/holdings/cash/fx/cny 键存在）。
- 基线：`python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py
  tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py`
  全绿（当前 238 passed 基线不回退）。

## 四、部署与配置

1. rsync 发布 + `systemctl restart qdii`（按 AGENTS.md 部署命令）。
2. 线上 MySQL 写入配置：
   - `sys_config.AGENT_API_KEY` = 随机生成的 32 位十六进制串；
   - `sys_config.ALERT_MIN_MARKET_VALUE` = `500`（预热开关，后续可调）。
3. 线上验证：无 key curl 404 / 错 key 401 / 对 key 200 结构完整；
   `journalctl -u qdii` 无新增异常。

## 五、明确不做（YAGNI）

- 不做提醒分级/白名单等多套关注定义（阈值口径先行，配置键可扩展）。
- 不改现有 `/api/portfolio/overview` 的鉴权与结构。
- 不做 API Key 的多 key/轮换/有效期管理（单静态 key，换 key 即改库）。
- 不处理本次日志排查发现的 twelvedata 429 / 新浪 403 问题（另行反馈决策）。
