# 已清仓持仓与卖点标记设计

- 日期：2026-10-11
- 状态：已确认（brainstorming 产出）
- 范围：卖出清仓后标的不再混入主表、新增「已清仓」分区（含已实现盈亏/卖出后
  涨跌幅）、K线弹层标记买卖点并列出每笔至今涨跌幅

## 1. 背景与问题

现状：`apply_trade` 清仓（数量减到 0）保留 holdings 行（email_trader.py:491
注释「减到 0 保留行」），行内 qty=0、enabled 照旧。但这些零值行以
「市值 $0 / 盈亏 $0」的形态直接混进持仓明细主表，无清仓语义、无卖出后视角；
trade_records 全量买卖记录仅在 overview 里带 `recent_trades(10)` 备而不用
（server.py:2554，前端不展示）。

用户需求（参考券商清仓列表/交易明细的实现）：

1. 清仓标的不清理，但要从主表沉出去，进独立「已清仓」分区；
2. 详情标记卖点：K线弹层在日K上画买卖点标记，并列出每笔成交至今涨跌幅；
3. 已清仓行要看「卖出后涨跌幅」（清仓以来价格相对清仓均价的走势，即
   「卖飞/卖对了」视角）与已实现盈亏。

## 2. 关键决定（brainstorming 确认记录）

| 问题 | 决定 |
|------|------|
| 已清仓呈现位置 | 主表下方独立「已清仓」分区，默认折叠（富途/长桥风格），主表只留 qty>0 |
| 详情形态 | 扩展现有 K线弹层（B▲/S▼ 标记 + 成交列表），不新建页面 |
| 成交列表口径 | 买卖都列（每笔至今涨跌幅：BUY=(现价−买价)/买价，SELL 同理） |
| 已实现盈亏 | 要显示，按 trade_records 推导；期初持仓用当时 holdings 成本价近似 |
| 后端架构 | 纯计算推导不落新表（60s 缓存），单一事实源，无回填脚本 |

## 3. 口径与算法

### 3.1 数据源

- `trade_records` 全量，按 `COALESCE(email_date, created_at), id` 升序回放；
- `holdings` 全量（不限 enabled），提供 symbol 的当前数量锚点、名称/市场、
  期初成本近似。

### 3.2 锚点反推期初仓位

```
q0 = holdings.quantity − Σ(BUY 记录量) + Σ(SELL 记录量)   # 每 symbol
```

- q0 = 0：email 记录完备，回放即完整轨迹；
- q0 > 0：email 记录前已有手工录入持仓，**期初成本 = q0 × 当前
  holdings.cost_price（近似值）**，该周期标记 `approx: true`；
- q0 < 0（手工改数量/删行导致轨迹失真）：钳制 q0=0，标记 `approx: true`，
  不中断计算；
- holdings 行已删除（手动清理过的历史 symbol）：按 q0=0 回放，周期照样成立。

### 3.3 周期切分（round trip）

从 q0 起逐笔回放数量：BUY 加、SELL 减；**每次数量归零 → 一个清仓周期闭合**。
周期字段：

| 字段 | 定义 |
|------|------|
| exit_date | 归零那笔 SELL 的 email_date |
| hold_days | 周期首笔成交日 → exit_date 的自然日数（期初周期无首笔日则用首条记录日，approx） |
| exit_avg | Σ卖额 / Σ卖量（周期内加权清仓均价） |
| realized_pnl | Σ卖额 − Σ买额 − 期初成本（仅首周期且 q0>0 时含期初成本） |
| realized_pct | realized_pnl / (Σ买额 + 期初成本) × 100 |
| approx | 期初近似/锚点钳制标记（前端灰字提示「成本近似」） |

- 同一 symbol 多次再入场 → 多个周期，全部输出；
- 数量未归零（部分减仓后仍持有）→ 无新周期，卖点只进 K线弹层；
- 回放中数量出现负值（记录乱序等）：钳制归零闭合周期，`approx: true`。

### 3.4 卖出后涨跌幅

- 已清仓分区行：`(现价 − exit_avg) / exit_avg × 100`；
- **现价不后端下发**，前端从 positions 数据按 symbol join（qty=0 行本就随
  腾讯批总线带行情；HKFUND 行 price 即 NAV；行被 enabled=0 或缺失时显示
  `--`）；
- K线弹层成交列表每笔至今涨跌幅：`(现价 − 该笔价) / 该笔价 × 100`，现价
  同样取 positions join，缺失退化最近一根日K收盘价。

## 4. 后端设计

### 4.1 ledger.py 新增纯函数

`build_closed_positions(trades: list[dict], holdings: list[dict]) -> list[dict]`

- 无 IO、无外部请求，输入输出全部显式（与 build_report / build_nav_series
  同风格，可独立单测）；
- 输出周期行列表 + 「无成交记录」占位行（holdings 中 qty==0 但无任何周期的
  symbol：exit/盈亏字段全 None，前端显示「无成交记录」标签）；
- 排序：exit_date 倒序，占位行沉底。

### 4.2 database.py 新增

- `get_all_trades() -> list[dict]`：trade_records 全量，
  `ORDER BY COALESCE(email_date, created_at), id`；
- `get_trades_by_symbol(symbol) -> list[dict]`：同序单 symbol
  （kline 载荷用）。

### 4.3 server.py

- `_closed_ctx_cache`（60s，模式仿 `_nav_ctx_cache` server.py:2609）：缓存
  全量 trades + holdings，重放在纯函数内做（微秒级）；
- `GET /api/portfolio/overview` 注入 `closed` 块（与 `recent_trades` 同一
  位置、同一异常吞没降级模式，server.py:2554）；
- **closed 块不进 SSE 推送载荷**：`_live_overview_payload` 只复制
  portfolio_cache（server.py:422），closed 仅端点注入——留痕数据秒级推送无
  意义，前端沿用 recent_trades 的保活合并模式（portfolio.html:2668）；
- `GET /api/kline/{symbol}` payload 新增 `trades` 数组
  （`[{date, side, price, quantity}]`，随现有 60s `(symbol,tf)` 缓存一并
  构造，server.py:2448）；
- 鉴权随现有端点（overview session cookie / kline `_auth_or_401`），成交价
  敏感度不高于已有 marks.cost，不新增暴露面。

### 4.4 请求规模（「少请求多数据」核对）

- **零新增外部请求**：清仓标的 enabled 保持 1，美股行情搭腾讯批总线顺带、
  港股/基金走既有通道；daily_bars 日更循环本就覆盖 enabled US 持仓
  （daily_bars.py:399），清仓标的历史 K 线持续入库，卖点标记有图可画；
- 新增内部开销：overview 每 60s 一次全量 trades+holdings 读取（两条 SQL，
  表量级千行内）+ 微秒级纯计算重放。

## 5. 前端设计（static/portfolio.html 单文件）

### 5.1 主表过滤与已清仓分区

- `renderPositions`：主表/移动端列表只渲染 `quantity > 0`；qty==0 行移交
  已清仓数据源；
- 持仓明细下方新增「已清仓」卡片：标题 + 数量徽标，默认折叠，点击展开；
- 表格列：名称/代码、清仓时间、持有天数、清仓均价、已实现盈亏($/%)、
  现价、卖出后涨跌幅、操作（「K线」入口，复用 `openKline`）；移动端卡片
  同构；
- 现价/涨跌幅：从 lastOverview.positions 建 symbol→price 映射 join，缺失
  显示 `--`；`approx` 行在盈亏旁加灰字「成本近似」；
- 无成交记录占位行：清仓时间/均价/盈亏列 `--` + 「无成交记录」标签；
- SSE 载荷到达时 positions 现价实时刷新（分区行现价/卖出后涨跌幅随之更新），
  `closed` 块缺失沿用 recent_trades 保活合并。

### 5.2 K线弹层扩展

- **标记**：日K（tf=day）蜡烛上按成交日画标记——BUY 在对应日期蜡烛下方
  红色 ▲（var(--up)，买=看涨，红涨绿跌站内配色）、SELL 在蜡烛上方绿色 ▼
  （var(--down)）；含方向字母 B/S，悬浮详情复用 klTip；
  日期匹配不到蜡烛（窗口外/停牌）只进列表不画标记；week/month/5m 不画标记；
- **成交列表**：弹层底部（klInfo 区上方）紧凑列表，每笔：方向徽标（买/卖）、
  日期、价格、数量、该笔至今涨跌幅（正红负绿，与站内涨跌配色一致）；
- 数据源：kline payload 新增 `trades` + positions join 现价（退化最近收盘价）。

## 6. 边界与降级

| 场景 | 行为 |
|------|------|
| 手动清零（无 SELL 记录） | 已清仓分区占位行「无成交记录」，无卖点可画 |
| 清仓标的 enabled=0 | 分区行保留，现价/卖出后涨跌幅 `--`；daily_bars 停更，旧K线标记仍画 |
| holdings 行被删除 | 周期按 q0=0 回放仍成立，历史清仓记录不丢 |
| 再入场 | 多周期多行；当前持仓周期不闭合，不进分区 |
| HK/HKFUND | 无 daily_bars，K线弹层维持现状（空图）；分区行正常（HKFUND 现价=NAV） |
| trade_records 空 | 分区仅剩 qty==0 占位行（若有）；K线无标记 |
| DB 异常 | overview closed 块降级 `[]`（吞异常同 recent_trades 模式） |

## 7. 测试

新增 `tests/test_closed_positions.py`（纯函数，无 DB）：

1. 单周期精确：buy 10@100 → sell 10@120：realized=200、exit_avg=120、
   hold_days 正确、approx=false；
2. 部分减仓后清仓：buy 10@100 → sell 4@110 → sell 6@120：realized=160、
   exit_avg=116；
3. 再入场两周期：归零后再买再卖 → 两个周期、exit_date 各自正确；
4. 期初锚点：holdings qty=5 无买入记录、cost=80 → sell 5@100：
   realized=100、approx=true；
5. 锚点负值钳制：records 净量 > holdings 数量 → q0=0、approx=true、不抛异常；
6. 未闭合周期：qty>0 → 不产生周期；
7. 占位行：qty==0 无记录 → 单条占位行字段全 None；
8. email_date NULL 排序回退 created_at。

API 层（仿 test_server_auth.py 的 fake db 模式）：

9. overview 响应含 closed 块且结构正确；DB 异常时降级 `[]` 不 500；
10. kline 响应含 trades 数组（monkeypatch get_trades_by_symbol）。

回归：现有基线 441 passed 不回归。

## 8. 非目标

- 不做港股/基金 K线与卖点标记（无 daily_bars 数据）；
- 不做分时（5m）图上的成交标记；
- 不新增独立页面/路由，不落新表，不做历史回填脚本；
- 不改 apply_trade 清仓保留行/摊薄成本的既有语义。
