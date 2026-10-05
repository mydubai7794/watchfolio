# 持仓买卖点提醒引擎 + 长桥推送 一体化设计

- 日期：2026-10-04（v2 修订：范围收缩至 13 只美股、日K 增量落库 + K 线渲染、
  除权方案、按历史回测实证标记最优策略）
- 状态：待用户审阅
- 范围：仅 13 只美股持仓（QQQ/QQQM/SMH/NVDA/AMD/MU/BABA/LITE/AMKR/PCG/
  F/SKHY/DRAM）；港股 03152 与场外基金邓普顿不参与本功能（行情链路维持现状）

## 1. 背景与目标

用户核心痛点：把握不好买卖点，需要可提高胜率的系统化提醒。本设计交付：

1. **提醒引擎**（一期）：四类规则（价位档位 / ATR 移动止损 / 趋势过滤 / 防追高），
   轮询驱动即可触发，网页可视化管理 + 网页提醒中心 + QQ 邮件触达；
2. **长桥推送接入**（二期）：免费 LV1 订阅推送（13 只美股盘前/盘中/盘后/
   夜盘全时段），替代高频轮询成为长桥时段主源；24h数据源 + 股票行情网关
   （***REMOVED***）降级为周末/假日/断流兜底（周末估算行情价值保留）；
3. **实时性升级**：复用现有 SSE 基础设施（`/api/stream` + `_broadcast`，server.py:281-509），
   持仓页接入实时报价与提醒推送；
4. **MySQL 保护**：push 高频 tick 不逐条落库，内存真相源 + 1s 降采样攒批。

胜率方法论定位：提醒 = 把决策前置成规则，系统只负责到点喊人；规则先回测验证
再上线，拍脑袋规则不过关不上线。

## 2. 关键结论（需求澄清阶段已确认）

| 问题 | 结论 |
|------|------|
| 长桥免费档够不够 | 够。美股 LV1（含夜盘）+ 港股 LV1 + 指数均免费；OPRA/LV2 用不到（踩坑记录 §2.4 实测） |
| 四类规则要新调 API 吗 | 行情侧零新数据源。日K 复用长桥 candlesticks REST（锚点恢复在用的端点）；唯一新增外部调用 = 长桥 WS push + QQ SMTP |
| MySQL 撑得住吗 | 不逐条落库就撑得住。三层架构（内存 → 降采样 → 攒批），日增 1-3 万行 |
| 前端 SSE | 可行且零新依赖，基础设施已存在，A股监控页已在用 |
| 提醒渠道 | 网页提醒中心 + QQ SMTP 邮件 |
| 规则配置 | 网页可视化管理（持仓页每票「策略」入口） |
| 监控范围 | 仅 13 只美股持仓（港股/场外基金不参与） |
| 胜率验证 | 已回测实证（附件 2026-10-04-backtest-report.md）：主打 atr_stop（+3.13%/笔）、骨架 MA200（回撤减半）、辅助超卖侧（66.9%）、放弃追高侧 |

## 3. 总体架构

```
数据入口
├─ 长桥 REST（已有端点）：candlesticks → daily_bars 表（每日刷新）
│    └→ 指标底座：ATR(22) / MA50 / MA200 / RSI(14) / 22日最高 —— 每日预算缓存
├─ 长桥 push WS（新）：13 只美股全时段 quote/depth → 回调
├─ 24h数据源 bookTicker + 股票行情网关（已有）：周末/假日/长桥断流兜底
├─ 腾讯总线 + FT NAV + twelvedata（已有）：A股监控/港股/场外基金，不动
      ↓ 统一更新 quotes_live（内存，唯一实时真相源，新全局组件）
      ├→ alert_engine.evaluate(symbol, price, ts)  每价一评（引擎不感知数据来源）
      │     ├→ alerts 表 → SSE `alert` 事件 → 提醒中心（铃铛未读）
      │     └→ 邮件队列 → QQ SMTP（聚合节流）
      ├→ 降采样聚合（1s 窗口取末值，价变才写）→ market_ticks 攒批 flush
      └→ SSE `quote` 事件（1s 节流批量快照）→ 前端持仓实时刷新
```

现有不动：A股 ETF 4 步决策模型、腾讯总线、FT NAV、记账、session 状态机
（`update_session_state` 先于任何价格覆盖的铁律延续到 push 回调路径）。

## 4. 模块设计

### 4.1 数据底座：daily_bars + 指标缓存（新文件 daily_bars.py）

- 表：`daily_bars(symbol, date, open, high, low, close, volume)`，
  UNIQUE(symbol, date)，INSERT IGNORE 幂等——**增量落库，库里已有的日子
  不重拉**（用户明确要求的语义：不存在才落库，每日只补昨日缺口）。
- 复权方案：长桥 candlesticks `adjust_type=ForwardAdjust` 前复权（回测已实测
  该参数可用），指标与收益口径天然连续；实证 F 有 16 次复权事件、
  NVDA/SMH/BABA/AMKR 亦有，前复权是硬需求。
- 增量刷新任务：每日一次（北京时间 07:00，美股收盘数据已定型），每 symbol
  先查库内 `MAX(date)`，只拉缺口区间（首建拉 1000 根 ≈ 4 年），**逐只间隔
  ≥3s**（限流契约：1s≤10 次、单账号长连接）。
- 除权检测：增量刷新时若重叠区间历史 close 与库内不一致（复权因子跳变），
  判定发生除权/拆股 → **整段重拉该 symbol 覆盖旧数据**，并发 info 提醒
  「XX 发生除权除息/拆股，请核对成本价与档位设置」（成本价是真实成交价，
  复权后需人工确认是否同步调整）。
- 符号覆盖探测：启动时核对返回集合（长桥静默空陷阱，踩坑记录 §2.5）；
  无数据标的记入 `DAILY_BARS_GAPS`（sys_config），指标类规则自动禁用并
  网页标注。
- 历史不足降级：SKHY（60 根）/DRAM（127 根）上市不足 200 日，MA200/ATR22
  不可用——自动只启用 price_plan（RSI14 可用时一并启用），网页标注
  「历史不足，趋势/止损规则暂不可用」，日K 积累够后自动放开。
- 指标缓存：内存 dict，每日刷新后重算——ATR(22)、MA50、MA200、RSI(14)、
  22 日最高价；实时求值只做比对，零运行时计算成本。

### 4.2 提醒引擎（新文件 alert_engine.py）

统一接口：`async evaluate(symbol, price, ts) -> list[AlertEvent]`。
一期由持仓轮询循环每轮调用（60s/600s 节奏）；二期由 push 回调直接调用。
四类规则可插拔（rule_type → handler 注册表），**权重与默认参数按 2026-10-04
历史回测实证结论设定**（详见 §4.8 与附件报告）：

| 权重 | 规则 | 定义 | 默认参数 | 实证依据（近 3 年 · 13 只） |
|------|------|------|---------|---------------------------|
| ★主打 | atr_stop 移动止损 | Chandelier：止损位 = 22日最高 − k×ATR(22)，随日K每日只升不降；实时价跌破触发 critical 提醒 | k=3（可调 2-4） | 胜率 43.4% 但**单笔期望 +3.13%**、均持 18 日（76 笔），唯一「亏小赚大」的正期望出场系统 |
| ★骨架 | trend_filter 趋势过滤 | 日收盘 vs MA200：上方=允许持仓环境、下穿=减仓环境提醒；**定位是仓位总开关而非买点信号** | MA200 | 最大回撤 44.1%→27.2% **减半**（95 买点）；5日胜率 56.8% |
| ★辅助 | extreme_guard 超卖加仓 | **仅超卖侧**：RSI(14)≤30 或对 MA50 乖离 ≤−8% → 加仓提醒（info） | RSI≤30 / 乖离 −8% | 超卖侧 5 日胜率 **66.9%**（166 次）；追高侧 57.4%≈随机，**按实证放弃**（不设默认规则） |
| 纪律 | price_plan 价位档位 | 成本价锚定档位，逐档触发一次 | **+8%/-5% 主力档**；-10%/+15% 低频延伸档 | +8% 档 60 日触及率 76.3%、-5% 65.5%；-10%（44.5%）空转=回撤保护；+15%（55.0%）仅强趋势可及 |

- 判定节奏：price_plan / atr_stop / extreme_guard 用实时价即时判定；
  trend_filter 仅用日收盘判定（防盘中假穿越），盘中只展示距离不触发。
- **判定状态落库（用户明确要求）**：`alert_rules.fired_state` JSON 持久化
  已触发档位、冷却到期时间、规则方向——重启不丢、可审计、可回放；每次
  触发同步写 `alerts` 表永久留痕。
- 冷却：同 symbol+rule+方向触发后冷却 4h（可配）；价格离开触发带 ±0.5%
  后冷却重置（可再次触发）。
- 规则配置存储：`alert_rules` 表每 symbol 一行，`params` JSON 存整票策略包
  `{price_plan: {...}, atr_stop: {...}, trend_filter: {...}, extreme_guard: {...}}`，
  全局默认值 + 单票覆盖。
- SKHY/DRAM 自动降级：历史不足只开 price_plan（§4.1），日K 够 200 根自动放开。
- 提醒级别：`critical`（atr_stop 触发=建议卖出/止损）立即发邮件；
  `info`（其余）仅 5 分钟聚合邮件 + 网页。

### 4.3 邮件通道（新文件 mailer.py）

- QQ SMTP（smtp.qq.com:465 SSL），发件人=收件人=本人 QQ 邮箱；
  授权码存 sys_config（`MAIL_ADDRESS` / `MAIL_AUTH_CODE`），
  **只经服务器管道写入，不进代码库/聊天窗口**（沿用 token 流转纪律）。
- 异步队列：引擎触发 → 队列 → 后台协程发送；发送失败 5min 重试，连续失败
  降频到每小时，仅记日志不阻塞。
- 聚合节流：info 级 5 分钟窗口内同标的合并一封；critical 立即单发。
- 模板字段：标的、规则名、当前价、触发条件（如止损位）、建议动作、时间。

### 4.4 长桥推送（新文件 longport_push.py）

- SDK：longbridge（优先）Config，`quote_ws_url` 已钉 `openapi-quote.longbridge.cn`
  （踩坑记录 §2.1）；token 复用现有 `maybe_refresh_token` 自动续期链路。
- 连接：单账号一条 WS 长连接，独立后台 task（`_lp_push_task`），指数退避重连
  （1s/2s/4s... 上限 60s），断连期间该标的自动回退轮询源。
- 订阅：仅 13 只美股 quote + depth（LV1 一档），`.US` 后缀铁律；看板指数
  （DJI/NDX/SPX/VIX.US）维持现有长桥 REST 主价节奏，不进 push（本功能范围外）。
- 时段分工路由（写入 quotes_live 时打 source 标）：

| 时段 | 主源 | 兜底 |
|------|------|------|
| 美股 盘前/盘中/盘后/夜盘 | 长桥 push | 断流→现有腾讯/24h数据源轮询路径 |
| 周末/假日 | 24h数据源 bookTicker + 股票行情网关逐只（现状逻辑，周末估算行情价值保留） | — |
| 长桥持续断流 | 自动切换回轮询（探测循环打标，复用 _h24_probe 思路） | — |

- **shadow 模式**：上线第一周只记日志不接管主源（journalctl 观测免费档夜盘
  实际推送频率/覆盖率），数据达标后 sys_config `LP_PUSH_MODE=live` 切换。
- 顺序契约：push 回调写入前先过 `update_session_state`（同现有铁律），
  防收盘定格价被覆盖。

### 4.5 内存真相源 + 降采样落库

- `quotes_live`：全局 dict {symbol: {price, bid, ask, ts, source}}，替代散落的
  局部缓存；所有数据入口（push/轮询/24h源）统一写入；API 读它不查库。
- 原始 tick 内存 ring buffer：每 symbol 最近 30 分钟（调试与排障用，不落库）。
- 降采样：per-symbol 1s 窗口取末值，价变才写；攒批队列每 3s `executemany`
  INSERT IGNORE → market_ticks（表结构不变，uk_sym_ts 天然幂等）。
- 量级估算：13 标的 × 1s × 价变才写 ≈ 日增 1-3 万行（现 ~5000 行/天，
  可控）；spark 重建逻辑不动。
- 轮询路径（一期）沿用现有「价变即写」，无需降采样。

### 4.6 SSE 与前端

- 复用 `/api/stream` + `_broadcast`（server.py:288），新增事件：
  - `event: quote`：1s 节流批量快照 `{items: [{symbol, price, bid, ask, ts, source}]}`，
    由 quotes_live 快照任务广播；一期由轮询循环每轮广播（60s），
    二期 push 到达即入快照（1s 合并）。
  - `event: alert`：提醒产生即推 `{id, symbol, rule, level, title, body}`。
- 持仓页 portfolio.html 接入 EventSource（抄 index.html:313 模式）：
  收到 quote 前端本地重算市值/盈亏；连接失败 3 次自动回退现有轮询。
- 规则可视化管理：持仓页每票「策略」入口 → 弹层编辑
  （档位表增删、ATR 开关+倍数、趋势开关、防追高开关+阈值）；保存至
  alert_rules；显示当前止损位/均线距离/RSI 值（来自指标缓存快照接口）。
- 提醒中心：顶栏铃铛（未读数徽标）+ 下拉最近提醒 + 全部历史页；
  点击标记已读（POST /api/alerts/{id}/read）。
- **持仓明细 K 线弹层**（用户新增需求）：点击持仓行标的 → 弹层渲染日K
  （daily_bars 前复权，canvas 自绘、复用现有高分屏适配/chart-tip/hover 轮子，
  零外部库——与前端现状一致）：
  - 主图：蜡烛 + MA50/MA200 叠加 + 标记线（成本价、-5%/-10%/+8%/+15%
    档位、Chandelier 止损挂顶线、22 日最高）；
  - 副图：RSI(14) 曲线 + 30/70 参考线；
  - 当日段：叠加 market_ticks 分时（盘中实时延伸最新一根）；
  - 数据：`GET /api/kline/{symbol}?range=`（daily_bars + 指标缓存 + 规则
    标记线一次下发，前端零计算）；SKHY/DRAM 历史不足时无 MA200/止损线，
    弹层标注原因。

### 4.7 回测（新文件 alert_backtest.py）

- `POST /api/backtest/run`：{symbols, rule_type, params, start, end} →
  {triggers: [...], after_5d/10d/20d 收益分布, 触发频次统计}。
- 数据：daily_bars（日K）为主；price_plan 也可用 market_ticks 分时回放。
- 先无 UI，API 返回 JSON（日志/表格展示二期可选）。

### 4.8 胜率实证依据（2026-10-04 回测 · 附件 2026-10-04-backtest-report.md）

13 只美股 × 长桥前复权日K（11 只 1000 根 ≈ 4 年；SKHY 60 根、DRAM 127 根
新上市）× 近 3 年窗口全量回测，四类规则结论（按信号次数加权）：

| 规则 | 核心数字 | 定位结论 |
|------|----------|----------|
| atr_stop | 胜率 43.4% / **单笔期望 +3.13%** / 均持 18 日 / 假止损率 38.2%（76 笔） | ★主打：低胜率高期望，唯一正期望出场系统 |
| trend_filter | 5日胜率 56.8% / **最大回撤 44.1%→27.2%**（95 买点） | ★骨架：价值在控回撤而非买点 |
| extreme_guard 超卖侧 | **5日胜率 66.9%**（166 次）；乖离≤−8% 同向印证 | ★辅助：胜率最高但信号稀疏，只作加仓提示 |
| extreme_guard 追高侧 | 57.4%（951 次）≈ 随机 | **放弃**：实证无优势 |
| price_plan 档位 | +8% 档触及率 76.3%、-5% 65.5%、-10% 44.5%、+15% 55.0% | +8%/-5% 主力档；-10% 空转=回撤保护 |

核心含义：**高胜率规则赚小钱、低胜率规则赚大钱——规则组合以期望与
可执行性为准，而非裸胜率**。上线后每季度用 daily_bars 增量数据复跑回测
（§4.7 API），监控参数漂移与失效场景（横盘震荡市假止损集中、强趋势股
RSI 钝化等，详见附件 §6.3）。

## 5. 数据库变更

```sql
CREATE TABLE IF NOT EXISTS daily_bars (
  symbol varchar(20) NOT NULL,
  date   date NOT NULL,
  open   decimal(18,6) NOT NULL,
  high   decimal(18,6) NOT NULL,
  low    decimal(18,6) NOT NULL,
  close  decimal(18,6) NOT NULL,
  volume bigint DEFAULT 0,
  UNIQUE KEY uk_sym_date (symbol, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alert_rules (
  symbol     varchar(20) NOT NULL PRIMARY KEY,
  params     json NOT NULL,
  fired_state json NOT NULL,
  enabled    tinyint(1) DEFAULT 1,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alerts (
  id      bigint AUTO_INCREMENT PRIMARY KEY,
  symbol  varchar(20) NOT NULL,
  rule    varchar(20) NOT NULL,
  level   varchar(10) NOT NULL DEFAULT 'info',
  title   varchar(120) NOT NULL,
  body    json NOT NULL,
  triggered_at datetime NOT NULL,
  is_read tinyint(1) DEFAULT 0,
  KEY idx_triggered (triggered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

sys_config 新键：`MAIL_ADDRESS`、`MAIL_AUTH_CODE`、`LP_PUSH_MODE`
（shadow/live）、`ALERT_COOLDOWN_HOURS`（默认 4）、`DAILY_BARS_GAPS`
（长桥无日K符号清单，探测结果落库供网页标注）。

## 6. API 变更清单

| 方法/路径 | 说明 |
|-----------|------|
| GET /api/alert/rules | 全部规则配置（含默认值合成） |
| PUT /api/alert/rules/{symbol} | 保存单票策略包 |
| GET /api/alerts?unread=1&limit= | 提醒历史 |
| POST /api/alerts/{id}/read | 标记已读 |
| POST /api/alerts/read_all | 全部已读 |
| GET /api/indicators/{symbol} | 指标快照（ATR/MA/RSI/止损位） |
| GET /api/kline/{symbol}?range= | K 线弹层数据（日K+指标+标记线一次下发） |
| POST /api/backtest/run | 规则回测 |
| GET /api/stream（已有） | 新增 quote / alert 事件类型 |

以上全部走现有 session cookie 鉴权。

## 7. 错误处理与降级

| 故障 | 行为 |
|------|------|
| 长桥 push 断连 | 指数退避重连；期间标的回退轮询源；恢复后自动接管 |
| 长桥某符号静默空 | 启动探测发现即剔除订阅列表，走现有兜底源，网页标注 |
| daily_bars 拉取失败 | 次日重试不阻塞；指标缓存保留旧值并标注数据日期 |
| SMTP 发送失败 | 5min 重试→每小时降频；提醒不丢（alerts 表为准） |
| SSE 客户端异常 | 现有 is_disconnected 清理机制（不动） |
| MySQL 攒批失败 | 队列滞留上限 1 万条丢弃最旧并告警日志（内存优先保服务） |

## 8. 命名与合规纪律

- 数据源在代码/日志/文案中只用系统内名称：长桥 / 24h数据源 / 股票行情网关 /
  腾讯 / FT / twelvedata；标识符 h24 前缀沿用。
- URL 域名属接口契约原样保留，不出现在面向用户的描述性输出。
- 新代码 `@author yys`，无 TODO/伪代码。

## 9. 测试计划

- 单测（pytest，本地跑不起服务）：
  - 四类规则求值（构造价格序列：触发/不触发/边界）
  - 冷却状态机（触发→冷却→离开触发带→重置）
  - 降采样聚合（1s 窗口末值、价变才写、攒批）
  - 时段分工路由（shadow/live、周末兜底切换）
  - 指标计算（ATR/RSI/MA 对已知序列校验）
  - 邮件聚合节流（窗口合并、critical 立即）
- 基线：现有 238 passed 不破。
- 线上验证：shadow 周 journalctl 观测 + 手工构造价格穿越（测试 symbol）
  验证 SSE alert 与邮件到达。

## 10. 实施顺序

一期（提醒引擎，轮询驱动）：daily_bars → 指标缓存 → alert_engine →
alerts/alert_rules 表与 API → mailer → SSE alert 事件 → 前端策略管理+提醒中心
+ K 线弹层（/api/kline）→ 回测 API → 单测。

二期（推送与实时性）：longport_push（shadow）→ quotes_live + 降采样落库 →
SSE quote 事件 → 前端持仓实时刷新 → shadow 一周观测 → live 切换。

## 11. 验收标准

1. 规则回测在真实历史上触发合理（无漏触发/疯狂重复触发）；
2. 单测全绿 + 238 基线不破；
3. 模拟穿越触发：网页铃铛收到 + SSE alert 事件 + 邮件到达（critical 即时、
   info 聚合）；
4. shadow 一周：push 覆盖 13/13 持仓美股、夜盘有推送记录、无静默缺数；
5. live 后 market_ticks 日增量 < 5 万行，服务无内存增长异常；
6. K 线弹层正确渲染前复权日K + 全部标记线；人为构造除权检测用例
   （改库内历史 close）触发整段重拉与核对提醒。
