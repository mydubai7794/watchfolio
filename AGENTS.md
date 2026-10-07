# qdii 项目开发规范（AGENTS.md）

券商风格持仓网站：美股 + 港股 + 基金 + 现金（USD 口径），域名 http://***REMOVED***。
含 A股 ETF 监控（4 步决策模型）。**调试一律在服务器上做（ssh + journalctl + curl），
禁止本地起服务**——双实例会让外部数据源请求翻倍触发风控。

## 核心原则（用户反复强调，优先级最高）

1. **少请求、多数据**：能批量不逐只（腾讯攒批总线 / 24h源 bookTicker symbols 批量），
   能缓存不重查（exchangeInfo 24h / spark 库内累积 / 锚点 60s 节流）；
2. **回显/折线全用数据库**：前端不直接消费外部行情，一切曲线从 market_ticks /
   portfolio_ticks 重建；请求失败用上一份落库数据，不放大请求；
3. **命名纪律**：该数据源在系统内统一称「24h数据源」（标识符用 h24/H24）。
   **严禁在代码、注释、文档、API 输出、提交信息等一切文本中出现任何外部交易所
   品牌名、同类产品名、相关敏感词汇等违禁词**；描述性文本只允许使用系统内数据源
   名称（腾讯/新浪/Yahoo/FT/twelvedata/长桥/24h数据源/股票行情网关）。
   URL 域名与线协议交易符号属于接口契约（改了数据源即断），允许原样保留，
   但**不得出现在任何面向用户的描述性输出里**。
4. **数据源输出描述约束**：所有面向用户的输出（API 响应、前端文案、日志、文档）
   描述数据来源时，只能使用上表/上文的系统内数据源名称，禁止复述外部接口的
   品牌名、产品名或营销称谓（本约束文本自身亦不得复述违禁词）。

## 模块结构

| 文件 | 职责 |
|------|------|
| server.py | FastAPI + 全部后台循环（portfolio/market/monitor/candidate/email/h24-probe/ledger-fx/macro） |
| macro_fetcher.py | 宏观四指标日更抓取解析（巴菲特/席勒CAPE/2s10s利差/恐慌贪婪，直连优先代理容错） |
| portfolio_fetcher.py | 持仓侧全部数据源抓取与解析（腾讯/新浪/Yahoo/FT/twelvedata/24h数据源） |
| ledger.py | 记账报表纯计算（点差/通道/盈亏恒等式 build_report + ECB 解析） |
| ledger_seed.py | v3 盘点 68 笔种子事件（幂等 upsert CLI，仅首次/修数跑） |
| fetcher.py | A股 ETF 监控抓取（腾讯 ETF + 新浪/东财期指智能路由） |
| tencent_bus.py | 腾讯行情攒批发送路由（微批合并：监控/候选/持仓/看板共用一个节奏） |
| email_trader.py | 券商成交邮件 → trade_records 幂等入库 → **自动应用持仓与现金**（汇丰「全部執行」/致富「待成交=0 终态」两类门控；apply_trade 先插表再动 holdings/cash，可安全重放；QQ IMAP，30min 双源同轮，任一源失败水位不推进按水位回溯）；fetch_za_notice_emails 众安通知（换汇/转出/派息/入金）仅供记账对账采样，**不进自动轮询** |
| database.py | MySQL 全部表结构与 CRUD（远程 ***REMOVED***:3914） |
| static/portfolio.html | 持仓前端（单文件，无构建） |

## 数据源矩阵（频次=正常态）

| 源 | 用途 | 网络 | 频次 |
|----|------|------|------|
| 腾讯 qt.gtimg.cn | A股ETF/美股/港股/看板混排（总线合并） | 直连 | 总线 3s 节流（监控 5s 驱动） |
| 新浪 | 期指/外汇/盘前盘后扩展价 | 直连 403→代理容错 | 随轮 |
| Yahoo v8 | 基金NAV兜底/美债VIX主价/USDHKD fx_spark | 代理优先→直连 | 三项主价 backfill 节奏（4h 冷却/5min 重试）+ fx_spark 低频 |
| FT GraphQL | 基金 NAV 主源 | 直连 | 60s |
| twelvedata | 美股兜底 + USDCNH(离岸) 主价 + 锚点恢复兜底 | 直连 | 8 credits/min，60s 节流 |
| 长桥 LongPort | 冷启动锚点恢复主源（日K收盘价）+ pre/post 扩展价 | 直连 | 扩展价随轮 30/60s；锚点后台逐只 4s |
| 24h数据源 | 收盘→开盘全段美股活价 | **直连可用**（公开行情镜像域名） | 闭市 60s（bookTicker 批量 weight 1） |
| 24h数据源·股票行情 | 无交易标的（AMKR/PCG/F）夜盘活价 | 直连（***REMOVED*** 网关，X-Auth-Token+X-H24-APIKEY 双鉴权同值） | 闭市 60s 逐只并发（weight 1/只）；日均 ~4.7K 次 ≈ 200K events/天套餐的 2.4% |
| HSBC/致富邮件 | 成交记录（自动应用持仓/现金，致富同发件人营销邮件靠解析门槛挡） | IMAP 直连 | 30min 双源同轮 |
| 众安银行邮件 | 换汇/转出/派息/入金通知 | IMAP 直连 | 按需（记账采样辅助，无自动轮询） |
| ECB frankfurter.dev | 记账汇率日线（CNY/HKD/USD） | 直连 | 每日 22:35（库空回填一次区间） |
| FRED | 宏观日更：巴菲特指标+美债2s10s利差（CSV API） | 直连（失败走前2代理） | 每日 22:40 macro 循环 |
| multpl | 宏观日更：席勒CAPE | 直连（失败走前2代理） | 同上 |
| CNN | 宏观日更：恐慌贪婪指数（失败回退VIX分位） | 直连（失败走前2代理） | 同上 |

## 关键口径契约（改动前必读）

- **一场口径三层**（恒等式）：`day_pnl(实时累计) = close_pnl(收盘定格) + overnight_pnl(夜盘增量)`；
  场基准 base=昨日收盘（regular 时段腾讯 f[4] 每轮刷新；2026-10-02 实证凌晨/盘前 f[4]
  均未切换、即上一场基准）、收盘定格 close=16:00 定格价（f[3] 收盘后至次日盘前实测
  全程保持收盘价；凌晨 closed<4 与盘前 pre 分支均用 f[3]，pre 仅缺失时填充防污染）。
  冷启动落在非 regular 时段 → 锚点恢复（base=prev 与 regular f[4] 同口径；
  base_restored 标记区分退化值，连续失败 3 次放弃，09:30 自愈）。主源长桥日K
  （单 symbol candlesticks，后台任务逐只、每只 4s 摊完，主循环触发 _anchor_task
  防重入）；长桥不可用回切 twelvedata 批量。
- **状态机顺序**：`update_session_state` 必须先于 24h覆盖（防 close_prices 被代理价污染）。
- **24h窗口** = `session != "regular"`（盘后+夜盘+盘前+周末/假日）；pre/post 真实扩展价优先，
  24h价兜底；closed 段直接覆盖。
- **同标的替代**：`H24_ALIAS = {"QQQM": "QQQ"}`，夜盘价 = 自身收盘定格 × 替代涨跌幅。
  24h源覆盖 10/14：MU/SKHY/DRAM/QQQ/QQM(推导)/BABA/LITE/AMD/SMH/NVDA 有；
  AMKR/PCG/F 无对应交易标的（F 为同名射频识别业务代号，严禁误配到其它同名符号），
  三只经股票行情端点（equity quote，单 symbol 逐只、双鉴权、取 bid/ask 中间价）
  补齐 → 14/14 全覆盖；接口失败时自然缺省回退新浪扩展价定格口径。
- **看板 spark**：market_ticks 统一 UTC；分时全由主价快照「价变即写」累积，
  不再单独请求分时接口（US30Y/US10Y/VIX 主价源 Yahoo，刷新沿用 backfill 节奏
  30min 断档/4h 冷却/5min 重试，当日 5m 分时顺带落库）；spark 取最近一个
  tick≥5 的「有效分时日」（杂点日自动回退上一完整时段）并以 [ts,price] 对
  下发（前端悬浮显示北京时间+价格）；组合 tick 写入条件
  `session != "closed" or overnight_used > 0`；写入口毛刺延迟确认（hold/discard/confirm）。
- **记账口径（ledger）**：总投入 84,850 CNY（全额口径，v3 勘误版）；恒等式
  `net_cny = 总资产×USDCNH − 总投入 = 增值 + 汇损 − 消耗`；fx 事件带双边金额 +
  ECB mid_rate（点差实时推导不落库）；balance 快照按「账户×币种」取最新对账
  cash_accounts；HKD→USD 静态 7.846。录入走 POST /api/ledger/events
  （ref_no 幂等），流程沉淀在 ~/.claude/skills/qdii-ledger-ingest。
- **代理分类**：proxy_pool.region（overseas/unknown），_h24_probe_loop 30min；
  24h源请求序：直连（_h24_direct_ok）→ 境外代理 → 通用池。
- **长桥限流契约**：官方 1s≤10 次、并发≤5、单账号一条长连接——调用侧必须主动分散：
  全局 `_LP_SEM=Semaphore(2)` + `_sdk_call` wait_for 15s（超时废弃 _ctx 并记失败退避）；
  fetch_anchor_quotes 顺序逐只；批量请求时主动分散到 60s 内（用户明确要求）。
- **长桥新平台契约（2026-10-04 迁移）**：SDK longbridge 优先/longport 兜底，Config 显式
  钉 openapi.longbridge.cn（新 app 仅新平台有效、旧 app 双平台通用）；指数符号必须
  .US 后缀（.SPX.US=标普，缺后缀静默空）；token 到期自动续期（maybe_refresh_token
  最后 7 天触发/6h 节流/写回 sys_config 热加载）；看板 VIX/DJI/NDX/SPX 四项指数长桥
  主价、Yahoo ^VIX/^DJI/^NDX/^GSPC 兜底。**401004 排查、换凭证、动
  longport_fetcher.py 前必读《长桥OpenAPI踩坑记录.md》**（平台矩阵/双 SDK 差异/
  token 转录截断事故/OPRA 权限）。
- **持仓轮慢轮假象**：`interval = 60s(夜盘有活价) else 600s`——24h活价失效的那轮切
  600s，fetch_time 十分钟不动 ≠ 卡死；cny（人民币累计）在 /api/portfolio/overview
  端点内用最新 market_cache USDCNH + sys_config(invested/expense) **实时重算**，
  不绑 600s 慢轮快照。
- **美元人民币口径**：页面值为 twelvedata **离岸** USD/CNH（7×24 交易，周末仍动，
  60s 快照低流动性时段跳动 ±20pips 正常）；与银行 App 在岸 USD/CNY 差数十 pips
  是在岸/离岸价差，非数据错误（勿当 bug 修）。

## 部署

```bash
rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' \
  --exclude='__pycache__' --exclude='akshare-code' --exclude='akshare-docs' \
  --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' --exclude='.pytest_cache' \
  --exclude='.idea' --exclude='uvicorn.log' --exclude='美股夜盘数据源研报.md' \
  ./ root@***REMOVED***:/opt/qdii/ && ssh root@***REMOVED*** "systemctl restart qdii"
```

线上验证：`systemctl is-active qdii` + `journalctl -u qdii --since '2 min ago'` +
带 session cookie curl API（token 从 qdii.sessions 表取，cookie 名 session_token）。
MySQL：`mysql -h127.0.0.1 -P3914 -udubai -p'***REMOVED***' qdii`。

诊断要点：服务端口 **8888**；`kill -USR1 <pid>` 转储全部后台 task 栈到 journalctl
（格式 `TASK <name> → file:line in func`），排查「卡死」先看它；sessions 表无
id/last_used_at 列，token 取法 `SELECT token FROM sessions WHERE expires > NOW()
ORDER BY created_at DESC LIMIT 1`。

## 测试

基线：`python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py
tests/test_server_auth.py tests/test_email_trader.py tests/test_longport.py
tests/test_alert_engine.py tests/test_alert_api.py tests/test_daily_bars.py
tests/test_longport_push.py tests/test_macro_fetcher.py`（当前 400 passed）。
tests/test_scorer.py 是存量坏（scorer.normalize 已删），不计入基线。
pytest 本地跑可以（不起服务）；**服务只能线上起一份**。

## 已知边界

- 免费代理池全是境内中转（9 个，仅 *REMOVED* 有境外出口），24h源靠镜像
  直连；直连失效时境外代理自动接管（探测循环打标）；
- Yahoo 常态 403 靠代理池 backfill，失败 5min 重试；
- 新浪 fx_susdhkd/DINIW 白天偶发 403（有 FX_FALLBACK）；
