# qdii 项目开发规范（AGENTS.md）

券商风格持仓网站：美股 + 港股 + 基金 + 现金（USD 口径），域名 http://***REMOVED***。
含 A股 ETF 监控（4 步决策模型）。**调试一律在服务器上做（ssh + journalctl + curl），
禁止本地起服务**——双实例会让外部数据源请求翻倍触发风控。

## 核心原则（用户反复强调，优先级最高）

1. **少请求、多数据**：能批量不逐只（腾讯攒批总线 / 加密源 bookTicker symbols 批量），
   能缓存不重查（exchangeInfo 24h / spark 库内累积 / 锚点 60s 节流）；
2. **回显/折线全用数据库**：前端不直接消费外部行情，一切曲线从 market_ticks /
   portfolio_ticks 重建；请求失败用上一份落库数据，不放大请求；
3. **命名纪律**：bStocks/币安/binance 统一称「加密市场数据源」（URL 域名除外）。

## 模块结构

| 文件 | 职责 |
|------|------|
| server.py | FastAPI + 全部后台循环（portfolio/market/monitor/candidate/email/crypto-probe） |
| portfolio_fetcher.py | 持仓侧全部数据源抓取与解析（腾讯/新浪/Yahoo/FT/twelvedata/加密源） |
| fetcher.py | A股 ETF 监控抓取（腾讯 ETF + 新浪/东财期指智能路由） |
| tencent_bus.py | 腾讯行情攒批发送路由（微批合并：监控/候选/持仓/看板共用一个节奏） |
| email_trader.py | HSBC 成交邮件解析 → trade_records 幂等入库（QQ IMAP，30min） |
| database.py | MySQL 全部表结构与 CRUD（远程 ***REMOVED***:3914） |
| static/portfolio.html | 持仓前端（单文件，无构建） |

## 数据源矩阵（频次=正常态）

| 源 | 用途 | 网络 | 频次 |
|----|------|------|------|
| 腾讯 qt.gtimg.cn | A股ETF/美股/港股/看板混排（总线合并） | 直连 | 总线 3s 节流（监控 5s 驱动） |
| 新浪 | 期指/外汇/盘前盘后扩展价 | 直连 403→代理容错 | 随轮 |
| Yahoo v8 | 基金NAV兜底/美债VIX/spark | 代理优先→直连 | 60s + backfill 冷却 4h/重试 5min |
| FT GraphQL | 基金 NAV 主源 | 直连 | 60s |
| twelvedata | 美股兜底 + 冷启动锚点恢复（open/close） | 直连 | 8 credits/min，60s 节流 |
| 加密市场数据源 | 收盘→开盘全段美股活价 | **直连可用**（data-api.binance.vision 镜像） | 闭市 60s（bookTicker 批量 weight 1） |
| HSBC 邮件 | 成交记录 | IMAP 直连 | 30min |

## 关键口径契约（改动前必读）

- **一场口径三层**（恒等式）：`day_pnl(实时累计) = close_pnl(收盘定格) + overnight_pnl(夜盘增量)`；
  场基准 base=当日 9:30 开盘（regular 时段腾讯 f[4] 每轮刷新）、收盘定格 close=16:00 定格价。
  冷启动落在非 regular 时段 → twelvedata open/close 锚点恢复（base_restored 标记区分退化值，
  连续失败 3 次放弃，09:30 自愈）。
- **状态机顺序**：`update_session_state` 必须先于加密覆盖（防 close_prices 被代理价污染）。
- **加密窗口** = `session != "regular"`（盘后+夜盘+盘前+周末/假日）；pre/post 真实扩展价优先，
  加密价兜底；closed 段直接覆盖。
- **同标的替代**：`CRYPTO_ALIAS = {"QQQM": "QQQ"}`，夜盘价 = 自身收盘定格 × 替代涨跌幅。
  加密源覆盖 10/14：MU/SKHY/DRAM/QQQ/QQM(推导)/BABA/LITE/AMD/SMH/NVDA 有；
  AMKR/PCG/F 无对应交易对（FUSDT 是 Findora 币，禁止误配）。
- **tick**：market_ticks 统一 UTC（HSI 腾讯分钟 -8h）；组合 tick 写入条件
  `session != "closed" or overnight_used > 0`；写入口毛刺延迟确认（hold/discard/confirm）。
- **代理分类**：proxy_pool.region（overseas/unknown），_crypto_probe_loop 30min；
  加密源请求序：直连（_crypto_direct_ok）→ 境外代理 → 通用池。

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

## 测试

基线：`python3 -m pytest tests/test_portfolio.py tests/test_decision_model.py
tests/test_server_auth.py tests/test_email_trader.py`（当前 185 passed）。
tests/test_scorer.py 是存量坏（scorer.normalize 已删），不计入基线。
pytest 本地跑可以（不起服务）；**服务只能线上起一份**。

## 已知边界

- 免费代理池全是境内中转（9 个，仅 *REMOVED* 有境外出口），加密源靠 vision
  镜像直连；直连失效时境外代理自动接管（探测循环打标）；
- Yahoo 常态 403 靠代理池 backfill，失败 5min 重试；
- 新浪 fx_susdhkd/DINIW 白天偶发 403（有 FX_FALLBACK）；
- subagent 通道不稳定（多次空返回），复杂调研直接主 Agent 做或用 codegraph。
