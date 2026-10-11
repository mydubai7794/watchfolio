# 决策上下文层（Agent Decision Context）设计

- 日期：2026-10-11
- 状态：已获批（对话定稿）
- 原则：站点只供**事实**不做判断；打分/建议留给外部模型；一份计算两处消费（agent 接口 + 页面面板）；以稳为主，日更数据间隔拉长

## 1. 背景与目标

对标 Rallies 类应用模式：其价值本质是「把结构化事实（RSI=62、距SPY +4.2%、HY OAS=3.18%、CEO 30日净买 141k 股）喂给通用模型推理」。本设计为 QDII 站点增加一层**决策上下文因子**，通过现有外部 Agent 接口 `GET /portfolio`（X-API-Key 鉴权）一次性下发，并在持仓页同源渲染「决策上下文」面板。

词频依据（用户对 50 条理由的统计）：SPY 对标 33、RSI 26、10Y 利率 20、市场广度 20、信用利差 19、公司 IR 数据 15、BEA/CPI 24。

## 2. 因子矩阵与请求成本

| 因子 | 实现 | 新增请求 |
|---|---|---|
| SPY 对标 | SPY 日K追加进 07:00 长桥刷新清单；个股 20日收益 − SPY 20日收益（纯计算） | +1/日 |
| RSI14 | 美股持仓已由日K循环算好（daily_bars.INDICATORS），纯暴露 | 0 |
| 10Y 利率 | US10Y 看板现价 + yield_2s10s 的 extra.dgs10（已抓取），纯暴露 | 0 |
| CPI YoY | FRED CPIAUCSL 尾部 14 行自算同比 | +1/日 |
| 信用利差 | FRED BAMLH0A0HYM2（高收益 OAS，pct） | +1/日 |
| 市场广度 | RSP(等权SP500)/SPY 比值 + 5/20 日趋势（日K纯计算） | +1/日 |
| 内部人交易 | SEC EDGAR Form 4（新模块 edgar_fetcher.py） | ~+12/日 |
| ~~结合比率~~ | **砍掉**（个股特定保险 KPI，与持仓无关） | — |

明确不做：通用财报指标挖掘、EDGAR 实时轮询（Form 4 法定 2 个工作日申报，每日一轮足够）、SPY/RSP 腾讯总线实时价（对标场景日K收盘粒度足够，砍掉实时链路改动）、PCG/F 排除名单（Agent 自己按仓位权重处理；asset_type='none' 预留人工通道但不主动使用）。

## 3. 架构分层

### 层1 · 零成本纯暴露
- RSI14：`daily_bars.get_indicators(symbol)["rsi14"]`（07:00 循环已覆盖全部 enabled 美股持仓）
- US10Y：market_cache key `US10Y`；DGS10：macro latest `yield_2s10s` 的 extra.dgs10
- VIX / 恐惧贪婪 / 巴菲特 / CAPE：`/api/macro` 已有，context 里引最新值

### 层2 · FRED 搭车（macro_fetcher.py 扩展）
- 新增 `fetch_cpi_yoy()`：`FRED_CSV_URL.format(sid="CPIAUCSL")`，需新解析函数 `parse_fred_csv_tail(text, n=14)` 返回尾部 [(date, value)] 列表（现有 parse_fred_csv 只取最新一行不够算同比）；YoY = (最新值/12个月前值 − 1) × 100，asof 取最新行日期；name=`cpi_yoy`，extra 带 `latest`、`year_ago`、`index_asof`
- 新增 `fetch_credit_spread()`：`FRED_CSV_URL.format(sid="BAMLH0A0HYM2")`，parse_fred_csv 现有函数即可；name=`credit_spread_hy`，value=pct
- 挂入 `FETCHERS` 元组，沿用 22:40 宏观循环（fetcher 间 2s 间隔、单项失败不中断、直连→代理容错——全部照抄现有模式）；FRED 不配浏览器 UA（现有注释：伪造 UA 会被 FRED 挂起）

### 层3 · 基准搭批（daily_bars.py 扩展）
- `daily_refresh_loop` 固定清单追加 `SPY`、`RSP`（VIX 同款追加法，daily_bars.py:404 附近）；`lp_symbol()` 自动加 .US 后缀
- 长桥限流契约照旧：串行、间隔 ≥3s，异常跳过记 gaps
- 广度 = RSP.close / SPY.close 比值及其 5/20 日变化（纯计算，读 `db.get_daily_bars`）

### 层4 · EDGAR 内部人（新模块 edgar_fetcher.py + edgar_daily_loop）

**端点**（全部官方免费）：
- ticker→CIK 映射：`https://www.sec.gov/files/company_tickers.json`（~600KB，格式 `{"0":{"cik_str":320193,"ticker":"AAPL","title":"..."},...}`）；模块级内存 dict 缓存，`sys_config.EDGAR_TICKER_MAP_TS` 记 7 天过期，过期/冷启动才重拉
- 公司申报索引：`https://data.sec.gov/submissions/CIK{cik:010d}.json`（CIK 10 位补零；recent 含 formType/filingDate/accessionNumber/primaryDocument 数组）
- Form 4 XML：`https://www.sec.gov/Archives/edgar/data/{cik}/{accession去连字符}/{primaryDocument}`

**User-Agent（合规且零敏感值入库）**：请求头 UA 从 `sys_config.EDGAR_UA` 读，代码缺省 `"QDII Portfolio Monitor"`；部署后在库配置含联系邮箱的完整 UA（真实域名属 AGENTS.local.md 敏感值，禁入代码）。

**限流契约（以稳为主）**：
- 模块级全局单例节流：任何请求距上一次 ≥2.0s（不足先 sleep），全程串行，实际峰值 ≤0.5 req/s（上限 10 req/s 的 1/20）
- HTTP 429/503：指数退避 2s→4s→8s，最多重试 2 次，仍失败该 URL 当轮放弃
- 404 / 解析失败：返回 None 不重试（数据缺，非故障）
- 网络异常：重试 1 次
- 单 symbol 失败跳过继续（macro 循环 gaps 哲学），整轮失败仅 WARNING 不轰炸

**每日循环 edgar_daily_loop**（server.py 注册，北京时间 23:30，错开 22:35 ledger-fx / 22:40 macro / 07:00 日K）：
1. ticker map 7 天过期才刷（1 请求）
2. 遍历 holdings（market=US、enabled=1）：
   - `asset_type IS NULL` → 探测：map 查 CIK（miss → 不写分类，记一次 WARNING）；拉 submissions recent.formType 集合，三类互斥、按优先级判定：含 `20-F` → `fpi`（BABA 自动收敛）；含 `10-K`/`10-Q` → `stock`；含 `N-1A`/`N-CSR`/`NPORT` → `etf`；三类都不含 → 保持 NULL 下轮再试；写回 holdings.asset_type
   - `asset_type='stock'` → 拉 submissions，筛 formType=="4" 且 filingDate > 水位 → 逐个拉 XML 解析入库；首轮（无水位）回看 90 天初始化
   - 其他（etf/fpi/none）→ 跳过，零请求
3. 水位：`sys_config.EDGAR_WATERMARK`（ISO 日期，本轮成功处理的最新 filingDate；探测失败不推进）
4. 8 只个股 × 每日 1 次 submissions + 新增 Form 4 若干 XML ≈ 10~15 请求/日，串行 2s 间隔整轮 <1 分钟

**Form 4 XML 解析**（xml.etree，纯函数 `parse_form4_xml(text) -> list[dict]`，单测 fixture 驱动）：
- reporter：`reportingOwner/reportingOwnerId/rptOwnerName`
- title：`reportingOwner/reportingOwnerRelationship/isOfficer` 下 officerTitle（isDirector=1 → "Director"）
- 每条 nonDerivative transaction 一行：shares=`transactionAmounts/transactionShares/value`、price=`transactionAmounts/transactionPricePerShare/value`、action=`transactionAmounts/transactionAcquiredDisposedCode/value`（A=买入/D=卖出）、tx_code=`transactionCoding/transactionCode`（P/S/G/A/F…原样保留，不过滤不过度解读，Agent 自判噪音）
- value_usd = shares × price（price 缺失时 None）
- trade_date=`transactionDate/value`、filed_date=`filingDate`（外层传入）

**聚合（纯函数 `build_insider_summary(rows, today)`，不碰 db）**：近 90 天（filed_date ≥ today−90）：`net_shares = ΣA.shares − ΣD.shares`、`net_value_usd` 同理、`buys`/`sells` = transaction 行数。

## 4. 数据库变更（database.py）

```sql
CREATE TABLE IF NOT EXISTS insider_tx (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(16) NOT NULL,
  cik VARCHAR(16) NOT NULL,
  accession VARCHAR(32) NOT NULL,
  filed_date DATE NOT NULL,
  trade_date DATE NULL,
  reporter VARCHAR(128) NOT NULL,
  title VARCHAR(128) DEFAULT '',
  action CHAR(1) NOT NULL COMMENT 'A=acquire D=dispose',
  tx_code VARCHAR(8) DEFAULT '',
  shares BIGINT NOT NULL,
  price DECIMAL(12,4) NULL,
  value_usd DECIMAL(16,2) NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tx (accession, reporter, action, shares, price),
  KEY idx_symbol_date (symbol, filed_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SEC Form 4 内部人交易(EDGAR官方源)';

ALTER TABLE holdings ADD COLUMN asset_type VARCHAR(8) NULL DEFAULT NULL
  COMMENT 'NULL未探测/stock个股/etf基金/fpi外国发行人无Form4/none人工排除';
```
- DDL 管理方式跟随 database.py 现有模式（CREATE TABLE IF NOT EXISTS 启动建表）；ALTER 用「列存在检测」幂等写法（参考现有加列迁移写法，若无先例则 `SHOW COLUMNS` 检测后 ALTER）
- CRUD：`save_insider_tx(rows)` 用 INSERT IGNORE（撞 uk_tx 幂等跳过）；`get_insider_tx(symbol, days=90)`；`set_holding_asset_type(symbol, asset_type)`；`get_holdings` 返回集带 asset_type
- macro_indicators 沿用零改动（upsert_macro_indicator 通用）

## 5. 出口层（server.py）

**纯读聚合函数 `_build_agent_context()`（60s `_agent_ctx_cache`，与 _nav_ctx_cache 同模式）**：
- spy：`db.get_daily_bars("SPY")` 最新 close 与前一日 chg_pct，asof=最新日期
- us10y：market_cache `US10Y` 现价 + macro `yield_2s10s` extra.dgs10（asof 对齐）
- credit_spread_hy / cpi_yoy：macro latest（缺则 null）
- breadth：RSP/SPY 日K比值 + chg_5d/chg_20d（任一 bars <21 根则趋势为 null）
- factors（每美股持仓）：`rsi14`（daily_bars.get_indicators）、`spy_rel_20d`（20日收益差，bars<21 为 null）、`insider_90d`（db 聚合；etf→null、fpi→`{"note":"fpi_no_form4"}`、无数据→null）
- `context_meta.edgar_ok`：上轮 EDGAR 循环是否成功（sys_config EDGAR_LAST_OK）

**注入**：`GET /portfolio`（agent_portfolio）与 `GET /api/portfolio/overview`（portfolio_overview）均加 `context` 键，同一函数产出；缺因子不影响主体字段（context 内逐项 null 降级）。

## 6. 前端（static/portfolio.html）

「决策上下文」面板独立区块（overview.context 同源渲染，风格沿用现有宏观/看板卡片）：
- 顶部因子卡行：SPY（价+日涨幅）、US10Y、CPI YoY、信用利差 HY OAS、市场广度（RSP/SPY 比值+5日趋势箭头）
- 持仓主表追加列：`RSI14`、`vs SPY(20d)`（pct 上色）、`内部人90d`（net_value_usd 缩写显示，null 显示 —，fpi 标 `FPI`，正净买入绿/负红）
- 不破坏现有主表结构，SSE 载荷不携带 context（面板随 overview 轮询刷新即可，60s 缓存粒度对齐）

## 7. 测试计划

- `tests/test_edgar_fetcher.py`：parse_form4_xml fixture（买/卖/多 transaction/price 缺失）、build_insider_summary（90 天窗口切割/净额/笔数）、ticker map 解析、自动分类三分支（10-K→stock、N-1A→etf、20-F→fpi）、节流器（连续请求间隔 ≥2s，用 monkeypatch 时钟验证）、429 退避序列（2/4/8s）、INSERT IGNORE 幂等（重复 save 同 rows 行数不变）
- `tests/test_agent_context.py`：_build_agent_context 结构（因子命中/缺省 null/fpi 标注/bars 不足降级）、/portfolio 与 overview 双出口注入、60s 缓存命中
- 现有 441 基线零回归

## 8. 边界与降级

- EDGAR 整轮失败 → factors.*.insider_90d 全 null + edgar_ok=false，次日自然重试，无重试轰炸
- FRED 新系列失败 → 沿用上次落库值（macro 循环既有哲学）
- SPY/RSP 日K失败（长桥 gaps）→ spy/breadth 块 null，不阻塞
- ticker map 拉取失败 → 新持仓探测顺延（asset_type 保持 NULL），存量 stock 照常轮询
- BABA（fpi）：静默零请求零报错，context 标注 fpi_no_form4
- 新增持仓：下轮 EDGAR 循环自动探测分类（用户核心诉求），无需任何人工配置；误判可 `UPDATE holdings SET asset_type='...'` 人工覆盖

## 9. 部署与验证

- rsync 常规部署 + `systemctl restart qdii`（按 AGENTS.md 部署命令，目标见 AGENTS.local.md）
- 库配置：`sys_config.EDGAR_UA` = 含真实联系邮箱的 UA（域名敏感值不入库）
- 线上验证：`journalctl -u qdii` 看 07:00 日K含 SPY/RSP、23:30 edgar 循环分类+Form4 入库日志；`curl` /portfolio 带 session cookie 验证 context 块；`SELECT symbol, COUNT(*) FROM insider_tx GROUP BY symbol` 验证入库
- pytest 全量通过后部署
