# 美股 / 港股基金行情数据源调研报告

> 调研日期：2026-10-01（北京时间；初轮实测于美股盘前 EDT 07:31，复轮于 EDT 09:27 盘前及东财解封后）
> 调研目标：为"跨市场持仓实时监控网站"评估美股个股/ETF（含盘前盘后）与港股场外基金的行情获取能力
> 覆盖标的：美光 MU、SK海力士 ADR（**SKHY**，NASDAQ 上市）、QQQM、SPYM、DRAM、DISK、邓普顿智选亚太股票入息基金 HKD(A Dis)（hk0000740297）
> v2 修订：更正海力士 ADR 代码（HXSCL→SKHY，此前对"OTC 粉单"的判断有误）；东财批量接口与市场号修正；新增三台代理实测、Yahoo v7/v8 全流程、**邓普顿官网净值 GraphQL API 完整逆向**

---

## 一、结论速览（能力矩阵）

| 标的 | 腾讯 qt.gtimg.cn | 新浪 hq.sinajs.cn | 东财 push2 | Yahoo(需代理) | 推荐方案 |
|---|---|---|---|---|---|
| 美股个股 MU | ✅ 常规时段，批量 | ✅ 常规 + **盘前盘后实时** | ✅ 105.MU（易封IP） | ✅ preMarketPrice | 腾讯(常规) + 新浪(盘前后) |
| **SKHY（海力士ADR，NASDAQ）** | ✅ usSKHY 实测通过 | ✅ gb_skhy 盘前价实测 | ✅ 105.SKHY 批量实测 | ✅ v7/v8 实测 | 同上（与其他美股完全一致） |
| 美股ETF QQQM/SPYM/DRAM/DISK | ✅ 同上 | ✅ 同上 | ✅ **107=AMEX**（SPYM/DRAM/DISK）；105=NASDAQ（QQQM） | ✅ | 同上 |
| 港股上市 ETF/基金 | ✅ hk 前缀（hk00700 实测） | 未测（非必需） | 116 市场（未验证） | - | 腾讯 hk 前缀 |
| 邓普顿智选亚太 hk0000740297 | ❌ | ❌ | ❌（非上市基金） | ❌ | **官网 GraphQL API（已完整打通，见 §4）** / 恒生U45722 备选 |

**核心结论**：
1. **7 个标的全部可自动化**：6 只美股（MU/SKHY/QQQM/SPYM/DRAM/DISK）四源全覆盖；邓普顿基金走官网 GraphQL API 每日 1-2 次抓官方 NAV。
2. 美股盘中主源 = 腾讯批量（1 请求×6 标的）；**盘前盘后主源 = 新浪 gb_ 批量**（境内免代理唯一含扩展时段实时价的源，本次两轮盘前实测与 Yahoo/CNBC 交叉一致）。
3. 海力士 ADR 正确代码为 **SKHY（SKHY.OQ，NASDAQ 正式上市的存托凭证）**——此前误按 OTC 粉单 HXSCL 调研，三大境内源/Yahoo 均无 HXSCL 属正常；**SKHY 在腾讯/新浪/东财/Yahoo 四源全部实测可用**，与其他美股处理方式完全一致，无需任何特殊逻辑。
4. 邓普顿基金为香港注册**非上市基金**（hk0000740297 为 ISIN，Bloomberg TEAEIAH HK），无盘中实时价，官方日频 NAV 已通过官网 PDS GraphQL API 打通（**实测拿到 30/09/2026 NAV $15.50，T 日即出**）。

---

## 二、项目现状（qdii 项目已有能力）

由 @code-reader 调研（文件:行号 详见附录A）：

- **架构**：FastAPI + asyncio + httpx + MySQL(aiomysql) + 原生 JS SSE 推送（server.py:312-340）；C# WinForms 的 StockWatcher 作为外部消费端轮询 `/api/quote`。
- **现有数据源仅 3 个**：腾讯（A股 ETF 批量行情，fetcher.py:35）、新浪（美股期指 hf_ 前缀，fetcher.py:36）、东财（期指，fetcher.py:40）。**无美股个股、无港股、无场外基金净值能力**（database.py:191-192 的 source 枚举只有 tencent/sina_futures/eastmoney_futures）。
- **限流机制（必须沿用）**：
  - `MIN_INTERVAL_SEC = 3.0`：每 (代理IP, 数据源) 二元组独立限速（config.py:17、fetcher.py:64-69）
  - 代理断路器：403/407 触发指数退避封禁，30s 起步、上限 600s、倍数 2.0（fetcher.py:82-84、137-160）
  - 代理池：MySQL `proxy_pool` 表 + `/api/proxy` CRUD + 连通性检测（server.py:485-541）；httpx `AsyncClient(proxy=...)` 原生支持 HTTP/SOCKS5（fetcher.py:230-232）
  - 批量请求分批并发、批间 sleep `MIN_INTERVAL_SEC`（fetcher.py:947-966）；监控循环每轮总时长 ≥3s（server.py:1044-1060）
  - 注意：docs/project-overview.md:59-69 写的是 4 秒/间隔，与代码 3.0 秒存在文档漂移，**以代码为准**

---

## 三、美股实测结果（详细证据）

### 3.1 腾讯：盘中主源 ✅

```
GET https://qt.gtimg.cn/q=usMU,usSKHY,usQQQM,usSPYM,usDRAM,usDISK
（GBK 需转码；代码格式 = "us"+裸代码，不带 .OQ/.AM 后缀）
```

实测 6 标的全部返回（含 SKHY=SK海力士 184.15、52周 199.87/124.77、市值 1933 亿 USD）。字段（`~` 分隔）：`[3]`最新价、`[4]`昨收、`[5]`今开、`[6]`成交量、`[30]`行情时间（**仅常规时段**）、`[31]/[32]`涨跌额/幅、`[33]/[34]`高低、`[35]`币种。尾部字段中有一个疑似扩展时段价（与新浪/CNBC 不一致、含义未证实），**不要依赖**。

### 3.2 新浪：盘前盘后主源 ✅

```
GET https://hq.sinajs.cn/list=gb_mu,gb_skhy,gb_qqqm,...
必须携带 Header: Referer: https://finance.sina.com.cn （GBK 编码）
```

字段（逗号分隔）：`[1]`最新价（常规）、`[3]`北京时间、`[4]`涨跌幅、`[26]`昨收；**`[21]/[22]/[23]` = 盘前或盘后 价/涨跌幅/涨跌额，`[24]` 为该报价美东时间**。

两轮盘前实测：
- EDT 07:31：MU 1054.94 -0.96%、QQQM 306.24 +0.52%
- EDT 09:27：MU 1051.995 -1.23%、**SKHY 185.56 +0.77%**、QQQM/SPYM/DRAM/DISK 均持续更新
- 与 Yahoo v7（SKHY 185.63 +0.80%，时间戳差所致）及 CNBC（参考报告 MU 1054.50）交叉一致 ✅
- 注意：`[22]/[23]` 个别标的符号缺失，**涨跌幅建议用 `[21]` 与昨收自行计算**；低流动性标的（DISK）更新间隔较长属正常

### 3.3 东方财富：校验源 ✅（批量接口 + 市场号修正）

```
GET http://push2.eastmoney.com/api/qt/ulist.np/get?secids=105.MU,105.SKHY,105.QQQM,107.SPYM,107.DRAM,107.DISK&fields=f2,f3,f12,f13,f14,f124&fltt=2&invt=2
```

实测（东财解封后）**一次请求返回全部 6 标的**，0.04s：
- **105 = NASDAQ**（MU/SKHY/QQQM）、**107 = AMEX**（SPYM/DRAM/DISK）——修正初版报告中 SPYM/DRAM/DISK 标 105 的错误
- f2=最新价 f3=涨跌幅 f12=代码 f13=市场号 f14=名称 f124=行情时间戳（收盘）
- `ulist.np/get` 批量接口比逐标的 `stock/get` 更适合轮询；代码归属可用 `searchapi.eastmoney.com/api/suggest/get?input=XXX&type=14&token=D43BF722C8E33BDC906FB84D85E326E8` 自动解析
- **封禁实证**：初轮测试中约 8~10 次连续请求（间隔数秒）触发 IP 级连接重置（push2/23.push2 全挂、push2his 不受影响），封禁为数小时级临时封禁（复轮已自然解封）→ **接东财必须：(代理,源) 严格 ≥3s + 代理池轮换 + 断路器退避，能用腾讯/新浪就不用东财**

### 3.4 Yahoo Finance：盘前后校验源 ✅（走代理，全流程打通）

- **v8 chart（免 crumb）**：`GET https://query1.finance.yahoo.com/v8/finance/chart/SKHY?range=1d&interval=1m&includePrePost=true` → 200，meta 含 `hasPrePostMarketData:true` 与 `currentTradingPeriod`（pre 04:00-09:30 / regular 09:30-16:00 / post 16:00-20:00 EDT，**可直接做时段调度元数据**）
- **v7 quote（需 cookie+crumb）三步流程实测通过**：
  1. `GET https://fc.yahoo.com`（404 但下发 Set-Cookie，`-c` 保存）
  2. `GET https://query1.finance.yahoo.com/v1/test/getcrumb`（`-b` 携带 cookie，返回 crumb）
  3. `GET .../v7/finance/quote?symbols=MU,SKHY,QQQM,SPYM,DRAM,DISK&crumb=<crumb>&fields=...`（UA 必需）
- v7 实测：6 标的全部返回 `marketState:PRE` + `preMarketPrice/preMarketChangePercent`（MU 1050.56 -1.37%、SKHY 185.63 +0.80% 等）✅
- 直连 403，必须走代理（见 §5）

### 3.5 海力士 ADR 代码勘误（HXSCL → SKHY）

- 初轮误将海力士 ADR 按 OTC 粉单代码 **HXSCL** 调研：腾讯/新浪/Yahoo/stooq/stockanalysis 均无此符号（Yahoo 明确 "No data found, symbol may be delisted"），东财 105.HXSCL 返回 data:null——**该判断作废**
- 正确代码 **SKHY（SKHY.OQ，NASDAQ 上市 ADR）**：腾讯/新浪/东财/Yahoo 四源实测全部可用（§3.1-3.4），与其他美股完全同构，无需特殊处理
- 东财粉单市场 `m:153`（akshare-code/akshare/stock/stock_us_pink.py:22-36）仍可作为未来 OTC 标的的备查路径

---

## 四、港股基金：官网 GraphQL 净值 API（已完整打通）✅

### 4.1 标的性质

Templeton Select APAC Equity Income Fund（邓普顿智选亚太股票入息基金）为**香港注册非上市互惠基金**，无盘中实时价，官方每日估值一次。标识（官网 API 实测返回）：

| 项 | 值 |
|---|---|
| ISIN | HK0000740297 |
| Bloomberg / Ticker | TEAEIAH HK / TEAEIAH |
| 官网 fundid / shclcode | **31402 / 2B**（A (Mdis) HKD 份额，2021-08-13 成立） |

### 4.2 API 端点与调用方式（逆向自官网 Angular SPA main.js，全部实测通过）

```
POST https://www.franklintempleton.com.hk/api/pds/price-and-performance
Headers: Content-Type: application/json
         Origin: https://www.franklintempleton.com.hk   （必带）
         User-Agent: Mozilla/5.0 ...
Body:    GraphQL query + variables
```

**① ISIN → fundId 反查（ProductLookup）**：

```json
{"query":"{ProductLookup(isin:\"HK0000740297\"){fundid shclcode cntrycode langcode isin bloomberg ticker}}"}
```

**② 最新 NAV（Overview，shareclass.nav）**：

```json
{"query":"query O($f:String!,$s:String!,$c:String!,$l:String!){Overview(fundid:$f,shareclasscode:$s,countrycode:$c,languagecode:$l){fundname shareclass{shclname shclcurr nav{navvalue navdate navchngvalue navchngpct currcode ytdtotretatnav navhigh52week navlow52week}}}}",
 "variables":{"f":"31402","s":"2B","c":"HK","l":"en_GB"}}
```

实测返回：`navvalue:"$15.50", navdate:"30/09/2026", navchngvalue:"$-0.12", navchngpct:"-0.77%", ytdtotretatnav:"30.91%", 52周 16.52/11.33`（**T 日 9/30 当天即有值**，比第三方快照更快）

**③ 历史 NAV（PricesHistory，日期参数为 yyyymmdd 整型）**：

```json
{"query":"{PricesHistory(fundid:\"31402\",shareclasscode:\"2B\",countrycode:\"HK\",languagecode:\"en_GB\",startdate:20260922,enddate:20261002){prices{navdate nav navchngpct currcode}}}"}
```

实测返回 9/22-9/30 完整日频序列（15.88→15.65→15.52→15.74→15.57→15.62→15.50）✅

### 4.3 实现注意事项

1. **无需鉴权**：无 cookie/token；`Origin` 头必带（同源校验）；内网 dev 端点（pdswebapi...fti-cloud.com，带明文 apikey）公网不可达，生产一律走官网反代路径
2. **GraphQL 内省开放**（`__schema`/`__type` 可用），字段全量为 snake_case（navvalue/navdate/navchngpct...），需要扩展（分红 DistributionHistory、组合 Portfolio 等）可直接内省
3. **数据清洗**：日期为 `dd/MM/yyyy` 字符串；navvalue 带货币符号前缀（`$15.50`）；**最新日可能出现两行（首行 0.00% 占位），按 navdate 去重取末行**
4. **抓取节奏**：日频数据，每日 2 次足够（建议香港时间 14:00 / 20:00 错峰，每次 1 请求，走境外代理）；净值日期落后 2 个交易日应告警
5. 备选源：恒生银行基金超市（内部代码 **U45722**）、stockevents.app（聚合页）——官网 API 已足够，备选仅登记不推荐

---

## 五、代理池实测（用户新增三节点）

测试方式：`curl -x http://*REMOVED*:***@host:7779 https://ipinfo.io/json`

| 入口 | 位置 | 实际出口 IP | 延迟 | 说明 |
|---|---|---|---|---|
| *REMOVED*:7779 | 春川（NLB 前置，主力） | ***REMOVED*（东京，Oracle AS31898）** | 0.56-0.79s | NLB 后端出口固定为东京（复测 3 次一致）；本机链路最优、**最快入口** |
| *REMOVED*:7779 | 东京直连（备用） | *REMOVED*（同上） | 0.97-1.89s | 与春川入口**共享出口 IP** |
| *REMOVED*:7779 | 洛杉矶 RackNerd | *REMOVED*（Santa Clara，AS36352） | ~0.97s | 独立美国出口 |

**关键结论**：
1. 三入口全部可用，认证方式 HTTP Basic（`*REMOVED*` / `*REMOVED*`），https 隧道正常
2. **对目标站点而言独立出口 IP 只有 2 个**（东京 *REMOVED* + 美西 *REMOVED*）——限流按出口 IP 计时，春川/东京两入口应视为**同一个 (proxy,source) 限流单元**，勿当作两个 IP 轮换
3. 实测通过能力：Yahoo v7/v8（东京出口 ✅）、ipinfo（✅）；Google/DDG html 对两个数据中心出口均 connection reset（正常，搜索引擎反爬）；FT 官网 API（东京出口 ✅）
4. 境内源（腾讯/新浪/东财）**不走这些代理**（境外出口访问境内源徒增延迟；东财封禁时换代理池里的境内代理或等待解封）
5. 建议入库 `proxy_pool`：两个独立出口 + 本地 `127.0.0.1:7680` 共同组成海外源代理池，`/api/proxy` 现有 CRUD/检测直接复用

---

## 六、限流与代理规范（新数据源接入必须遵守）

沿用项目现有实现（详见第二节），结合实测教训：

1. **速率维度**：`(出口IP, 数据源)` 二元组独立计时 ≥3s（fetcher.py:64-69）。新增 source 键建议：`tencent_us` / `sina_us` / `eastmoney_us` / `tencent_hk` / `ft_fund_nav` / `yahoo_us`。
2. **每轮 HTTP 预算**：美股全持仓 = 腾讯批量 1 次 + 新浪批量 1 次（免代理、境内直连）即可覆盖常规+盘前后；东财批量与 Yahoo v7 作为低频校验源（30-60s）；基金 NAV 每日 2 次。与项目"每轮 2 次 HTTP"哲学一致。
3. **东财专项**：实测 ~8-10 次连续请求触发 IP 级临时封禁（数小时）；严格 3s+代理池+断路器，或干脆只作 60s 校验源。
4. **Yahoo 专项**：cookie+crumb 需维护（过期 429/401 时重走三步流程）；UA 必带；走海外代理池。
5. **FT 官网专项**：每日 2 次礼貌抓取，走海外代理；`Origin` 头必带。
6. **编码**：腾讯/新浪 GBK 转码，东财/Yahoo/FT UTF-8；新浪必带 Referer。
7. **时段调度**（北京时间，夏令时）：美股盘前 16:00-21:30（新浪 10s 主）→ 盘中 21:30-04:00（腾讯 5-10s 主）→ 盘后 04:00-08:00（新浪 10s 主）→ 休市心跳 60s+。**不要硬编码时间表**：用 Yahoo v8 `currentTradingPeriod` 或东财 f80 自校准边界（DST 切换日自动正确）。仿 `_trading_hours_scheduler`（server.py:828-895）扩展多市场。

---

## 七、推荐技术方案（网站实时持仓）

```
持仓录入(多市场代码) → 分市场路由 fetcher → SSE 推送前端
```

1. **美股（含 SKHY，无特殊逻辑）**：常规时段腾讯 `q=usXXXX,...` 批量；盘前/盘后新浪 `list=gb_xx,...` 批量读 `[21]-[24]`；降级链：腾讯↔新浪互备 → 东财 ulist 批量（挂代理）→ Yahoo v7（挂代理）
2. **港股上市标的**：腾讯 `hk` 前缀批量（可并入同一轮腾讯请求）
3. **邓普顿基金**：独立 `fund_nav` 类型，FT 官网 GraphQL 每日 2 次（fundid=31402 / shclcode=2B 已锁定，也可用 ProductLookup 按 ISIN 动态反查），前端标注"官方日频净值"
4. **数据库**：`etf_targets` 泛化为多市场监控表（加 market/currency/source 字段）；NAV 历史存 `history_ticks` 或独立 fund_nav_history
5. **代理池**：境内源直连或境内代理；海外源（Yahoo/FT）用新三节点+本地 7680（注意 §5 出口 IP 去重结论）

---

## 附录A：项目限流/代理实现索引（@code-reader 提供）

| 项 | 位置 | 值/说明 |
|---|---|---|
| MIN_INTERVAL_SEC | config.py:17 | 3.0 秒/(代理,源) |
| DEFAULT_SAFE_REQ_PER_MIN | config.py:16 | 20（种子入库，监控循环未读取） |
| (proxy,source) 限速 | fetcher.py:64-69 | `_source_last_used` 二元组 |
| 断路器 | fetcher.py:82-84,137-160 | 30s→600s 指数退避，成功清除 |
| 代理轮询/降级 | fetcher.py:163-222 | 全冷却则 sleep 等待，无代理才直连 |
| K线分批限流 | fetcher.py:947-966 | 按代理池分批，批间 3s |
| 监控循环节奏 | server.py:1044-1060 | 每轮 ≥3s，出错也 sleep 3s |
| proxy_pool 表 | database.py:139-151 | url/latency_ms/alive/last_check |
| 代理管理 API | server.py:485-541 | CRUD + baidu 连通性检测（海外代理需换检测目标） |
| SSE 推送 | server.py:312-340 | /api/stream，30s 心跳 |
| OTC 粉单参考 | akshare-code/akshare/stock/stock_us_pink.py:22-36 | fs=m:153（备查） |

## 附录B：用户参考报告要点（已采纳/交叉印证）

- CNBC `quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=MU|SKHY&exthrs=1&output=json`：免鉴权批量，含 `curmktstatus`/`ExtendedMktQuote`（盘前后备源，60s）
- Nasdaq `api.nasdaq.com/api/quote/MU/info?assetclass=stocks`：逐标的，有 CF 风控（备源）
- DISK 全称核实：三家一致为 **Tema Memory ETF**（如预期为 Global X 数据中心 ETF 请在持仓 App 复核）
- 腾讯代码格式：`us`+裸代码，带 `.OQ/.AM` 后缀单码请求会 none_match
- 涨跌幅一律用「扩展时段价 − 常规昨收」自算（新浪 [22]/[23] 有符号缺失）
- 券商 OpenAPI（富途/长桥/Webull/老虎）为更稳的方案 B：官方实时含盘前后、文档化限流，代价是开户+常驻网关
- 恒生基金超市代码 U45722、stockevents.app 作为 NAV 备选源

## 附录C：遗留小项（不影响方案成立）

1. 东财 `116.XXXXX`（港股）未实测（当前无港股上市持仓需求）
2. 新浪 gb_ `[25]` 字段在盘前时段显示的含义（疑似常规收盘时间戳，盘后行为待上线后复核）
3. PricesHistory 返回最新日双行（首行 0.00% 占位）的去重规则已在 §4.3 说明
4. `/api/proxy` 连通性检测目标为 baidu，海外代理入库后需增加海外检测目标（如 ipinfo.io）
