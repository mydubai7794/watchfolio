# 持仓收益监控模块 设计文档（spec）

> 日期：2026-10-01
> 状态：需求已确认（用户拍板 5 项决策 + 汇率合并调整），授权自主实施
> 模块：美股+港股基金持仓实时收益网站（券商风格），叠加在 qdii 项目上

## 一、背景与目标

用户持仓分散在多个 App（6 只美股 + 1 只港股场外基金），需要单一网站聚合展示：
持仓列表、当前价/净值、今日涨跌、今日盈亏、持仓（累计）盈亏、总资产走势图（当日分时 + 历史日线）。
数据源结论已由《docs/us-hk-quote-api-research.md》实测背书。

**必须遵守**：不影响现有 A股 ETF 竞价监控/候选池/SSE/代理池功能；数据获取保守（10s 级 + 兜底源）。

## 二、已确认决策

| 决策点 | 结论 |
|---|---|
| 币种口径 | **全部折算 USD**（唯一 HKD 标的=邓普顿基金） |
| 持仓维护 | 网页可视化编辑（增删改：代码/名称/数量/成本价/币种） |
| 走势图 | 当日分时 + 历史日线双图（1月/3月/全部切换） |
| 登录 | 算术图形验证码（PNG）+ token 有效期 30 天 |
| 监控启停 | **全系统取消手动启停**：删除 monitor start/stop/mode 路由与前端按钮，A股监控纯 auto |
| 汇率获取 | **与基金 NAV 拉取合并为同一任务**：每次取 NAV 前先取一次 HKD→USD（新浪 fx_susdhkd，失败兜底 7.80），内存缓存供全局换算 |
| 部署 | <server-ip>:8888（端口已开），systemd 守护，完成后停本地 8000 服务 |

## 三、样例持仓（种子数据，幂等插入）

| symbol | name | market | quantity | cost_price | currency |
|---|---|---|---|---|---|
| MU | 美光科技 | US | 100 | 900.00 | USD |
| SKHY | SK海力士 ADR | US | 100 | 150.00 | USD |
| QQQM | 纳斯达克100ETF-Invesco | US | 200 | 250.00 | USD |
| SPYM | 标普500ETF-SPDR | US | 300 | 75.00 | USD |
| DRAM | 存储芯片ETF-Roundhill | US | 200 | 45.00 | USD |
| DISK | 存储芯片ETF-Tema | US | 200 | 30.00 | USD |
| HK0000740297 | 邓普顿智选亚太股票入息基金 A (Mdis) HKD | HKFUND | 5000 | 13.00 | HKD |

用户后续提供实际股数/成本后，通过网页编辑或 SQL 更新。

## 四、数据层（database.py 扩展）

### 4.1 新表（init_tables 中幂等建表）

```sql
CREATE TABLE IF NOT EXISTS holdings (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    symbol      VARCHAR(20)  NOT NULL UNIQUE COMMENT '代码 MU/SKHY/HK0000740297',
    name        VARCHAR(40)  NOT NULL DEFAULT '',
    market      VARCHAR(10)  NOT NULL DEFAULT 'US' COMMENT 'US=美股 HKFUND=港股场外基金',
    quantity    DECIMAL(14,4) NOT NULL DEFAULT 0 COMMENT '数量/份额',
    cost_price  DECIMAL(14,4) NOT NULL DEFAULT 0 COMMENT '成本价(原币种)',
    currency    VARCHAR(5)   NOT NULL DEFAULT 'USD' COMMENT 'USD/HKD',
    enabled     TINYINT(1)   DEFAULT 1,
    sort_order  INT          DEFAULT 0,
    created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='持仓列表';

CREATE TABLE IF NOT EXISTS portfolio_ticks (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts             DATETIME(3) NOT NULL COMMENT '北京时间采样点',
    trade_date     DATE NOT NULL COMMENT '美东交易日(pre/regular/post 所属 ET 日期)',
    total_value_usd DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '总资产USD',
    day_pnl_usd    DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '今日盈亏USD',
    total_pnl_usd  DECIMAL(16,2) NOT NULL DEFAULT 0 COMMENT '持仓盈亏USD',
    day_pnl_pct    DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '今日收益率%',
    total_pnl_pct DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '累计收益率%',
    INDEX idx_trade_ts (trade_date, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组合市值采样(盘中10s/扩展时段30s，休市不写)';

CREATE TABLE IF NOT EXISTS fund_nav_history (
    nav_date    DATE PRIMARY KEY COMMENT '净值日期',
    nav         DECIMAL(12,4) NOT NULL DEFAULT 0 COMMENT '净值(HKD)',
    nav_change_pct DECIMAL(10,4) NOT NULL DEFAULT 0 COMMENT '日涨跌%'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金官方日频净值(FT)';
```

### 4.2 CRUD 函数

`get_holdings(enabled_only)` / `upsert_holding(...)` / `delete_holding(id)` / `update_holding_names(naming)`；
`save_portfolio_tick(...)` / `get_portfolio_intraday(trade_date)` / `get_portfolio_daily(days)`（每美东交易日取最后一条 tick 聚合）；
`upsert_fund_nav(nav_date, nav, pct)` / `get_latest_fund_nav()` / `get_fund_nav_history(days)`。

> 细化：美股一个交易时段（北京时间 16:00–次日 08:00）跨两个北京自然日，因此 ticks 增加 `trade_date`（美东交易日），当日分时与历史日线均按 `trade_date` 聚合；休市心跳不写 tick。

## 五、采集层（新文件 portfolio_fetcher.py）

### 5.1 美股时段判断（不硬编码北京时间）

`zoneinfo.ZoneInfo("America/New_York")` 取美东当前时间：周末→`closed`；04:00–09:30→`pre`；09:30–16:00→`regular`；16:00–20:00→`post`；其余→`closed`。DST 自动正确。函数 `us_market_session() -> ("pre"|"regular"|"post"|"closed", now_et_str)`。

### 5.2 行情采集主循环（server.py 启动的独立 task）

节奏（保守）：`regular` 10s / `pre`,`post` 30s / `closed` 600s。

每轮（只处理 market='US' 的启用持仓）：
1. **腾讯批量** 1 次：`http://qt.gtimg.cn/q=usMU,usSKHY,...`（GBK 转码；解析字段：名称[1]、最新价[3]、昨收[4]、时间[30]、涨跌额[31]、涨跌幅[32]、高低[33][34]、币种[35]）
2. 扩展时段（pre/post）追加**新浪批量** 1 次：`https://hq.sinajs.cn/list=gb_mu,gb_skhy,...`（必须 `Referer: https://finance.sina.com.cn`，GBK；盘前盘后价=字段[21]，其时间戳=[24]；[21] 为 0 或缺失时回退腾讯收盘价并在响应里标 `ext="none"`）
3. **兜底**：主源连续失败 ≥3 轮 → 东财 `http://push2.eastmoney.com/api/qt/ulist.np/get?secids=105.MU,105.SKHY,105.QQQM,107.SPYM,107.DRAM,107.DISK&fields=f2,f3,f12,f13,f14,f124&fltt=2&invt=2`（市场号 105=NASDAQ、107=AMEX；该源仅常规价、无扩展时段；**频率≤1次/分钟**，直连，遵守 ≥3s/(IP,源)）
4. 组合汇总计算（见 5.4）→ 写 `portfolio_ticks` → 更新内存快照缓存 `portfolio_cache`

httpx 直连（服务器为腾讯云境内机器，境内源直连）；异常吞掉记日志，下轮重试。

### 5.3 基金 NAV + 汇率合并任务（独立 task）

每日北京时间 14:00 与 20:00 各执行一次（启动时先跑一次）：
1. **先取汇率**：新浪 `hq.sinajs.cn/list=fx_susdhkd`（带 Referer；实测字段[1]=7.8466 即 USD/HKD 汇率）→ 存内存 `fx_usdhkd`（含义：1 USD = fx_usdhkd HKD）+ `fx_update_time`；解析失败或数值不在 [7.5, 8.5] 合理区间 → **固定 7.80** 并标记 `fx_fallback=true`。HKD→USD 换算统一为 `usd = hkd / fx_usdhkd`
2. **再取 NAV**：POST `https://www.franklintempleton.com.hk/api/pds/price-and-performance`，Headers `Content-Type: application/json` + `Origin: https://www.franklintempleton.com.hk` + UA，GraphQL：
   - Overview（fundid=31402, shareclasscode=2B, countrycode=HK, languagecode=en_GB）→ `nav{navvalue navdate navchngvalue navchngpct ytdtotretatnav}`
   - navvalue 形如 `$15.50` 需去符号；navdate `dd/MM/yyyy` 解析为 DATE
   - 同日多行 0.00% 占位问题：直接以 Overview 为准（单行）
   - upsert `fund_nav_history`
   - 网络路径：优先 proxy_pool 中 alive 代理（东京/美国节点），无可用代理则直连重试一次；连续失败仅记日志，明日再试
3. 汇率兜底链：新浪失败或越界→7.80（并在 overview 响应标注 `fx_fallback: true`）

### 5.4 盈亏计算（所有金额统一 USD，`usd = hkd / fx_usdhkd`）

对每个 US 持仓：
- `price_now` = regular 时段取腾讯价；pre/post 取新浪[21]（有则用，同时 `price_ext=true`）
- `today_pnl = (price_now − pre_close) × quantity`
- `position_pnl = (price_now − cost_price) × quantity`（USD 标的原生）
- `value = price_now × quantity`

HKFUND 持仓：
- `price_now = 最新 NAV(HKD)`，`value_usd = nav × quantity / fx_usdhkd`
- `today_pnl_usd = (nav − prev_nav) × quantity / fx_usdhkd`（prev_nav 取 fund_nav_history 前一交易日）
- `position_pnl_usd = (nav − cost_price) × quantity / fx_usdhkd`

组合汇总：`total_value`、`day_pnl`、`total_pnl` 及对应百分比（今日收益率基准=Σ(美股昨收+基金昨 NAV 的 USD 市值)）。

### 5.5 内存缓存

`portfolio_cache = {"fetch_time", "session", "quotes": {symbol: {...}}, "summary": {...}, "fx_usdhkd", "fx_fallback", "errors": [...]}`，overview 接口直读缓存，无行情时返回 stale 数据并标注。

## 六、API 层（server.py 扩展）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/captcha` | 免鉴权。生成算术验证码 PNG(base64)+captcha_id，5 分钟有效，一次性 |
| POST | `/api/portfolio/holding` | 新增持仓（symbol/name/market/quantity/cost_price/currency） |
| PUT | `/api/portfolio/holding/{id}` | 修改持仓 |
| DELETE | `/api/portfolio/holding/{id}` | 删除持仓 |
| GET | `/api/portfolio/overview` | 汇总+持仓明细+行情+时段+汇率（登录） |
| GET | `/api/portfolio/intraday` | 当日 ticks（登录） |
| GET | `/api/portfolio/daily?range=30\|90\|all` | 日线（登录） |

登录改造：
- `LoginInput` 增加 `captcha_id`、`captcha_answer`；校验通过才比对用户名密码
- `SESSION_TTL = 30 * 24 * 3600`
- 验证码存内存 `{captcha_id: (answer, expires)}`，校验后即删；登录失败次数>10/分钟 简单限速（内存计数）
- PNG 用 Pillow 生成（requirements.txt 增加 `pillow`）：算式 `a op b = ?`（a,b∈[2,19]，op ∈ +/−，结果非负），随机颜色/旋转/干扰线

手动启停移除（全系统自动）：
- 删除路由 `/api/monitor/start`、`/api/monitor/stop`、`/api/monitor/mode` 与 `monitor_mode` 变量；`/api/monitor/status` 保留（去掉 mode 字段）
- `_trading_hours_scheduler` 保留（auto 常开）；lifespan 新增 `_portfolio_loop`、`_fund_nav_task` 两个 task
- 同步更新受影响测试（tests/test_http_apis.py 若引用上述路由则改为断言 404/删除用例）

## 七、前端

### 7.1 新页面 `static/portfolio.html`（零外部依赖，原生 canvas）

- 顶部：返回主监控页链接 + 标题"我的持仓" + 行情时间/时段徽标（盘前/盘中/盘后/休市）+ 数据源健康
- 汇总卡片区（券商风格）：总资产(USD)、今日盈亏（$ + %，红涨绿跌沿用项目配色 #e94560/#00b894）、持仓盈亏（$ + %）
- 走势图区：当日分时曲线（当日期间累计收益或总资产，10s 粒度）；历史日线（range 切换 1月/3月/全部）
- 持仓表：名称/代码、现价（扩展时段价带"盘前/盘后"角标）、今日涨跌%、持仓市值、今日盈亏、持仓盈亏、持仓成本、数量；HKFUND 行显示净值+日期并标"官方日频"
- 编辑功能：右上"编辑持仓"→ 弹窗列表（增/改/删，字段：代码/名称/市场/数量/成本/币种/启用/排序）
- 轮询 `/api/portfolio/overview` 10s；图表数据按需拉 intraday/daily，日线图切 range 时重拉
- 与 index.html 同风格（深色 #1a1a2e / #16213e / #e94560）

### 7.2 `static/index.html` 修改

- header 增加"我的持仓"按钮（跳 /static/portfolio.html）
- 删除监控启停按钮及其 JS（toggleMonitor 等）；status 区保留自动状态展示

### 7.3 `static/login.html` 修改

- 表单增加"验证码"输入框 + 验证码图片（点击刷新，dataURL）
- doLogin 携带 captcha_id/captcha_answer

## 八、测试

新增 `tests/test_portfolio.py`（全部离线 mock，不发真实网络请求）：
- 腾讯 us 响应解析（GBK 样例字符串）
- 新浪 gb_ 响应解析（含盘前价/缺盘前价两种）
- FT GraphQL 响应清洗（`$15.50`→15.50、`30/09/2026`→date、双行去重规则）
- `us_market_session()` 用 freeze 时间参数化（pre/regular/post/closed/DST 边界）
- 盈亏计算纯函数（US/HKFUND/汇总/汇率兜底）
- holdings CRUD 与种子幂等（连测试库或 mock execute）

回归：`pytest tests/ -x` 全绿（重点 test_http_apis.py 因路由删除需更新）。

## 九、部署（<server-ip>:8888）

1. rsync 代码（排除 .git/akshare-code/akshare-docs/juejin/StockWatcher/__pycache__/uvicorn.log/.idea/.pytest_cache）
2. 服务器 `/opt/qdii` venv：`python3 -m venv venv && venv/bin/pip install -r requirements.txt`（含新增 pillow）
3. systemd unit `/etc/systemd/system/qdii.service`：`ExecStart=/opt/qdii/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8888`，WorkingDirectory=/opt/qdii，Restart=always；`config.py` 的 `SERVER_PORT` 改 8888（或 unit env `SERVER_PORT=8888`，采用前者简单）
4. DB 连接：unit env `DB_HOST=127.0.0.1`（MySQL 在本机 3914；部署时先验证 `mysql -h127.0.0.1 -P3914` 连通，不通则保留 <server-ip> 公网地址）
5. proxy_pool 插入三台海外代理（幂等 INSERT IGNORE）：春川/东京/洛杉矶（url 含账号密码）；连通性检测目标从 baidu 换 https://ipinfo.io/ip（海外代理用 baidu 检测会误判——改为按代理分别检测不现实，统一换成 ipinfo）
6. 启动后验证清单：
   - `curl http://127.0.0.1:8888/api/captcha` 返回 JSON 含 base64
   - 浏览器：登录页验证码显示、admin/hello9987 登录成功、跳转主页
   - /static/portfolio.html：汇总卡片有数、持仓 7 条、行情时间在跳
   - `journalctl -u qdii -f`：portfolio_loop / fund_nav 正常、无异常栈
   - 现有功能：/api/quote 正常返回（StockWatcher 依赖）
7. 停本地服务：`kill 2365780`（本地 uvicorn 8000）
8. 公网验证：`http://<server-ip>:8888`（用户侧浏览器最终确认）

## 十、约束与规范

- 不破坏现有功能：etf_targets/history_ticks/candidate_pool/代理池/SSE/决策模型全部不动（仅删除手动启停）
- 新增 Java 无关；Python 新代码 `@author yys` 约定不适用（纯 Python），注释中文、禁 TODO/伪代码
- 数据获取保守：所有外源请求遵守 ≥3s/(IP,源) 或更宽；东财兜底 ≤1次/分钟
- 目录/文件全小写无连字符：`portfolio_fetcher.py`、`portfolio.html`
- 部署后本地服务停掉，线上为唯一实例（DB 共用同一 MySQL，无数据迁移问题）

## 十一、口径记录

- portfolio_ticks 的 total_value_usd 记录**总资产（含现金折算）**：现金 USD 直加、HKD 按当时 USD/HKD 汇率折算后并入 total_value；今日盈亏/持仓盈亏仍为持仓口径（现金无盈亏）。
- 现金变动当日的历史 tick **不追溯**（曲线仅从变更时点起反映新现金口径）；本次上线切换口径时，对 2026-10-01 全天 294 条 tick 已做一次性回补：每条加计固定现金增量（按当日实时汇率折算；具体数值见本地基线，发布版脱敏）。
- 回补脚本 `/tmp/opencode/backfill_cash.py`（一次性，不进 git）；后续现金编辑（PUT /api/portfolio/cash）只影响当日及之后的 tick 曲线。
