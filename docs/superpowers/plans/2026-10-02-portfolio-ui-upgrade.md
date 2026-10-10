# 持仓页券商级 UI 升级 + 会话持久化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 分时图改为昨收基线口径、持仓列表可排序、整体 UI 对标主流券商风格、盘前盘后参考盈亏标注、移动端适配、会话 token 落库支持长效测试 token。

**Architecture:** 会话从内存字典迁移到 MySQL `sessions` 表（保留过期校验）；前端 portfolio.html 整体重做（深色券商风、分时基线=昨收、表头排序、盘前后标签细化、响应式卡片列表）；后端 intraday/overview 接口已具备所需字段（`points[].p`、`summary.base_value`），无需改动。

**Tech Stack:** FastAPI + aiomysql/PyMySQL（现有 db 层）、原生 HTML/CSS/JS + Canvas（现有前端栈）、pytest。

---

## 现状关键事实（实施者必读）

- `/api/portfolio/intraday` 返回 `{points:[{ts, v(总资产含现金), p(当日盈亏)}]}`（server.py:1279-1287）——分时基线改造只用 `p` 与 `v`，无需后端改动。
- `/api/portfolio/overview` 的 `summary` 含 `total_value/holdings_value/cash_usd/day_pnl/day_pnl_pct/total_pnl/total_pnl_pct/base_value`；顶层含 `session(pre/regular/post/closed)/positions/cash/fx_usdhkd`。`position` 字段：`symbol/name/market/price/change_pct/value/today_pnl/position_pnl/position_pnl_pct/cost_price/quantity/ext(pre/post·regular/none)/stale/quote_time/nav_date`。
- 盘前盘后价已并入美股盈亏计算（build_positions 用 `ext_price` 当 price_now），`ext="pre/post"`。
- 昨收总资产 = `summary.base_value + summary.cash_usd`（base_value 为持仓昨收口径）。
- 会话现状：`server.py` 内存字典 `_sessions`（定义 :70，写入 :84，校验 :96-100，登出 :392，/api/me :403）。登录流程：`/api/captcha` → `/api/login`（校验一次性验证码）→ 返回 `{token}`，前端存 localStorage 带 `Authorization: Bearer`。
- 前端 `static/portfolio.html` 现为 376 行单文件（内联 CSS/JS），Canvas 画图，10s 轮询 overview。
- 涨跌色惯例：**红涨绿跌**（`cls()`），保持不变。
- 数据库连接：`database.py` 的 `Database` 类（async），表用 `CREATE TABLE IF NOT EXISTS` 在 `init_tables()` 建立。

---

### Task 1: 会话 token 落库（sessions 表 + CRUD + server 接线）

**Files:**
- Modify: `database.py`（init_tables 增表 + 3 个方法）
- Modify: `server.py`（_sessions 字典替换为 DB 读写）
- Modify: `tests/test_server_auth.py`（会话相关用例改 mock DB）
- Create: `scripts/make_token.py`（造长效测试 token 的运维小工具）

- [ ] **Step 1: 写失败测试**

`tests/test_server_auth.py` 中现有用例直接操作 `server._sessions`（:26/:79/:112），全部改为 monkeypatch `db` 的三个新方法。新增：

```python
def test_session_persisted_via_db(monkeypatch):
    """登录成功后会话写入 DB 而非内存字典。"""
    calls = []
    async def fake_create(token, username, ttl):
        calls.append((token, username))
    monkeypatch.setattr(server.db, "create_session", fake_create)
    # monkeypatch 验证码与密码校验后 POST /api/login
    server._captchas["cid"] = {"answer": 7, "expires": time.time() + 60}
    r = server.client.post("/api/login", json={
        "username": "admin", "password": "hello9987",
        "captcha_id": "cid", "captcha_answer": "7"})
    assert r.status_code == 200
    assert len(calls) == 1 and calls[0][1] == "admin"
    assert not hasattr(server, "_sessions") or not server._sessions

def test_expired_token_rejected(monkeypatch):
    """过期 token 401（DB 层过期校验）。"""
    async def fake_get(token):
        return {"username": "t", "expires": time.time() - 1}
    monkeypatch.setattr(server.db, "get_session", fake_get)
    r = server.client.get("/api/portfolio/overview",
                          headers={"Authorization": "Bearer tok"})
    assert r.status_code == 401
```

- [ ] **Step 2: 跑测试确认失败**

Run: `python3 -m pytest tests/test_server_auth.py -q`
Expected: FAIL（`db.create_session` 不存在 / `_sessions` 仍在）

- [ ] **Step 3: database.py 实现**

init_tables 追加：

```sql
CREATE TABLE IF NOT EXISTS sessions (
  token VARCHAR(64) PRIMARY KEY,
  username VARCHAR(64) NOT NULL,
  expires DATETIME NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

Database 类新增（跟随现有 aiomysql 风格，参考 `get_cash_accounts` 的写法）：

```python
async def create_session(self, token: str, username: str, ttl: int):
    """写入会话（expires = 当前时间 + ttl 秒）。"""
    expires = datetime.now() + timedelta(seconds=ttl)
    async with self._conn() as conn:
        await conn.execute(
            "INSERT INTO sessions (token, username, expires) VALUES (%s, %s, %s)",
            (token, username, expires))

async def get_session(self, token: str) -> dict | None:
    """查会话，过期则删除并返回 None（过期即失效）。"""
    async with self._conn() as conn:
        row = await conn.fetch_one("SELECT username, expires FROM sessions WHERE token=%s", (token,))
        if not row:
            return None
        if row["expires"] < datetime.now():
            await conn.execute("DELETE FROM sessions WHERE token=%s", (token,))
            return None
        return {"username": row["username"], "expires": row["expires"]}

async def delete_session(self, token: str):
    async with self._conn() as conn:
        await conn.execute("DELETE FROM sessions WHERE token=%s", (token,))
```

（按项目现有连接池/游标封装实际调整方法名，语义不变：INSERT、SELECT+过期删除、DELETE。）

- [ ] **Step 4: server.py 接线**

- 删除 `_sessions` 字典及所有引用（:70/:84/:96-100/:392/:403）。
- 登录成功处：`token = secrets.token_hex(24)` → `await db.create_session(token, u, SESSION_TTL)`（SESSION_TTL 现有常量，30 天）→ 响应返回 token 不变。
- auth 校验处：`session = await db.get_session(token)`，None → 401。
- 登出：`await db.delete_session(token)`；`/api/me` 同理用 get_session。
- 保持 401 响应体与现有一致，前端 patchFetch 的 401 跳转行为不变。

- [ ] **Step 5: 造 token 工具 `scripts/make_token.py`**

```python
#!/usr/bin/env python3
"""造长效测试 token：python3 scripts/make_token.py [有效期天数，默认 36500]"""
import sys, asyncio, secrets
from datetime import datetime, timedelta
import database

async def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 36500
    token = secrets.token_hex(24)
    db = database.Database()
    await db.init_tables()
    await db.create_session(token, "admin", days * 86400)
    print(token)

asyncio.run(main())
```

（按 database.py 实际构造方式调整；输出仅 token 一行，便于脚本捕获。）

- [ ] **Step 6: 全量测试通过**

Run: `python3 -m pytest tests/ -q`（tests/test_portfolio.py + test_decision_model.py + test_server_auth.py）
Expected: 全部 PASS

- [ ] **Step 7: Commit**

```bash
git add database.py server.py tests/test_server_auth.py scripts/make_token.py
git commit -m "feat(auth): 会话token落库sessions表并支持长效测试token"
```

---

### Task 1.5: 市场参考指标看板（后端）

**Files:**
- Modify: `portfolio_fetcher.py`（新浪油品解析 + 看板抓取 + minute sparkline）
- Modify: `server.py`（_market_loop 60s 循环 + overview 附带 market 字段）
- Test: `tests/test_portfolio.py`

**最终看板 11 项（用户宏观情报升级版，实测时间 2026-10-02）：**

| key | 名称 | 数据源（实测验证） | spark |
|-----|------|------------------|-------|
| QQQ | 纳指100 | 腾讯 us（混排批量） | 腾讯minute ✓ |
| SPY | 标普500 | 腾讯 us | ✓ |
| SOXX | 费城半导体 | 腾讯 us | ✓ |
| HSI | 恒生指数 | 腾讯 hk | ✓ |
| BZ | 布伦特原油 | 腾讯 hf_OIL（混排）；新浪 hf_OIL 兜底 | ✗ |
| GC | COMEX黄金 | 腾讯 hf_GC；新浪兜底 | ✗ |
| DXY | 美元指数 | 新浪 DINIW（f[1]价 f[3]昨收 f[9]名称） | ✗ |
| US30Y | 美债30Y收益率 | Yahoo v8 chart `^TYX`（走代理池） | ✓ 近5日 |
| US10Y | 美债10Y收益率 | Yahoo v8 chart `^TNX` | ✓ |
| VIX | 恐慌指数 | Yahoo v8 chart `^VIX` | ✓ |
| USDHKD | 美元/港元 | fx_state 现成（前端拼） | ✗ |

**关键实测事实：**
- 腾讯混排一个请求：`q=usQQQ,hkHSI,hf_OIL,hf_GC` 全返回；hf_ 行为逗号分隔布局：f[0]现价/f[7]昨结/f[13]名称（无尾部成交量字段，新浪版有 f[14]）
- Yahoo v7 quote 已封（Unauthorized）；**v8 chart 可用**：`query1.finance.yahoo.com/v8/finance/chart/^TYX?interval=1d&range=5d` → meta.regularMarketPrice / meta.chartPreviousClose / indicators.adjclose（近5日 spark），需代理+UA
- 新浪 DINIW：`"时间,最新,最新,昨收,...,名称,日期"`，昨收取 f[3]
- **昨结修正**：hf_ 系列涨跌必须用 f[7] 昨结自算（旧实现误用 f[2] 买价）
- TLT/GLD/USO 移除（US30Y 真源已得、GC 期货替代 GLD ETF、WTI 冗余）

- [ ] **Step 1: 失败测试**（tests/test_portfolio.py 改造+追加）

```python
def test_parse_sina_oil():
    # 昨结=f[7]=98.03（修正：旧实现误用 f[2] 买价）
    raw = 'var hq_str_hf_OIL="101.18,3.21,101.09,101.12,101.88,96.55,00:45:07,98.03,97.66,0,6,7,2026-10-02,布伦特原油";\n'
    q = pf.parse_sina_oil(raw)
    assert q["price"] == 101.18 and q["pre_close"] == 98.03
    assert abs(q["change_pct"] - (101.18-98.03)/98.03*100) < 1e-9

def test_parse_tencent_hf_line():
    # 腾讯混排返回中的 hf_ 行（逗号布局，无尾部成交量）
    raw = 'v_hf_OIL="101.18,3.21,101.09,101.12,101.88,96.55,00:45:07,98.03,97.66,0,6,7,2026-10-02,布伦特原油";\n'
    q = pf.parse_tencent_quotes(raw, ["OIL"], {"OIL": "hf"})
    assert q["OIL"]["price"] == 101.18 and q["OIL"]["pre_close"] == 98.03
    assert q["OIL"]["name"] == "布伦特原油"

def test_parse_sina_diniw():
    raw = 'var hq_str_DINIW="00:45:25,102.1406,102.1406,101.4743,7389,101.4825,102.1766,101.4377,102.1406,美元指数,2026-10-02";\n'
    q = pf.parse_sina_diniw(raw)
    assert q["price"] == 102.1406 and q["pre_close"] == 101.4743
    assert q["name"] == "美元指数"

def test_parse_yahoo_chart():
    raw = json.dumps({"chart":{"result":[{"meta":{"regularMarketPrice":5.599,
        "chartPreviousClose":5.639},"indicators":{"adjclose":[{"adjclose":[5.60,5.61,5.599]}]}}]},"error":None})
    q = pf.parse_yahoo_chart(raw)
    assert q["price"] == 5.599 and q["pre_close"] == 5.639
    assert q["spark"] == [5.60, 5.61, 5.599]

def test_parse_tencent_minute():
    raw = json.dumps({"code":0,"data":{"usQQQ":{"data":{"data":["0930 738.00 100","0931 738.50 200"],"date":""}}}})
    pts = pf.parse_tencent_minute(raw)
    assert pts == [("0930", 738.0), ("0931", 738.5)]
```

- [ ] **Step 2: 跑测试确认失败** → FAIL
- [ ] **Step 3: 实现**（portfolio_fetcher.py，中文注释）

```python
def parse_sina_oil(raw: str) -> dict:
    """解析新浪/腾讯布伦特 hf_OIL：f[0]现价 f[7]昨结 f[13]名称，涨跌幅自算；字段不足返回 {}。"""
    m = re.search(r'hf_OIL="([^"]*)"', raw)
    if not m: return {}
    f = m.group(1).split(",")
    if len(f) < 14 or not f[0]: return {}
    price, pre = float(f[0]), float(f[7] or 0)
    return {"price": price, "pre_close": pre,
            "change_pct": (price-pre)/pre*100 if pre > 0 else 0.0, "name": f[13]}

def parse_sina_diniw(raw: str) -> dict:
    """解析新浪美元指数 DINIW：f[1]现价 f[3]昨收 f[9]名称。"""
    m = re.search(r'DINIW="([^"]*)"', raw)
    if not m: return {}
    f = m.group(1).split(",")
    if len(f) < 10 or not f[1]: return {}
    price, pre = float(f[1]), float(f[3] or 0)
    return {"price": price, "pre_close": pre,
            "change_pct": (price-pre)/pre*100 if pre > 0 else 0.0, "name": f[9]}

def parse_yahoo_chart(raw: str) -> dict:
    """解析 Yahoo v8 chart（^TYX/^TNX/^VIX）：meta 现价/昨收 + adjclose 近5日 spark。"""
    try:
        r = json.loads(raw)["chart"]["result"][0]
        meta = r["meta"]
        spark = [v for v in r["indicators"]["adjclose"][0]["adjclose"] if v]
        return {"price": float(meta["regularMarketPrice"]),
                "pre_close": float(meta.get("chartPreviousClose") or 0),
                "change_pct": (meta["regularMarketPrice"]-meta["chartPreviousClose"])
                              / meta["chartPreviousClose"] * 100
                              if meta.get("chartPreviousClose") else 0.0,
                "spark": spark[-60:]}
    except Exception:
        return {}
```

`parse_tencent_quotes` 扩展：prefixes 值支持 "hf"——行匹配 `v_hf{sym}="..."` 时按逗号布局解析（f[0]/f[7]/f[13]，change_pct 自算）。

`MARKET_PANEL` 更新为 11 项常量（上表，USDHKD 由前端从 fx 拼，后端不重复）：

```python
MARKET_PANEL = [
    ("QQQ", "usQQQ", "纳指100", "USD", True),
    ("SPY", "usSPY", "标普500", "USD", True),
    ("SOXX", "usSOXX", "费城半导体", "USD", True),
    ("HSI", "hkHSI", "恒生指数", "HKD", True),
    ("BZ", "hfOIL", "布伦特原油", "USD", False),
    ("GC", "hfGC", "COMEX黄金", "USD", False),
    ("DXY", "", "美元指数", "", False),      # 新浪 DINIW
    ("US30Y", "", "美债30Y", "%", True),     # Yahoo ^TYX，单位百分点
    ("US10Y", "", "美债10Y", "%", True),
    ("VIX", "", "恐慌指数", "", True),
]
```

`fetch_market_panel(session=None)` 改造：
- 腾讯一个混排请求（6 代码：usQQQ,usSPY,usSOXX,hkHSI,hfOIL,hfGC，prefixes us/hk/hf）
- 新浪一次：`list=DINIW,hf_OIL,hf_GC`（DINIW 主用；hf_OIL/hf_GC 作腾讯 hf 失败兜底）
- Yahoo v8 并发 3 个 chart 请求（^TYX/^TNX/^VIX，UA 伪装，**走代理池**——复用 fetch_fund_nav 的代理调用模式，失败重试一轮直连）
- spark：腾讯 minute 仅对 QQQ/SPY/SOXX/HSI；Yahoo 项用 adjclose 日线
- 单项失败不致命；收益率类（US30Y/US10Y）price 保留 3 位小数（前端再 fmt）

- [ ] **Step 4: server 接线**

- 全局 `market_cache: list[dict] = []`；新协程 `_market_loop()`：60s 周期 `market_cache = await pf.fetch_market_panel()`，异常钳制 120s，启动挂到 lifespan（与 _portfolio_loop 并列）。
- `/api/portfolio/overview` 响应追加 `"market": market_cache`。

- [ ] **Step 5: 测试+编译+Commit**

```bash
python3 -m pytest tests/ -q && python3 -m py_compile portfolio_fetcher.py server.py
git add portfolio_fetcher.py server.py tests/test_portfolio.py
git commit -m "feat(market): 市场参考指标看板(QQQ/SPY/SOXX/GLD/布油/TLT/恒指)+分时sparkline"
```

---

### Task 2: 持仓页前端整体重做（券商风+分时基线+排序+市场看板+移动端）

**Files:**
- Rewrite: `static/portfolio.html`（单文件重写，保留既有 API 协议与 esc/patchFetch/编辑器逻辑骨架）

**设计规范（对标主流券商，红涨绿跌保持）：**

- [ ] **Step 1: 新主题 CSS 变量与布局**

```css
:root {
  --bg: #0d1017;          /* 页面近黑 */
  --card: #161a23;        /* 卡片 */
  --card2: #1d2230;       /* 次级/表头 */
  --line: #262c3a;        /* 分隔线 */
  --txt: #e8ecf3; --txt2: #9aa3b5; --txt3: #5c6675;
  --up: #f04a5e; --down: #2ebd85;   /* 红涨绿跌 */
  --accent: #f04a5e;
}
body { background: var(--bg); color: var(--txt); font-size: 14px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", sans-serif; }
```

卡片圆角 12px、无边框改用 `box-shadow: 0 1px 0 var(--line)` 或细边框；数字用 `font-variant-numeric: tabular-nums`；顶部改为「账户总览大卡」：总资产 28px 粗体 + 今日盈亏 + 持仓盈亏 + 现金三列并排（参考众安银行卡片：单张大卡内三段指标，而非三张小卡）。

- [ ] **Step 2: HTML 结构**

```
header（导航：返回监控 | 我的持仓 | session徽章 | 编辑 | 时间/汇率/登出）
账户总览大卡：总资产(大) 昨收(小) | 今日盈亏(参考*) | 持仓盈亏 | 现金
市场看板网格：8 个小方块（QQQ/SPY/SOXX/GLD/布油/TLT/恒指/USDHKD），券商行情卡样式
  每块：名称+现价+涨跌幅(红绿) + 底部 60x22 sparkline canvas（无 spark 数据则隐藏）
  桌面 grid 4 列、平板 3 列、移动 2 列（或横向滑动）
图表卡：tab(当日分时/1月/3月/全部) + canvas + 图表信息条(昨收→现价)
持仓列表卡：桌面 table(表头可点排序) / 移动端卡片列表
编辑弹窗（保留现有功能：行内编辑/新增/扣现金/现金编辑）
```

- [ ] **Step 3.5: 市场看板渲染**

```js
// renderOverview 内：renderMarket(d.market || [])
function renderMarket(list) {
  const box = document.getElementById('marketGrid');
  box.innerHTML = '';
  list.forEach(m => {
    const card = document.createElement('div');
    card.className = 'mkt-card';
    card.innerHTML = `<div class="mkt-head"><span>${esc(m.name)}</span>
      <span class="${cls(m.change_pct)}">${sign(m.change_pct)}${fmt(m.change_pct)}%</span></div>
      <div class="mkt-price ${cls(m.change_pct)}">${fmt(m.price, m.price>1000?1:2)}<em>${m.currency==='HKD'?' HKD':''}</em></div>
      <canvas class="mkt-spark" width="220" height="44" ${m.spark&&m.spark.length>2?'':'style="display:none"'}></canvas>`;
    box.appendChild(card);
    if (m.spark && m.spark.length > 2) drawSpark(card.querySelector('canvas'), m.spark, m.change_pct >= 0);
  });
}
function drawSpark(cv, data, up) {
  const ctx = cv.getContext('2d'), W = cv.width, H = cv.height;
  const mn = Math.min(...data), mx = Math.max(...data), rg = (mx - mn) || 1;
  const px = i => 4 + (W - 8) * i / (data.length - 1);
  const py = v => 6 + (H - 12) * (1 - (v - mn) / rg);
  ctx.strokeStyle = up ? '#f04a5e' : '#2ebd85'; ctx.lineWidth = 2; ctx.beginPath();
  data.forEach((v, i) => i ? ctx.lineTo(px(i), py(v)) : ctx.moveTo(px(i), py(v)));
  ctx.stroke();
}
// USD/HKD 方块数据从 overview.fx_usdhkd 拼：{key:'USDHKD', name:'美元/港元', price:fx, change_pct:null→隐藏涨跌}
```

- [ ] **Step 3: 分时图改为昨收基线（核心）**

```js
// intraday 数据映射改为 p（当日盈亏），日线保持 v（总资产）
chartData = chartMode === 'intraday'
  ? (d.points || []).map(p => ({x: p.ts, y: p.p}))
  : (d.days || []).map(p => ({x: p.date, y: p.v}));
```

drawIntraday 关键逻辑：
- 基线 y=0（昨收）画中位虚线 `#5c6675`；y 域取 `max(|p|)` 对称：`ymax = max(...ys.map(Math.abs), 1e-9); ymin = -ymax;`（0 居中）
- 左轴盈亏额 `±$xxx`，右轴相对昨收百分比 `±x.xx%`（昨收总资产 `baseTotal` = 最新点 `v - p` 或 overview 的 `base_value + cash_usd`，从 refresh() 时缓存全局 `baseTotalCache`）
- 曲线颜色按最后点正负 `--up/--down`；曲线与基线间半透明面积填充（`rgba(240,74,94,.15)` / `rgba(46,189,133,.15)`，用 ctx.closePath 到基线再 fill）
- 图表信息条：`昨收 $xxx → $yyy (+$p / +x.x%)`
- drawDaily：保持现有绝对值曲线，首值虚线基准 + 右轴加区间百分比（`(v-v0)/v0`）

- [ ] **Step 4: 表头排序**

```js
let sortKey = 'value', sortDir = -1;   // 默认市值降序
const SORTABLE = {price:1, change_pct:1, value:1, today_pnl:1,
                  position_pnl:1, position_pnl_pct:1, cost_price:1, quantity:1};
function sortBy(key) {
  if (!SORTABLE[key]) return;
  sortDir = (sortKey === key) ? -sortDir : -1;
  sortKey = key; renderPositions(lastOverview);
}
// thead: <th onclick="sortBy('value')">市值 <span class="arrow" id="ar_value"></span></th>
// renderPositions 内排序：
positions.sort((a,b) => {
  const va = a[sortKey] ?? -1e18, vb = b[sortKey] ?? -1e18;
  return (va - vb) * sortDir;
});
// 排序后刷新箭头：active 的 th 显示 ▲(dir=1)/▼(dir=-1)
```

null 值（stale 行）排最后：`?? -1e18`（降序时自然沉底，升序时在前——可接受）。

- [ ] **Step 5: 盘前盘后参考盈亏（致富风格）**

- session ∈ pre/post 时：session 徽章文案「盘前」「盘后」；总览卡今日盈亏标签追加 `(参考)`；持仓行美股 `ext==='pre/post'` 的 tag 按 session 显示 `盘前` / `盘后`（底色 pre 用 `#3d5a8a`、post 用 `#8a6d3d`）。
- 持仓行该股 quote_time 旁显示扩展时段时间（现有逻辑已用 ext_time）。

- [ ] **Step 6: 移动端适配（≤768px）**

```css
@media (max-width: 768px) {
  .content { padding: 12px; }
  .overview-card .metrics { flex-direction: column; gap: 10px; }
  table.holdings { display: none; }
  .mobile-list { display: block; }
  .modal { width: 100%; height: 100%; max-height: none; border-radius: 0; }
  .chart-info { flex-wrap: wrap; }
}
```

- 移动端持仓卡片（与 table 同数据源双渲染，`renderPositions` 同时填两者）：

```html
<div class="m-card">
  <div class="m-top"><span class="m-name">英伟达 <tags></span>
    <span class="m-price">222.40 <em class="up">+1.2%</em></span></div>
  <div class="m-bottom">
    <span>市值 <b>$xxx</b></span><span>今日 <b class="up">+$x</b></span><span>持仓 <b class="up">+$x</b></span>
  </div>
</div>
```

- header 紧凑：隐藏 fxInfo 文案为图标行、按钮改小；canvas resize 已有（保留 `window.addEventListener('resize', drawChart)`）。
- 编辑弹窗表格横向滚动：`.modal table { display: block; overflow-x: auto; }`

- [ ] **Step 7: 保留既有安全与交互**

- `esc()` 双引号转义、`patchFetch` 401 跳转、删除确认、reopenEditor 确认逻辑全部保留。
- 空数据/加载态：positions 为空显示「暂无持仓，点击右上角编辑添加」。

- [ ] **Step 8: 本地验证**

Run: `node --check`（提取 script 段）或 `python3 -c` 简单解析闭合；用本地 server（若有 DB）`python3 -m uvicorn server:app --port 8000` + 造 token 后 curl 验证页面 200 与 JS 无语法错误（浏览器无法自动化时以 node --check 为准）。

- [ ] **Step 9: Commit**

```bash
git add static/portfolio.html
git commit -m "feat(ui): 持仓页券商风重做：分时昨收基线+表头排序+盘前后参考盈亏+移动端适配"
```

---

### Task 3: 评审修复 + 部署上线

**Files:** 按评审结论修复；服务器 /opt/qdii。

- [ ] **Step 1: 质量评审**（code-reviewer subagent：分时基线口径、排序 null 处理、移动端双渲染一致性、sessions DB 化的并发/过期清理、XSS 保留完整性）
- [ ] **Step 2: 修复评审问题并全量测试** `python3 -m pytest tests/ -q`
- [ ] **Step 3: 部署**：`rsync -az --delete --exclude='.git' --exclude='.codegraph' --exclude='venv' --exclude='__pycache__' --exclude='akshare-code' --exclude='juejin' --exclude='StockWatcher' --exclude='*.pyc' ./ root@<server-ip>:/opt/qdii/ && ssh root@<server-ip> "systemctl restart qdii"`
- [ ] **Step 4: 公网验证**（用 make_token 造长效 token）：
  - `/api/captcha` 200
  - `/api/portfolio/overview` 200 且 summary 字段完整
  - `/api/portfolio/intraday` points 含 p
  - 过期 token → 401
  - 页面 `/static/portfolio.html` 200
- [ ] **Step 5: Commit（若有修复）+ 反馈用户新功能说明**

---

## Self-Review 结论

- 需求覆盖：分时基线(Task2 Step3)、排序(Step4)、UI 重做(Step1-2)、盘前后参考盈亏(Step5)、移动端(Step6)、token 表存+过期校验(Task1)、部署(Task3) —— 全覆盖。
- 类型一致性：`create_session(token, username, ttl)` / `get_session(token)->dict|None` / `delete_session(token)` 在 Task1 内自洽；前端字段与 overview/intraday 现有返回一致。
- 无占位符：排序/分时/移动端均给出可落地代码。
