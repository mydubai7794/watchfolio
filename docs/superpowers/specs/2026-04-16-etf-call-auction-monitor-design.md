# A 股跨境 ETF 集合竞价微观盘口套利监控系统 - 设计文档

## 1. 业务背景

针对 A 股跨境（QDII）ETF 的早盘集合竞价末期（09:20-09:30）微观盘口监控工具。通过捕捉定价错误和情绪不对称，实施"吃 Tick"套利——在竞价末期以开盘价买入 T+0 跨境 ETF，开盘后价格上涨 1 个 Tick 即卖出锁利。

核心策略：不判断趋势，猎杀定价错误。寻找同时具备强势买盘（安全垫）、低溢折率、期指情绪确认的标的。

## 2. 架构方案：FastAPI + SSE

**技术栈**：Python 3.12 + FastAPI + akshare + httpx[socks] + aiomysql

后端 FastAPI 单进程，前端极简 HTML/JS 通过 SSE（Server-Sent Events）接收服务端推送，无轮询间隔浪费。

用户手动启停监控，竞价特有逻辑（期指风控杀分）作为可选项由用户在 UI 上开关。

## 3. 项目结构

```
qdii/
├── .mcp.json          # MCP 数据库连接配置
├── config.py          # 全局配置（权重、阈值、DB 连接）
├── database.py        # MySQL 读写（ETF 池、代理池、配置）
├── fetcher.py         # 数据抓取（akshare + httpx + 代理轮询）
├── scorer.py          # 得分计算引擎
├── server.py          # FastAPI 主服务（SSE + REST API）
├── static/
│   └── index.html     # 单页面看板（HTML + 内联 CSS/JS）
└── requirements.txt   # 依赖清单
```

| 模块 | 职责 | 依赖 |
|------|------|------|
| `config.py` | 从 DB 加载配置，提供全局默认值 | database.py |
| `database.py` | ETF 增删查、代理池管理、键值配置 | aiomysql |
| `fetcher.py` | 并发抓取盘口 + 期指 + IOPV，代理轮询 | akshare, httpx, database.py |
| `scorer.py` | 得分计算、溢折率、期指风控 | fetcher.py 输出的原始数据 |
| `server.py` | SSE 推送 + REST 管理 API + 静态文件 | FastAPI, 所有模块 |
| `static/index.html` | 看板 UI + 代理管理面板 + ETF 管理 | 浏览器原生 JS |

## 4. 数据库设计

MySQL 连接：`<server-ip>:3914/<db-user>/qdii`

### 4.1 表 `etf_targets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | VARCHAR(6) PK | ETF 代码，如 `513100` |
| `name` | VARCHAR(20) | 名称，如 `纳指ETF` |
| `futures_index` | VARCHAR(20) | 关联期指标识，如 `NQ`、`DAX`、`YM` |
| `prev_nav` | DECIMAL(6,4) | 前一日净值（IOPV 估算兜底用） |
| `enabled` | TINYINT(1) DEFAULT 1 | 是否启用监控 |
| `created_at` | DATETIME | 创建时间 |

### 4.2 表 `proxy_pool`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | INT AUTO_INCREMENT PK | 主键 |
| `url` | VARCHAR(100) UNIQUE | 代理地址，如 `socks5://1.2.3.4:1080` |
| `latency_ms` | INT | 最近一次检测延迟（毫秒） |
| `alive` | TINYINT(1) DEFAULT 1 | 是否存活 |
| `last_check` | DATETIME | 最近一次存活检测时间 |
| `created_at` | DATETIME | 创建时间 |

### 4.3 表 `sys_config`

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | VARCHAR(50) PK | 配置键 |
| `value` | VARCHAR(200) | 配置值（JSON 字符串或标量） |
| `remark` | VARCHAR(100) | 备注说明 |

预设配置项：
- `w_ratio` = `0.7`（买/卖比权重）
- `w_premium` = `0.3`（溢价率权重）
- `futures_kill_threshold` = `-1.5`（期指跌幅杀分阈值 %）
- `safe_req_per_min` = `20`（单 IP 安全请求频率/分钟）

## 5. 数据流

### 5.1 运行时循环

```
用户点击"开始监控" → POST /api/monitor/start
        │
        ▼
  启动 asyncio background task:
  loop:
    1. 从 DB 加载 ETF 池 + 代理池
    2. 计算刷新间隔:
       - 单 IP 最低间隔 3 秒（per-IP 限制）
       - N 个代理可并发 N 路请求，整体间隔 = 单轮耗时 / 可并行度
    3. fetcher 批量抓取盘口数据（优先单次批量接口，降级逐个请求）
    4. fetcher 抓取期指数据
    5. scorer 计算得分
    6. 结果推入 SSE 队列
    7. await asyncio.sleep(interval)
```

### 5.2 代理轮询与频率控制

**频率计算公式**：

- 单 IP 安全频率：`safe_req_per_min`（默认 20 次/分钟，即最低 3 秒/次）
- 批量接口场景（推荐）：一次请求拿全量 ETF 数据，每轮只需 1 次请求
  - 单 IP：interval = 3 秒
  - N 个代理轮询：interval = 3 / N 秒（每个 IP 仍满足 3 秒限制）
- 逐个请求场景（降级）：M 只 ETF 需要 M 次请求
  - N 个代理轮流：interval = max(M * 3 / N, 3) 秒

**关键约束**：每个 IP 相邻两次请求间隔不低于 3 秒。系统整体刷新间隔取决于总请求数和可用代理数的比值。

### 5.3 SSE 事件

| event 类型 | 触发时机 | payload |
|------------|---------|---------|
| `snapshot` | 每轮抓取完成 | 全部 ETF 得分列表 JSON 数组 |
| `status` | 监控启停、代理状态变化 | `{running, proxy_count, interval_sec, timestamp}` |
| `error` | 某标的抓取失败 | `{code, message}` |

### 5.4 REST API

| 方法 | 路径 | 用途 |
|------|------|------|
| GET | `/api/stream` | SSE 推送连接 |
| POST | `/api/monitor/start` | 开始监控 |
| POST | `/api/monitor/stop` | 停止监控 |
| GET | `/api/etf` | 获取 ETF 列表 |
| POST | `/api/etf` | 新增 ETF |
| DELETE | `/api/etf/{code}` | 删除 ETF |
| GET | `/api/proxy` | 获取代理池 |
| POST | `/api/proxy` | 新增代理 |
| DELETE | `/api/proxy/{id}` | 删除代理 |
| POST | `/api/proxy/{id}/check` | 手动检测代理存活 |
| GET | `/api/config` | 获取配置 |
| PUT | `/api/config/{key}` | 更新配置项 |

## 6. 得分模型

### 6.1 核心公式

```
Score = W1 * normalize(Ratio) - W2 * normalize(Premium)
```

默认 W1=0.7, W2=0.3，可通过 `sys_config` 调整。

### 6.2 Ratio（买/卖强度比）

```
Ratio = 买一量 / max(卖一量, 1)
```

跨标的 Min-Max 归一化到 [0, 1]。

### 6.3 Premium（溢折率）

```
Premium = (当前价 - IOPV) / IOPV * 100
```

IOPV 获取策略（混合方案）：
1. 优先：akshare 实时 IOPV
2. 兜底：`prev_nav * (1 + 关联期指涨跌幅)`
3. UI 标注来源（`IOPV` / `EST`）

归一化后取反——折价得分高，高溢价得分低。

### 6.4 期指风控（用户可开关）

条件：`关联期指涨跌幅 < futures_kill_threshold`（默认 -1.5%）且 `ETF 涨幅 > 0`
→ Score 直接置 0。

### 6.5 特殊情况

| 场景 | 处理 |
|------|------|
| 卖一量=0（涨停封板） | Ratio=999，综合评判 |
| 买一量=0 | Score 置 0，前端标红 |
| 期指数据缺失 | 风控逻辑跳过，不杀分 |
| IOPV 和期指都缺失 | Premium=0 兜底 |

## 7. Web UI

### 7.1 页面结构

单页面三个区域（Tab 切换）：

**Tab 1：监控看板**（默认主页面）

顶部状态栏：`[开始监控] [停止监控] 竞价风控: [开/关] ● 连接状态 Ns/轮`

表格列：代码 | 名称 | 得分 | 当前价 | 涨幅% | 溢价% | 买一量(手) | 卖一量(手) | 买/卖比值 | 期指走势

按 Score 倒序排列。

视觉突出规则：
- **Top 3 且 Score > 70**：深绿底色 + 白色粗体 + 得分加大字号
- **Score > 50**：浅绿底色
- **Score = 0（被杀分）**：灰色底色 + 删除线
- **买一量=0**：整行标红

溢价列标注：实时 IOPV 直接显示百分比，估算值后缀 `(估)`。

**Tab 2：代理管理**

表格展示所有代理（地址、延迟、存活状态），支持增删和手动存活检测。底部显示"当前可用数"和"理论刷新间隔"。

**Tab 3：ETF 标的池管理**

右上角"管理标的"按钮弹出模态框，展示已配置 ETF 列表，支持新增（输入代码自动补全名称）、删除、临时禁用。

### 7.2 技术实现

- 纯原生 JS，不引入前端框架
- SSE：`new EventSource('/api/stream')`
- 表格更新：收到 `snapshot` 后 `innerHTML` 整表重绘
- 断线自动重连

## 8. 容错与工程

| 场景 | 处理 |
|------|------|
| 单只 ETF 抓取失败 | 跳过，不阻塞本轮，SSE 推送 error |
| 代理请求超时/失败 | 标记不可用，切下一个代理重试 |
| 全部代理失效 | 降级用本机直连，推送警告 |
| SSE 连接断开 | 前端 EventSource 自动重连 |
| akshare 接口异常 | 捕获异常，日志记录，UI 显示"数据源异常" |
| MySQL 连接断开 | aiomysql 连接池自动重连 |

日志：Python `logging` 模块，输出到 stdout。记录每轮刷新耗时、代理状态变化、得分异常。

依赖：
```
fastapi
uvicorn
aiomysql
akshare
httpx[socks]
```

部署：`pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple && python server.py`，浏览器打开 `http://localhost:8000`。
