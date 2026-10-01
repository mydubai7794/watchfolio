# QDII ETF 集合竞价监控系统 - 项目说明文档

## 1. 项目概述

本系统是一个 **QDII ETF 集合竞价实时监控与评分排序工具**，核心目标是捕捉 A 股开盘前（09:20-09:30）集合竞价期间的 ETF 定价偏差机会，辅助交易决策。

### 适用场景

- 利用海外期货指数与 A 股 QDII ETF 的价格偏差进行套利判断
- 在集合竞价阶段通过买卖盘力量对比发现异动标的
- 实时监控 ETF 的溢价/折价率，避免高位接盘

### 技术栈

| 层级 | 技术 |
|------|------|
| 后端框架 | FastAPI + uvicorn |
| 异步运行时 | Python asyncio |
| HTTP 客户端 | httpx（原生支持 SOCKS5 代理） |
| 数据库 | MySQL (aiomysql 连接池) |
| 前端 | 原生 HTML/JS + SSE (EventSource) |
| 数据源 | 腾讯财经 HTTP 接口 + 新浪财经 HTTP 接口 |

---

## 2. 项目结构

```
qdii/
├── config.py          # 全局配置（数据库连接、权重默认值、期指映射）
├── database.py        # MySQL 异步 CRUD（连接池管理、ETF/代理/配置增删改查）
├── fetcher.py         # 数据抓取模块（腾讯 ETF 行情 + 新浪期指行情）
├── scorer.py          # 评分引擎（归一化、加权评分、期货杀跌、排名）
├── server.py          # FastAPI 主服务（SSE 推送 + REST API + 后台监控循环）
├── init_db.py         # 一次性初始化脚本（建表 + 种子 ETF 数据）
├── requirements.txt   # Python 依赖
├── static/
│   └── index.html     # 前端仪表盘（监控看板 + 代理管理 + ETF 管理）
├── tests/
│   └── test_http_apis.py  # HTTP 接口测试
├── docs/
│   └── http-api-research.md  # 数据源接口调研文档
└── akshare-code/      # 早期 akshare 方案参考（已弃用）
```

---

## 3. 设计思路

### 3.1 纯 HTTP 接口替代 akshare

项目初期使用 akshare 库获取行情数据，但在代理环境下存在 HTTPS CONNECT 隧道问题。重构为**纯 HTTP 直连接口**：

- **ETF 行情**：腾讯财经 `http://qt.gtimg.cn/q=sh513100,sz159941,...`
- **期指行情**：新浪财经 `http://hq.sinajs.cn/list=hf_NQ,hf_ES,...`
- 纯 HTTP 协议，无需 HTTPS CONNECT 隧道，代理兼容性好
- 每轮仅 **2 次 HTTP 请求**（ETF 批量 + 期指批量），效率极高

### 3.2 代理轮换与速率限制

```
速率限制维度：(代理IP, 数据源) 二元组
最小请求间隔：4 秒 / (代理, 数据源)
```

- 每个代理对每个数据源独立限速，同一代理可同时请求腾讯和新浪
- 轮询选取已冷却的代理，所有代理冷却时等待最短冷却时间
- 无代理配置时自动降级为直连（同样按数据源独立限速）
- 请求失败自动换代理重试

### 3.3 SSE 实时推送

采用 Server-Sent Events (SSE) 替代轮询：

- 服务端主动推送 `snapshot`、`status`、`error` 三类事件
- 浏览器原生 `EventSource` API 接收，无需 WebSocket 库
- 30 秒心跳保活，断线自动重连

### 3.4 数据流架构

```
┌─────────────────────────────────────────────────────────┐
│                     后台监控循环                          │
│                                                         │
│  ① 从 MySQL 加载 ETF 列表、代理池、评分权重               │
│                    ↓                                     │
│  ② 腾讯 HTTP 接口 → ETF 批量行情（价格、IOPV、盘口）       │
│     新浪 HTTP 接口 → 期指批量行情（当前价、昨结算价）       │
│                    ↓                                     │
│  ③ 合并期指涨跌幅到每只 ETF                               │
│                    ↓                                     │
│  ④ 评分引擎：归一化 → 加权评分 → 期货杀跌 → 排名          │
│                    ↓                                     │
│  ⑤ SSE 广播 snapshot 给所有前端客户端                     │
│                    ↓                                     │
│  ⑥ 睡眠 → 回到 ①                                        │
└─────────────────────────────────────────────────────────┘
```

---

## 4. 计算方法

### 4.1 核心指标

#### 买卖比 (Ratio)

```
Ratio = buy1_vol / max(sell1_vol, 1)
```

- 买一量与卖一量的比值，反映买卖力量对比
- `buy1_vol == 0` 时 Ratio = 0（无买盘，强制评分归零）

#### 溢价率 (Premium)

```
Premium = (price - IOPV) / IOPV × 100%
```

- `price`：ETF 当前价格
- `IOPV`：基金份额参考净值（来自腾讯接口字段 [78]）
- IOPV 为 0 时 Premium = 0（接口未返回参考净值）

#### 委比 (Weibi)

```
Weibi = (总买量 - 总卖量) / (总买量 + 总卖量) × 100%
```

- 总买量 = 5 档买量之和
- 总卖量 = 5 档卖量之和
- 反映整体盘口偏向

#### 期指涨跌幅 (Futures Change)

```
Futures_Change = (current_price - prev_settle) / prev_settle × 100%
```

- `current_price`：期货当前价格（新浪接口字段 [0]）
- `prev_settle`：上一交易日结算价（新浪接口字段 [7]）

### 4.2 评分公式

#### 第一步：Min-Max 归一化

对全量 ETF 分别归一化 Ratio 和 Premium 到 [0, 1]：

```
norm(v) = (v - min) / (max - min)
```

- 所有值相同时统一返回 1.0
- Premium 取负值后再归一化（越小 = 折价越深 = 越好）

#### 第二步：加权评分

```
Score = W_ratio × Ratio_norm - W_premium × Premium_norm
```

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `W_ratio` | 0.7 | 买卖比权重（越大越重视买盘力量） |
| `W_premium` | 0.3 | 溢价率权重（越大越重视折价幅度） |

> 权重可通过 `sys_config` 表在线调整，无需重启。

#### 第三步：特殊处理

1. **无买盘归零**：`buy1_vol == 0` → Score = 0
2. **期货杀跌**：当期指跌超阈值且 ETF 涨幅为正时，Score = 0

```
if futures_kill_enabled
   AND futures_change < threshold (-1.5%)
   AND etf_change_pct > 0
then Score = 0
```

#### 第四步：降序排名

所有 ETF 按 Score 从高到低排列，前端按排名展示。

---

## 5. 数据库设计

### 5.1 etf_targets（监控 ETF 列表）

| 字段 | 类型 | 说明 |
|------|------|------|
| code | VARCHAR(6) PK | ETF 代码 |
| name | VARCHAR(20) | ETF 名称（由接口自动回写） |
| futures_index | VARCHAR(20) | 关联期指标识（NQ/ES/YM/DAX/NK） |
| prev_nav | DECIMAL(8,4) | 上一交易日净值估算 |
| enabled | TINYINT(1) | 1=启用 0=停用 |
| created_at | DATETIME | 创建时间 |

### 5.2 proxy_pool（代理 IP 池）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT PK | 自增主键 |
| url | VARCHAR(100) UNIQUE | 代理地址 |
| latency_ms | INT | 延迟（毫秒） |
| alive | TINYINT(1) | 1=可用 0=不可用 |
| last_check | DATETIME | 最后检测时间 |
| created_at | DATETIME | 创建时间 |

### 5.3 sys_config（系统配置）

| 字段 | 类型 | 说明 |
|------|------|------|
| key | VARCHAR(50) PK | 配置键 |
| value | VARCHAR(200) | 配置值 |
| remark | VARCHAR(100) | 备注 |

**种子配置：**

| key | value | 说明 |
|-----|-------|------|
| w_ratio | 0.7 | 买卖比权重 |
| w_premium | 0.3 | 溢价率权重 |
| futures_kill_threshold | -1.5 | 期指跌幅熔断阈值(%) |
| safe_req_per_min | 20 | 安全请求频率(次/分钟) |

---

## 6. 期货指数与 ETF 示例

### 6.1 支持的期货指数

| 标识 | 名称 | 新浪代码 | 跟踪市场 |
|------|------|----------|----------|
| NQ | 纳斯达克 100 指数期货 | `hf_NQ` | 美股纳斯达克 |
| ES | 标普 500 指数期货 | `hf_ES` | 美股标普 500 |
| YM | 道琼斯工业指数期货 | `hf_YM` | 美国道琼斯 |
| DAX | 德国 DAX 指数期货 | `hf_DAX` | 德国法兰克福 |
| NK | 日经 225 指数期货 | `hf_NK` | 日本东京 |

### 6.2 默认监控 ETF

| 代码 | 名称 | 关联期指 | 交易所 | 跟踪标的 |
|------|------|----------|--------|----------|
| 513100 | 纳指 ETF | NQ | 上交所 | 纳斯达克 100 |
| 159941 | 纳指 ETF | NQ | 深交所 | 纳斯达克 100 |
| 513300 | 纳斯达克 ETF | NQ | 上交所 | 纳斯达克指数 |
| 513500 | 标普 500ETF | ES | 上交所 | 标普 500 |
| 513400 | 道琼斯 ETF | YM | 上交所 | 道琼斯工业 |
| 513030 | 德国 ETF | DAX | 上交所 | 德国 DAX |
| 513520 | 日经 ETF | NK | 上交所 | 日经 225 |
| 513050 | 中概互联网 ETF | NQ | 上交所 | 中概互联网 |

### 6.3 代码前缀规则

ETF 代码在请求腾讯接口时需要加市场前缀：

```
代码首位为 5, 6, 7, 8, 9 → 加 "sh" 前缀（上海）
代码首位为其他数字       → 加 "sz" 前缀（深圳）
```

示例：
- `513100` → `sh513100`（上海）
- `159941` → `sz159941`（深圳）

---

## 7. API 接口

### 7.1 SSE 实时推送

```
GET /api/stream
```

SSE 事件类型：

| 事件 | 用途 |
|------|------|
| `snapshot` | ETF 排名快照（含评分、价格、溢价率、期指涨跌等） |
| `status` | 监控状态（运行中、代理数、刷新间隔等） |
| `error` | 错误消息 |

### 7.2 监控控制

```
POST /api/monitor/start          # 启动监控
POST /api/monitor/stop           # 停止监控
GET  /api/monitor/status         # 查询状态
POST /api/monitor/futures_kill?enabled=true  # 开启/关闭期货杀跌
```

### 7.3 ETF 管理

```
GET    /api/etf                  # 列表
POST   /api/etf                  # 添加（body: code, name, futures_index）
DELETE /api/etf/{code}           # 删除
POST   /api/etf/{code}/toggle?enabled=true  # 启用/停用
```

### 7.4 代理管理

```
GET    /api/proxy                # 列表
POST   /api/proxy                # 添加（body: url）
DELETE /api/proxy/{id}           # 删除
POST   /api/proxy/{id}/check     # 检测连通性
```

### 7.5 系统配置

```
GET /api/config                  # 列表
PUT /api/config/{key}            # 更新（body: value）
```

---

## 8. 前端仪表盘

### 8.1 监控看板

- 顶部操作栏：启动/停止监控、期货杀跌开关、连接状态指示
- 隔夜外盘栏：美股三大指数收盘数据（启动时加载一次）
- ETF 排名表：代码、名称、得分、现价、涨跌幅、溢价率、委比、外盘量、买卖比、期指趋势

**行高亮规则：**
| 条件 | 样式 |
|------|------|
| Top 3 且 Score > 70 | 深绿背景 |
| Score > 50 | 浅绿背景 |
| Score = 0 且 buy1_vol = 0 | 红色背景（无买盘） |
| Score = 0 且 premium > 0 | 灰色+删除线（被杀跌） |

### 8.2 代理管理

- 输入框添加新代理（支持 SOCKS5/HTTP）
- 代理列表：ID、地址、延迟、状态、操作
- 一键检测连通性（通过 httpbin.org 验证）

### 8.3 ETF 管理（弹窗）

- 添加 ETF：输入代码、名称、关联期指
- 列表管理：启用/停用开关、删除

---

## 9. 数据源详情

### 9.1 腾讯 ETF 接口

```
URL:     http://qt.gtimg.cn/q=sh513100,sz159941,...
编码:    GBK
格式:    v_{带前缀代码}="字段0~字段1~...~字段87"
```

关键字段索引：

| 索引 | 内容 |
|------|------|
| [1] | ETF 名称 |
| [3] | 当前价格 |
| [7] | 外盘量 |
| [8] | 内盘量 |
| [9] | 买一价 |
| [10] | 买一量 |
| [19] | 卖一价 |
| [20] | 卖一量 |
| [32] | 涨跌幅(%) |
| [78] | IOPV（基金份额参考净值） |

### 9.2 新浪期指接口

```
URL:     http://hq.sinajs.cn/list=hf_NQ,hf_ES,...
Header:  Referer: http://finance.sina.com.cn
编码:    GBK
格式:    var hq_str_hf_NQ="字段0,字段1,..."
```

关键字段索引：

| 索引 | 内容 |
|------|------|
| [0] | 当前价格 |
| [7] | 上一交易日结算价 |
| [12] | 日期 |
| [13] | 合约名称 |

### 9.3 隔夜美股接口

```
URL:     http://hq.sinajs.cn/list=gb_$dji,gb_$ixic,gb_$inx
符号:    gb_$dji (道琼斯), gb_$ixic (纳斯达克), gb_$inx (标普500)
频率:    启动时查一次
```

---

## 10. 部署与运行

### 环境准备

```bash
pip install -r requirements.txt
```

### 初始化数据库

```bash
python init_db.py
```

建表并写入种子 ETF 和默认配置，幂等执行。

### 启动服务

```bash
python server.py
```

服务默认监听 `0.0.0.0:8000`，浏览器访问 `http://localhost:8000`。

### 配置项

所有配置均支持环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| DB_HOST | ***REMOVED*** | MySQL 地址 |
| DB_PORT | 3914 | MySQL 端口 |
| DB_USER | *REMOVED* | MySQL 用户 |
| DB_PASS | - | MySQL 密码 |
| DB_NAME | qdii | 数据库名 |
