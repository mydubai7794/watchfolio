# 纯 HTTP 接口替代 akshare — 最终结论

## 背景

代理不支持 HTTPS CONNECT 隧道，akshare 底层全部走 HTTPS（东财 API），导致抓取失败。
需改用纯 HTTP 接口。

## akshare 底层实现分析

| akshare 函数 | 底层 URL | 协议 | 问题 |
|---|---|---|---|
| `fund_etf_spot_em` | `https://88.push2.eastmoney.com/api/qt/clist/get` | **HTTPS** | 需分页，每页 100 条，IOPV 在字段 `f441` |
| `futures_global_spot_em` | `https://futsseapi.eastmoney.com/list/COMEX,...` | **HTTPS** | 需分页，每页 20 条，622 个合约 |

两个接口纯 HTTP 均不可用（connection reset），必须走 HTTPS。

## 替代方案 (已验证可用)

### 1. ETF 行情 — 腾讯 HTTP 接口 (一站搞定)

**URL:** `http://qt.gtimg.cn/q=sz159561,sh513400,sz159941,sz159660,sh513100`
**协议:** 纯 HTTP，无需 headers，GBK 编码
**格式:** `v_sz159561="字段0~字段1~...~字段87"` (88 个字段，`~` 分隔)

| 索引 | 字段 | 示例 (sz159561) |
|------|------|---------|
| `[1]` | 名称 | 德国ETF嘉实 |
| `[2]` | 代码 | 159561 |
| `[3]` | 当前价 | 1.314 |
| `[4]` | 昨收 | 1.320 |
| `[9]` | 买一价 | 1.314 |
| `[10]` | 买一量(手) | 16457 |
| `[19]` | 卖一价 | 1.315 |
| `[20]` | 卖一量(手) | 1713 |
| `[32]` | 涨跌幅(%) | -0.45 |
| `[78]` | **IOPV 实时参考净值** | **1.3126** |
| `[80]` | IOPV 涨跌(元) | -0.08 |
| `[81]` | 昨日净值 | 1.3058 |

溢价率 = (price - iopv) / iopv × 100

### 2. 期指行情 — 新浪 HTTP 接口

**URL:** `http://hq.sinajs.cn/list=hf_NQ,hf_ES,hf_YM,hf_DAX,hf_NK`
**协议:** 纯 HTTP，**必须** `Referer: http://finance.sina.com.cn`，GBK 编码
**格式:** `var hq_str_hf_NQ="字段0,字段1,...,字段14"` (15 字段，逗号分隔)

| 索引 | 字段 | 示例 (hf_NQ) |
|------|------|------|
| `[0]` | 最新价 | 26468.480 |
| `[7]` | 昨结算价 | 26487.250 |
| `[12]` | 日期 | 2026-04-17 |
| `[13]` | 合约名 | 纳斯达克指数期货 |

涨跌幅 = (price - prev_settle) / prev_settle × 100

**代码映射:**
| key | 新浪代码 | 合约名 |
|-----|---------|--------|
| NQ | hf_NQ | 纳斯达克指数期货 |
| ES | hf_ES | 标普500指数期货 |
| YM | hf_YM | 道琼斯指数期货 |
| DAX | hf_DAX | DAX (欧盘时段有数据) |
| NK | hf_NK | 日经225指数期货 |

### 3. 备用 IOPV — 新浪专用接口 (交叉验证用)

**URL:** `http://hq.sinajs.cn/list=sz159561_iopv,sh513400_iopv,...`
**Headers:** `Referer: http://finance.sina.com.cn`
**格式:** `var hq_str_sz159561_iopv="2026-04-17,11:30:00,1.3126"`

与腾讯 `[78]` 完全一致 (偏差 0.000%)，仅作备用。

## 交叉验证结果

| ETF | 腾讯[78] | 新浪 _iopv | 偏差 |
|-----|---------|-----------|------|
| sz159561 | 1.3126 | 1.3126 | 0.000% |
| sh513400 | 1.2083 | 1.2083 | 0.000% |
| sz159941 | 1.3555 | 1.3555 | 0.000% |
| sz159660 | 1.9709 | 1.9709 | 0.000% |
| sh513100 | 1.8050 | 1.8050 | 0.000% |

## 腾讯外盘期指（2026-10-08 修订）

早期测试 `usNQ`, `NQmain`, `hf_NQ`, `.IXIC`, `.DJI`, `NDX` 等格式均
`v_pv_none_match`——**那是代码前缀没找对，不是腾讯无此能力**：正确前缀是
`fu`（`fuNQ`/`fuES`/`fuYM`/`fuNIY`=CME日经，实测实时，f[0]="delay" 为外盘
分类标记非延迟）。2026-10-08 起期指主源已切腾讯 fu 前缀（搭总线同批），
新浪 hf_ 降为深冷备。fuDAX 无返回（DAX 三源均缺系存量状态）。

## 最终方案：每轮 1~2 次 HTTP 请求（2026-10-08 修订）

1. `GET http://qt.gtimg.cn/q=...` → ETF + 全球指数 gz + 期指 fu 一批全出
2. （仅腾讯失败时降级）`GET https://futsseapi.eastmoney.com/list/COBOT` → 期指
