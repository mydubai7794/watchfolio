# K线弹层增强设计（悬浮详情/量能/MACD/KDJ/平移缩放/日月周K/库内分时K）

- 日期：2026-10-04
- 状态：已批准（用户确认方案A + 库内分时 + 量能固定副图+指标切换）
- 作者：yys

## 1. 背景与目标

现有 K线弹层（Task 8）仅 蜡烛图 + 标注线（成本/档位/止损/22日高）+ RSI 副图，range 固定 120 根，无交互。目标升级为专业看图体验：

1. 悬浮十字线 + 信息浮层（开高低收/涨跌幅/量能 + RSI/MACD/KDJ 数值）
2. 拖动平移、滚轮/双指缩放（纯客户端开窗，零请求）
3. 周期切换：日K / 周K / 月K / 分时(5m，仅有数据时显示)
4. 副图：量能固定 + RSI/MACD/KDJ 标签切换

## 2. 数据源与额度约束（硬边界）

- **长桥历史K额度**：按自然月计、月初加满不累计、同标的当月重复请求仅计 1 次；开户档 100 只/月，频次 30s≤60 次（官方文档）。本项目仅 13 只美股消耗历史K额度（每日 07:00 daily_refresh_loop 日线增量）。
- **本功能零新增长桥历史K请求**：周/月K 由 daily_bars 服务端聚合；分时K 由 market_ticks 聚合。额度消耗与现状完全一致。
- 分时严格只用库（用户裁定）：market_ticks 无数据的标的隐藏分时入口；当前仅 QQQ 有 2 天稀疏数据，live 推送切换后自动扩充。
- market_ticks 仅 (symbol, ts, price) 无量能字段 → 分时副图量能以 tick 计数（活跃度近似）。

## 3. 后端设计

### 3.1 daily_bars.py 新增纯函数（与 RSI 同模式，可测试）

- `macd(closes, fast=12, slow=26, signal=9) -> {"dif": [...], "dea": [...], "hist": [...]}`（标准 EMA 算法；勘误：实现采用国内通达信口径——EMA 首值 seed，全序列非 None 的暖机近似值，柱=2×(DIF−DEA)；月K 等短序列下 MACD 为暖机近似，接受该展示口径）
- `kdj(highs, lows, closes, n=9, m1=3, m2=3) -> {"k": [...], "d": [...], "j": [...]}`（RSV→K/D 平滑，J=3K−2D）
- `aggregate_bars(bars, tf) -> bars`：周/月聚合——开=首根开、收=末根收、高=区间最高、低=区间最低、量=区间和；按 ISO 周（周一起始）/自然月分组，日期取组首交易日。
- 单元测试：已知小序列精确断言（MACD/KDJ 手工验算值）、聚合边界（跨月/跨周、单根组、空输入）。

### 3.2 kline 端点扩展（server.py）

`GET /api/kline/{symbol}?tf=day|week|month|5m&range=N`

- day：现状（daily_bars + 全指标序列）
- week/month：服务端 aggregate_bars 后按聚合序列计算 RSI/MACD/KDJ（与前端零重复实现）
- 5m：market_ticks 聚合（UTC ts 按 300s 分桶：开=桶首价、收=桶末价、高/低=桶内极值、量=tick 数）；无 ticks 返回空 bars（前端据此隐藏分时 tab）
- 响应新增 `macd_series`/`kdj_series`（与 rsi_series 同为与 bars 等长的平铺数组，warm-up 为 null）；`volume_mode: "volume"|"ticks"`（分时标记 tick 计数，前端量能轴标签切换）
- marks（成本/档位/止损/22日高）各周期保留（价格位与周期无关）
- range 上限钳制提升至 1000（现有 get_daily_bars limit 钳制放宽）；周/月/分时在服务端由全量日线/ticks 聚合后按 range 截断
- 服务端 60s 内存缓存（symbol×tf）避免同弹层反复打开重算重查（指标计算亚毫秒级，缓存主要省 DB 查询与聚合）

### 3.3 边界

- SKHY(60根)/DRAM(127根) 月K 仅 10-25 根，MACD(26) 大部 null——正常降级，前端空段断线
- 分时跨日多天数据同图展示（x 轴连续桶序，非交易日跳空不补桶）
- Decimal 归一：日线 close 为 DECIMAL，聚合/指标全链 float()（既有坑前例 atr_wilder/_fwd_ret）

## 4. 前端设计（portfolio.html K线弹层重构）

- 周期 tabs：日K/周K/月K/分时（打开弹层先查一次 5m 可用性？否——分时 tab 惰性加载：首次点击才请求，空 bars 则 tab 隐藏并提示）
- 一次拉满（day range=1000），pan/zoom 纯客户端开窗：可见根数 20~全量，双击复位；切周期换数据源（各 tf 独立缓存窗口状态，切回不重拉 60s 内）
- 悬浮：十字线（主图+副图联动）+ 浮层（日期、开高低收、涨跌幅、量/tick数、RSI/MACD/KDJ 三指标数值同屏，无论副图选哪个）
- 副图布局：主图（蜡烛+标注线）→ 量能副图（固定）→ 指标副图（RSI/MACD/KDJ 标签切换；MACD=DIF/DEA 线+柱，KDJ=三线）
- 交互：鼠标拖动平移、滚轮缩放（以指针为锚）；触摸拖动、双指缩放、点按显示浮层；弹层打开即清空 canvas（既有约定）
- 移动端：窄屏副图高度压缩，浮层定位防溢出

## 5. 测试与验收

- 后端：tests/test_daily_bars.py 增 macd/kdj/aggregate 用例；tests/test_alert_api.py 增 kline tf 参数用例（fake db：day/week/month/5m/空分时/缓存）
- 前端：script 提取 node --check；线上人工验收（QQQ 分时/拖动缩放/三指标悬浮）
- 回归：全量 pytest 基线（当前 337）不减
