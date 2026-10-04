# 长桥 OpenAPI 踩坑记录（2026-10-04 新平台迁移实战复盘）

> 渐进式披露：先查 §1 速查表，命中症状再看 §2 对应小节；§3 是方法论。
> 改 `longport_fetcher.py` 前必读；排查 401004 / 换 token / 换 app / 指数取数前必读。

## §1 TL;DR 速查表

| 症状 | 根因 | 修复 |
|------|------|------|
| 401004 token invalid（换新 token 后才出现） | token 转录丢字符（1086→1085，JWT 签名段损坏） | **勿手工转录**：`wc -c` 验长度 + JWT 三段 base64 可解码；token 只在服务器文件/库间管道流转 |
| 401004（新 app hk_ 新号段 + 默认域名） | 新 app 仅在新平台有效，旧 SDK 默认选旧平台域名 | 显式 `http_url=https://openapi.longbridge.cn` |
| 401004（模拟盘身份 token） | 行情网关拒绝模拟盘身份 | 长桥后台重新生成，选**正式账户**身份 |
| 指数 quote 静默空（无报错无数据） | 指数符号必须带 `.US` 后缀（`.NDX` 空、`.NDX.US` 正常） | 用 `.DJI.US/.NDX.US/.SPX.US/.VIX.US`；`.HSI.HK` 恒指也静默空（走腾讯） |
| 标普 500 取不到（.INX 静默空） | `.INX` 是旧平台/雅虎系符号 | 新平台符号是 `.SPX.US` |
| 期权实时行情 301604 no quote access | OPRA 期权行情为付费权限，正式账户也不含 | 期权链（到期日/行权价）免费可查，实时价无权限——不要反复重试 |
| pip 装 `longport` 装不到"最新版" | 包已改名 `longbridge` | `pip install longbridge`；旧名 `longport` 仍可用（3.0.18+） |
| `Config(..., enable_overnight=True)` TypeError（新 SDK） | longbridge 新 SDK 无该参数 | 按包名分支构造；新 SDK 夜盘价默认可用 |
| token 90 天到期只能手动换？ | 旧结论（旧 SDK 无 refresh）已过时 | longbridge SDK `Config.refresh_access_token()` 默认续 90 天；生产已接 `maybe_refresh_token()` 自动续期 |
| 境内服务器连 openapi.longbridge.com 失败 | .com 境外主站不可达 | 用境内加速 `openapi.longbridge.cn`（.com/.cn 鉴权互通） |

## §2 分主题复盘

### 2.1 新旧平台与域名体系（2026-10 实测）

| 平台 | HTTP | 行情 WS | 新 app（hk_ 新号段） | 旧 app |
|------|------|---------|------------------|--------|
| 新 | `https://openapi.longbridge.com` / **.cn 加速** | `wss://openapi-quote.longbridge.cn/v2` | ✅ | ✅（双平台通用） |
| 旧 | `https://openapi.longportapp.cn` | `wss://openapi-quote.longportapp.cn/v2` | ❌ 401004 | ✅ |

- .com 与 .cn 鉴权互通；境内服务器**必须用 .cn**。
- 项目现状：`longport_fetcher.py` 顶部常量 `LONGBRIDGE_HTTP_URL/LONGBRIDGE_QUOTE_WS_URL` 钉死新平台 .cn，两个 SDK 都显式传参覆盖默认域名。

### 2.2 双 SDK 差异（longbridge 新 / longport 旧）

| 维度 | longbridge（新） | longport（旧） |
|------|-----------------|----------------|
| Config 签名 | `(app_key, app_secret, access_token, http_url, quote_ws_url)`，**无 enable_overnight** | 同名参数 + `enable_overnight=True` 开夜盘价 |
| token 续期 | `Config.refresh_access_token(expired_at=None) -> str`（默认续 90 天，Legacy API Key 模式） | ❌ 无此方法 |
| QuoteContext/quote/candlesticks | 同名同出参（SecurityQuote 字段一致） | 同左 |

项目采用 `longbridge` 优先导入、`longport` 兜底（`_SDK_NAME` 标记），`_SDK_HAS_REFRESH` 控制自动续期开关。

### 2.3 token 排查铁律（401004 九成是它）

1. **完整性**：`echo -n "$TOKEN" | wc -c` 对长度；JWT 三段分别 base64 可解码（payload 含 `exp`）。转录丢一个字符→签名段损坏→401004，且日志看不出与"token 过期"的区别。
2. **平台匹配**：新 app 走旧平台域名=401004；先确认 Config 的 http_url。
3. **身份**：模拟盘 token 被行情网关拒（正式账户身份生成）。
4. **流转纪律**：token 只在「服务器文件 → SQL/库」管道内流转，不经过聊天窗口手工复制（本项目 401004 事故即第 916 字符丢 `6`）。
5. 自动续期链路：`maybe_refresh_token()`（最后 7 天触发、6h 节流、成功写回 `sys_config.LONGPORT_ACCESS_TOKEN` 热加载）；状态看 `/api/datasource/status` 的 `token_refresh` 字段。

### 2.4 行情权限矩阵（正式账户 hk_f2b5 实测）

| 品种 | 结果 |
|------|------|
| 美股现货 LV1 + pre/post 扩展价 + 夜盘 | ✅ 免费永久 |
| 港股 / A股 ETF | ✅（体验套餐） |
| 指数 `.DJI/.NDX/.SPX/.VIX.US` | ✅（**必须 .US 后缀**，缺后缀静默空） |
| `.HSI.HK` 恒指 | ❌ 静默空（项目恒指走腾讯 hkHSI） |
| OPRA 美股期权实时行情 | ❌ 301604（期权链 35 到期日/282 档行权价免费，合约如 `QQQ260930C405000.US`） |
| US30Y/US10Y 收益率、商品期货 BZ/GC、DXY、外汇 | ❌ 无品种（项目分别走 Yahoo/腾讯/新浪/TD） |

### 2.5 静默空陷阱

符号无权限/不存在时 quote **不报错、直接不返回该 symbol**——调用侧若不核对返回集合就会"静默缺数"。判定方法：单符号逐个试，返回空=符号错或无权限；带 `.US` 后缀再试一次。

### 2.6 限流契约（沿用，两 SDK 一致）

官方 1s≤10 次、并发≤5、单账号一条长连接。项目侧：`_LP_SEM=Semaphore(2)` + `_sdk_call` wait_for 15s 超时废弃 ctx；日K锚点逐只 4s 摊开；批量请求主动分散到 60s 内。

## §3 排查方法论

- **双变量对照**：换 token 后报错，先用旧 token 复跑同代码（旧 token 通→新 token 问题；旧 token 也挂→代码/平台问题）。本次事故旧凭证在新平台全通，一轮锁定 token 本身。
- **最小复现**：SDK 异常先退到裸 `curl`/单文件脚本（服务器 `/home/ubuntu/lb_vps_test.py` 即验证脚本），排除业务代码干扰。
- **错误码优先**：401004 只说明"鉴权失败"，不区分过期/损坏/身份/平台——按 §2.3 顺序逐项排除，别急着换 token 重试。
- **结论要带平台上下文**：「.INX 静默空」是旧平台结论，新平台 `.SPX.US` 可用——写文档时标注结论的适用域，防误导后人。
