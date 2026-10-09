# Spec：敏感信息审计结论与密钥配置化改造（提交前过滤）

- 日期：2026-10-09
- 作者：yys
- 状态：待实施（本 spec 只含审计结论与方案，尚未改任何代码）
- 范围：全仓库（已跟踪文件 + 待提交未跟踪文件 + 本地 git 历史）

---

## 一、背景与目标

代码与库需要提交（当前仓库 **无 remote，纯本地历史**，尚可安全做历史清洗）。
目标：

1. 工作区代码/文档**零明文密钥**：所有凭据改为「环境变量 / MySQL sys_config / 不入库的本地文件」三级供给；
2. 提交前有**机器可校验**的扫描门禁（scripts/check_secrets.py）；
3. 已入历史的密钥完成**历史清洗**，并按泄露处理**全部轮换**；
4. 改造对线上部署**零影响**（服务器侧先注入环境变量再发版）。

本 spec 文档自身不含任何明文密钥（一律用「首4位+长度+行号」脱敏指纹定位）。

---

## 二、审计结论（现状清单）

### P0-A 代码内真实凭据明文（os.getenv 兜底值）

| # | 位置 | 内容 | 轮换渠道 |
|---|------|------|----------|
| 1 | config.py:8 | MySQL 密码（`4BlB…` 24位，账号 *REMOVED*@***REMOVED***:3914） | 服务器 MySQL 改密 |
| 2 | config.py:5 | MySQL 公网 IP 兜底（基础设施信息） | — |
| 3 | portfolio_fetcher.py:64 | 24h股票行情网关鉴权 key（`Rd84…` 64位，双 header 同值） | 网关侧重置 |
| 4 | portfolio_fetcher.py:69 | twelvedata API key（`5299…` 32位hex） | 官网重置 |
| 5 | fetcher.py:75 | 东财期指 token（`58b2…` 32位hex） | 东财接口参数 |
| 6 | email_trader.py:34-36 | QQ 邮箱账号（个人隐私）+ IMAP 授权码（`ioup…` 16位） | QQ 邮箱重新生成授权码 |
| 7 | longport_fetcher.py:79-95 | 长桥 APP_KEY（`hk_28f7…`）/ APP_SECRET（`d138…` 64位hex）/ ACCESS_TOKEN（`hk_m_eyJ…` JWT，exp≈2027-01） | 长桥后台重置 token |
| 8 | tests/test_longport.py:23-28 | `REAL_TOKEN`：真实 token 的截断变体（同 JWT 头、伪签名，仍暴露账号标识 ak/sid） | 随 #7 轮换后改为纯伪造夹具 |

### P0-B 配置/工具文件明文且被 git 跟踪

| # | 位置 | 内容 |
|---|------|------|
| 9 | .mcp.json:11-14 | MySQL host/user/密码明文（opencode 的 db-analyzer MCP 配置） |

### P0-C 历史泄露（关键事实）

- `git log --all -S` 证实：MySQL 密码已进入 **≥7 个历史提交**（config.py、email_trader.py、AGENTS.md、docs/superpowers/plans/*.md 多份）；
- EQUITY key、IMAP 授权码、长桥凭证同理均在历史中（随各自文件首次提交进入）；
- 结论：**仅清理工作区不够**。仓库一旦推送任何远端即视同泄露，必须做第四节的历史清洗 + 第三节的全量轮换。

### P1-A 文档内嵌基础设施信息与明文命令

| 文件 | 内容 |
|------|------|
| AGENTS.md:135-145 | 部署段含 `ssh root@***REMOVED***`、`mysql -p'明文'`、域名 `***REMOVED***` |
| docs/superpowers/plans/2026-10-01-portfolio-module.md（1467,1487 行等） | `mysql -p'明文'` + 服务器 IP |
| docs/superpowers/plans/2026-10-03-ledger-module.md（1272,1291 行） | 同上 |
| docs/superpowers/plans/2026-10-04-alert-engine-and-longport-push.md（2006,2014,2022,2526 行） | 同上 |
| docs/superpowers/plans/2026-10-04-kline-modal-upgrade.md（996 行） | 同上 |
| docs/superpowers/plans/2026-10-05-alert-filter-agent-api.md（553 行） | 同上 |
| docs/superpowers/plans/2026-10-09-nav-twr.md（594 行） | 同上 |
| docs/api-agent-portfolio.md、docs/系统全景说明.md、docs/数据源请求规模调研与新浪替代方案.md、docs/superpowers/specs/2026-10-04-*-design.md、AGENTS.md 数据源矩阵 | 域名/IP（`*REMOVED*`、`***REMOVED***`、网关域名）为**接口契约类信息**，级别 P1，可统一占位符化 |

### P1-B 个人隐私数据（非密钥但绝不入库）

| # | 位置 | 内容 | 现状 |
|---|------|------|------|
| 10 | monthStatement/ | 10 份银行电子结单 PDF（汇丰/券商月结单） | **未跟踪且未 ignore**，`git add -A` 即入库 |
| 11 | 账户成本盈亏盘点_20261003.md | 个人账户成本/盈亏明细 | **已被 git 跟踪** |
| 12 | .playwright-cli/ | 控制台日志（含域名、会话 401 探测记录） | 未跟踪未 ignore |
| 13 | email_trader.py:34 | 个人 QQ 邮箱地址 | 见 P0-A#6 |

### P2 低危（可保留或顺手处理）

- StockWatcher/src/StockConfig.cs:21 默认 API URL 指向自有域名（接口契约，属参考代码，可保留）；
- `.claude/`、`.idea/`、`qdii.db`、`uvicorn.log`、`akshare-code/`、`juejin/` 已在 .gitignore 且未跟踪 ✓；
- akshare-docs/ 在 ignore 规则加入前已被跟踪（无敏感内容，仅仓库体积问题，非本 spec 范围）；
- 登录口令存 DB、AGENT_API_KEY/MAIL_*_CODE 存 sys_config（**本就是正确范本**，无硬编码）✓。

---

## 三、密钥轮换（必须执行，先于/伴随历史清洗）

上述 #1、#3、#4、#5、#6、#7 全部轮换一次；新值**只**写入：

1. 服务器 `/opt/qdii/qdii.env`（systemd `EnvironmentFile=`，权限 600，root 属主）；
2. MySQL `sys_config`（长桥三凭证、AGENT_API_KEY、MAIL_ADDRESS/MAIL_AUTH_CODE 已有该机制）。

轮换清单：MySQL *REMOVED* 密码 → 24h网关 key → twelvedata key → 东财 token → QQ 授权码（重新生成会使旧码失效）→ 长桥 access token（后台 Reset）。

---

## 四、git 历史清洗（二选一，先 `tar czf /tmp/opencode/qdii-git-backup.tgz .git` 备份）

**方案 A（推荐）**：`git filter-repo --replace-text`（pip install git-filter-repo）

- replacements.txt 内容 = 第三节 6 组密钥的完整明文 → `***REMOVED***`（文件用后即删，不入库）；
- 顺带替换文档中的 `mysql -p'…'` 整段命令与 IP/域名字符串；
- 无 remote，无需协调任何人；清洗后 `git log --all -S<指纹>` 逐项复核为 0 条。

**方案 B**：清理工作区后 `rm -rf .git && git init` 重建首提交（丢历史，最彻底）。

---

## 五、代码改造（目标架构：三级配置供给）

### 5.1 原则

- **代码零兜底明文**：`os.getenv("KEY", "")`，未配置时按模块语义**降级 + 打日志**（数据源缺席回退链已有），不得 fail 整个服务（部署兼容：服务器先注入 env 再发版）；
- **env 变量名统一**：已有的 `QDII_` 前缀保持（QDII_DB_*、QDII_IMAP_PASSWORD、QDII_EASTMONEY_TOKEN），补齐 `QDII_EQUITY_API_KEY`、`QDII_TWELVEDATA_API_KEY`、`QDII_LP_APP_KEY/APP_SECRET/ACCESS_TOKEN`；
- **文档零真实值**：命令示例一律 `-p"$QDII_DB_PASS"` 引用环境变量；IP/域名统一 `<server-ip>`/`<your-domain>` 占位。

### 5.2 改动点清单

| 文件 | 改动 |
|------|------|
| config.py:5-8 | 兜底 IP/密码改空串；新增 `load_env_file()` 可选读取 `/opt/qdii/qdii.env`（KEY=VALUE，# 注释）便于 systemd 之外的启动方式 |
| portfolio_fetcher.py:64,69 | 两 key 兜底改空；`EQUITY_API_KEY` 改读 `QDII_EQUITY_API_KEY`；为空时三只无交易标的回退新浪扩展价（现有缺省路径） |
| fetcher.py:75 | token 兜底改空；为空时东财路径带空参（东财对 token 不强校验，失败自动走降级链） |
| email_trader.py:34-36 | 账号与授权码全部 env 化（`QDII_IMAP_USER`/`QDII_IMAP_PASSWORD`）；为空时邮件轮询跳过并打日志（现有「任一源失败水位不推进」语义不变） |
| longport_fetcher.py:78-95 | 删除 `DEFAULT_APP_KEY/APP_SECRET/ACCESS_TOKEN` 三常量；凭证只从 sys_config（现有热加载）+ env 兜底读取；全空时锚点恢复直接回切 twelvedata（现有路径） |
| tests/test_longport.py:23-28 | `REAL_TOKEN` 改为程序内拼接的纯伪造 JWT（随机 payload + 假签名），不得含真实 ak/sid |
| .mcp.json | `git rm --cached .mcp.json` + 加入 .gitignore；新增 `.mcp.json.example`（同结构、值为占位符）；本机保留真实文件（不入库） |
| AGENTS.md | 敏感行（部署命令、mysql、域名/IP）替换为占位符；真实运维命令移入 `AGENTS.local.md`（gitignore），AGENTS.md 顶部注明「运维敏感值见 AGENTS.local.md」 |
| docs/superpowers/plans/*.md（6 份）、specs 若干 | 所有 `mysql -p'…'` → `mysql -p"$QDII_DB_PASS"`；IP/域名占位符化（历史文件属过程记录，随方案 A 一并 replace-text，工作区不再出现明文） |
| 账户成本盈亏盘点_20261003.md | `git rm --cached` + gitignore（本地保留） |

### 5.3 .gitignore 增补

```
monthStatement/
.playwright-cli/
.mcp.json
AGENTS.local.md
qdii.env
*.local.md
账户成本盈亏盘点*.md
```

### 5.4 部署适配（零影响保障）

1. 服务器先创建 `/opt/qdii/qdii.env`（全部 QDII_* 真实值 + 长桥凭证），`chmod 600`；
2. systemd unit 增加 `EnvironmentFile=/opt/qdii/qdii.env`；
3. AGENTS.md 的 rsync 命令 exclude 列表增加 `--exclude='qdii.env' --exclude='*.local.md' --exclude='.mcp.json'`；
4. 长桥三凭证继续走 sys_config seed（服务器库内已有），env 仅作冷启动兜底。

---

## 六、提交前门禁：scripts/check_secrets.py

新增扫描脚本（本 spec 的验收工具）：

- 输入：工作区全部文本文件（默认）或 `--cached`（暂存区）、`--history`（全历史 blob）；
- 规则：
  1. 已知指纹集（上述 6 组密钥完整明文，存于脚本内 sha256 前 16 位，**不存明文**）；
  2. 通用模式：`hk_[0-9a-f]{16,}`、`eyJhbGciOi`（JWT 头）、32 位 hex 引号串、`password\s*=\s*['"][^'"]{8,}`、`mysql -p'`；
  3. 白名单：tests 内显式 `# fake-secret` 标注行、`.mcp.json.example` 占位符；
- 退出码非 0 即阻断提交；可挂 pre-commit 钩子（可选）。

## 七、实施 Task 划分（可委派 subagent）

| Task | 内容 | 验收 |
|------|------|------|
| T1 | 5.2 代码侧 6 文件改造 + tests 修正 | 基线 pytest 441 全过 |
| T2 | .mcp.json 退跟踪 + example + .gitignore 增补 + 两个隐私 md/PDF 处置 | git status 干净 |
| T3 | AGENTS.md + docs 脱敏（占位符化 + AGENTS.local.md 迁移） | 全库 grep 指纹 0 命中 |
| T4 | scripts/check_secrets.py + 自测用例 | 脚本对现工作区报 0、对注入样本报 1 |
| T5 | 服务器 qdii.env + systemd EnvironmentFile + rsync exclude 更新 + 发版冒烟 | journalctl 无 ERROR、行情/邮件/长桥三链路正常 |
| T6 | 密钥轮换（第三节清单） | 新值生效、旧值失效 |
| T7 | git 历史清洗（方案 A）+ 复核 | `git log --all -S<各组指纹>` 全 0 条 |

## 八、验收标准（DoD）

1. `python3 scripts/check_secrets.py --history` 退出码 0；
2. 全库 grep：DB 密码 / EQUITY key / twelvedata / 东财 token / IMAP 授权码 / 长桥三凭证 / QQ 邮箱 / 服务器 IP / 域名 —— 工作区与历史均 0 命中（IP/域名允许仅存在于占位符说明）；
3. 基线 pytest 441 通过；线上 `systemctl is-active qdii` 且三数据链路（行情、邮件、长桥）journal 无 ERROR；
4. `git status` 中不再出现 monthStatement/、.playwright-cli/、账户盘点 md。
