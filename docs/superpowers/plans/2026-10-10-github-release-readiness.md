# GitHub 发布前准备（2026-10-10）

## 目标

推送 GitHub 前完成：①隐私终确认（git 历史 + 工作区）②功能可用性 ③README ④通用化 ⑤未实现功能盘点 ⑥配置示例统一。执行模型统一委派 `@general-flashx`（glm-5.3-flashx）。

## 已拍板决策（用户 2026-10-10）

1. **ledger_seed.py**（69 笔真实流水）：退跟踪 + gitignore + filter-repo 历史整路径删除 + 另建 `ledger_seed.example.py` 示例骨架；本地原文件保留可继续用。
2. **口径硬编码**（总投入数值 docstring、cny_invested seed 个人拆分明细注释、电费注释）：全部中性化（数值出库、公式保留、测试断言参数值保留但注释去个人化）。
3. **标的清单**（server.py 美股 14 只、init_db.py 种子 ETF 18 只）：不抽配置，README 标注修改位置。

## 任务分解

### Wave1（并行 A/B 两个 flashx）
- **A：ledger_seed 出库 + py 中性化**
  - `git rm --cached ledger_seed.py` + `.gitignore` 加行（本地文件保留）
  - 新建 `ledger_seed.example.py`（E() 辅助 + 少量假数据 + 幂等用法注释）
  - `database.py` cny_invested seed 改空 + 通用注释；`ledger.py` docstring 通用化；`tests/` 电费/总投入数值相关注释中性化（断言数值不动）
  - 全 py 复扫个人事实（真实金额+渠道组合、建行/汇丰/众安+金额、v3 盘点引用）
  - 验收：test_ledger/test_portfolio 全过 + grep 复扫零命中
- **B：非 py 资产 PII 审计 + 未实现盘点**
  - 审计 static/*.html、docs/**、AGENTS.md、根 md、scripts/：姓名/手机号/邮箱/IP域名/金额+渠道/个人叙述，分级处置（隐私必改、叙述中性化、占位符不动）
  - AGENTS.md 模块表 ledger_seed 行同步改为 example 描述
  - 未实现/半自动功能盘点 → `/tmp/opencode/unimplemented-features.md`（众安邮件仅采样、DAX 无源、录入手动、自建网关可选、代理池境内限制等）

### Wave1 评审：主 agent spec 合规审 + 统一 commit

### Wave2
- **主 agent**：`.git` 备份确认后 `git filter-repo --invert-paths --path ledger_seed.py` 第 5 轮历史清洗 + blob 复验（新增模式：流水金额、真名）
- **C（flashx）**：`qdii.env.example`（14+ 键全注释：作用/获取渠道/不配后果）+ `README.md`（简介/功能/模块架构/数据源 key 获取表格/快速开始含初始口令/配置说明/标的修改位置/未实现清单引用 B 报告/测试）

### Wave3
- **D（flashx）功能可用性验证**：全量 pytest（≥468）；冷启动降级演练（无 qdii.env 起服务不炸、有 env 正常，连线上库只读不动）；线上只读回归（journalctl 无 ERROR + API 200）

### Wave4：全局终审
- code-reviewer 全局审 + `check_secrets --history` + PII blob 扫零命中 + 最终检查单（LICENSE 待用户定、密钥轮换重申、可见性建议）

## 风险与回滚
- filter-repo 前 `.git` 全量备份至 `/tmp/opencode/git-backup-final/`；历史清洗不可逆但本地 ledger_seed.py 永久保留
- seed 中性化影响面：grep tests 对 cny_invested/expense 依赖，若有断言依赖默认值需同步改测试（参数化注入）
- README 中 24h数据源/股票行情网关定位为「自建可选增强」，无 key 自动降级新浪扩展价（违禁词规范约束不变）
