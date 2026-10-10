#!/usr/bin/env python3
"""提交前密钥扫描门禁：扫描将进入版本库的文件，命中敏感串/通用密钥模式即阻断。

扫描范围：
- 默认：git 已跟踪文件 + 未跟踪且未被 .gitignore 忽略的文件（即将进入提交的全部内容）；
- --cached：仅暂存区（git diff --cached --name-only）；
- --history：对指纹逐条回溯全历史提交（慢，仅供历史清洗复核）——字面量指纹用
  `git log --all -S<串>`，正则规则用 `git log --all -G<ERE>`（pickaxe 正则版，
  规则须提供 ERE 兼容的历史版正则）。

指纹来源（三保险）：
- 内置分段指纹：已知敏感串离线拆段硬编码——任何单行、任何单一常量都不含完整
  敏感串，运行时以 "".join(parts) 还原参与匹配，保证在没有 qdii.env 的环境
  （CI / 他人 clone）里仍能检出残留；
- 通用形态正则：对「长桥 access_token」这类已被历史清洗波及、无法拆段保存的
  指纹，以及 JWT / URL 内嵌凭证等自描述形态，用正则逐行匹配；
- 动态加载：项目根 qdii.env 存在时叠加注册全部 value（长度 ≥8 才注册，防
  qdii / 3914 之类短通用值造成海量误报）。

文本判定：常见文本扩展直接扫描；其余文件按内容尝试 utf-8 解码，失败视为
二进制跳过。单文件 >2MB 跳过；排除 vendored 参考目录；文件级白名单 +
行级白名单（行尾 `# secret-ok`）。

输出：`HIT 文件:行号 [规则名] 脱敏片段`（首 3 字符…尾 3 字符，任何输出绝不
回显完整敏感串）；退出码：0 干净 / 1 有命中 / 2 用法或 git 错误。
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent.parent

# 单文件大小上限（超过跳过）
MAX_FILE_SIZE = 2 * 1024 * 1024
# 常见文本扩展（快捷通道；不在集合内的文件仍按内容 utf-8 解码兜底判定）
TEXT_EXTS = {
    ".py", ".md", ".json", ".html", ".js", ".cs", ".txt", ".yml", ".yaml",
    ".toml", ".sh", ".example", ".j2", ".cfg", ".ini", ".sql", ".css",
}
# vendored 参考目录（路径任一层命中即整体排除）
EXCLUDE_DIRS = {"akshare-docs", "akshare-code", "juejin", "StockWatcher"}
# 文件级白名单（仓库根相对路径）：检测器自身/其测试、密钥文件本体（本就不该进扫描范围）
WHITELIST_FILES = {
    "scripts/check_secrets.py",
    "tests/test_check_secrets.py",
    "qdii.env",
    "AGENTS.local.md",
    ".mcp.json",
}
# 行级白名单标记：行尾出现即跳过该行
LINE_WHITELIST_MARK = "# secret-ok"

# ==================== 内置分段指纹（拆段存储自证：任何单行不含完整敏感串） ====================
# 短串按段长 ≤7 均衡拆（任何 ≥8 字符连续子串都不会完整落在单一常量内）；
# 超长 token（JWT 类）按 ~82 字符拆段。join 后即为完整串，仅运行时在内存还原。

_S01 = (  # QDII_DB_PASS，join 后按规则「数据库密码」参与匹配
    "4BlB6U",
    "OLx9P6",
    "FwH0TB",
    "gY0xd2",
)

_S02 = (  # QDII_EQUITY_API_KEY，join 后按规则「股票行情网关密钥」参与匹配
    "Rd84hAS",
    "zWdZbuS",
    "fQ5EQKL",
    "K6T9k5S",
    "e9eEkP",
    "4gff7x",
    "imhNyA",
    "JqeEka",
    "qlNCk3",
    "wM6Umk",
)

_S03 = (  # QDII_TWELVEDATA_API_KEY，join 后按规则「twelvedata密钥」参与匹配
    "529949d",
    "7bad749",
    "c8b55e",
    "4ada86",
    "164b8c",
)

_S04 = (  # QDII_EASTMONEY_TOKEN，join 后按规则「东财token」参与匹配
    "58b2fa8",
    "f54638b",
    "60b87d",
    "69b319",
    "69089c",
)

_S05 = (  # QDII_IMAP_USER，join 后按规则「邮箱账号」参与匹配
    "257289",
    "1795@q",
    "q.com",
)

_S06 = (  # QDII_IMAP_PASSWORD，join 后按规则「邮箱授权码」参与匹配
    "ioupcn",
    "gjhni",
    "hdjee",
)

_S07 = (  # QDII_LP_APP_KEY，join 后按规则「长桥app_key」参与匹配
    "hk_28f7",
    "80e3ebd",
    "95022e8",
    "d3fd7f7",
    "a913738",
)

_S08 = (  # QDII_LP_APP_SECRET，join 后按规则「长桥app_secret」参与匹配
    "d138089",
    "bb5751e",
    "ed36915",
    "6ab87303",
    "f483f97",
    "cc3940b",
    "690db1a",
    "cd9e9e8",
    "6264260",
)

_S09 = ()  # 已废弃：长桥 access_token 分段指纹因历史清洗被 *REMOVED* 占位污染，
           # join 后非真实 token（指纹失效）。检测改由通用形态正则
           # 「长桥access_token」（hk_m_eyJ 前缀 + 长 JWT 体）承担，见
           # _register_default_patterns；历史回溯走 git -G（ERE 兼容）。

_S10 = (  # AGENTS.local.md 服务器地址，join 后按规则「服务器IP」参与匹配
    "43.142",
    ".45.63",
)

_S11 = (  # AGENTS.local.md 主站域名，join 后按规则「主站域名」参与匹配
    "888.du",
    "bai779",
    "4.cn",
)

_S12 = (  # AGENTS.local.md 股票行情网关域名，join 后按规则「网关域名」参与匹配
    "bi.du",
    "bai77",
    "94.cn",
)

_S13 = (  # 两域名的公共裸域，join 后按规则「自有域名」参与匹配
    "dubai7",
    "794.cn",
)

# 指纹登记表：（规则名，分段元组）
_BUILTIN_PARTS = [
    ("数据库密码", _S01),
    ("股票行情网关密钥", _S02),
    ("twelvedata密钥", _S03),
    ("东财token", _S04),
    ("邮箱账号", _S05),
    ("邮箱授权码", _S06),
    ("长桥app_key", _S07),
    ("长桥app_secret", _S08),
    ("服务器IP", _S10),
    ("主站域名", _S11),
    ("网关域名", _S12),
    ("自有域名", _S13),
]


class SecretDetector:
    """敏感串/通用模式检测器：字面量指纹精确匹配 + 通用正则模式。"""

    def __init__(self):
        self._literals = []   # [(规则名, 完整串)]
        self._patterns = []   # [(规则名, 编译后正则, ERE历史版)]
        self._register_default_patterns()

    def _register_default_patterns(self):
        """通用检测模式（每个实例自带；孤立 JWT 头片段等自描述文本不误报）。

        每条规则附带 ERE 兼容的「历史版」正则供 git log -G 全历史回溯使用：
        git pickaxe 正则只支持 POSIX ERE（无 (?i)/(?:(?!…) 等 PCRE 特性，
        字符类内 \\s 语义也不同于 Python re），Python 与 ERE 语法不一致时
        必须显式给 history_ere（近似版宁可多报不可漏报）。
        """
        # 长桥 app_key 形态（ERE 兼容，历史版同体）
        self.register_pattern("长桥key形态", r"hk_[0-9a-f]{16,}")
        # 长桥 access_token 特有形态：hk_m_ 前缀 + 长 JWT 体（ERE 兼容）。
        # 原 _S09 分段指纹因历史清洗被 *REMOVED* 占位污染已报废，改由本规则承担
        self.register_pattern("长桥access_token", r"hk_m_eyJ[A-Za-z0-9_.-]{100,}")
        # 完整三段式 JWT（header.payload.signature；ERE 兼容）
        self.register_pattern(
            "裸JWT",
            r"eyJhbGciOi[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{20,}")
        # 明文密码进命令行（引号内 ≥4 字符密码实体；`-p'明文'`/`-p'…'` 等占位符写法不报；
        # ERE 兼容）
        self.register_pattern("命令行明文密码", r"mysql -p'[^']{4,}'")
        # 疑似硬编码密钥赋值；负向断言排除「值为全大写蛇形环境变量键名」的伪硬编码
        # （如 CFG_KEY_APP_SECRET = "LONGPORT_APP_SECRET" 这类配置键名常量）。
        # ERE 无 (?i)/负向前视 → 历史版为大小写枚举近似（无键名排除，宁可多报）
        self.register_pattern(
            "疑似硬编码密钥赋值",
            r"(?i)(?:password|passwd|secret|api_key|apikey)\s*[=:]\s*['\"]"
            r"(?-i:(?![A-Z0-9_]{12,}['\"]))[^'\"]{12,}['\"]",
            history_ere="(password|passwd|secret|api_key|apikey"
                        "|Password|Passwd|Secret|API_KEY|ApiKey|APIKEY)"
                        "[[:space:]]*[=:][[:space:]]*['\"][^'\"]{12,}")
        # URL 内嵌凭证（user:pass@host 形态）。豁免保留测试域主机：
        # *.test / *.example / *.invalid / localhost / 127.0.0.1。
        # ERE 无负向前视 → 历史版为无豁免近似（多报测试域假凭证，复核时人工识别）
        self.register_pattern(
            "URL内嵌凭证",
            r"https?://[^/\s:'\"]+:[^/\s'\"]+@"
            r"(?!(?:[0-9A-Za-z-]+\.)*(?i:test|example|invalid)\b"
            r"|localhost\b|127\.0\.0\.1\b)"
            r"[0-9A-Za-z.-]+",
            history_ere=r"https?://[^[:space:]@/]+:[^[:space:]@]+@")

    def register_secret(self, value, rule="运行时注册"):
        """运行时注册敏感串指纹；短于 8 字符的值忽略（防通用短值误报）。"""
        if value and len(value) >= 8:
            self._literals.append((rule, value))

    def register_pattern(self, rule, regex, history_ere=None):
        """注册通用检测正则（逐行 search）。

        history_ere：git log -G 用的 POSIX ERE 历史版；None 表示 regex 本身
        ERE 兼容可直接复用。
        """
        self._patterns.append((rule, re.compile(regex), history_ere or regex))

    def iter_literals(self):
        """返回全部已注册字面量指纹的副本（--history -S 回溯用）。"""
        return list(self._literals)

    def iter_history_patterns(self):
        """返回全部正则规则的 ERE 历史版副本（--history -G 回溯用）。"""
        return [(rule, ere) for rule, _pat, ere in self._patterns]

    def scan_line(self, line):
        """扫描单行，返回 [(规则名, 匹配串)]；同串去重，互为子串的命中只保留最长。"""
        raw = []
        for rule, val in self._literals:
            if val in line:
                raw.append((rule, val))
        for rule, pat, _ere in self._patterns:
            m = pat.search(line)
            if m:
                raw.append((rule, m.group(0)))
        raw.sort(key=lambda item: -len(item[1]))
        hits, kept = [], []
        for rule, matched in raw:
            if any(matched in prev for prev in kept):
                continue
            kept.append(matched)
            hits.append((rule, matched))
        return hits


# 模块级检测器实例：加载内置指纹（通用模式由 __init__ 自带）
detector = SecretDetector()
for _rule, _parts in _BUILTIN_PARTS:
    detector.register_secret("".join(_parts), rule=_rule)


def load_env_literals(root):
    """项目根 qdii.env 存在时动态叠加注册全部 value（长度 ≥8），双保险。"""
    env_file = root / "qdii.env"
    if not env_file.is_file():
        return
    try:
        text = env_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.strip().strip('"').strip("'")
        if val:
            detector.register_secret(val, rule=f"qdii.env:{key.strip()}")


def redact(matched):
    """脱敏显示：首 3 字符…尾 3 字符；过短片段整体打码。"""
    if len(matched) <= 6:
        return "***"
    return matched[:3] + "…" + matched[-3:]


def format_hit(rel, lineno, rule, matched):
    """单条命中报告行（不含完整敏感串）。"""
    return f"HIT {rel}:{lineno} [{rule}] {redact(matched)}"


def should_scan(rel):
    """文件级过滤：白名单排除 + vendored 目录排除。rel 为仓库根相对 posix 路径。"""
    if rel in WHITELIST_FILES:
        return False
    parts = PurePosixPath(rel).parts
    if any(p in EXCLUDE_DIRS for p in parts[:-1]):
        return False
    return True


def read_lines(path):
    """读取文本行；超大文件/二进制（utf-8 解码失败）/不可读返回 None。"""
    try:
        if path.stat().st_size > MAX_FILE_SIZE:
            return None
        data = path.read_bytes()
    except OSError:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return text.splitlines()


def scan_file(path, detector=None):
    """扫描单文件，返回 [(行号, 规则名, 匹配串)]；不抛异常，静默跳过不可读文件。"""
    lines = read_lines(path)
    if lines is None:
        return []
    return scan_lines(lines, detector)


def scan_lines(lines, detector=None):
    """扫描文本行（含行级白名单过滤），返回 [(行号, 规则名, 匹配串)]。"""
    det = detector if detector is not None else globals()["detector"]
    out = []
    for lineno, line in enumerate(lines, 1):
        if line.rstrip().endswith(LINE_WHITELIST_MARK):
            continue
        for rule, matched in det.scan_line(line):
            out.append((lineno, rule, matched))
    return out


def read_index_lines(root, rel):
    """读取暂存区 blob 内容（git show :<rel>），返回行列表。

    条目不存在（如已暂存删除的文件）/超限/二进制返回 None；git 异常按该文件
    跳过（文件列表本身来自 git diff --cached，此处失败只影响单文件）。
    """
    proc = subprocess.run(
        ["git", "-C", str(root), "-c", "core.quotepath=off", "show", f":{rel}"],
        capture_output=True)
    if proc.returncode != 0:
        return None
    if len(proc.stdout) > MAX_FILE_SIZE:
        return None
    try:
        text = proc.stdout.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return text.splitlines()


def _git(root, args):
    """执行 git 命令返回 stdout；失败退出码 2（用法/环境错误）。"""
    proc = subprocess.run(
        ["git", "-C", str(root), "-c", "core.quotepath=off"] + args,
        capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"git 命令失败（{' '.join(args)}）：{proc.stderr.strip()}",
              file=sys.stderr)
        raise SystemExit(2)
    return proc.stdout


def collect_files(root, cached=False):
    """收集待扫描文件（仓库根相对路径）。

    默认 = git 已跟踪 + 未跟踪且未被 .gitignore 忽略（git ls-files --others
    --exclude-standard 天然排除 qdii.env / AGENTS.local.md / .mcp.json）；
    --cached = 仅暂存区。
    """
    if cached:
        out = _git(root, ["diff", "--cached", "--name-only"])
        return [ln.strip() for ln in out.splitlines() if ln.strip()]
    tracked = _git(root, ["ls-files"]).splitlines()
    others = _git(root, ["ls-files", "--others", "--exclude-standard"]).splitlines()
    seen, rels = set(), []
    for ln in tracked + others:
        rel = ln.strip()
        if rel and rel not in seen:
            seen.add(rel)
            rels.append(rel)
    return rels


def scan_worktree(root, cached=False, quiet=False):
    """扫描工作区/暂存区，流式打印命中，返回命中总数。

    默认扫工作区文件内容；--cached 扫暂存区 blob（git show :<rel>，与将进入
    提交的内容一致，不受工作区未暂存改动干扰）。
    """
    n = 0
    for rel in collect_files(root, cached=cached):
        if not should_scan(rel):
            continue
        if cached:
            lines = read_index_lines(root, rel)
            if lines is None:
                continue
            hits = scan_lines(lines)
        else:
            path = root / rel
            if not path.is_file():
                continue
            hits = scan_file(path)
        for lineno, rule, matched in hits:
            n += 1
            if not quiet:
                print(format_hit(rel, lineno, rule, matched))
    return n


def scan_history(root, quiet=False):
    """全历史回溯：字面量指纹 `git log --all -S<串>`、正则规则 ERE 版
    `git log --all -G<ERE>`，汇总命中提交数。"""
    jobs = [("字面", rule, val, "-S")
            for rule, val in detector.iter_literals()]
    jobs += [("模式", rule, ere, "-G")
             for rule, ere in detector.iter_history_patterns()]
    total = 0
    for _kind, rule, needle, flag in jobs:
        proc = subprocess.run(
            ["git", "-C", str(root), "-c", "core.quotepath=off",
             "log", "--all", flag, needle, "--oneline"],
            capture_output=True, text=True)
        if proc.returncode != 0:
            # git 异常不得静默判净（fail-open 会把真泄露放行）：退出码 2 阻断
            # （错误信息只带规则名，不带指纹明文/-G 正则本身）
            print(f"git log 历史回溯失败（规则[{rule}]）：{proc.stderr.strip()}",
                  file=sys.stderr)
            raise SystemExit(2)
        commits = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        if not commits:
            continue
        total += len(commits)
        if not quiet:
            print(f"HIT [历史:{rule}] {len(commits)} 个提交")
            for ln in commits[:20]:
                print(f"  {ln}")
            if len(commits) > 20:
                print(f"  …（其余 {len(commits) - 20} 条省略）")
    return total


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="提交前密钥扫描门禁：命中已知敏感串或通用密钥模式即退出码 1")
    parser.add_argument("--cached", action="store_true", help="仅扫暂存区文件")
    parser.add_argument("--history", action="store_true",
                        help="全历史回溯复核（慢）")
    parser.add_argument("--quiet", action="store_true", help="只输出统计行")
    args = parser.parse_args(argv)
    if args.cached and args.history:
        parser.error("--cached 与 --history 互斥")

    load_env_literals(ROOT)
    if args.history:
        n = scan_history(ROOT, quiet=args.quiet)
    else:
        n = scan_worktree(ROOT, cached=args.cached, quiet=args.quiet)
    print(f"检出 {n} 处")
    return 1 if n else 0


if __name__ == "__main__":
    sys.exit(main())
