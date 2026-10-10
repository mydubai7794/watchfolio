"""check_secrets 门禁脚本单元测试（纯单元：不依赖 qdii.env 存在、不依赖网络与服务）。"""
import importlib.util
import random
import string
import sys
from pathlib import Path

# scripts/ 非包目录，按文件路径加载被测模块
_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "check_secrets.py"
_spec = importlib.util.spec_from_file_location("check_secrets", _SCRIPT)
check_secrets = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("check_secrets", check_secrets)
_spec.loader.exec_module(check_secrets)


def _rand_secret(n=20):
    """确定性随机构造串（固定种子，避免运行间波动）。"""
    rng = random.Random(20261010)
    return "".join(rng.choices(string.ascii_letters + string.digits, k=n))


def _fresh_detector():
    """独立检测器实例：仅含待测指纹，不受全局内置/动态指纹干扰。"""
    return check_secrets.SecretDetector()


def test_register_secret_detects(tmp_path):
    """运行时注册 API：注册随机 20 字符串后，含该串的临时文件被检出。"""
    secret = _rand_secret(20)
    d = _fresh_detector()
    d.register_secret(secret, rule="测试注册")
    f = tmp_path / "sample.py"
    f.write_text(f'client_token = "{secret}"\n', encoding="utf-8")
    hits = check_secrets.scan_file(f, detector=d)
    assert hits, "已注册串未被检出"
    lineno, rule, matched = hits[0]
    assert lineno == 1
    assert rule == "测试注册"
    assert matched == secret


def test_builtin_fingerprints_valid():
    """内置分段指纹集非空（≥10 项），每项 ≥2 段且 join 后长度 ≥8。"""
    parts_list = check_secrets._BUILTIN_PARTS
    assert len(parts_list) >= 10
    for rule, parts in parts_list:
        assert len(parts) >= 2, rule
        assert len("".join(parts)) >= 8, rule


def test_generic_patterns(tmp_path):
    """通用模式：长桥 key 形态 / 裸 JWT / 命令行明文密码各自命中。"""
    d = _fresh_detector()
    fake_jwt = ("eyJhbGciOiJIUzI1NiJ9."  # 伪造样例头（非真实凭证）
                "eyJzdWIiOiJ0ZXN0MTIzNDU2NzgifQ."
                "aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789")
    f = tmp_path / "patterns.txt"
    f.write_text(
        'k1 = "hk_abcdef0123456789ab"\n'
        f'token = "{fake_jwt}"\n'
        "cmd = \"mysql -p'NotARealPass123' -h db\"\n",
        encoding="utf-8")
    hits = check_secrets.scan_file(f, detector=d)
    rules = {rule for _, rule, _ in hits}
    assert "长桥key形态" in rules
    assert "裸JWT" in rules
    assert "命令行明文密码" in rules


def test_line_whitelist_secret_ok(tmp_path):
    """行级白名单：行尾 `# secret-ok` 跳过该行，无标记行正常检出。"""
    secret = _rand_secret(20)
    d = _fresh_detector()
    d.register_secret(secret, rule="测试注册")
    f = tmp_path / "w.py"
    f.write_text(
        f'a = "{secret}"  # secret-ok\n'
        f'b = "{secret}"\n',
        encoding="utf-8")
    hits = check_secrets.scan_file(f, detector=d)
    assert len(hits) == 1
    assert hits[0][0] == 2


def test_redacted_report_hides_plaintext(tmp_path):
    """脱敏输出：命中报告不含被注册的完整明文（首3…尾3）。"""
    secret = _rand_secret(20)
    d = _fresh_detector()
    d.register_secret(secret, rule="测试注册")
    f = tmp_path / "r.py"
    f.write_text(f'v = "{secret}"\n', encoding="utf-8")
    hits = check_secrets.scan_file(f, detector=d)
    assert hits
    rendered = "\n".join(
        check_secrets.format_hit(f.name, lineno, rule, matched)
        for lineno, rule, matched in hits)
    assert secret not in rendered
    assert "…" in rendered


def test_scope_filters():
    """文件级白名单与 vendored 目录排除；普通源码文件保留。"""
    should_scan = check_secrets.should_scan
    assert not should_scan("scripts/check_secrets.py")
    assert not should_scan("tests/test_check_secrets.py")
    assert not should_scan("qdii.env")
    assert not should_scan("AGENTS.local.md")
    assert not should_scan(".mcp.json")
    assert not should_scan("akshare-docs/readme.md")
    assert not should_scan("StockWatcher/src/x.py")
    assert should_scan("server.py")
    assert should_scan("docs/note.md")


def test_binary_and_oversize_skipped(tmp_path):
    """二进制（utf-8 解码失败）与超限大文件跳过，不报异常不误报。"""
    f_bin = tmp_path / "blob.bin"
    f_bin.write_bytes(b"\xff\xfe\x00\x01" * 16)
    assert check_secrets.scan_file(f_bin) == []
    big = tmp_path / "big.log"
    big.write_text("x" * (check_secrets.MAX_FILE_SIZE + 1), encoding="utf-8")
    assert check_secrets.scan_file(big) == []


def test_builtin_fingerprint_end_to_end_detect(tmp_path):
    """端到端：内置分段指纹 join 还原后写入临时文件，全局 detector 扫描
    必须命中对应规则名（验证「拆段存储 → 运行时还原 → 实际可检出」全链路；
    本测试文件在文件级白名单内，写入 tmp_path 的明文不入仓库）。"""
    rule_name, parts = check_secrets._BUILTIN_PARTS[0]   # 「数据库密码」_S01
    secret = "".join(parts)
    f = tmp_path / "leak.py"
    f.write_text(f'DB_PASS = "{secret}"\n', encoding="utf-8")
    hits = check_secrets.scan_file(f)   # 不传 detector → 用全局实例
    rules = {rule for _, rule, _ in hits}
    assert rule_name in rules, f"内置指纹规则[{rule_name}]未被全局检测器检出"
    assert any(matched == secret for _, _, matched in hits)


def test_pattern_hardcoded_assignment_value_is_env_keyname_not_hit(tmp_path):
    """模式4负例：值=全大写蛇形环境变量键名（如 TWELVEDATA_API_KEY）属
    配置键名常量引用，负向断言排除，不报。"""
    d = _fresh_detector()
    f = tmp_path / "cfg_negative.py"
    f.write_text('API_KEY = "TWELVEDATA_API_KEY"\n', encoding="utf-8")
    assert check_secrets.scan_file(f, detector=d) == []


def test_pattern_hardcoded_assignment_plain_value_hit(tmp_path):
    """模式4正例：普通明文（≥12 字符、非全大写蛇形键名）赋值报。"""
    d = _fresh_detector()
    f = tmp_path / "cfg_positive.py"
    f.write_text('api_key = "live-plain-secret-123"\n', encoding="utf-8")
    hits = check_secrets.scan_file(f, detector=d)
    rules = {rule for _, rule, _ in hits}
    assert "疑似硬编码密钥赋值" in rules
