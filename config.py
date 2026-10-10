"""全局配置"""
import os
from pathlib import Path


def load_env_file() -> None:
    """加载 env 文件注入环境变量（只 setdefault 不覆盖已有值，幂等可重复调用）。@author yys

    顺序：项目根 qdii.env（本地开发）优先，其次 /opt/qdii/qdii.env（服务器
    systemd EnvironmentFile 位），第一个存在且可读的文件生效。格式 KEY=VALUE
    每行一条，# 开头整行注释、空行跳过，无 = 的行解析失败跳过不抛错；
    文件不存在/无权限静默返回。真实密钥全部经环境变量注入（不入库），
    代码零兜底明文。
    """
    # 项目根按本文件位置推导（非 cwd 相对路径：从任意工作目录启动均命中），
    # 与 /opt/qdii/qdii.env 第二路径在生产部署目录重合，无副作用
    local_env = Path(__file__).resolve().parent / "qdii.env"
    for path in (str(local_env), "/opt/qdii/qdii.env"):
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                raw_lines = f.readlines()
        except OSError:
            continue
        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
        return   # 第一个成功读取的文件生效


load_env_file()   # 模块导入即注入：下方 DB_* 等常量求值时环境变量已就位

# MySQL 连接（敏感项优先读 QDII_ 前缀环境变量，缺省本机/空值，真实值走 env 注入）
DB_HOST = os.getenv("QDII_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("QDII_DB_PORT", "3914"))
DB_USER = os.getenv("QDII_DB_USER", "")
DB_PASS = os.getenv("QDII_DB_PASS", "")
DB_NAME = os.getenv("QDII_DB_NAME", "qdii")
DB_POOL_SIZE = 5

# 得分模型默认权重
DEFAULT_W_RATIO = 0.7
DEFAULT_W_PREMIUM = 0.3
DEFAULT_FUTURES_KILL_THRESHOLD = -1.5
DEFAULT_SAFE_REQ_PER_MIN = 20
MIN_INTERVAL_SEC = 3.0  # 每 IP 每 3 秒最多一次请求（代理轮转限速语义）

# 攒批总线真实拉取节拍（对 qt.gtimg.cn 的最高请求频率；腾讯基本不封禁，
# 10s 兼顾监控时效与保源，2026-10-08 腾讯主源收敛定版）
TENCENT_BUS_INTERVAL_SEC = 10.0
# 监控循环驱动间隔（数据粒度 10s，与总线节拍对齐；竞价窗口捕捉从 5s 放宽）
MONITOR_INTERVAL_SEC = 10.0

# 服务端口
SERVER_PORT = 8888

# 交易时间段 [(start_h, start_m, end_h, end_m), ...]
TRADING_SESSIONS = [
    (9, 15, 11, 30),   # 上午盘
    (13, 0, 15, 0),    # 下午盘
]

# 期指符号映射 (ETF futures_index -> 显示名称)
# 只保留美股三大期指：纳指、标普、道指
FUTURES_NAME_MAP = {
    "NQ": "纳斯达克",
    "ES": "标普",
    "YM": "道琼斯",
}

CANDIDATE_REFRESH_SEC = 10.0  # 候选池缓存刷新间隔(秒)，与总线节拍对齐
