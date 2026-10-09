"""全局配置"""
import os

# MySQL 连接（敏感项优先读 QDII_ 前缀环境变量，缺省回退现值保持部署零影响）
DB_HOST = os.getenv("QDII_DB_HOST", "***REMOVED***")
DB_PORT = int(os.getenv("QDII_DB_PORT", "3914"))
DB_USER = os.getenv("QDII_DB_USER", "*REMOVED*")
DB_PASS = os.getenv("QDII_DB_PASS", "***REMOVED***")
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
