"""全局配置"""
import os

# MySQL 连接
DB_HOST = os.getenv("DB_HOST", "***REMOVED***")
DB_PORT = int(os.getenv("DB_PORT", "3914"))
DB_USER = os.getenv("DB_USER", "*REMOVED*")
DB_PASS = os.getenv("DB_PASS", "n7VO5yOzTDavUocK")
DB_NAME = os.getenv("DB_NAME", "qdii")
DB_POOL_SIZE = 5

# 得分模型默认权重
DEFAULT_W_RATIO = 0.7
DEFAULT_W_PREMIUM = 0.3
DEFAULT_FUTURES_KILL_THRESHOLD = -1.5
DEFAULT_SAFE_REQ_PER_MIN = 20
MIN_INTERVAL_SEC = 3.0

# 服务端口
SERVER_PORT = 8000

# 期指符号映射 (ETF futures_index -> akshare futures 名称关键字)
FUTURES_NAME_MAP = {
    "NQ": "纳斯达克",
    "ES": "标普",
    "YM": "道琼斯",
    "DAX": "DAX",
    "NK": "日经",
}
