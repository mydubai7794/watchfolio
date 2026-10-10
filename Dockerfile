# QDII 持仓网站镜像（python:3.12-slim 单阶段）
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 先装依赖再拷代码：requirements 未变时命中层缓存，改代码不触发重装。
# cryptography 为数据库新鉴权插件的握手兜底（缺它时 aiomysql 底层认证会失败）。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt cryptography

# 全部代码 + static/ 静态文件随上下文进入镜像
# （.dockerignore 已排除 qdii.env 等密钥与本地大目录，构建上下文最小化）
COPY . .

EXPOSE 8888

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8888"]
