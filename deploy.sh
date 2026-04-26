#!/bin/bash
set -e

# 从 .env 文件读取配置
if [ ! -f .env ]; then
  echo "错误: 缺少 .env 文件，请复制 .env.example 并填写配置"
  exit 1
fi

set -a; source .env; set +a

: "${REMOTE_HOST:?缺少 REMOTE_HOST}"
: "${REMOTE_USER:?缺少 REMOTE_USER}"
: "${SSH_KEY:=~/.ssh/id_ed25519}"
: "${OPENCLAW_API_KEY:?缺少 OPENCLAW_API_KEY}"

REMOTE_DIR=/usr/docker/gaogao-api
SRC_DIR=$REMOTE_DIR/src

echo "=== 1. 传输源码到服务器 ==="
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST "mkdir -p $SRC_DIR/server $SRC_DIR/src/types"

scp -i $SSH_KEY \
  package.json pnpm-lock.yaml tsconfig.server.json Dockerfile \
  $REMOTE_USER@$REMOTE_HOST:$SRC_DIR/

scp -i $SSH_KEY server/*.ts \
  $REMOTE_USER@$REMOTE_HOST:$SRC_DIR/server/

scp -i $SSH_KEY src/types/report.ts \
  $REMOTE_USER@$REMOTE_HOST:$SRC_DIR/src/types/

echo "=== 2. 远程构建并部署 ==="
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST << REMOTE_SCRIPT
set -e

SRC_DIR=/usr/docker/gaogao-api/src
SHARED_NET=openclaw-net

cd $SRC_DIR

echo "--- 构建镜像 ---"
docker build -t gaogao-api:latest .

echo "--- 创建共享网络 ---"
docker network create $SHARED_NET 2>/dev/null || true

echo "--- 加入 openclaw 到共享网络 ---"
docker network connect $SHARED_NET openclaw 2>/dev/null || true

echo "--- 停止旧容器 ---"
docker stop gaogao-api 2>/dev/null || true
docker rm gaogao-api 2>/dev/null || true

echo "--- 启动新容器 ---"
docker run -d \
  --name gaogao-api \
  --network $SHARED_NET \
  --restart unless-stopped \
  -p 1555:1555 \
  -e PORT=1555 \
  -e OPENCLAW_BASE_URL=http://openclaw:18789/v1 \
  -e OPENCLAW_API_KEY=${OPENCLAW_API_KEY} \
  -e OPENCLAW_MODEL=openclaw/report-agent \
  -e OPENCLAW_REMOTE_HOST= \
  -e REPORT_OUTPUT_DIR=/home/node/.openclaw/workspace/report-agent/reports \
  -e OPENCLAW_REMOTE_REPORT_DIR=/home/node/.openclaw/workspace/report-agent/reports \
  -v /usr/docker/openclaw/workspace/report-agent/reports:/home/node/.openclaw/workspace/report-agent/reports \
  gaogao-api:latest

echo "=== 3. 等待启动 ==="
sleep 3

echo "=== 容器状态 ==="
docker ps --filter name=gaogao-api --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "=== 启动日志 ==="
docker logs --tail 20 gaogao-api

echo ""
echo "=== 部署完成 ==="

REMOTE_SCRIPT

echo "=== 全部完成 ==="
