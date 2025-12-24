#!/bin/bash

# LiteMQ 集群启动脚本

set -e

echo "=== 启动 LiteMQ 集群 ==="

# 检查是否已部署
if ! docker-compose ps | grep -q "Up"; then
    echo "LiteMQ 集群未部署，请先运行: ./scripts/deploy.sh"
    exit 1
fi

# 启动服务
echo "启动所有服务..."
docker-compose start

# 等待服务就绪
echo "等待服务就绪..."
sleep 5

# 检查服务状态
echo "服务状态:"
docker-compose ps

echo "LiteMQ 集群启动完成"
