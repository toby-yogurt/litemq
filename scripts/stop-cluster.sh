#!/bin/bash

# LiteMQ 集群停止脚本

set -e

echo "=== 停止 LiteMQ 集群 ==="

# 检查是否在运行
if docker-compose ps | grep -q "Up"; then
    echo "停止所有服务..."
    docker-compose stop

    echo "LiteMQ 集群已停止"
else
    echo "LiteMQ 集群未在运行"
fi
