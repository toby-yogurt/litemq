#!/bin/bash

# LiteMQ 部署脚本

set -e

echo "=== LiteMQ 部署脚本 ==="

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "错误: Docker 未安装"
    exit 1
fi

# 检查docker-compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "错误: docker-compose 未安装"
    exit 1
fi

# 创建数据目录
echo "创建数据目录..."
mkdir -p data/nameserver
mkdir -p data/broker-master
mkdir -p data/broker-slave
mkdir -p data/prometheus
mkdir -p data/grafana

# 构建镜像
echo "构建Docker镜像..."
docker-compose build

# 启动服务
echo "启动LiteMQ集群..."
docker-compose up -d

# 等待服务启动
echo "等待服务启动..."
sleep 10

# 检查服务状态
echo "检查服务状态..."
docker-compose ps

# 显示访问信息
echo ""
echo "=== LiteMQ 部署完成 ==="
echo "NameServer: http://localhost:9876"
echo "Broker Master: http://localhost:10911"
echo "Broker Slave: http://localhost:10912"
echo "Prometheus: http://localhost:9090"
echo "Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "查看日志: docker-compose logs -f [service_name]"
echo "停止服务: docker-compose down"
