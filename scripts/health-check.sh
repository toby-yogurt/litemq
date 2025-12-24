#!/bin/bash

# LiteMQ 健康检查脚本

echo "=== LiteMQ 健康检查 ==="

# 检查服务状态
echo "检查服务状态..."
if docker-compose ps | grep -q "Up"; then
    echo "✅ 服务正在运行"
else
    echo "❌ 服务未运行"
    exit 1
fi

# 检查NameServer
echo "检查NameServer..."
if curl -s http://localhost:9876/health > /dev/null 2>&1; then
    echo "✅ NameServer 健康"
else
    echo "❌ NameServer 不健康"
fi

# 检查Broker Master
echo "检查Broker Master..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Broker Master 健康"
else
    echo "❌ Broker Master 不健康"
fi

# 检查Broker Slave
echo "检查Broker Slave..."
if curl -s http://localhost:8081/health > /dev/null 2>&1; then
    echo "✅ Broker Slave 健康"
else
    echo "❌ Broker Slave 不健康"
fi

# 检查Prometheus
echo "检查Prometheus..."
if curl -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
    echo "✅ Prometheus 健康"
else
    echo "❌ Prometheus 不健康"
fi

echo "健康检查完成"
