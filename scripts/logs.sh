#!/bin/bash

# LiteMQ 日志查看脚本

SERVICE=${1:-"broker-master"}

echo "=== LiteMQ $SERVICE 日志 ==="

if [ "$SERVICE" = "all" ]; then
    docker-compose logs -f
else
    docker-compose logs -f $SERVICE
fi
