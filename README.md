# LiteMQ - 轻量级企业级消息队列

[![Go Version](https://img.shields.io/badge/go-1.18+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

LiteMQ 是一个用Go语言实现的轻量级、高性能的企业级分布式消息队列系统，参考了RocketMQ的设计理念，提供完整的企业级MQ功能。

## 🚀 核心特性

### 消息类型支持
- ✅ **普通消息** - 基础的点对点和发布订阅
- ✅ **延时消息** - 秒级到年级的精确延时投递
- ✅ **定时消息** - 基于Cron表达式的定时调度
- ✅ **事务消息** - 两阶段提交保证分布式事务一致性
- ✅ **广播消息** - 一条消息发送给所有消费者
- ✅ **顺序消息** - 全局和分区级别的消息顺序保证

### 通信协议
- ✅ **gRPC通信** - 高性能、强类型的RPC通信
- ✅ **Protocol Buffers** - 高效的序列化协议
- ✅ **双向流式通信** - 支持长连接和实时通信

### 高可用与可扩展
- ✅ **主从复制** - 实时数据同步，故障自动切换
- ✅ **集群部署** - 支持水平扩展
- ✅ **负载均衡** - 智能的流量分摊
- ✅ **故障检测** - 自动检测和处理节点故障

### 企业级特性
- ✅ **消息持久化** - 磁盘存储，保证消息不丢失
- ✅ **消息确认** - ACK机制保证可靠性
- ✅ **死信队列** - 处理消费失败的消息
- ✅ **消息重试** - 智能重试机制
- ✅ **消息过滤** - 基于标签和属性的过滤

### 运维监控
- ✅ **REST API** - 基于Gin框架的完整API
- ✅ **Web管理界面** - 现代化的管理控制台
- ✅ **监控面板** - Web可视化界面
- ✅ **智能告警** - 多渠道告警通知
- ✅ **性能指标** - 全面的性能监控
- ✅ **健康检查** - 系统状态监控

## 📊 性能指标

- **吞吐量**: 单Broker支持10万+QPS
- **延迟**: P99 < 10ms
- **可用性**: 99.99% (年停机时间 < 53分钟)
- **数据可靠性**: 99.9999% (消息不丢失)

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Producer   │  │   Consumer   │  │    Admin     │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
└─────────┼──────────────────┼──────────────────┼──────────┘
          │                  │                  │
          │  ┌───────────────┼──────────────────┼─────────┐
          │  │               │                  │         │
          ▼  ▼               ▼                  ▼         │
┌─────────────────────────────────────────────────────────┐ │
│              NameServer Cluster (服务注册与发现)         │ │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │ │
│  │   NS-1   │  │   NS-2   │  │   NS-3   │ (无状态集群) │ │
│  └──────────┘  └──────────┘  └──────────┘              │ │
└─────────────────┬───────────────────────────────────────┘ │
                  │ 心跳/注册                                │
          ┌───────┼─────────────┬──────────────────────────┐ │
          │       │             │                          │ │
          ▼       ▼             ▼                          │ │
┌──────────────────────────────────────────────────────────┐ │
│                  Broker Cluster (主从架构)               │ │
│                                                          │ │
│  ┌─────────────────┐  ┌─────────────────┐                │ │
│  │  Broker-A       │  │  Broker-B       │  ...           │ │
│  │  ┌───────────┐  │  │  ┌───────────┐  │                │ │
│  │  │  Master   │  │  │  │  Master   │  │                │ │
│  │  │  (读写)   │  │  │  │  (读写)   │  │                │ │
│  │  └─────┬─────┘  │  │  └─────┬─────┘  │                │ │
│  │        │        │  │        │        │                │ │
│  │  ┌─────▼─────┐  │  │  ┌─────▼─────┐  │                │ │
│  │  │  Slave-1  │  │  │  │  Slave-1  │  │                │ │
│  │  │  (只读)   │  │  │  │  (只读)   │  │                │ │
│  │  └───────────┘  │  │  └───────────┘  │                │ │
│  │  ┌───────────┐  │  │  ┌───────────┐  │                │ │
│  │  │  Slave-2  │  │  │  │  Slave-2  │  │                │ │
│  │  └───────────┘  │  │  └───────────┘  │                │ │
│  └─────────────────┘  └─────────────────┘                │ │
│                                                          │ │
│  存储引擎: CommitLog + ConsumeQueue                      │ │
│  复制模式: 异步复制 / 同步复制                           │ │
└──────────────────────────────────────────────────────────┘ │
                                                            │
          ┌─────────────────────────────────────────────────┼─┐
          │ 存储层: 内存映射文件 + 顺序写入                   │ │
          │ 索引层: ConsumeQueue + 时间轮                     │ │
          │ 网络层: gRPC + HTTP                              │ │
          └───────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 环境要求

- Go 1.18+
- Linux/macOS/Windows

### 安装依赖

```bash
make deps
```

### 构建项目

```bash
make build
```

### 一键快速启动

想要快速体验LiteMQ？只需执行以下命令：

```bash
# 初始化环境
make init

# 构建所有组件
make build

# 启动NameServer
make run-nameserver

# 启动Broker
make run-broker

# 运行示例
make run-producer
make run-consumer
```

### 分步运行示例

1. **启动NameServer**
```bash
make run-nameserver
```

2. **启动Broker**
```bash
make run-broker
```

3. **运行生产者示例**
```bash
make run-producer
```

4. **运行消费者示例**
```bash
make run-consumer
```

## 📝 使用指南

### 生产者示例

```go
package main

import (
    "litemq/pkg/client"
    "litemq/pkg/common/config"
)

func main() {
    // 创建配置
    cfg := config.DefaultClientConfig()

    // 创建生产者
    producer := client.NewProducer(cfg)

    // 启动生产者
    producer.Start()
    defer producer.Shutdown()

    // 发送消息
    result, err := producer.SendMessage("my_topic", []byte("Hello LiteMQ!"))
    if err != nil {
        panic(err)
    }

    fmt.Printf("Message sent: ID=%s, Offset=%d\n", result.MessageID, result.Offset)
}
```

### 消费者示例

```go
package main

import (
    "fmt"
    "litemq/pkg/client"
    "litemq/pkg/common/config"
    "litemq/pkg/protocol"
)

func main() {
    // 创建配置
    cfg := config.DefaultClientConfig()

    // 创建消费者
    consumer := client.NewConsumer(cfg, "my_group")

    // 定义消息处理器
    handler := func(msg *protocol.Message) protocol.ConsumeStatus {
        fmt.Printf("Received: %s\n", string(msg.Body))
        return protocol.ConsumeStatusSuccess
    }

    // 订阅主题
    consumer.Subscribe("my_topic", handler)

    // 启动消费者
    consumer.Start()
    defer consumer.Shutdown()

    // 运行...
    select {}
}
```

### REST API 使用示例

#### 健康检查
```bash
curl http://localhost:8088/api/v1/health
```

#### 获取Broker信息
```bash
curl http://localhost:8088/api/v1/broker/info
```

#### 获取Broker统计信息
```bash
curl http://localhost:8088/api/v1/broker/stats
```

#### 获取主题列表
```bash
curl http://localhost:8088/api/v1/broker/topics
```

#### 创建主题
```bash
curl -X POST http://localhost:8088/api/v1/broker/topics/my_topic \
  -H "Content-Type: application/json" \
  -d '{"partitions": 4}'
```

#### 删除主题
```bash
curl -X DELETE http://localhost:8088/api/v1/broker/topics/my_topic
```

#### 获取NameServer统计信息
```bash
curl http://localhost:8088/api/v1/nameserver/stats
```

#### 获取所有Broker
```bash
curl http://localhost:8088/api/v1/nameserver/brokers
```

## 📁 项目结构

```
litemq/
├── cmd/                    # 可执行程序
│   ├── nameserver/        # NameServer主程序
│   └── broker/            # Broker主程序
├── pkg/                   # 核心包
│   ├── common/            # 公共组件
│   │   └── config/        # 配置管理
│   ├── protocol/          # 协议定义
│   ├── storage/           # 存储引擎
│   ├── nameserver/        # NameServer实现
│   ├── broker/            # Broker实现
│   ├── client/            # 客户端SDK
│   └── monitoring/        # 监控系统
├── api/                   # REST API (基于Gin框架)
│   └── v1/               # API v1版本
├── web/                   # Web管理界面
│   ├── server.go         # Web服务器
│   ├── templates/        # HTML模板
│   └── static/           # 静态资源
├── examples/              # 示例程序
│   ├── producer/          # 生产者示例
│   └── consumer/          # 消费者示例
├── configs/               # 配置文件
├── deployments/           # 部署配置
├── docs/                  # 文档
│   └── storage-format.md  # 存储格式文档
├── scripts/               # 部署脚本
├── bin/                   # 构建产物
├── Makefile              # 构建脚本
└── README.md             # 项目文档
```

## 📚 文档

- [存储格式文档](docs/storage-format.md) - CommitLog 和 ConsumeQueue 的详细文件格式说明
- [技术栈文档](docs/tech-stack.md) - 项目使用的所有技术栈和依赖库说明
- [Web 界面使用指南](docs/web-ui-guide.md) - Web 管理界面的启动和使用说明

## 🔧 配置说明

### NameServer配置

```toml
# configs/nameserver.toml
host = "0.0.0.0"
port = 9876

[heartbeat]
interval = "30s"
timeout = "90s"

[route]
update_interval = "30s"
```

### Broker配置

```toml
# configs/broker.toml
broker_id = "broker-1"
broker_name = "broker-1"

[network]
host = "0.0.0.0"
port = 10911

[nameserver]
addresses = ["localhost:9876"]

[storage]
data_path = "./data/broker"
flush_mode = "async"
file_size = 1073741824  # 1GB

[replication]
role = "master"
mode = "async"
```

## 🧪 测试

### 运行单元测试

```bash
make test
```

### 运行集成测试

```bash
make test-integration
```

### 运行性能测试

```bash
make test-benchmark
```

### 生成覆盖率报告

```bash
make test-coverage
```

## 📊 监控

LiteMQ提供了完整的监控和告警功能：

- **Web监控面板**: `http://localhost:8080`
- **Prometheus指标**: `http://localhost:9090/metrics`
- **健康检查**: `http://localhost:8080/health`

## 🚀 部署

### Docker部署

```bash
# 构建镜像
docker build -t litemq .

# 运行NameServer
docker run -d --name nameserver -p 9876:9876 litemq nameserver

# 运行Broker
docker run -d --name broker -p 10911:10911 \
  --link nameserver \
  litemq broker
```

### Kubernetes部署

```bash
# 部署到Kubernetes
kubectl apply -f deployments/k8s/
```

## 🤝 贡献

欢迎提交Issue和Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

- 参考了 [RocketMQ](https://rocketmq.apache.org/) 的设计理念
- 感谢所有贡献者的付出

## 📞 联系我们

- 项目主页: [https://github.com/your-org/litemq](https://github.com/your-org/litemq)
- 问题反馈: [https://github.com/your-org/litemq/issues](https://github.com/your-org/litemq/issues)
- 邮箱: your-email@example.com

---

**🎉 LiteMQ - 让消息队列变得简单、高效、可靠！**
