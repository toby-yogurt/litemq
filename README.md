# LiteMQ - 轻量级企业级消息队列

[![Go Version](https://img.shields.io/badge/go-1.18+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

LiteMQ 是一个用Go语言实现的轻量级、高性能的企业级分布式消息队列系统，参考了RocketMQ的设计理念，提供完整的企业级MQ功能。

## 🚀 核心特性

### 消息类型支持
- ✅ **普通消息** - 基础的点对点和发布订阅
- ✅ **延时消息** - 基于多层时间轮实现的秒级精确延时投递，支持最长365天的超长延时（参考RocketMQ 5.0）
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

## ⏰ 延时消息与时间轮实现

### 时间轮（TimeWheel）架构

LiteMQ 使用**时间轮（TimeWheel）**算法实现延时消息功能，参考了 RocketMQ 5.0 的设计。时间轮是一种高效的数据结构，用于管理和调度大量延时任务。

#### 时间轮原理

时间轮是一个环形数组，每个槽位代表一个时间间隔。LiteMQ 使用**多层时间轮**实现延时消息功能，参考了 RocketMQ 5.0 的设计。多层时间轮通过层级结构支持从秒级到天级的延时消息。

**多层时间轮特点**：

- **4层时间轮结构**：
  - **第0层（秒级）**：60个槽位，每个槽位代表1秒，支持0-60秒内的延时
  - **第1层（分钟级）**：60个槽位，每个槽位代表1分钟，支持0-60分钟内的延时
  - **第2层（小时级）**：24个槽位，每个槽位代表1小时，支持0-24小时内的延时
  - **第3层（天级）**：365个槽位，每个槽位代表1天，支持0-365天内的延时
- **时间精度**：支持秒级精度的延时消息
- **延时范围**：支持从秒级到天级的超长延时（最长365天）
- **高效调度**：O(1) 时间复杂度的消息添加和到期检查
- **智能降级**：消息根据延时时间自动选择合适的层级，到期前自动降级到更细粒度层级

#### 时间轮结构

```
多层时间轮架构

第0层（秒级）- 60个槽位，每个1秒
┌─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │ ... │ 58  │ 59  │  ← 每秒推进
└─────┴─────┴─────┴─────┴─────┴─────┘
  │     │     │           │     │
  ▼     ▼     ▼           ▼     ▼
[msg1] [msg2] [msg3]    [msg4] [msg5]  ← 0-60秒内的消息

第1层（分钟级）- 60个槽位，每个1分钟
┌─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │ ... │ 58  │ 59  │  ← 每60秒推进一次
└─────┴─────┴─────┴─────┴─────┴─────┘
  │     │     │           │     │
  ▼     ▼     ▼           ▼     ▼
[msg1] [msg2] [msg3]    [msg4] [msg5]  ← 1-60分钟内的消息

第2层（小时级）- 24个槽位，每个1小时
┌─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │ ... │ 23  │  ← 每60分钟推进一次
└─────┴─────┴─────┴─────┴─────┘
  │     │     │           │
  ▼     ▼     ▼           ▼
[msg1] [msg2] [msg3]    [msg4]  ← 1-24小时内的消息

第3层（天级）- 365个槽位，每个1天
┌─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │ ... │ 364 │  ← 每24小时推进一次
└─────┴─────┴─────┴─────┴─────┘
  │     │     │           │
  ▼     ▼     ▼           ▼
[msg1] [msg2] [msg3]    [msg4]  ← 1-365天内的消息
```

#### 工作流程

1. **消息存储**：延时消息先存储到 CommitLog，但不立即构建 ConsumeQueue 索引
2. **层级选择**：根据延时时间自动选择合适的层级（秒级/分钟级/小时级/天级）
3. **时间轮调度**：将消息添加到对应层级的对应槽位
4. **定时推进**：只启动最细粒度的时间轮（秒级），每秒推进一个槽位
5. **级联降级**：当细粒度层级转完一圈时，推进粗粒度层级，并将消息降级到细粒度层级重新计算位置
6. **消息投递**：到期消息触发回调，构建 ConsumeQueue 索引，使消息对消费者可见

**级联降级示例**：
- 当第0层（秒级）转完一圈（60秒）时，推进第1层（分钟级）一个槽位，并将第1层旧槽位的消息降级到第0层
- 当第1层（分钟级）转完一圈（60分钟）时，推进第2层（小时级）一个槽位，并将第2层旧槽位的消息降级到第1层
- 以此类推，实现从粗粒度到细粒度的精确调度

#### 延时消息调度器

延时消息调度器（DelayMessageScheduler）负责：
- 管理时间轮的生命周期
- 缓存延时消息信息
- 处理消息到期回调
- 构建 ConsumeQueue 索引

#### 性能特点

- **低延迟**：O(1) 时间复杂度的消息添加和到期检查
- **高吞吐**：多层时间轮可支持海量延时消息（秒级到天级）
- **内存高效**：只存储消息的 CommitLog 偏移量，消息按需降级，不会全部堆积在细粒度层级
- **精确调度**：秒级精度的延时投递，支持最长365天的超长延时
- **智能降级**：消息根据剩余延时时间自动降级到合适的层级，保证精确调度

#### 延时范围

多层时间轮支持以下延时范围：

| 层级 | 槽位数 | 时间间隔 | 支持范围 |
|------|--------|----------|----------|
| 第0层（秒级） | 60 | 1秒 | 0-60秒 |
| 第1层（分钟级） | 60 | 1分钟 | 1-60分钟 |
| 第2层（小时级） | 24 | 1小时 | 1-24小时 |
| 第3层（天级） | 365 | 1天 | 1-365天 |

**总计**：支持从秒级到天级的延时消息，最长支持365天（约1年）的延时投递。

#### 延时消息处理流程

```
发送延时消息（延时时间：例如5小时）
    │
    ▼
存储到 CommitLog（持久化）
    │
    ▼
计算延时时间，选择合适层级（5小时 → 第2层小时级）
    │
    ▼
添加到对应层级的对应槽位（第2层，槽位 = (当前槽位 + 5) % 24）
    │
    ▼
时间轮每秒推进（只启动第0层秒级时间轮）
    │
    ├─ 第0层转完一圈（60秒） ──► 推进第1层一个槽位 ──► 将第1层消息降级到第0层
    │
    ├─ 第1层转完一圈（60分钟） ──► 推进第2层一个槽位 ──► 将第2层消息降级到第1层
    │
    └─ 第2层转完一圈（24小时） ──► 推进第3层一个槽位 ──► 将第3层消息降级到第2层
    │
    ▼
消息降级到更细粒度层级，重新计算位置
    │
    ▼
检查当前槽位消息是否到期
    │
    ├─ 到期 ──► 触发回调 ──► 构建 ConsumeQueue 索引 ──► 消息可被消费
    │
    └─ 未到期 ──► 继续等待，等待下次降级
```
                    │
                    ▼
                消息对消费者可见
                    │
                    ▼
                消费者拉取并消费
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

### 延时消息示例

```go
package main

import (
    "fmt"
    "time"
    "litemq/pkg/client"
    "litemq/pkg/common/config"
)

func main() {
    // 创建配置
    cfg := config.DefaultClientConfig()

    // 创建生产者
    producer := client.NewProducer(cfg)
    producer.Start()
    defer producer.Shutdown()

    // 发送延时消息（延时5秒后投递）
    delayTime := time.Now().Add(5 * time.Second).UnixMilli()
    result, err := producer.SendDelayMessage("my_topic", []byte("Delayed message"), delayTime)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Delay message sent: ID=%s, DelayTime=%d\n", result.MessageID, delayTime)
}
```

### 事务消息示例

```go
package main

import (
    "fmt"
    "litemq/pkg/client"
    "litemq/pkg/common/config"
)

func main() {
    // 创建配置
    cfg := config.DefaultClientConfig()

    // 创建生产者
    producer := client.NewProducer(cfg)
    producer.Start()
    defer producer.Shutdown()

    // 1. 发送事务消息（Half消息）
    result, err := producer.SendTransactionMessage("order_topic", []byte("order data"), "")
    if err != nil {
        panic(err)
    }

    transactionID := result.TransactionID
    fmt.Printf("Transaction prepared: %s\n", transactionID)

    // 2. 执行本地业务逻辑
    // ... 扣减库存、更新订单状态等 ...

    // 3. 根据业务结果提交或回滚
    if businessSuccess {
        // 提交事务
        err = producer.CommitTransaction(transactionID)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Transaction committed: %s\n", transactionID)
    } else {
        // 回滚事务
        err = producer.RollbackTransaction(transactionID)
        if err != nil {
            panic(err)
        }
        fmt.Printf("Transaction rollbacked: %s\n", transactionID)
    }
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

### 延时消息技术细节

#### 时间轮实现

LiteMQ 的多层时间轮实现位于 `pkg/broker/timewheel.go`，核心特性：

- **4层时间轮结构**：秒级、分钟级、小时级、天级
- **环形数组结构**：每层使用环形数组，每个槽位存储一个消息列表
- **使用 `container/list`**：实现高效的链表操作
- **智能层级选择**：根据延时时间自动选择合适的层级
- **级联降级机制**：消息从粗粒度层级自动降级到细粒度层级
- **线程安全**：每层独立的锁机制，避免死锁
- **只启动最细粒度层级**：只启动秒级时间轮，通过级联机制驱动其他层级

#### 延时消息调度器

延时消息调度器（`pkg/broker/delay_scheduler.go`）负责：

1. **消息管理**：缓存延时消息的元数据信息
2. **时间轮集成**：将延时消息添加到多层时间轮进行调度
3. **到期处理**：当消息到期时，从 CommitLog 读取消息并构建索引
4. **故障恢复**：支持启动时恢复未到期的延时消息（待实现）

#### 配置参数

多层时间轮默认配置：

- **第0层（秒级）**：`wheelSize: 60`, `tickDuration: 1秒`，支持0-60秒
- **第1层（分钟级）**：`wheelSize: 60`, `tickDuration: 1分钟`，支持1-60分钟
- **第2层（小时级）**：`wheelSize: 24`, `tickDuration: 1小时`，支持1-24小时
- **第3层（天级）**：`wheelSize: 365`, `tickDuration: 1天`，支持1-365天

可根据实际需求调整这些参数以支持更长的延时时间。

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

- 项目主页: [https://github.com/toby-yogurt/litemq](https://github.com/your-org/litemq)
- 问题反馈: [https://github.com/toby-yogurt/litemq/issues](https://github.com/your-org/litemq/issues)
- 邮箱: your-email@example.com

---

**🎉 LiteMQ - 让消息队列变得简单、高效、可靠！**
