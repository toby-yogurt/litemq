# 延时消息示例

这个示例展示了如何使用 LiteMQ 发送和消费延时消息。

## 运行示例

```bash
# 1. 启动 NameServer
make run-nameserver

# 2. 启动 Broker
make run-broker

# 3. 运行延时消息示例
cd examples/delay_message
go run main.go
```

## 示例说明

### 功能演示

示例会发送以下延时消息：

1. **5秒后执行** - 使用秒级时间轮
2. **10秒后执行** - 使用秒级时间轮
3. **30秒后执行** - 使用秒级时间轮（跨分钟边界）
4. **2分钟后执行** - 使用分钟级时间轮

### 工作原理

延时消息使用多层时间轮实现：

1. **消息存储**：延时消息先存储到 CommitLog，但不立即构建 ConsumeQueue 索引
2. **时间轮调度**：根据延时时间选择合适的层级
3. **时间推进**：只启动秒级时间轮，每秒推进一个槽位
4. **消息投递**：到期时构建索引，使消息对消费者可见

### 代码示例

```go
// 创建生产者
producer := client.NewProducer(cfg)
producer.Start()

// 发送延时消息（5秒后执行）
delayTime := time.Now().Add(5 * time.Second).UnixMilli()
result, err := producer.SendDelayMessage("delay_topic", []byte("延时消息"), delayTime)

// 创建消费者
consumer := client.NewConsumer(cfg, "delay_consumer_group")
consumer.Subscribe("delay_topic", messageHandler)
consumer.Start()
```

## 延时范围

- **0-60秒**：使用秒级时间轮
- **1-60分钟**：使用分钟级时间轮
- **1-24小时**：使用小时级时间轮
- **1-365天**：使用天级时间轮

