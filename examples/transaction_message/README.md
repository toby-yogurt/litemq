# 事务消息示例

这个示例展示了如何使用 LiteMQ 实现分布式事务消息的两阶段提交机制。

## 运行示例

```bash
# 1. 启动 NameServer
make run-nameserver

# 2. 启动 Broker
make run-broker

# 3. 运行事务消息示例
cd examples/transaction_message
go run main.go
```

## 示例说明

### 场景演示

示例包含三个场景：

#### 场景1: 事务成功提交
1. 发送事务消息（Half消息）
2. 执行本地事务（创建订单）
3. 提交事务

#### 场景2: 事务回滚
1. 发送事务消息（Half消息）
2. 执行本地事务（检查库存，发现不足）
3. 回滚事务

#### 场景3: 事务回查
1. 发送事务消息（Half消息）
2. 执行本地事务（但网络故障，未收到确认）
3. Broker主动回查事务状态

### 工作原理

事务消息使用两阶段提交（2PC）机制：

#### 阶段1: 预提交（Half消息）
- 生产者发送事务消息到Broker
- Broker存储为Half消息（PREPARED状态）
- 消息存储在系统内部Topic（RMQ_SYS_TRANS_HALF_TOPIC）
- 消费者不可见（未构建ConsumeQueue索引）

#### 阶段2: 提交/回滚
- 生产者执行本地事务
- 根据结果发送提交或回滚命令
- Broker处理命令：
  - **提交**：将消息从Half消息存储移动到正常Topic，构建索引
  - **回滚**：删除Half消息

#### 回查机制
- Broker定期检查长时间未决的事务消息
- 主动向生产者查询事务状态
- 根据查询结果决定提交或回滚
- 确保事务的最终一致性

### 代码示例

```go
// 创建生产者
producer := client.NewProducer(cfg)
producer.Start()

// 1. 发送事务消息（Half消息）
result, err := producer.SendTransactionMessage("transaction_topic", []byte("订单数据"), "")
transactionID := result.MessageID

// 2. 执行本地事务
businessSuccess := executeLocalTransaction()

// 3. 根据结果提交或回滚
if businessSuccess {
    producer.CommitTransaction(transactionID)
} else {
    producer.RollbackTransaction(transactionID)
}
```

## 优势

- ✅ 保证分布式事务的最终一致性
- ✅ 支持事务回查，处理网络故障
- ✅ 消息不丢失，即使生产者崩溃也能恢复
- ✅ 消费者只看到已提交的事务消息

