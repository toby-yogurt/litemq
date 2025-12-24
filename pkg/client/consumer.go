package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "litemq/api/proto"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

// Consumer 消息消费者
type Consumer struct {
	config          *config.ClientConfig
	groupName       string
	topics          []string
	nameServerAddrs []string
	brokerClient    pb.BrokerServiceClient
	grpcConn        *grpc.ClientConn
	currentBroker   string
	running         bool
	mutex           sync.RWMutex
	wg              sync.WaitGroup

	// 消费状态
	offsets        map[string]map[int]int64 // topic -> queueID -> offset
	messageHandler MessageHandler
}

// MessageHandler 消息处理函数
type MessageHandler func(msg *protocol.Message) protocol.ConsumeStatus

// NewConsumer 创建消费者
func NewConsumer(cfg *config.ClientConfig, groupName string) *Consumer {
	return &Consumer{
		config:          cfg,
		groupName:       groupName,
		nameServerAddrs: cfg.NameServers,
		running:         false,
		offsets:         make(map[string]map[int]int64),
	}
}

// Subscribe 订阅主题
func (c *Consumer) Subscribe(topic string, handler MessageHandler) error {
	return c.subscribe(topic, handler, false)
}

// SubscribeBroadcast 订阅广播消息
func (c *Consumer) SubscribeBroadcast(topic string, handler MessageHandler) error {
	return c.subscribe(topic, handler, true)
}

// subscribe 订阅主题（内部方法）
func (c *Consumer) subscribe(topic string, handler MessageHandler, broadcast bool) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 检查是否已订阅
	for _, t := range c.topics {
		if t == topic {
			return fmt.Errorf("topic %s is already subscribed", topic)
		}
	}

	// 添加主题
	c.topics = append(c.topics, topic)

	// 设置消息处理器
	c.messageHandler = handler

	// 初始化偏移量
	c.offsets[topic] = make(map[int]int64)

	logger.Info("Subscribed to topic",
		"topic", topic,
		"group", c.groupName)
	return nil
}

// Start 启动消费者
func (c *Consumer) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return fmt.Errorf("consumer is already running")
	}

	if len(c.topics) == 0 {
		return fmt.Errorf("no topics subscribed")
	}

	// 初始化连接
	if err := c.initConnections(); err != nil {
		return fmt.Errorf("failed to initialize connections: %v", err)
	}

	// 预先建立到 broker 的连接（所有 topic 使用同一个 broker）
	brokerAddr, err := c.selectBroker(c.topics[0])
	if err == nil {
		if err := c.connectToBroker(brokerAddr); err != nil {
			logger.Warn("Failed to pre-connect to broker, will connect on demand",
				"broker", brokerAddr,
				"error", err)
		}
	}

	// 启动消费循环
	c.running = true
	for _, topic := range c.topics {
		c.wg.Add(1)
		go c.consumeLoop(topic)
	}

	logger.Info("Consumer started",
		"group", c.groupName,
		"topics", c.topics)
	return nil
}

// Shutdown 关闭消费者
func (c *Consumer) Shutdown() error {
	c.mutex.Lock()
	if !c.running {
		c.mutex.Unlock()
		return nil
	}

	c.running = false
	// 关闭gRPC连接
	var grpcConn *grpc.ClientConn
	if c.grpcConn != nil {
		grpcConn = c.grpcConn
		c.grpcConn = nil
	}
	c.mutex.Unlock()

	// 在锁外关闭连接，避免阻塞
	if grpcConn != nil {
		grpcConn.Close()
	}

	// 在锁外等待goroutine完成，避免死锁
	c.wg.Wait()

	logger.Info("Consumer shutdown", "group", c.groupName)
	return nil
}

// consumeLoop 消费循环
func (c *Consumer) consumeLoop(topic string) {
	defer c.wg.Done()

	for c.IsRunning() {
		if err := c.pullAndConsume(topic); err != nil {
			logger.Warn("Error in consume loop", "topic", topic, "error", err)
			time.Sleep(time.Second) // 错误后等待1秒重试
		}
	}
}

// pullAndConsume 拉取并消费消息
func (c *Consumer) pullAndConsume(topic string) error {
	// 获取消费偏移量
	offsets := c.getConsumeOffsets(topic)

	// 为每个队列拉取消息
	for queueID, offset := range offsets {
		messages, err := c.pullMessages(topic, queueID, offset, c.config.PullBatchSize)
		if err != nil {
			logger.Warn("Failed to pull messages",
				"topic", topic,
				"queue", queueID,
				"error", err)
			continue
		}

		if len(messages) == 0 {
			// 没有消息，等待一下
			time.Sleep(c.config.PullInterval)
			continue
		}

		// 消费消息
		if err := c.consumeMessages(topic, queueID, messages); err != nil {
			logger.Warn("Failed to consume messages",
				"topic", topic,
				"queue", queueID,
				"error", err)
			continue
		}
	}

	return nil
}

// pullMessages 拉取消息
func (c *Consumer) pullMessages(topic string, queueID int, offset int64, maxCount int) ([]*protocol.Message, error) {
	// 选择Broker
	brokerAddr, err := c.selectBroker(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to select broker: %v", err)
	}

	// 发送拉取消息请求
	return c.pullFromBroker(brokerAddr, topic, queueID, offset, maxCount)
}

// selectBroker 选择Broker
func (c *Consumer) selectBroker(topic string) (string, error) {
	// 从NameServer获取路由信息
	routeInfo, err := c.fetchRouteInfo(topic)
	if err != nil {
		return "", fmt.Errorf("failed to fetch route info: %v", err)
	}

	if len(routeInfo.BrokerAddrs) == 0 {
		return "", fmt.Errorf("no broker available for topic %s", topic)
	}

	// 简单选择第一个Broker
	return routeInfo.BrokerAddrs[0], nil
}

// fetchRouteInfo 获取路由信息
func (c *Consumer) fetchRouteInfo(topic string) (*TopicRouteInfo, error) {
	// 选择NameServer
	nsAddr := c.selectNameServer()

	// 发送获取路由信息请求
	logger.Info("Fetching route info",
		"topic", topic,
		"nameserver", nsAddr)

	// 模拟返回路由信息
	return &TopicRouteInfo{
		TopicName:   topic,
		BrokerAddrs: []string{"localhost:10911"},
	}, nil
}

// selectNameServer 选择NameServer
func (c *Consumer) selectNameServer() string {
	if len(c.nameServerAddrs) == 0 {
		return ""
	}
	return c.nameServerAddrs[0]
}

// pullFromBroker 从Broker拉取消息
func (c *Consumer) pullFromBroker(brokerAddr, topic string, queueID int, offset int64, maxCount int) ([]*protocol.Message, error) {
	// 先检查连接状态（使用读锁）
	c.mutex.RLock()
	needConnect := c.currentBroker != brokerAddr || c.grpcConn == nil
	client := c.brokerClient
	c.mutex.RUnlock()

	// 如果需要连接，使用写锁建立连接
	if needConnect {
		c.mutex.Lock()
		// 双重检查，可能其他 goroutine 已经建立了连接
		if c.currentBroker != brokerAddr || c.grpcConn == nil {
			if err := c.connectToBroker(brokerAddr); err != nil {
				c.mutex.Unlock()
				return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddr, err)
			}
		}
		client = c.brokerClient
		c.mutex.Unlock()
	}

	// 发送拉取消息请求（不需要持有锁）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Info("Sending pull message request",
		"broker", brokerAddr,
		"group", c.groupName,
		"topic", topic,
		"queueId", queueID,
		"offset", offset,
		"maxCount", maxCount)

	resp, err := client.PullMessage(ctx, &pb.PullMessageRequest{
		GroupName: c.groupName,
		Topic:     topic,
		QueueId:   int32(queueID),
		Offset:    offset,
		MaxCount:  int32(maxCount),
	})
	if err != nil {
		logger.Warn("Failed to pull messages from broker",
			"broker", brokerAddr,
			"topic", topic,
			"queueId", queueID,
			"error", err)
		return nil, fmt.Errorf("failed to pull messages: %v", err)
	}

	logger.Info("Received pull message response",
		"topic", topic,
		"queueId", queueID,
		"messageCount", len(resp.Messages))

	// 转换消息格式
	messages := make([]*protocol.Message, len(resp.Messages))
	for i, protoMsg := range resp.Messages {
		messages[i] = convertProtoToMessage(protoMsg)
	}

	//// 临时返回空消息列表
	//messages := make([]*protocol.Message, 0)
	return messages, nil
}

// connectToBroker 连接到Broker
func (c *Consumer) connectToBroker(brokerAddr string) error {
	// 关闭现有连接
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}

	// 创建新连接
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	c.grpcConn = conn
	c.brokerClient = pb.NewBrokerServiceClient(conn)
	c.currentBroker = brokerAddr

	logger.Info("Consumer connected to broker", "broker", brokerAddr)
	return nil
}

// convertProtoToMessage 将proto消息转换为内部消息格式
func convertProtoToMessage(protoMsg *pb.Message) *protocol.Message {
	msg := protocol.NewMessage(protoMsg.Topic, protoMsg.Body)

	msg.MessageID = protoMsg.MessageId
	msg.Tags = protoMsg.Tags
	msg.Keys = protoMsg.Keys
	msg.Properties = protoMsg.Properties
	msg.MessageType = protocol.MessageType(protoMsg.MessageType)
	msg.Priority = int(protoMsg.Priority)
	msg.Reliability = int(protoMsg.Reliability)
	msg.DelayTime = protoMsg.DelayTime
	msg.TransactionID = protoMsg.TransactionId
	msg.MessageStatus = protocol.MessageStatus(protoMsg.MessageStatus)
	msg.ShardingKey = protoMsg.ShardingKey
	msg.Broadcast = protoMsg.Broadcast
	msg.BornTimestamp = protoMsg.BornTimestamp
	msg.BornHost = protoMsg.BornHost
	msg.QueueID = int(protoMsg.QueueId)
	msg.QueueOffset = protoMsg.QueueOffset
	msg.CommitLogOffset = protoMsg.CommitLogOffset
	msg.StoreSize = int(protoMsg.StoreSize)
	msg.StoreTimestamp = protoMsg.StoreTimestamp
	msg.ConsumeStartTime = protoMsg.ConsumeStartTime
	msg.ConsumeEndTime = protoMsg.ConsumeEndTime
	msg.ConsumeCount = int(protoMsg.ConsumeCount)

	return msg
}

// consumeMessages 消费消息
func (c *Consumer) consumeMessages(topic string, queueID int, messages []*protocol.Message) error {
	for _, msg := range messages {
		// 调用消息处理器
		status := c.messageHandler(msg)

		// 根据消费结果处理
		switch status {
		case protocol.ConsumeStatusSuccess:
			// 更新消费偏移量
			c.updateConsumeOffset(topic, queueID, msg.QueueOffset+1)
			// 发送消费确认
			c.ackMessage(topic, queueID, msg.QueueOffset)

		case protocol.ConsumeStatusRetry:
			// 重试消息，不更新偏移量
			logger.Info("Message will be retried later", "message_id", msg.MessageID)

		case protocol.ConsumeStatusFail:
			// 消费失败，记录错误但继续处理
			logger.Warn("Failed to consume message", "message_id", msg.MessageID)
		}
	}

	return nil
}

// ackMessage 发送消费确认
func (c *Consumer) ackMessage(topic string, queueID int, offset int64) error {
	// 选择Broker
	brokerAddr, err := c.selectBroker(topic)
	if err != nil {
		return fmt.Errorf("failed to select broker for ack: %v", err)
	}

	// 发送消费确认命令
	cmd := protocol.NewCommand(protocol.CommandConsumeAck)
	cmd.SetHeader("group", c.groupName)
	cmd.SetHeader("topic", topic)
	cmd.SetHeader("queue_id", fmt.Sprintf("%d", queueID))
	cmd.SetHeader("offset", fmt.Sprintf("%d", offset))

	logger.Info("Sending ack",
		"broker", brokerAddr,
		"topic", topic,
		"queue", queueID,
		"offset", offset)

	// 发送到Broker
	return nil
}

// getConsumeOffsets 获取消费偏移量
func (c *Consumer) getConsumeOffsets(topic string) map[int]int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	offsets, exists := c.offsets[topic]
	if !exists || len(offsets) == 0 {
		// 如果不存在或为空，默认从队列 0 的 offset 0 开始消费
		return map[int]int64{0: 0}
	}

	// 复制一份避免并发问题
	result := make(map[int]int64)
	for queueID, offset := range offsets {
		result[queueID] = offset
	}

	return result
}

// updateConsumeOffset 更新消费偏移量
func (c *Consumer) updateConsumeOffset(topic string, queueID int, offset int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.offsets[topic]; !exists {
		c.offsets[topic] = make(map[int]int64)
	}

	c.offsets[topic][queueID] = offset
}

// initConnections 初始化连接
func (c *Consumer) initConnections() error {
	// 初始化到NameServer和Broker的连接
	logger.Info("Initializing connections for consumer group", "group", c.groupName)
	return nil
}

// Seek 跳转到指定偏移量
func (c *Consumer) Seek(topic string, queueID int, offset int64) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return fmt.Errorf("consumer is not running")
	}

	if _, exists := c.offsets[topic]; !exists {
		c.offsets[topic] = make(map[int]int64)
	}

	c.offsets[topic][queueID] = offset
	logger.Info("Consumer seek offset",
		"group", c.groupName,
		"offset", offset,
		"topic", topic,
		"queue", queueID)

	return nil
}

// GetStats 获取统计信息
func (c *Consumer) GetStats() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return map[string]interface{}{
		"group_name": c.groupName,
		"topics":     c.topics,
		"running":    c.running,
		"offsets":    c.offsets,
	}
}

// IsRunning 检查消费者是否正在运行
func (c *Consumer) IsRunning() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.running
}
