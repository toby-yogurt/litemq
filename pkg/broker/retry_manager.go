package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// RetryManager 服务端消息重试管理器
// 管理消费失败的消息，按照重试策略重新投递
type RetryManager struct {
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 重试队列：consumerGroup -> topic -> retryQueue
	retryQueues map[string]map[string]*RetryQueue
	mu          sync.RWMutex

	// 重试配置
	retryLevels []time.Duration // 重试级别（延时时间）
	maxRetry    int             // 最大重试次数

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// RetryQueue 重试队列
type RetryQueue struct {
	ConsumerGroup string
	Topic         string
	Messages      []*RetryMessage
	mu            sync.RWMutex
}

// RetryMessage 重试消息
type RetryMessage struct {
	Message         *protocol.Message
	OriginalOffset  int64
	RetryCount      int
	NextRetryTime   time.Time
	CommitLogOffset int64 // 重试消息在CommitLog中的偏移量
}

// NewRetryManager 创建重试管理器
func NewRetryManager(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *RetryManager {
	// 默认重试级别（参考RocketMQ）
	retryLevels := []time.Duration{
		10 * time.Second, // 第1次重试：10秒后
		30 * time.Second, // 第2次重试：30秒后
		1 * time.Minute,  // 第3次重试：1分钟后
		2 * time.Minute,  // 第4次重试：2分钟后
		3 * time.Minute,  // 第5次重试：3分钟后
		4 * time.Minute,  // 第6次重试：4分钟后
		5 * time.Minute,  // 第7次重试：5分钟后
		6 * time.Minute,  // 第8次重试：6分钟后
		7 * time.Minute,  // 第9次重试：7分钟后
		8 * time.Minute,  // 第10次重试：8分钟后
		9 * time.Minute,  // 第11次重试：9分钟后
		10 * time.Minute, // 第12次重试：10分钟后
		20 * time.Minute, // 第13次重试：20分钟后
		30 * time.Minute, // 第14次重试：30分钟后
		1 * time.Hour,    // 第15次重试：1小时后
		2 * time.Hour,    // 第16次重试：2小时后
	}

	return &RetryManager{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		retryQueues:     make(map[string]map[string]*RetryQueue),
		retryLevels:     retryLevels,
		maxRetry:        16, // 最多重试16次
		stopCh:          make(chan struct{}),
		running:         false,
	}
}

// Start 启动重试管理器
func (rm *RetryManager) Start() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.running {
		return fmt.Errorf("retry manager is already running")
	}

	rm.running = true

	// 启动重试投递协程
	rm.wg.Add(1)
	go rm.retryLoop()

	logger.Info("Retry manager started")
	return nil
}

// Stop 停止重试管理器
func (rm *RetryManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.running {
		return
	}

	rm.running = false
	close(rm.stopCh)
	rm.wg.Wait()

	logger.Info("Retry manager stopped")
}

// AddRetryMessage 添加重试消息
func (rm *RetryManager) AddRetryMessage(
	consumerGroup, topic string,
	msg *protocol.Message,
	originalOffset int64,
	retryCount int,
) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 检查是否超过最大重试次数
	if retryCount >= rm.maxRetry {
		logger.Warn("Message exceeded max retry count, moving to dead letter queue",
			"messageId", msg.MessageID,
			"consumerGroup", consumerGroup,
			"topic", topic,
			"retryCount", retryCount)
		// 超过最大重试次数，应该移动到死信队列
		// 这里返回错误，由调用方处理
		return fmt.Errorf("max retry count exceeded: %d", retryCount)
	}

	// 获取重试延时时间
	retryDelay := rm.getRetryDelay(retryCount)

	// 创建重试消息
	retryMsg := &RetryMessage{
		Message:        msg.Clone(),
		OriginalOffset: originalOffset,
		RetryCount:     retryCount,
		NextRetryTime:  time.Now().Add(retryDelay),
	}

	// 设置重试Topic（格式：%RETRY%consumerGroup）
	retryTopic := fmt.Sprintf("%%RETRY%%%s", consumerGroup)
	retryMsg.Message.Topic = retryTopic

	// 设置重试属性
	if retryMsg.Message.Properties == nil {
		retryMsg.Message.Properties = make(map[string]string)
	}
	retryMsg.Message.Properties["retry_count"] = fmt.Sprintf("%d", retryCount)
	retryMsg.Message.Properties["original_topic"] = topic
	retryMsg.Message.Properties["original_offset"] = fmt.Sprintf("%d", originalOffset)

	// 存储到CommitLog
	offset, err := rm.commitLog.AppendMessage(retryMsg.Message)
	if err != nil {
		return fmt.Errorf("failed to append retry message to commitlog: %v", err)
	}
	retryMsg.CommitLogOffset = offset

	// 添加到重试队列
	rm.getOrCreateRetryQueue(consumerGroup, topic).addMessage(retryMsg)

	logger.Info("Retry message added",
		"messageId", msg.MessageID,
		"consumerGroup", consumerGroup,
		"topic", topic,
		"retryCount", retryCount,
		"nextRetryTime", retryMsg.NextRetryTime)

	return nil
}

// retryLoop 重试投递循环
func (rm *RetryManager) retryLoop() {
	defer rm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			rm.processRetryMessages()
		}
	}
}

// processRetryMessages 处理到期的重试消息
func (rm *RetryManager) processRetryMessages() {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	now := time.Now()

	for _, topicQueues := range rm.retryQueues {
		for _, queue := range topicQueues {
			queue.mu.Lock()
			// 查找到期的消息
			expiredMessages := make([]*RetryMessage, 0)
			remainingMessages := make([]*RetryMessage, 0)

			for _, retryMsg := range queue.Messages {
				if now.After(retryMsg.NextRetryTime) || now.Equal(retryMsg.NextRetryTime) {
					expiredMessages = append(expiredMessages, retryMsg)
				} else {
					remainingMessages = append(remainingMessages, retryMsg)
				}
			}

			queue.Messages = remainingMessages
			queue.mu.Unlock()

			// 投递到期的重试消息
			for _, retryMsg := range expiredMessages {
				rm.deliverRetryMessage(retryMsg)
			}
		}
	}
}

// deliverRetryMessage 投递重试消息
func (rm *RetryManager) deliverRetryMessage(retryMsg *RetryMessage) {
	// 获取原始Topic
	originalTopic := retryMsg.Message.GetProperty("original_topic")
	if originalTopic == "" {
		logger.Warn("Original topic not found in retry message",
			"messageId", retryMsg.Message.MessageID)
		return
	}

	// 创建投递消息（恢复到原始Topic）
	deliveryMsg := retryMsg.Message.Clone()
	deliveryMsg.Topic = originalTopic
	deliveryMsg.MessageType = protocol.MessageTypeNormal

	// 构建ConsumeQueue索引，使消息对消费者可见
	cq, err := rm.consumeQueueMgr.GetOrCreateConsumeQueue(originalTopic, deliveryMsg.QueueID)
	if err != nil {
		logger.Warn("Failed to get consume queue for retry message",
			"messageId", retryMsg.Message.MessageID,
			"topic", originalTopic,
			"error", err)
		return
	}

	if err := cq.AppendMessage(deliveryMsg); err != nil {
		logger.Warn("Failed to append retry message to consume queue",
			"messageId", retryMsg.Message.MessageID,
			"topic", originalTopic,
			"error", err)
		return
	}

	logger.Info("Retry message delivered",
		"messageId", retryMsg.Message.MessageID,
		"topic", originalTopic,
		"retryCount", retryMsg.RetryCount)
}

// getRetryDelay 获取重试延时时间
func (rm *RetryManager) getRetryDelay(retryCount int) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	if retryCount >= len(rm.retryLevels) {
		// 超过配置的重试级别，使用最后一个级别
		return rm.retryLevels[len(rm.retryLevels)-1]
	}
	return rm.retryLevels[retryCount]
}

// getOrCreateRetryQueue 获取或创建重试队列
func (rm *RetryManager) getOrCreateRetryQueue(consumerGroup, topic string) *RetryQueue {
	if rm.retryQueues[consumerGroup] == nil {
		rm.retryQueues[consumerGroup] = make(map[string]*RetryQueue)
	}

	if rm.retryQueues[consumerGroup][topic] == nil {
		rm.retryQueues[consumerGroup][topic] = &RetryQueue{
			ConsumerGroup: consumerGroup,
			Topic:         topic,
			Messages:      make([]*RetryMessage, 0),
		}
	}

	return rm.retryQueues[consumerGroup][topic]
}

// addMessage 添加消息到重试队列
func (rq *RetryQueue) addMessage(msg *RetryMessage) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.Messages = append(rq.Messages, msg)
}

// GetStats 获取重试管理器统计信息
func (rm *RetryManager) GetStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	totalRetryMessages := 0
	for _, topicQueues := range rm.retryQueues {
		for _, queue := range topicQueues {
			queue.mu.RLock()
			totalRetryMessages += len(queue.Messages)
			queue.mu.RUnlock()
		}
	}

	return map[string]interface{}{
		"running":            rm.running,
		"totalRetryQueues":   len(rm.retryQueues),
		"totalRetryMessages": totalRetryMessages,
		"maxRetry":           rm.maxRetry,
	}
}
