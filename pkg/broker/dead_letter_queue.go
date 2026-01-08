package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// DeadReason 死信原因
type DeadReason int

const (
	DeadReasonMaxRetry     DeadReason = 1 // 超过最大重试次数
	DeadReasonTTLExpired   DeadReason = 2 // TTL过期
	DeadReasonQueueFull    DeadReason = 3 // 队列溢出
	DeadReasonConsumeError DeadReason = 4 // 消费异常
	DeadReasonFilterOut    DeadReason = 5 // 过滤丢弃
	DeadReasonManual       DeadReason = 6 // 手动移到死信队列
)

// String 返回死信原因的字符串表示
func (dr DeadReason) String() string {
	switch dr {
	case DeadReasonMaxRetry:
		return "MaxRetry"
	case DeadReasonTTLExpired:
		return "TTLExpired"
	case DeadReasonQueueFull:
		return "QueueFull"
	case DeadReasonConsumeError:
		return "ConsumeError"
	case DeadReasonFilterOut:
		return "FilterOut"
	case DeadReasonManual:
		return "Manual"
	default:
		return "Unknown"
	}
}

// DeadMessage 死信消息
type DeadMessage struct {
	// 原始消息
	OriginalMessage *protocol.Message

	// 死信信息
	DeadReason    DeadReason `json:"dead_reason"`
	DeadTime      int64      `json:"dead_time"`      // 进入死信时间戳（毫秒）
	RetryCount    int        `json:"retry_count"`    // 重试次数
	MaxRetry      int        `json:"max_retry"`      // 最大重试次数
	LastError     string     `json:"last_error"`     // 最后错误信息
	ConsumerGroup string     `json:"consumer_group"` // 消费组
	ConsumerID    string     `json:"consumer_id"`    // 消费者ID

	// 上下文
	StackTrace string            `json:"stack_trace,omitempty"` // 异常堆栈
	Properties map[string]string `json:"properties,omitempty"`  // 扩展属性
}

// DeadLetterQueue 死信队列管理器
type DeadLetterQueue struct {
	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 死信消息存储: topic -> messageID -> DeadMessage
	deadMessages map[string]map[string]*DeadMessage
	mu           sync.RWMutex

	// 配置
	maxRetryCount int           // 最大重试次数（默认3次）
	retryInterval time.Duration // 重试间隔（默认60秒）
	retentionDays int           // 保留天数（默认7天）

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewDeadLetterQueue 创建死信队列管理器
func NewDeadLetterQueue(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *DeadLetterQueue {
	return &DeadLetterQueue{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		deadMessages:    make(map[string]map[string]*DeadMessage),
		maxRetryCount:   3,                // 默认最大重试3次
		retryInterval:   60 * time.Second, // 默认60秒重试间隔
		retentionDays:   7,                // 默认保留7天
		stopCh:          make(chan struct{}),
		running:         false,
	}
}

// Start 启动死信队列管理器
func (dlq *DeadLetterQueue) Start() error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if dlq.running {
		return fmt.Errorf("dead letter queue is already running")
	}

	dlq.running = true

	// 启动清理任务（定期清理过期死信消息）
	dlq.wg.Add(1)
	go dlq.cleanupLoop()

	logger.Info("Dead letter queue started",
		"maxRetryCount", dlq.maxRetryCount,
		"retryInterval", dlq.retryInterval,
		"retentionDays", dlq.retentionDays)

	return nil
}

// Stop 停止死信队列管理器
func (dlq *DeadLetterQueue) Stop() {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if !dlq.running {
		return
	}

	dlq.running = false
	close(dlq.stopCh)

	dlq.wg.Wait()

	logger.Info("Dead letter queue stopped")
}

// AddDeadMessage 添加死信消息
func (dlq *DeadLetterQueue) AddDeadMessage(
	msg *protocol.Message,
	consumerGroup string,
	reason DeadReason,
	errorMsg string,
) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	// 构建死信Topic名称（格式：%DLQ%{OriginalTopic}）
	deadTopic := dlq.buildDeadTopic(msg.Topic)

	// 创建死信消息
	deadMsg := &DeadMessage{
		OriginalMessage: msg.Clone(),
		ConsumerGroup:   consumerGroup,
		DeadReason:      reason,
		DeadTime:        time.Now().UnixMilli(),
		RetryCount:      dlq.getRetryCount(msg),
		MaxRetry:        dlq.maxRetryCount,
		LastError:       errorMsg,
		Properties:      make(map[string]string),
	}

	// 保存原始Topic信息
	deadMsg.Properties["original_topic"] = msg.Topic
	deadMsg.Properties["original_queue_id"] = fmt.Sprintf("%d", msg.QueueID)
	deadMsg.Properties["dead_reason"] = reason.String()

	// 初始化Topic的死信消息映射
	if dlq.deadMessages[deadTopic] == nil {
		dlq.deadMessages[deadTopic] = make(map[string]*DeadMessage)
	}

	// 存储死信消息
	dlq.deadMessages[deadTopic][msg.MessageID] = deadMsg

	// 将死信消息存储到CommitLog和ConsumeQueue
	if err := dlq.storeDeadMessage(deadMsg, deadTopic); err != nil {
		logger.Warn("Failed to store dead message",
			"messageId", msg.MessageID,
			"deadTopic", deadTopic,
			"error", err)
		// 即使存储失败，也保留在内存中
	}

	logger.Info("Dead message added",
		"messageId", msg.MessageID,
		"originalTopic", msg.Topic,
		"deadTopic", deadTopic,
		"reason", reason.String(),
		"consumerGroup", consumerGroup,
		"retryCount", deadMsg.RetryCount)

	return nil
}

// GetDeadMessages 获取死信消息列表
func (dlq *DeadLetterQueue) GetDeadMessages(originalTopic string, limit int) []*DeadMessage {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	deadTopic := dlq.buildDeadTopic(originalTopic)
	topicDeadMessages, exists := dlq.deadMessages[deadTopic]
	if !exists {
		return []*DeadMessage{}
	}

	messages := make([]*DeadMessage, 0, limit)
	count := 0
	for _, deadMsg := range topicDeadMessages {
		if count >= limit {
			break
		}
		// 返回副本
		messages = append(messages, deadMsg.clone())
		count++
	}

	return messages
}

// RetryDeadMessage 重试死信消息
func (dlq *DeadLetterQueue) RetryDeadMessage(originalTopic, messageID string) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	deadTopic := dlq.buildDeadTopic(originalTopic)
	topicDeadMessages, exists := dlq.deadMessages[deadTopic]
	if !exists {
		return fmt.Errorf("dead topic not found: %s", deadTopic)
	}

	deadMsg, exists := topicDeadMessages[messageID]
	if !exists {
		return fmt.Errorf("dead message not found: %s", messageID)
	}

	// 检查是否可以重试
	if deadMsg.RetryCount >= deadMsg.MaxRetry {
		return fmt.Errorf("max retry count reached: %d", deadMsg.MaxRetry)
	}

	// 增加重试次数
	deadMsg.RetryCount++
	deadMsg.Properties["last_retry_time"] = fmt.Sprintf("%d", time.Now().UnixMilli())

	logger.Info("Dead message retry",
		"messageId", messageID,
		"originalTopic", originalTopic,
		"retryCount", deadMsg.RetryCount,
		"maxRetry", deadMsg.MaxRetry)

	// 将消息重新投递到原始Topic
	// 如果函数参数为空，则从Properties中获取
	topicToUse := originalTopic
	if topicToUse == "" {
		topicFromProps := deadMsg.Properties["original_topic"]
		if topicFromProps == "" {
			return fmt.Errorf("original topic not found in dead message properties")
		}
		topicToUse = topicFromProps
	}

	// 创建新的消息（重置重试次数）
	msg := deadMsg.OriginalMessage.Clone()
	msg.Topic = topicToUse
	msg.MessageType = protocol.MessageTypeNormal

	// 重置重试次数
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties["retry_count"] = "0"
	msg.Properties["retry_from_dlq"] = "true"
	msg.Properties["original_dead_time"] = fmt.Sprintf("%d", deadMsg.DeadTime)

	// 通过 CommitLog 重新存储消息
	offset, err := dlq.commitLog.AppendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to append retry message to commitlog: %v", err)
	}

	msg.CommitLogOffset = offset

	// 构建 ConsumeQueue 索引
	cq, err := dlq.consumeQueueMgr.GetOrCreateConsumeQueue(originalTopic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	logger.Info("Dead message retried",
		"messageId", msg.MessageID,
		"originalTopic", originalTopic,
		"retryCount", deadMsg.RetryCount+1)

	return nil
}

// DeleteDeadMessage 删除死信消息
func (dlq *DeadLetterQueue) DeleteDeadMessage(originalTopic, messageID string) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	deadTopic := dlq.buildDeadTopic(originalTopic)
	topicDeadMessages, exists := dlq.deadMessages[deadTopic]
	if !exists {
		return fmt.Errorf("dead topic not found: %s", deadTopic)
	}

	if _, exists := topicDeadMessages[messageID]; !exists {
		return fmt.Errorf("dead message not found: %s", messageID)
	}

	delete(topicDeadMessages, messageID)

	// 如果Topic没有死信消息了，删除Topic映射
	if len(topicDeadMessages) == 0 {
		delete(dlq.deadMessages, deadTopic)
	}

	logger.Info("Dead message deleted",
		"messageId", messageID,
		"originalTopic", originalTopic)

	return nil
}

// buildDeadTopic 构建死信Topic名称
func (dlq *DeadLetterQueue) buildDeadTopic(originalTopic string) string {
	return fmt.Sprintf("%%DLQ%%%s", originalTopic)
}

// getRetryCount 从消息属性中获取重试次数
func (dlq *DeadLetterQueue) getRetryCount(msg *protocol.Message) int {
	retryStr := msg.GetProperty("retry_count")
	if retryStr == "" {
		return 0
	}

	var retryCount int
	fmt.Sscanf(retryStr, "%d", &retryCount)
	return retryCount
}

// storeDeadMessage 存储死信消息到CommitLog和ConsumeQueue
func (dlq *DeadLetterQueue) storeDeadMessage(deadMsg *DeadMessage, deadTopic string) error {
	// 创建死信消息的副本
	msg := deadMsg.OriginalMessage.Clone()
	msg.Topic = deadTopic
	msg.MessageType = protocol.MessageTypeNormal

	// 设置死信消息属性
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	for k, v := range deadMsg.Properties {
		msg.Properties[k] = v
	}
	msg.Properties["dead_reason"] = deadMsg.DeadReason.String()
	msg.Properties["dead_time"] = fmt.Sprintf("%d", deadMsg.DeadTime)
	msg.Properties["retry_count"] = fmt.Sprintf("%d", deadMsg.RetryCount)
	msg.Properties["consumer_group"] = deadMsg.ConsumerGroup

	// 存储到CommitLog
	offset, err := dlq.commitLog.AppendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to append dead message to commitlog: %v", err)
	}

	msg.CommitLogOffset = offset

	// 构建ConsumeQueue索引
	cq, err := dlq.consumeQueueMgr.GetOrCreateConsumeQueue(deadTopic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	return nil
}

// cleanupLoop 清理过期死信消息的循环
func (dlq *DeadLetterQueue) cleanupLoop() {
	defer dlq.wg.Done()

	ticker := time.NewTicker(24 * time.Hour) // 每天清理一次
	defer ticker.Stop()

	for {
		select {
		case <-dlq.stopCh:
			return
		case <-ticker.C:
			dlq.cleanupExpiredMessages()
		}
	}
}

// cleanupExpiredMessages 清理过期的死信消息
func (dlq *DeadLetterQueue) cleanupExpiredMessages() {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	now := time.Now()
	expireTime := now.AddDate(0, 0, -dlq.retentionDays)
	expireTimestamp := expireTime.UnixMilli()

	cleanedCount := 0

	for deadTopic, topicDeadMessages := range dlq.deadMessages {
		for messageID, deadMsg := range topicDeadMessages {
			if deadMsg.DeadTime < expireTimestamp {
				delete(topicDeadMessages, messageID)
				cleanedCount++

				logger.Debug("Cleaned expired dead message",
					"messageId", messageID,
					"deadTopic", deadTopic,
					"deadTime", deadMsg.DeadTime)
			}
		}

		// 如果Topic没有死信消息了，删除Topic映射
		if len(topicDeadMessages) == 0 {
			delete(dlq.deadMessages, deadTopic)
		}
	}

	if cleanedCount > 0 {
		logger.Info("Cleaned expired dead messages",
			"count", cleanedCount,
			"retentionDays", dlq.retentionDays)
	}
}

// GetStats 获取死信队列统计信息
func (dlq *DeadLetterQueue) GetStats() map[string]interface{} {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	totalDeadMessages := 0
	topicStats := make(map[string]int)

	for deadTopic, topicDeadMessages := range dlq.deadMessages {
		count := len(topicDeadMessages)
		topicStats[deadTopic] = count
		totalDeadMessages += count
	}

	return map[string]interface{}{
		"running":           dlq.running,
		"totalDeadMessages": totalDeadMessages,
		"topicStats":        topicStats,
		"maxRetryCount":     dlq.maxRetryCount,
		"retentionDays":     dlq.retentionDays,
	}
}

// clone 克隆死信消息
func (dm *DeadMessage) clone() *DeadMessage {
	clone := &DeadMessage{
		OriginalMessage: dm.OriginalMessage.Clone(),
		DeadReason:      dm.DeadReason,
		DeadTime:        dm.DeadTime,
		RetryCount:      dm.RetryCount,
		MaxRetry:        dm.MaxRetry,
		LastError:       dm.LastError,
		ConsumerGroup:   dm.ConsumerGroup,
		ConsumerID:      dm.ConsumerID,
		StackTrace:      dm.StackTrace,
		Properties:      make(map[string]string),
	}

	for k, v := range dm.Properties {
		clone.Properties[k] = v
	}

	return clone
}
