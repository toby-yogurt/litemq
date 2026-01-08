package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// ConsumerInfo 消费者信息
type ConsumerInfo struct {
	GroupName     string            `json:"group_name"`
	ConsumerID    string            `json:"consumer_id"`
	Topics        []string          `json:"topics"`
	Status        string            `json:"status"` // online, offline
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	Properties    map[string]string `json:"properties"`
	// 订阅信息：topic -> filter expression
	Subscriptions map[string]string `json:"subscriptions"` // topic -> filter expression
}

// ConsumerGroup 消费者组
type ConsumerGroup struct {
	GroupName    string                   `json:"group_name"`
	Consumers    map[string]*ConsumerInfo `json:"consumers"`
	TopicOffsets map[string]map[int]int64 `json:"topic_offsets"` // topic -> queueID -> offset
	Mutex        sync.RWMutex             `json:"-"`             // 不序列化
}

// ConsumerManager 消费者管理器
type ConsumerManager struct {
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager
	deadLetterQueue *DeadLetterQueue
	orderMessageMgr *OrderMessageManager
	retryManager    *RetryManager
	flowControl     *FlowControl
	checkPoint      *storage.CheckPoint
	config          *config.BrokerConfig
	consumerGroups  map[string]*ConsumerGroup // groupName -> ConsumerGroup
	running         bool
	stopCh          chan struct{}
	mutex           sync.RWMutex
	wg              sync.WaitGroup
}

// NewConsumerManager 创建消费者管理器
func NewConsumerManager(commitLog *storage.CommitLog, consumeQueueMgr *storage.ConsumeQueueManager, cfg *config.BrokerConfig) *ConsumerManager {
	return &ConsumerManager{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		config:          cfg,
		consumerGroups:  make(map[string]*ConsumerGroup),
		stopCh:          make(chan struct{}),
		running:         false,
		// deadLetterQueue 将在 Broker 初始化后设置
	}
}

// SetDeadLetterQueue 设置死信队列管理器
func (cm *ConsumerManager) SetDeadLetterQueue(dlq *DeadLetterQueue) {
	cm.deadLetterQueue = dlq
}

// SetOrderMessageManager 设置顺序消息管理器
func (cm *ConsumerManager) SetOrderMessageManager(omm *OrderMessageManager) {
	cm.orderMessageMgr = omm
}

// SetRetryManager 设置重试管理器
func (cm *ConsumerManager) SetRetryManager(rm *RetryManager) {
	cm.retryManager = rm
}

// SetFlowControl 设置流控管理器
func (cm *ConsumerManager) SetFlowControl(fc *FlowControl) {
	cm.flowControl = fc
}

// SetCheckPoint 设置检查点管理器
func (cm *ConsumerManager) SetCheckPoint(cp *storage.CheckPoint) {
	cm.checkPoint = cp
}

// Start 启动消费者管理器
func (cm *ConsumerManager) Start() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.running {
		return fmt.Errorf("consumer manager is already running")
	}

	cm.running = true

	// 启动消费者清理任务
	cm.wg.Add(1)
	go cm.consumerCleanupLoop()

	logger.Info("Consumer manager started")
	return nil
}

// Stop 停止消费者管理器
func (cm *ConsumerManager) Stop() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.running {
		return
	}

	cm.running = false
	close(cm.stopCh)

	cm.wg.Wait()
	logger.Info("Consumer manager stopped")
}

// consumerCleanupLoop 消费者清理循环
func (cm *ConsumerManager) consumerCleanupLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // 每30秒清理一次
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.cleanupExpiredConsumers()
		}
	}
}

// cleanupExpiredConsumers 清理过期消费者
func (cm *ConsumerManager) cleanupExpiredConsumers() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	now := time.Now()
	expiredGroups := make([]string, 0)

	for groupName, group := range cm.consumerGroups {
		group.Mutex.Lock()

		expiredConsumers := make([]string, 0)
		for consumerID, consumer := range group.Consumers {
			// 如果最后心跳时间超过2分钟，认为消费者已离线
			if now.Sub(consumer.LastHeartbeat) > 2*time.Minute {
				expiredConsumers = append(expiredConsumers, consumerID)
			}
		}

		// 移除过期消费者
		for _, consumerID := range expiredConsumers {
			delete(group.Consumers, consumerID)
			logger.Info("Removed expired consumer",
				"consumer_id", consumerID,
				"group", groupName)
		}

		// 如果消费者组为空，标记为过期
		if len(group.Consumers) == 0 {
			expiredGroups = append(expiredGroups, groupName)
		}

		group.Mutex.Unlock()
	}

	// 移除空消费者组
	for _, groupName := range expiredGroups {
		delete(cm.consumerGroups, groupName)
		logger.Info("Removed empty consumer group", "group", groupName)
	}
}

// RegisterConsumer 注册消费者
func (cm *ConsumerManager) RegisterConsumer(groupName, consumerID string, topics []string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// 获取或创建消费者组
	group, exists := cm.consumerGroups[groupName]
	if !exists {
		group = &ConsumerGroup{
			GroupName:    groupName,
			Consumers:    make(map[string]*ConsumerInfo),
			TopicOffsets: make(map[string]map[int]int64),
		}
		cm.consumerGroups[groupName] = group
	}

	group.Mutex.Lock()
	defer group.Mutex.Unlock()

	// 注册消费者
	consumer := &ConsumerInfo{
		GroupName:     groupName,
		ConsumerID:    consumerID,
		Topics:        topics,
		Status:        "online",
		LastHeartbeat: time.Now(),
		Properties:    make(map[string]string),
		Subscriptions: make(map[string]string), // 初始化订阅信息
	}
	group.Consumers[consumerID] = consumer

	// 初始化主题偏移量
	for _, topic := range topics {
		if _, exists := group.TopicOffsets[topic]; !exists {
			group.TopicOffsets[topic] = make(map[int]int64)
		}
	}

	logger.Info("Consumer registered", "consumer_id", consumerID, "group", groupName)

	// 如果是广播消息，注册消费者组到广播管理器
	// 从配置获取队列数量，如果没有配置则使用默认值4
	queueCount := 4
	if cm.config != nil {
		// 可以从配置中获取队列数量，这里暂时使用默认值
		// 后续可以在配置中添加 QueueCount 字段
	}
	// 为每个Topic注册消费者组
	for _, topic := range topics {
		// 这里需要访问广播管理器，但ConsumerManager没有直接引用
		// 可以通过Broker获取，或者通过依赖注入
		// 暂时先不实现，后续可以通过Broker的接口调用
		_ = topic
		_ = queueCount
	}

	return nil
}

// UnregisterConsumer 注销消费者
func (cm *ConsumerManager) UnregisterConsumer(groupName, consumerID string) error {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group not found: %s", groupName)
	}

	group.Mutex.Lock()
	defer group.Mutex.Unlock()

	if _, exists := group.Consumers[consumerID]; !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	delete(group.Consumers, consumerID)
	logger.Info("Consumer unregistered", "consumer_id", consumerID, "group", groupName)

	// 释放消费者持有的所有队列锁
	if cm.orderMessageMgr != nil {
		cm.orderMessageMgr.ReleaseAllQueueLocks(groupName, consumerID)
	}

	return nil
}

// UpdateConsumerHeartbeat 更新消费者心跳
func (cm *ConsumerManager) UpdateConsumerHeartbeat(groupName, consumerID string) error {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group not found: %s", groupName)
	}

	group.Mutex.Lock()
	defer group.Mutex.Unlock()

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	consumer.LastHeartbeat = time.Now()
	consumer.Status = "online"

	return nil
}

// HandlePullMessage 处理拉取消息
func (cm *ConsumerManager) HandlePullMessage(cmd *protocol.Command) *protocol.Command {
	// 解析请求参数
	var groupName, topic string
	var queueID int
	var offset int64
	var maxCount int

	// 从命令体解析参数
	_ = groupName // 临时使用
	_ = topic
	_ = queueID
	_ = offset
	_ = maxCount

	// 拉取消息（从命令中获取consumerID，如果没有则使用groupName）
	consumerID := groupName // 简化处理，实际应该从命令中解析
	messages, err := cm.pullMessages(groupName, topic, queueID, offset, maxCount, consumerID)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseSystemError,
			fmt.Sprintf("failed to pull messages: %v", err))
	}

	// 返回消息列表
	response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
	response.SetExtField("messages", messages)
	response.SetExtField("count", len(messages))

	return response
}

// HandleConsumeAck 处理消费确认
func (cm *ConsumerManager) HandleConsumeAck(cmd *protocol.Command) *protocol.Command {
	// 解析确认参数
	var groupName, topic string
	var queueID int
	var offset int64

	// 从命令体解析参数
	_ = groupName // 临时使用
	_ = topic
	_ = queueID
	_ = offset

	// 更新消费偏移量
	if err := cm.updateConsumeOffset(groupName, topic, queueID, offset); err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseSystemError,
			fmt.Sprintf("failed to update consume offset: %v", err))
	}

	return protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
}

// pullMessages 拉取消息
func (cm *ConsumerManager) pullMessages(groupName, topic string, queueID int, offset int64, maxCount int, consumerID string) ([]*protocol.Message, error) {
	// 流控检查：消费限流
	if cm.flowControl != nil {
		// 检查拉取频率和批量大小限流
		allowed, err := cm.flowControl.CheckPullLimit(consumerID, maxCount)
		if !allowed {
			return nil, fmt.Errorf("consumer pull flow control limit exceeded: %v", err)
		}

		// 检查并发线程限流
		allowed, err = cm.flowControl.CheckConsumerThreadLimit(consumerID)
		if !allowed {
			return nil, fmt.Errorf("consumer thread limit exceeded: %v", err)
		}

		// 获取线程（增加活跃线程数）
		if err := cm.flowControl.AcquireConsumerThread(consumerID); err != nil {
			return nil, fmt.Errorf("failed to acquire consumer thread: %v", err)
		}
		// 注意：需要在消费完成后释放线程，实际应该在消费逻辑中处理
	}

	// 如果是顺序消息，需要先锁定队列
	if cm.orderMessageMgr != nil {
		// 尝试锁定队列（对于顺序消息，同一队列只能被一个消费者消费）
		if err := cm.orderMessageMgr.LockQueue(topic, queueID, consumerID); err != nil {
			// 队列已被其他消费者锁定，返回空消息列表
			logger.Debug("Queue is locked by another consumer",
				"topic", topic,
				"queueID", queueID,
				"consumerID", consumerID,
				"error", err)
			return []*protocol.Message{}, nil
		}
	}

	// 获取或创建ConsumeQueue
	cq, err := cm.consumeQueueMgr.GetOrCreateConsumeQueue(topic, queueID)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create consume queue for topic %s queue %d: %v", topic, queueID, err)
	}

	// 计算起始索引
	startIndex := offset / 24 // 每个索引项24字节

	logger.Info("Pulling messages",
		"group", groupName,
		"topic", topic,
		"queue", queueID,
		"offset", offset,
		"startIndex", startIndex,
		"maxCount", maxCount,
		"queueEntryCount", cq.GetEntryCount())

	// 获取索引项
	entries, err := cq.GetEntries(startIndex, maxCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get consume queue entries: %v", err)
	}

	logger.Info("Got entries from consume queue",
		"topic", topic,
		"queue", queueID,
		"entryCount", len(entries))

	messages := make([]*protocol.Message, 0, len(entries))

	// 根据索引读取消息
	for _, entry := range entries {
		msg, err := cm.readMessageByOffset(entry.Offset, entry.MessageSize)
		if err != nil {
			logger.Warn("Failed to read message",
				"offset", entry.Offset,
				"messageSize", entry.MessageSize,
				"error", err)
			continue
		}

		// 应用消息过滤（从消费者订阅信息中获取过滤条件）
		if filter := cm.getConsumerFilter(groupName, topic); filter != nil {
			if !FilterMessage(msg, filter) {
				// 消息不匹配过滤条件，跳过
				logger.Debug("Message filtered out",
					"messageId", msg.MessageID,
					"topic", topic,
					"filter", filter.Expression)
				continue
			}
		}

		messages = append(messages, msg)
		logger.Info("Successfully read message from commitlog",
			"messageId", msg.MessageID,
			"topic", msg.Topic,
			"queueId", msg.QueueID,
			"offset", entry.Offset)
	}

	logger.Info("Pulled messages",
		"topic", topic,
		"queue", queueID,
		"messageCount", len(messages))

	return messages, nil
}

// PullMessages 拉取消息（供gRPC调用）
func (cm *ConsumerManager) PullMessages(groupName, topic string, queueID int, offset int64, maxCount int, consumerID string) ([]*protocol.Message, error) {
	return cm.pullMessages(groupName, topic, queueID, offset, maxCount, consumerID)
}

// AckMessage 确认消息消费（供gRPC调用）
func (cm *ConsumerManager) AckMessage(groupName, topic string, queueID int, offset int64) error {
	// 更新消费者偏移量
	return cm.updateConsumeOffset(groupName, topic, queueID, offset)
}

// readMessageByOffset 根据偏移量读取消息
func (cm *ConsumerManager) readMessageByOffset(offset int64, size int32) (*protocol.Message, error) {
	// 从CommitLog读取消息
	commitLogMsg, err := cm.commitLog.ReadMessage(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read message from commitlog at offset %d: %v", offset, err)
	}

	return commitLogMsg.Message, nil
}

// updateConsumeOffset 更新消费偏移量
func (cm *ConsumerManager) updateConsumeOffset(groupName, topic string, queueID int, offset int64) error {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group not found: %s", groupName)
	}

	group.Mutex.Lock()
	defer group.Mutex.Unlock()

	// 更新检查点的消费者偏移量
	if cm.checkPoint != nil {
		if err := cm.checkPoint.UpdateConsumerOffset(groupName, topic, queueID, offset); err != nil {
			logger.Warn("Failed to update checkpoint consumer offset",
				"group", groupName,
				"topic", topic,
				"queueID", queueID,
				"offset", offset,
				"error", err)
		}
	}

	// 确保主题偏移量映射存在
	if _, exists := group.TopicOffsets[topic]; !exists {
		group.TopicOffsets[topic] = make(map[int]int64)
	}

	// 更新偏移量
	group.TopicOffsets[topic][queueID] = offset

	return nil
}

// GetConsumeOffset 获取消费偏移量
func (cm *ConsumerManager) GetConsumeOffset(groupName, topic string, queueID int) (int64, error) {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return 0, fmt.Errorf("consumer group not found: %s", groupName)
	}

	group.Mutex.RLock()
	defer group.Mutex.RUnlock()

	if topicOffsets, exists := group.TopicOffsets[topic]; exists {
		if offset, exists := topicOffsets[queueID]; exists {
			return offset, nil
		}
	}

	return 0, nil // 默认从头开始消费
}

// GetConsumerGroups 获取所有消费者组
func (cm *ConsumerManager) GetConsumerGroups() []*ConsumerGroup {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	groups := make([]*ConsumerGroup, 0, len(cm.consumerGroups))
	for _, group := range cm.consumerGroups {
		groups = append(groups, group)
	}

	return groups
}

// GetConsumerGroup 获取消费者组
func (cm *ConsumerManager) GetConsumerGroup(groupName string) (*ConsumerGroup, bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	group, exists := cm.consumerGroups[groupName]
	return group, exists
}

// GetConsumerCount 获取消费者数量
func (cm *ConsumerManager) GetConsumerCount() int {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	count := 0
	for _, group := range cm.consumerGroups {
		group.Mutex.RLock()
		count += len(group.Consumers)
		group.Mutex.RUnlock()
	}

	return count
}

// GetTopicCount 获取主题数量
func (cm *ConsumerManager) GetTopicCount() int {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	topicSet := make(map[string]struct{})

	for _, group := range cm.consumerGroups {
		group.Mutex.RLock()
		for _, consumer := range group.Consumers {
			for _, topic := range consumer.Topics {
				topicSet[topic] = struct{}{}
			}
		}
		group.Mutex.RUnlock()
	}

	return len(topicSet)
}

// HandleConsumeFailure 处理消费失败
// 当消息消费失败时，根据重试次数决定是否移入死信队列
func (cm *ConsumerManager) HandleConsumeFailure(
	msg *protocol.Message,
	consumerGroup string,
	consumerID string,
	errorMsg string,
) error {
	if cm.deadLetterQueue == nil {
		logger.Warn("Dead letter queue not initialized, cannot handle consume failure",
			"messageId", msg.MessageID)
		return fmt.Errorf("dead letter queue not initialized")
	}

	// 获取消息的重试次数
	retryCount := cm.getRetryCount(msg)
	maxRetry := cm.getMaxRetry(msg)

	logger.Info("Handling consume failure",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"consumerGroup", consumerGroup,
		"retryCount", retryCount,
		"maxRetry", maxRetry,
		"error", errorMsg)

	// 检查是否超过最大重试次数
	if retryCount >= maxRetry {
		// 超过最大重试次数，移入死信队列
		if err := cm.deadLetterQueue.AddDeadMessage(
			msg,
			consumerGroup,
			DeadReasonMaxRetry,
			errorMsg,
		); err != nil {
			return fmt.Errorf("failed to add message to dead letter queue: %v", err)
		}

		logger.Warn("Message moved to dead letter queue",
			"messageId", msg.MessageID,
			"topic", msg.Topic,
			"consumerGroup", consumerGroup,
			"retryCount", retryCount)
	} else {
		// 未超过最大重试次数，添加到重试管理器
		if cm.retryManager != nil {
			// 获取消息的原始偏移量
			originalOffset := msg.CommitLogOffset
			if offsetStr := msg.GetProperty("original_offset"); offsetStr != "" {
				if _, err := fmt.Sscanf(offsetStr, "%d", &originalOffset); err != nil {
					// 解析失败，使用默认的 CommitLogOffset
					logger.Warn("Failed to parse original_offset, using CommitLogOffset",
						"offsetStr", offsetStr,
						"error", err)
				}
			}

			// 添加到重试队列
			if err := cm.retryManager.AddRetryMessage(
				consumerGroup,
				msg.Topic,
				msg,
				originalOffset,
				retryCount,
			); err != nil {
				// 如果添加到重试管理器失败（例如超过最大重试次数），移入死信队列
				logger.Warn("Failed to add message to retry manager, moving to dead letter queue",
					"messageId", msg.MessageID,
					"error", err)
				if err := cm.deadLetterQueue.AddDeadMessage(
					msg,
					consumerGroup,
					DeadReasonMaxRetry,
					errorMsg,
				); err != nil {
					return fmt.Errorf("failed to add message to dead letter queue: %v", err)
				}
			} else {
				logger.Info("Message added to retry manager",
					"messageId", msg.MessageID,
					"topic", msg.Topic,
					"retryCount", retryCount+1,
					"maxRetry", maxRetry)
			}
		} else {
			// 重试管理器未初始化，仅增加重试次数并记录
			cm.incrementRetryCount(msg)
			logger.Info("Message will be retried (retry manager not available)",
				"messageId", msg.MessageID,
				"topic", msg.Topic,
				"retryCount", retryCount+1,
				"maxRetry", maxRetry)
		}
	}

	return nil
}

// getRetryCount 从消息属性中获取重试次数
func (cm *ConsumerManager) getRetryCount(msg *protocol.Message) int {
	retryStr := msg.GetProperty("retry_count")
	if retryStr == "" {
		return 0
	}

	var retryCount int
	if _, err := fmt.Sscanf(retryStr, "%d", &retryCount); err != nil {
		// 解析失败，返回0
		return 0
	}
	return retryCount
}

// getMaxRetry 获取最大重试次数（从配置或消息属性）
func (cm *ConsumerManager) getMaxRetry(msg *protocol.Message) int {
	// 优先从消息属性获取
	maxRetryStr := msg.GetProperty("max_retry")
	if maxRetryStr != "" {
		var maxRetry int
		if _, err := fmt.Sscanf(maxRetryStr, "%d", &maxRetry); err == nil {
			return maxRetry
		}
	}

	// 默认返回3次
	return 3
}

// incrementRetryCount 增加消息的重试次数
func (cm *ConsumerManager) incrementRetryCount(msg *protocol.Message) {
	retryCount := cm.getRetryCount(msg)
	retryCount++

	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties["retry_count"] = fmt.Sprintf("%d", retryCount)
	msg.Properties["last_retry_time"] = fmt.Sprintf("%d", time.Now().UnixMilli())
}

// GetConsumerStats 获取消费者统计信息
func (cm *ConsumerManager) GetConsumerStats() map[string]interface{} {
	return map[string]interface{}{
		"consumer_groups": len(cm.consumerGroups),
		"consumers":       cm.GetConsumerCount(),
		"topics":          cm.GetTopicCount(),
	}
}

// IsRunning 检查是否正在运行
func (cm *ConsumerManager) IsRunning() bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.running
}

// getConsumerFilter 获取消费者的过滤条件
func (cm *ConsumerManager) getConsumerFilter(groupName, topic string) *MessageFilter {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return nil
	}

	group.Mutex.RLock()
	defer group.Mutex.RUnlock()

	// 查找该消费者组的第一个消费者（简化实现，实际应该根据consumerID查找）
	// 注意：这里应该根据consumerID查找，但为了简化，我们查找第一个有该topic订阅的消费者
	for _, consumer := range group.Consumers {
		if consumer.Subscriptions != nil {
			if filterExpr, exists := consumer.Subscriptions[topic]; exists && filterExpr != "" {
				// 解析过滤表达式
				return ParseFilter(filterExpr)
			}
		}
	}

	return nil
}

// SetConsumerFilter 设置消费者的过滤条件
func (cm *ConsumerManager) SetConsumerFilter(groupName, consumerID, topic, filterExpression string) error {
	cm.mutex.RLock()
	group, exists := cm.consumerGroups[groupName]
	cm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("consumer group not found: %s", groupName)
	}

	group.Mutex.Lock()
	defer group.Mutex.Unlock()

	consumer, exists := group.Consumers[consumerID]
	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	if consumer.Subscriptions == nil {
		consumer.Subscriptions = make(map[string]string)
	}

	consumer.Subscriptions[topic] = filterExpression

	logger.Info("Consumer filter set",
		"groupName", groupName,
		"consumerID", consumerID,
		"topic", topic,
		"filter", filterExpression)

	return nil
}
