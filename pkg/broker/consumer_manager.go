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
	}
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
	}
	group.Consumers[consumerID] = consumer

	// 初始化主题偏移量
	for _, topic := range topics {
		if _, exists := group.TopicOffsets[topic]; !exists {
			group.TopicOffsets[topic] = make(map[int]int64)
		}
	}

	logger.Info("Consumer registered", "consumer_id", consumerID, "group", groupName)
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

	// 拉取消息
	messages, err := cm.pullMessages(groupName, topic, queueID, offset, maxCount)
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
func (cm *ConsumerManager) pullMessages(groupName, topic string, queueID int, offset int64, maxCount int) ([]*protocol.Message, error) {
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
func (cm *ConsumerManager) PullMessages(groupName, topic string, queueID int, offset int64, maxCount int) ([]*protocol.Message, error) {
	return cm.pullMessages(groupName, topic, queueID, offset, maxCount)
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
