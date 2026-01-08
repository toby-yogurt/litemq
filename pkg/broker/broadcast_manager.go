package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// BroadcastManager 广播消息管理器
// 负责管理广播消息的存储和分发
// 广播消息的特点：每个消费者组都有独立的消费队列，消息会被复制到所有消费者组的队列
type BroadcastManager struct {
	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 广播消息存储：topic -> consumerGroup -> queueID -> messages
	// 每个消费者组都有独立的队列，保证所有消费者都能收到消息
	broadcastQueues map[string]map[string]map[int]*BroadcastQueue
	mu              sync.RWMutex

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// BroadcastQueue 广播队列（每个消费者组每个队列一个）
type BroadcastQueue struct {
	Topic         string
	ConsumerGroup string
	QueueID       int
	Messages      []*BroadcastMessageEntry
	mu            sync.RWMutex
}

// BroadcastMessageEntry 广播消息条目
type BroadcastMessageEntry struct {
	Message         *protocol.Message
	CommitLogOffset int64
	StoreTime       int64
	QueueOffset     int64
}

// NewBroadcastManager 创建广播消息管理器
func NewBroadcastManager(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *BroadcastManager {
	return &BroadcastManager{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		broadcastQueues: make(map[string]map[string]map[int]*BroadcastQueue),
		stopCh:          make(chan struct{}),
		running:         false,
	}
}

// Start 启动广播消息管理器
func (bm *BroadcastManager) Start() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.running {
		return fmt.Errorf("broadcast manager is already running")
	}

	bm.running = true

	logger.Info("Broadcast manager started")

	return nil
}

// Stop 停止广播消息管理器
func (bm *BroadcastManager) Stop() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if !bm.running {
		return
	}

	bm.running = false
	close(bm.stopCh)

	bm.wg.Wait()

	logger.Info("Broadcast manager stopped")
}

// AddBroadcastMessage 添加广播消息
// 广播消息会被复制到所有已注册的消费者组的队列中
func (bm *BroadcastManager) AddBroadcastMessage(msg *protocol.Message, commitLogOffset int64) error {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if !bm.running {
		return fmt.Errorf("broadcast manager is not running")
	}

	// 验证消息类型
	if !msg.Broadcast && msg.MessageType != protocol.MessageTypeBroadcast {
		return fmt.Errorf("message is not a broadcast message")
	}

	// 获取或创建广播队列
	broadcastQueues := bm.getOrCreateBroadcastQueues(msg.Topic)

	// 为每个消费者组创建消息副本并添加到队列
	for consumerGroup, queueMap := range broadcastQueues {
		for queueID, queue := range queueMap {
			// 创建消息条目
			entry := &BroadcastMessageEntry{
				Message:         msg.Clone(),
				CommitLogOffset: commitLogOffset,
				StoreTime:       time.Now().UnixMilli(),
				QueueOffset:     int64(len(queue.Messages)),
			}

			// 添加到队列
			queue.mu.Lock()
			queue.Messages = append(queue.Messages, entry)
			queue.mu.Unlock()

			// 构建ConsumeQueue索引（每个消费者组都有独立的索引）
			broadcastTopic := bm.buildBroadcastTopic(msg.Topic, consumerGroup)
			if err := bm.buildConsumeQueueIndex(msg, broadcastTopic, queueID, commitLogOffset); err != nil {
				logger.Warn("Failed to build consume queue index for broadcast message",
					"topic", msg.Topic,
					"consumerGroup", consumerGroup,
					"queueID", queueID,
					"error", err)
			}

			logger.Debug("Broadcast message added to queue",
				"messageId", msg.MessageID,
				"topic", msg.Topic,
				"consumerGroup", consumerGroup,
				"queueID", queueID,
				"queueOffset", entry.QueueOffset)
		}
	}

	logger.Info("Broadcast message added",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"consumerGroups", len(broadcastQueues))

	return nil
}

// RegisterConsumerGroup 注册消费者组（用于广播消息）
// 当新的消费者组注册时，会为其创建独立的广播队列
func (bm *BroadcastManager) RegisterConsumerGroup(topic, consumerGroup string, queueCount int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// 初始化Topic的映射
	if bm.broadcastQueues[topic] == nil {
		bm.broadcastQueues[topic] = make(map[string]map[int]*BroadcastQueue)
	}

	// 初始化消费者组的映射
	if bm.broadcastQueues[topic][consumerGroup] == nil {
		bm.broadcastQueues[topic][consumerGroup] = make(map[int]*BroadcastQueue)
	}

	// 为每个队列创建广播队列
	for queueID := 0; queueID < queueCount; queueID++ {
		if bm.broadcastQueues[topic][consumerGroup][queueID] == nil {
			bm.broadcastQueues[topic][consumerGroup][queueID] = &BroadcastQueue{
				Topic:         topic,
				ConsumerGroup: consumerGroup,
				QueueID:       queueID,
				Messages:      make([]*BroadcastMessageEntry, 0),
			}
		}
	}

	logger.Info("Consumer group registered for broadcast",
		"topic", topic,
		"consumerGroup", consumerGroup,
		"queueCount", queueCount)
}

// PullBroadcastMessage 拉取广播消息
func (bm *BroadcastManager) PullBroadcastMessage(topic, consumerGroup string, queueID int, offset int64, maxCount int) ([]*protocol.Message, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	// 获取广播队列
	queue, exists := bm.getBroadcastQueue(topic, consumerGroup, queueID)
	if !exists {
		return []*protocol.Message{}, nil
	}

	queue.mu.RLock()
	defer queue.mu.RUnlock()

	// 检查偏移量是否有效
	if offset < 0 || offset >= int64(len(queue.Messages)) {
		return []*protocol.Message{}, nil
	}

	// 计算结束位置
	endOffset := offset + int64(maxCount)
	if endOffset > int64(len(queue.Messages)) {
		endOffset = int64(len(queue.Messages))
	}

	// 获取消息
	messages := make([]*protocol.Message, 0, endOffset-offset)
	for i := offset; i < endOffset; i++ {
		entry := queue.Messages[i]
		messages = append(messages, entry.Message)
	}

	return messages, nil
}

// GetConsumeOffset 获取消费偏移量（广播消息的偏移量由客户端维护）
func (bm *BroadcastManager) GetConsumeOffset(topic, consumerGroup string, queueID int) int64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	queue, exists := bm.getBroadcastQueue(topic, consumerGroup, queueID)
	if !exists {
		return 0
	}

	queue.mu.RLock()
	defer queue.mu.RUnlock()

	return int64(len(queue.Messages))
}

// getOrCreateBroadcastQueues 获取或创建广播队列映射
func (bm *BroadcastManager) getOrCreateBroadcastQueues(topic string) map[string]map[int]*BroadcastQueue {
	if bm.broadcastQueues[topic] == nil {
		bm.broadcastQueues[topic] = make(map[string]map[int]*BroadcastQueue)
	}
	return bm.broadcastQueues[topic]
}

// getBroadcastQueue 获取广播队列
func (bm *BroadcastManager) getBroadcastQueue(topic, consumerGroup string, queueID int) (*BroadcastQueue, bool) {
	if bm.broadcastQueues[topic] == nil {
		return nil, false
	}

	if bm.broadcastQueues[topic][consumerGroup] == nil {
		return nil, false
	}

	queue, exists := bm.broadcastQueues[topic][consumerGroup][queueID]
	return queue, exists
}

// buildBroadcastTopic 构建广播Topic名称（格式：{OriginalTopic}@broadcast@{ConsumerGroup}）
func (bm *BroadcastManager) buildBroadcastTopic(originalTopic, consumerGroup string) string {
	return fmt.Sprintf("%s@broadcast@%s", originalTopic, consumerGroup)
}

// buildConsumeQueueIndex 构建ConsumeQueue索引
func (bm *BroadcastManager) buildConsumeQueueIndex(msg *protocol.Message, topic string, queueID int, commitLogOffset int64) error {
	// 获取或创建ConsumeQueue
	cq, err := bm.consumeQueueMgr.GetOrCreateConsumeQueue(topic, queueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	// 设置消息的Topic和QueueID
	msg.Topic = topic
	msg.QueueID = queueID
	msg.CommitLogOffset = commitLogOffset

	// 追加索引项
	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	return nil
}

// GetStats 获取广播管理器统计信息
func (bm *BroadcastManager) GetStats() map[string]interface{} {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	totalQueues := 0
	totalMessages := 0
	topicStats := make(map[string]int)

	for topic, consumerGroups := range bm.broadcastQueues {
		topicMessageCount := 0
		for _, queues := range consumerGroups {
			for _, queue := range queues {
				queue.mu.RLock()
				messageCount := len(queue.Messages)
				queue.mu.RUnlock()
				topicMessageCount += messageCount
				totalQueues++
			}
		}
		topicStats[topic] = topicMessageCount
		totalMessages += topicMessageCount
	}

	return map[string]interface{}{
		"running":       bm.running,
		"totalQueues":   totalQueues,
		"totalMessages": totalMessages,
		"topicStats":    topicStats,
	}
}
