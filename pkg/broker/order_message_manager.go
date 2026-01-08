package broker

import (
	"fmt"
	"sync"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

// OrderMessageManager 顺序消息管理器
// 负责管理顺序消息的路由和消费顺序保证
type OrderMessageManager struct {
	// 队列锁定：topic -> queueID -> consumerID
	// 用于保证同一队列同时只能被一个消费者消费
	queueLocks map[string]map[int]string // topic -> queueID -> consumerID
	mu         sync.RWMutex

	// 分片键到队列ID的映射：topic -> shardingKey -> queueID
	// 保证相同分片键的消息路由到同一队列
	shardingMap map[string]map[string]int // topic -> shardingKey -> queueID
	shardingMu  sync.RWMutex
}

// NewOrderMessageManager 创建顺序消息管理器
func NewOrderMessageManager() *OrderMessageManager {
	return &OrderMessageManager{
		queueLocks:  make(map[string]map[int]string),
		shardingMap: make(map[string]map[string]int),
	}
}

// RouteOrderMessage 路由顺序消息到队列
// 根据 ShardingKey 将消息路由到对应的队列，保证相同 ShardingKey 的消息在同一队列
func (omm *OrderMessageManager) RouteOrderMessage(msg *protocol.Message, queueCount int) (int, error) {
	if msg.ShardingKey == "" {
		// 如果没有分片键，使用消息ID的哈希值
		msg.ShardingKey = msg.MessageID
	}

	omm.shardingMu.Lock()
	defer omm.shardingMu.Unlock()

	// 初始化Topic的分片映射
	if omm.shardingMap[msg.Topic] == nil {
		omm.shardingMap[msg.Topic] = make(map[string]int)
	}

	// 检查是否已有该分片键的映射
	if queueID, exists := omm.shardingMap[msg.Topic][msg.ShardingKey]; exists {
		return queueID, nil
	}

	// 计算队列ID（使用分片键的哈希值）
	queueID := omm.calculateQueueID(msg.ShardingKey, queueCount)

	// 保存分片键到队列ID的映射
	omm.shardingMap[msg.Topic][msg.ShardingKey] = queueID

	logger.Debug("Order message routed",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"shardingKey", msg.ShardingKey,
		"queueID", queueID)

	return queueID, nil
}

// LockQueue 锁定队列（保证同一队列同时只能被一个消费者消费）
func (omm *OrderMessageManager) LockQueue(topic string, queueID int, consumerID string) error {
	omm.mu.Lock()
	defer omm.mu.Unlock()

	// 初始化Topic的队列锁定映射
	if omm.queueLocks[topic] == nil {
		omm.queueLocks[topic] = make(map[int]string)
	}

	// 检查队列是否已被其他消费者锁定
	if lockedBy, exists := omm.queueLocks[topic][queueID]; exists {
		if lockedBy != consumerID {
			return fmt.Errorf("queue %d of topic %s is locked by consumer %s", queueID, topic, lockedBy)
		}
		// 已被当前消费者锁定，直接返回
		return nil
	}

	// 锁定队列
	omm.queueLocks[topic][queueID] = consumerID

	logger.Debug("Queue locked",
		"topic", topic,
		"queueID", queueID,
		"consumerID", consumerID)

	return nil
}

// UnlockQueue 解锁队列
func (omm *OrderMessageManager) UnlockQueue(topic string, queueID int, consumerID string) {
	omm.mu.Lock()
	defer omm.mu.Unlock()

	if omm.queueLocks[topic] == nil {
		return
	}

	// 检查是否是当前消费者锁定的
	if lockedBy, exists := omm.queueLocks[topic][queueID]; exists {
		if lockedBy == consumerID {
			delete(omm.queueLocks[topic], queueID)

			// 如果Topic没有锁定的队列了，删除Topic映射
			if len(omm.queueLocks[topic]) == 0 {
				delete(omm.queueLocks, topic)
			}

			logger.Debug("Queue unlocked",
				"topic", topic,
				"queueID", queueID,
				"consumerID", consumerID)
		}
	}
}

// UnlockAllQueues 解锁消费者锁定的所有队列（用于消费者下线时）
func (omm *OrderMessageManager) UnlockAllQueues(consumerID string) {
	omm.mu.Lock()
	defer omm.mu.Unlock()

	for topic, queueLocks := range omm.queueLocks {
		for queueID, lockedBy := range queueLocks {
			if lockedBy == consumerID {
				delete(queueLocks, queueID)
			}
		}

		// 如果Topic没有锁定的队列了，删除Topic映射
		if len(queueLocks) == 0 {
			delete(omm.queueLocks, topic)
		}
	}

	logger.Info("All queues unlocked for consumer",
		"consumerID", consumerID)
}

// GetLockedQueues 获取消费者锁定的队列列表
func (omm *OrderMessageManager) GetLockedQueues(consumerID string) map[string][]int {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	result := make(map[string][]int)

	for topic, queueLocks := range omm.queueLocks {
		for queueID, lockedBy := range queueLocks {
			if lockedBy == consumerID {
				if result[topic] == nil {
					result[topic] = make([]int, 0)
				}
				result[topic] = append(result[topic], queueID)
			}
		}
	}

	return result
}

// calculateQueueID 根据分片键计算队列ID
func (omm *OrderMessageManager) calculateQueueID(shardingKey string, queueCount int) int {
	// 使用简单的哈希算法
	hash := 0
	for _, char := range shardingKey {
		hash = hash*31 + int(char)
	}

	// 确保队列ID在有效范围内
	queueID := hash % queueCount
	if queueID < 0 {
		queueID = -queueID
	}

	return queueID
}

// IsQueueLocked 检查队列是否被锁定
func (omm *OrderMessageManager) IsQueueLocked(topic string, queueID int) bool {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	if omm.queueLocks[topic] == nil {
		return false
	}

	_, exists := omm.queueLocks[topic][queueID]
	return exists
}

// IsQueueLockedByConsumer 检查队列是否被指定消费者锁定
func (omm *OrderMessageManager) IsQueueLockedByConsumer(topic string, queueID int, consumerID string) bool {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	if omm.queueLocks[topic] == nil {
		return false
	}

	lockedBy, exists := omm.queueLocks[topic][queueID]
	return exists && lockedBy == consumerID
}

// IsOrderTopic 检查Topic是否是顺序消息Topic
// 简化实现：如果Topic有队列锁定或分片映射，认为是顺序消息Topic
func (omm *OrderMessageManager) IsOrderTopic(topic string) bool {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	omm.shardingMu.RLock()
	defer omm.shardingMu.RUnlock()

	// 如果Topic有队列锁定或分片映射，认为是顺序消息Topic
	hasLocks := omm.queueLocks[topic] != nil && len(omm.queueLocks[topic]) > 0
	hasShardings := omm.shardingMap[topic] != nil && len(omm.shardingMap[topic]) > 0

	return hasLocks || hasShardings
}

// ReleaseAllQueueLocks 释放消费者组的所有队列锁（用于消费者下线时）
func (omm *OrderMessageManager) ReleaseAllQueueLocks(groupName, consumerID string) {
	omm.mu.Lock()
	defer omm.mu.Unlock()

	for topic, queueLocks := range omm.queueLocks {
		for queueID, lockedBy := range queueLocks {
			if lockedBy == consumerID {
				delete(queueLocks, queueID)
			}
		}

		// 如果Topic没有锁定的队列了，删除Topic映射
		if len(queueLocks) == 0 {
			delete(omm.queueLocks, topic)
		}
	}

	logger.Info("All queue locks released for consumer",
		"groupName", groupName,
		"consumerID", consumerID)
}

// GetStats 获取顺序消息管理器统计信息
func (omm *OrderMessageManager) GetStats() map[string]interface{} {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	omm.shardingMu.RLock()
	defer omm.shardingMu.RUnlock()

	totalLocks := 0
	for _, queueLocks := range omm.queueLocks {
		totalLocks += len(queueLocks)
	}

	totalShardings := 0
	for _, shardings := range omm.shardingMap {
		totalShardings += len(shardings)
	}

	return map[string]interface{}{
		"lockedQueues": totalLocks,
		"shardingKeys": totalShardings,
		"topics":       len(omm.queueLocks),
	}
}
