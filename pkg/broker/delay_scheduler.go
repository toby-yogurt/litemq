package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// DelayMessageScheduler 延时消息调度器
// 负责管理延时消息的存储和调度
type DelayMessageScheduler struct {
	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 时间轮
	timeWheel *TimeWheel

	// 延时消息缓存（用于到期时恢复消息信息）
	delayMessages map[int64]*DelayMessageInfo
	mu            sync.RWMutex

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// DelayMessageInfo 延时消息信息
type DelayMessageInfo struct {
	Message         *protocol.Message
	CommitLogOffset int64
	StoreTime       int64
}

// NewDelayMessageScheduler 创建延时消息调度器
func NewDelayMessageScheduler(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *DelayMessageScheduler {
	// 创建多层时间轮：
	// - 第0层：秒级（60个槽位，每个1秒，支持0-60秒）
	// - 第1层：分钟级（60个槽位，每个1分钟，支持0-60分钟）
	// - 第2层：小时级（24个槽位，每个1小时，支持0-24小时）
	// - 第3层：天级（365个槽位，每个1天，支持0-365天）
	timeWheel := NewTimeWheel()

	ds := &DelayMessageScheduler{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		timeWheel:       timeWheel,
		delayMessages:   make(map[int64]*DelayMessageInfo),
		stopCh:          make(chan struct{}),
		running:         false,
	}

	// 设置时间轮到期回调
	timeWheel.SetExpireCallback(ds.onMessageExpire)

	return ds
}

// Start 启动延时消息调度器
func (ds *DelayMessageScheduler) Start() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.running {
		return fmt.Errorf("delay message scheduler is already running")
	}

	ds.running = true

	// 启动时间轮
	if err := ds.timeWheel.Start(); err != nil {
		return fmt.Errorf("failed to start time wheel: %v", err)
	}

	logger.Info("Delay message scheduler started")

	return nil
}

// Stop 停止延时消息调度器
func (ds *DelayMessageScheduler) Stop() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.running {
		return
	}

	ds.running = false
	close(ds.stopCh)

	// 停止时间轮
	ds.timeWheel.Stop()

	// 等待所有goroutine结束
	ds.wg.Wait()

	logger.Info("Delay message scheduler stopped")
}

// AddDelayMessage 添加延时消息
// 延时消息会先存储到 CommitLog，但不立即构建 ConsumeQueue 索引
// 当延时时间到达时，再构建索引使消息可被消费
func (ds *DelayMessageScheduler) AddDelayMessage(msg *protocol.Message, commitLogOffset int64) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.running {
		return fmt.Errorf("delay message scheduler is not running")
	}

	// 验证消息类型
	if msg.MessageType != protocol.MessageTypeDelay {
		return fmt.Errorf("message is not a delay message")
	}

	// 验证延时时间
	if msg.DelayTime <= 0 {
		return fmt.Errorf("invalid delay time: %d", msg.DelayTime)
	}

	// 保存延时消息信息
	ds.delayMessages[commitLogOffset] = &DelayMessageInfo{
		Message:         msg,
		CommitLogOffset: commitLogOffset,
		StoreTime:       time.Now().UnixMilli(),
	}

	// 添加到时间轮
	if err := ds.timeWheel.AddMessage(msg.DelayTime, commitLogOffset); err != nil {
		// 如果添加失败，从缓存中移除
		delete(ds.delayMessages, commitLogOffset)
		return fmt.Errorf("failed to add message to time wheel: %v", err)
	}

	logger.Info("Added delay message to scheduler",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"commitLogOffset", commitLogOffset,
		"delayTime", msg.DelayTime,
		"delayMs", msg.DelayTime-time.Now().UnixMilli())

	return nil
}

// onMessageExpire 消息到期回调
func (ds *DelayMessageScheduler) onMessageExpire(commitLogOffset int64) {
	ds.mu.Lock()
	info, exists := ds.delayMessages[commitLogOffset]
	if !exists {
		ds.mu.Unlock()
		logger.Warn("Delay message info not found",
			"commitLogOffset", commitLogOffset)
		return
	}
	// 从缓存中移除
	delete(ds.delayMessages, commitLogOffset)
	ds.mu.Unlock()

	// 从 CommitLog 读取消息（确保获取最新状态）
	commitLogMsg, err := ds.commitLog.ReadMessage(commitLogOffset)
	if err != nil {
		logger.Error("Failed to read delay message from commitlog",
			"commitLogOffset", commitLogOffset,
			"error", err)
		return
	}

	msg := commitLogMsg.Message

	// 验证消息仍然是延时消息
	if msg.MessageType != protocol.MessageTypeDelay {
		logger.Warn("Message type changed, skipping",
			"messageId", msg.MessageID,
			"messageType", msg.MessageType)
		return
	}

	// 验证延时时间是否真的到期
	now := time.Now().UnixMilli()
	if msg.DelayTime > now {
		// 延时时间未到，重新添加到时间轮
		logger.Debug("Delay time not reached, re-adding to time wheel",
			"messageId", msg.MessageID,
			"delayTime", msg.DelayTime,
			"now", now)

		ds.mu.Lock()
		ds.delayMessages[commitLogOffset] = info
		ds.mu.Unlock()

		if err := ds.timeWheel.AddMessage(msg.DelayTime, commitLogOffset); err != nil {
			logger.Error("Failed to re-add message to time wheel",
				"messageId", msg.MessageID,
				"error", err)
		}
		return
	}

	// 构建 ConsumeQueue 索引，使消息可以被消费
	if err := ds.buildConsumeQueueIndex(msg); err != nil {
		logger.Error("Failed to build consume queue index for delay message",
			"messageId", msg.MessageID,
			"topic", msg.Topic,
			"commitLogOffset", commitLogOffset,
			"error", err)
		return
	}

	logger.Info("Delay message expired and indexed",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"queueId", msg.QueueID,
		"commitLogOffset", commitLogOffset,
		"delayTime", msg.DelayTime,
		"actualDelay", now-msg.StoreTimestamp)
}

// buildConsumeQueueIndex 构建 ConsumeQueue 索引
func (ds *DelayMessageScheduler) buildConsumeQueueIndex(msg *protocol.Message) error {
	// 如果 QueueID 未设置，默认使用 0
	if msg.QueueID == 0 {
		msg.QueueID = 0
	}

	// 获取或创建 ConsumeQueue
	cq, err := ds.consumeQueueMgr.GetOrCreateConsumeQueue(msg.Topic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	// 追加索引项
	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	return nil
}

// GetStats 获取调度器统计信息
func (ds *DelayMessageScheduler) GetStats() map[string]interface{} {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	timeWheelStats := ds.timeWheel.GetStats()

	return map[string]interface{}{
		"running":         ds.running,
		"pendingMessages": len(ds.delayMessages),
		"timeWheel":       timeWheelStats,
	}
}

// RecoverDelayMessages 恢复延时消息（用于启动时恢复未到期的延时消息）
func (ds *DelayMessageScheduler) RecoverDelayMessages() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	logger.Info("Starting delay message recovery...")

	// 获取当前时间
	now := time.Now().UnixMilli()

	// 从 CommitLog 扫描消息
	// 使用批量读取方式扫描所有消息
	recoveredCount := 0
	currentOffset := int64(0)
	batchSize := 100 // 每次读取100条消息

	for {
		// 批量读取消息
		messages, err := ds.commitLog.ReadMessages(currentOffset, batchSize)
		if err != nil {
			// 如果读取失败或没有更多消息，停止扫描
			break
		}

		if len(messages) == 0 {
			break
		}

		// 处理每条消息
		for _, commitLogMsg := range messages {
			msg := commitLogMsg.Message

			// 检查是否是延时消息
			if msg.MessageType == protocol.MessageTypeDelay {
				// 检查是否还未到期
				if msg.DelayTime > now {
					// 重新添加到时间轮
					if err := ds.timeWheel.AddMessage(msg.DelayTime, msg.CommitLogOffset); err != nil {
						logger.Warn("Failed to recover delay message",
							"messageId", msg.MessageID,
							"commitLogOffset", msg.CommitLogOffset,
							"error", err)
					} else {
						// 缓存消息信息
						ds.delayMessages[msg.CommitLogOffset] = &DelayMessageInfo{
							Message:         msg,
							CommitLogOffset: msg.CommitLogOffset,
							StoreTime:       msg.StoreTimestamp,
						}
						recoveredCount++
					}
				} else {
					// 延时消息已到期，直接构建索引
					logger.Debug("Delay message already expired during recovery, building index",
						"messageId", msg.MessageID,
						"commitLogOffset", msg.CommitLogOffset)
					if err := ds.buildConsumeQueueIndex(msg); err != nil {
						logger.Warn("Failed to build consume queue index for expired delay message",
							"messageId", msg.MessageID,
							"error", err)
					}
				}
			}

			// 更新偏移量
			currentOffset = commitLogMsg.Offset + int64(commitLogMsg.Size)
		}

		// 如果读取的消息数少于批次大小，说明已经读取完所有消息
		if len(messages) < batchSize {
			break
		}
	}

	logger.Info("Delay message recovery completed",
		"recoveredCount", recoveredCount)

	return nil
}
