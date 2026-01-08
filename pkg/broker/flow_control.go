package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"litemq/pkg/common/logger"
)

// FlowControl 流控管理器
// 负责生产限流和消费限流
type FlowControl struct {
	// 生产限流配置
	producerTPSLimit   int64 // 单Producer最大TPS
	producerBytesLimit int64 // 单Producer最大流量（字节/秒）
	queueMaxMessages   int64 // 单Queue最大堆积消息数

	// 消费限流配置
	pullMinInterval    time.Duration // 拉取最小间隔
	pullMaxBatchSize   int           // 拉取最大批量大小
	consumerMaxThreads int           // 单Consumer最大并发线程数

	// 生产限流统计（按Producer ID）
	producerStats map[string]*ProducerStats
	producerMu    sync.RWMutex

	// 消费限流统计（按Consumer ID）
	consumerStats map[string]*ConsumerStats
	consumerMu    sync.RWMutex

	// 队列堆积统计（按Topic+QueueID）
	queueStats map[string]*QueueStats
	queueMu    sync.RWMutex

	// 清理协程控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// ProducerStats 生产者统计
type ProducerStats struct {
	LastSecond      int64 // 上一秒的消息数
	LastSecondTime  int64 // 上一秒的时间戳（秒）
	LastSecondBytes int64 // 上一秒的字节数
	mu              sync.Mutex
}

// ConsumerStats 消费者统计
type ConsumerStats struct {
	LastPullTime  int64 // 上次拉取时间（毫秒）
	ActiveThreads int32 // 当前活跃线程数
	mu            sync.Mutex
}

// QueueStats 队列统计
type QueueStats struct {
	MessageCount int64 // 当前消息数
	mu           sync.Mutex
}

// NewFlowControl 创建流控管理器
func NewFlowControl(
	producerTPSLimit int64,
	producerBytesLimit int64,
	queueMaxMessages int64,
	pullMinInterval time.Duration,
	pullMaxBatchSize int,
	consumerMaxThreads int,
) *FlowControl {
	fc := &FlowControl{
		producerTPSLimit:   producerTPSLimit,
		producerBytesLimit: producerBytesLimit,
		queueMaxMessages:   queueMaxMessages,
		pullMinInterval:    pullMinInterval,
		pullMaxBatchSize:   pullMaxBatchSize,
		consumerMaxThreads: consumerMaxThreads,
		producerStats:      make(map[string]*ProducerStats),
		consumerStats:      make(map[string]*ConsumerStats),
		queueStats:         make(map[string]*QueueStats),
		stopCh:             make(chan struct{}),
		running:            false,
	}

	return fc
}

// Start 启动流控管理器
func (fc *FlowControl) Start() error {
	fc.producerMu.Lock()
	defer fc.producerMu.Unlock()

	if fc.running {
		return fmt.Errorf("flow control is already running")
	}

	fc.running = true

	// 启动清理协程（定期清理过期的统计信息）
	fc.wg.Add(1)
	go fc.cleanupLoop()

	logger.Info("Flow control started",
		"producerTPSLimit", fc.producerTPSLimit,
		"producerBytesLimit", fc.producerBytesLimit,
		"queueMaxMessages", fc.queueMaxMessages,
		"pullMinInterval", fc.pullMinInterval,
		"pullMaxBatchSize", fc.pullMaxBatchSize,
		"consumerMaxThreads", fc.consumerMaxThreads)

	return nil
}

// Stop 停止流控管理器
func (fc *FlowControl) Stop() {
	fc.producerMu.Lock()
	defer fc.producerMu.Unlock()

	if !fc.running {
		return
	}

	fc.running = false
	close(fc.stopCh)
	fc.wg.Wait()

	logger.Info("Flow control stopped")
}

// CheckProducerLimit 检查生产限流
// 返回是否允许发送，以及错误信息
func (fc *FlowControl) CheckProducerLimit(producerID string, messageSize int) (bool, error) {
	if fc.producerTPSLimit <= 0 && fc.producerBytesLimit <= 0 {
		return true, nil // 未启用限流
	}

	fc.producerMu.Lock()
	stats, exists := fc.producerStats[producerID]
	if !exists {
		stats = &ProducerStats{
			LastSecondTime: time.Now().Unix(),
		}
		fc.producerStats[producerID] = stats
	}
	fc.producerMu.Unlock()

	stats.mu.Lock()
	defer stats.mu.Unlock()

	now := time.Now().Unix()
	currentSecond := now

	// 如果跨秒了，重置统计
	if currentSecond != stats.LastSecondTime {
		stats.LastSecond = 0
		stats.LastSecondBytes = 0
		stats.LastSecondTime = currentSecond
	}

	// 检查TPS限流
	if fc.producerTPSLimit > 0 {
		if stats.LastSecond >= fc.producerTPSLimit {
			return false, fmt.Errorf("producer TPS limit exceeded: %d/%d",
				stats.LastSecond, fc.producerTPSLimit)
		}
	}

	// 检查流量限流
	if fc.producerBytesLimit > 0 {
		if stats.LastSecondBytes+int64(messageSize) > fc.producerBytesLimit {
			return false, fmt.Errorf("producer bytes limit exceeded: %d/%d bytes",
				stats.LastSecondBytes+int64(messageSize), fc.producerBytesLimit)
		}
	}

	// 更新统计
	stats.LastSecond++
	stats.LastSecondBytes += int64(messageSize)

	return true, nil
}

// CheckQueueLimit 检查队列堆积限流
func (fc *FlowControl) CheckQueueLimit(topic string, queueID int, currentCount int64) (bool, error) {
	if fc.queueMaxMessages <= 0 {
		return true, nil // 未启用限流
	}

	key := fmt.Sprintf("%s:%d", topic, queueID)

	fc.queueMu.Lock()
	stats, exists := fc.queueStats[key]
	if !exists {
		stats = &QueueStats{}
		fc.queueStats[key] = stats
	}
	fc.queueMu.Unlock()

	stats.mu.Lock()
	defer stats.mu.Unlock()

	// 更新消息数
	stats.MessageCount = currentCount

	// 检查是否超过限制
	if stats.MessageCount >= fc.queueMaxMessages {
		return false, fmt.Errorf("queue message limit exceeded: %d/%d messages",
			stats.MessageCount, fc.queueMaxMessages)
	}

	return true, nil
}

// UpdateQueueCount 更新队列消息数
func (fc *FlowControl) UpdateQueueCount(topic string, queueID int, count int64) {
	key := fmt.Sprintf("%s:%d", topic, queueID)

	fc.queueMu.Lock()
	stats, exists := fc.queueStats[key]
	if !exists {
		stats = &QueueStats{}
		fc.queueStats[key] = stats
	}
	fc.queueMu.Unlock()

	stats.mu.Lock()
	stats.MessageCount = count
	stats.mu.Unlock()
}

// CheckPullLimit 检查拉取限流
func (fc *FlowControl) CheckPullLimit(consumerID string, batchSize int) (bool, error) {
	// 检查批量大小限流
	if fc.pullMaxBatchSize > 0 && batchSize > fc.pullMaxBatchSize {
		return false, fmt.Errorf("pull batch size limit exceeded: %d/%d",
			batchSize, fc.pullMaxBatchSize)
	}

	// 检查拉取频率限流
	if fc.pullMinInterval > 0 {
		fc.consumerMu.Lock()
		stats, exists := fc.consumerStats[consumerID]
		if !exists {
			stats = &ConsumerStats{}
			fc.consumerStats[consumerID] = stats
		}
		fc.consumerMu.Unlock()

		stats.mu.Lock()
		defer stats.mu.Unlock()

		now := time.Now().UnixMilli()
		if stats.LastPullTime > 0 {
			elapsed := time.Duration(now - stats.LastPullTime)
			if elapsed < fc.pullMinInterval {
				return false, fmt.Errorf("pull interval too short: %v < %v",
					elapsed, fc.pullMinInterval)
			}
		}

		stats.LastPullTime = now
	}

	return true, nil
}

// CheckConsumerThreadLimit 检查消费者并发线程限流
func (fc *FlowControl) CheckConsumerThreadLimit(consumerID string) (bool, error) {
	if fc.consumerMaxThreads <= 0 {
		return true, nil // 未启用限流
	}

	fc.consumerMu.Lock()
	stats, exists := fc.consumerStats[consumerID]
	if !exists {
		stats = &ConsumerStats{}
		fc.consumerStats[consumerID] = stats
	}
	fc.consumerMu.Unlock()

	stats.mu.Lock()
	defer stats.mu.Unlock()

	if int(atomic.LoadInt32(&stats.ActiveThreads)) >= fc.consumerMaxThreads {
		return false, fmt.Errorf("consumer thread limit exceeded: %d/%d threads",
			atomic.LoadInt32(&stats.ActiveThreads), fc.consumerMaxThreads)
	}

	return true, nil
}

// AcquireConsumerThread 获取消费者线程（增加活跃线程数）
func (fc *FlowControl) AcquireConsumerThread(consumerID string) error {
	allowed, err := fc.CheckConsumerThreadLimit(consumerID)
	if !allowed {
		return err
	}

	fc.consumerMu.RLock()
	stats, exists := fc.consumerStats[consumerID]
	fc.consumerMu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer stats not found: %s", consumerID)
	}

	atomic.AddInt32(&stats.ActiveThreads, 1)
	return nil
}

// ReleaseConsumerThread 释放消费者线程（减少活跃线程数）
func (fc *FlowControl) ReleaseConsumerThread(consumerID string) {
	fc.consumerMu.RLock()
	stats, exists := fc.consumerStats[consumerID]
	fc.consumerMu.RUnlock()

	if !exists {
		return
	}

	atomic.AddInt32(&stats.ActiveThreads, -1)
}

// cleanupLoop 清理循环（定期清理过期的统计信息）
func (fc *FlowControl) cleanupLoop() {
	defer fc.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-fc.stopCh:
			return
		case <-ticker.C:
			fc.cleanup()
		}
	}
}

// cleanup 清理过期的统计信息
func (fc *FlowControl) cleanup() {
	now := time.Now().Unix()
	expireTime := now - 300 // 5分钟未使用则清理

	// 清理生产者统计
	fc.producerMu.Lock()
	for id, stats := range fc.producerStats {
		stats.mu.Lock()
		if stats.LastSecondTime < expireTime {
			delete(fc.producerStats, id)
		}
		stats.mu.Unlock()
	}
	fc.producerMu.Unlock()

	// 清理消费者统计
	fc.consumerMu.Lock()
	for id, stats := range fc.consumerStats {
		stats.mu.Lock()
		lastPullTime := stats.LastPullTime / 1000 // 转换为秒
		if lastPullTime < expireTime && atomic.LoadInt32(&stats.ActiveThreads) == 0 {
			delete(fc.consumerStats, id)
		}
		stats.mu.Unlock()
	}
	fc.consumerMu.Unlock()
}

// GetStats 获取流控统计信息
func (fc *FlowControl) GetStats() map[string]interface{} {
	fc.producerMu.RLock()
	producerCount := len(fc.producerStats)
	fc.producerMu.RUnlock()

	fc.consumerMu.RLock()
	consumerCount := len(fc.consumerStats)
	fc.consumerMu.RUnlock()

	fc.queueMu.RLock()
	queueCount := len(fc.queueStats)
	fc.queueMu.RUnlock()

	return map[string]interface{}{
		"producerTPSLimit":   fc.producerTPSLimit,
		"producerBytesLimit": fc.producerBytesLimit,
		"queueMaxMessages":   fc.queueMaxMessages,
		"pullMinInterval":    fc.pullMinInterval.String(),
		"pullMaxBatchSize":   fc.pullMaxBatchSize,
		"consumerMaxThreads": fc.consumerMaxThreads,
		"producerCount":      producerCount,
		"consumerCount":      consumerCount,
		"queueCount":         queueCount,
	}
}
