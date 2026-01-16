package broker

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
)

// TimeWheel 多层时间轮，用于延时消息调度
// 参考 RocketMQ 5.0 的实现
// 支持秒级、分钟级、小时级、天级的多层时间轮
type TimeWheel struct {
	// 时间轮层级
	wheels []*wheelLevel // 多层时间轮

	// 消息回调
	onExpire func(commitLogOffset int64) // 消息到期时的回调函数

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// wheelLevel 时间轮层级
type wheelLevel struct {
	// 层级配置
	tickDuration time.Duration // 每个槽位的时间间隔
	wheelSize    int           // 时间轮大小

	// 时间轮结构
	slots []*list.List // 每个槽位存储一个消息列表
	mu    sync.RWMutex

	// 当前指针位置
	currentSlot int // 当前槽位索引

	// 时间管理
	startTime time.Time // 时间轮启动时间
	ticker    *time.Ticker

	// 层级信息
	level     int         // 层级索引（0=秒级, 1=分钟级, 2=小时级, 3=天级）
	nextLevel *wheelLevel // 下一层时间轮（更粗粒度，level+1）
	prevLevel *wheelLevel // 上一层时间轮（更细粒度，level-1）
}

// DelayMessageEntry 延时消息条目
type DelayMessageEntry struct {
	CommitLogOffset int64 // CommitLog偏移量
	ExpireTime      int64 // 到期时间戳（毫秒）
}

// NewTimeWheel 创建多层时间轮
// 默认创建4层时间轮：
// - 第0层：秒级（60个槽位，每个1秒，支持0-60秒）
// - 第1层：分钟级（60个槽位，每个1分钟，支持0-60分钟）
// - 第2层：小时级（24个槽位，每个1小时，支持0-24小时）
// - 第3层：天级（365个槽位，每个1天，支持0-365天）
func NewTimeWheel() *TimeWheel {
	tw := &TimeWheel{
		wheels:  make([]*wheelLevel, 0, 4),
		stopCh:  make(chan struct{}),
		running: false,
	}

	// 创建第3层：天级（365个槽位，每个1天）
	dayLevel := newWheelLevel(24*time.Hour, 365, 3, nil, nil)
	tw.wheels = append(tw.wheels, dayLevel)

	// 创建第2层：小时级（24个槽位，每个1小时）
	hourLevel := newWheelLevel(time.Hour, 24, 2, dayLevel, nil)
	tw.wheels = append(tw.wheels, hourLevel)
	dayLevel.prevLevel = hourLevel

	// 创建第1层：分钟级（60个槽位，每个1分钟）
	minuteLevel := newWheelLevel(time.Minute, 60, 1, hourLevel, nil)
	tw.wheels = append(tw.wheels, minuteLevel)
	hourLevel.prevLevel = minuteLevel

	// 创建第0层：秒级（60个槽位，每个1秒）
	secondLevel := newWheelLevel(time.Second, 60, 0, minuteLevel, nil)
	tw.wheels = append(tw.wheels, secondLevel)
	minuteLevel.prevLevel = secondLevel

	// 反转数组，使第0层（秒级）在第一个位置
	for i, j := 0, len(tw.wheels)-1; i < j; i, j = i+1, j-1 {
		tw.wheels[i], tw.wheels[j] = tw.wheels[j], tw.wheels[i]
	}

	return tw
}

// newWheelLevel 创建时间轮层级
func newWheelLevel(tickDuration time.Duration, wheelSize int, level int, nextLevel *wheelLevel, prevLevel *wheelLevel) *wheelLevel {
	wl := &wheelLevel{
		tickDuration: tickDuration,
		wheelSize:    wheelSize,
		slots:        make([]*list.List, wheelSize),
		currentSlot:  0,
		level:        level,
		nextLevel:    nextLevel,
		prevLevel:    prevLevel,
	}

	// 初始化所有槽位
	for i := 0; i < wheelSize; i++ {
		wl.slots[i] = list.New()
	}

	return wl
}

// SetExpireCallback 设置消息到期回调函数
func (tw *TimeWheel) SetExpireCallback(callback func(commitLogOffset int64)) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.onExpire = callback
}

// Start 启动时间轮
func (tw *TimeWheel) Start() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.running {
		return fmt.Errorf("time wheel is already running")
	}

	now := time.Now()
	for _, wheel := range tw.wheels {
		wheel.startTime = now
		wheel.ticker = time.NewTicker(wheel.tickDuration)
	}

	tw.running = true

	// 启动最细粒度的时间轮（第0层，秒级）
	tw.wg.Add(1)
	go tw.tickLoop(tw.wheels[0])

	logger.Info("Multi-level time wheel started",
		"levels", len(tw.wheels),
		"secondLevel", tw.wheels[0].wheelSize,
		"minuteLevel", tw.wheels[1].wheelSize,
		"hourLevel", tw.wheels[2].wheelSize,
		"dayLevel", tw.wheels[3].wheelSize)

	return nil
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.running {
		return
	}

	tw.running = false
	close(tw.stopCh)

	// 停止所有层级的时间轮
	for _, wheel := range tw.wheels {
		if wheel.ticker != nil {
			wheel.ticker.Stop()
		}
	}

	tw.wg.Wait()

	logger.Info("Multi-level time wheel stopped")
}

// AddMessage 添加延时消息到时间轮
// delayTime: 延时时间戳（毫秒）
// commitLogOffset: CommitLog偏移量
func (tw *TimeWheel) AddMessage(delayTime int64, commitLogOffset int64) error {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if !tw.running {
		return fmt.Errorf("time wheel is not running")
	}

	// 计算延时时间（毫秒）
	now := time.Now().UnixMilli()
	delayMs := delayTime - now

	if delayMs <= 0 {
		// 延时时间已过，立即触发
		if tw.onExpire != nil {
			go tw.onExpire(commitLogOffset)
		}
		return nil
	}

	// 计算应该放在哪个层级和槽位
	delayDuration := time.Duration(delayMs) * time.Millisecond
	targetLevel, targetSlot := tw.calculateTargetSlot(delayDuration)

	// 创建消息条目
	entry := &DelayMessageEntry{
		CommitLogOffset: commitLogOffset,
		ExpireTime:      delayTime,
	}

	// 添加到对应层级的槽位
	targetLevel.mu.Lock()
	targetLevel.slots[targetSlot].PushBack(entry)
	targetLevel.mu.Unlock()

	logger.Debug("Added delay message to time wheel",
		"commitLogOffset", commitLogOffset,
		"delayTime", delayTime,
		"delayMs", delayMs,
		"level", targetLevel.level,
		"targetSlot", targetSlot,
		"currentSlot", targetLevel.currentSlot)

	return nil
}

// calculateTargetSlot 计算目标层级和槽位
func (tw *TimeWheel) calculateTargetSlot(delayDuration time.Duration) (*wheelLevel, int) {
	// 从最细粒度的时间轮开始检查
	for i, wheel := range tw.wheels {
		// 计算该层级能表示的最大时间范围
		maxDuration := time.Duration(wheel.wheelSize) * wheel.tickDuration

		// 如果延时时间在该层级范围内，放在该层级
		if delayDuration < maxDuration {
			// 计算应该放在哪个槽位
			ticks := int(delayDuration / wheel.tickDuration)

			// 修复边界条件：当ticks正好是wheelSize的整数倍时，
			// 应该放在最后一个槽位，避免放在当前槽位导致立即执行
			var targetSlot int
			if ticks%wheel.wheelSize == 0 && ticks > 0 {
				targetSlot = wheel.wheelSize - 1
			} else {
				targetSlot = (wheel.currentSlot + ticks) % wheel.wheelSize
			}

			return wheel, targetSlot
		}

		// 如果延时时间超过该层级范围，检查下一层
		// 如果已经是最后一层，放在最后一层的最后一个槽位
		if i == len(tw.wheels)-1 {
			lastSlot := wheel.wheelSize - 1
			return wheel, lastSlot
		}
	}

	// 理论上不会到这里，但为了安全返回最后一层
	lastWheel := tw.wheels[len(tw.wheels)-1]
	return lastWheel, lastWheel.wheelSize - 1
}

// tickLoop 时间轮推进循环（只启动最细粒度的时间轮）
func (tw *TimeWheel) tickLoop(wheel *wheelLevel) {
	defer tw.wg.Done()

	for {
		select {
		case <-tw.stopCh:
			return
		case <-wheel.ticker.C:
			tw.tick(wheel)
		}
	}
}

// tick 推进一个时间槽位
func (tw *TimeWheel) tick(wheel *wheelLevel) {
	wheel.mu.Lock()
	defer wheel.mu.Unlock()

	// 获取当前槽位的消息列表
	currentList := wheel.slots[wheel.currentSlot]
	now := time.Now().UnixMilli()

	// 处理当前槽位中到期的消息
	var expiredEntries []*DelayMessageEntry
	for e := currentList.Front(); e != nil; {
		entry := e.Value.(*DelayMessageEntry)
		next := e.Next()

		// 检查是否到期
		if entry.ExpireTime <= now {
			expiredEntries = append(expiredEntries, entry)
			currentList.Remove(e)
		}

		e = next
	}

	// 触发到期消息的回调（在锁外执行，避免阻塞）
	if len(expiredEntries) > 0 && tw.onExpire != nil {
		for _, entry := range expiredEntries {
			go tw.onExpire(entry.CommitLogOffset)
		}

		logger.Debug("Delay messages expired",
			"level", wheel.level,
			"slot", wheel.currentSlot,
			"count", len(expiredEntries))
	}

	// 推进到下一个槽位
	wheel.currentSlot = (wheel.currentSlot + 1) % wheel.wheelSize

	// 如果当前层级转完一圈，需要处理上一层级的消息
	// 例如：第0层（秒级）转完一圈（60秒）时，第1层（分钟级）应该推进一个槽位
	if wheel.currentSlot == 0 && wheel.prevLevel != nil {
		// 推进上一层级的槽位，并将其消息降级到当前层级
		tw.cascadeMessages(wheel.prevLevel, wheel)
	}
}

// cascadeMessages 将上一层级的消息降级到当前层级
// 当当前层级转完一圈时，推进上一层级的槽位，并将其消息重新分配到当前层级
// fromLevel: 上一层级（更粗粒度，level更高）
// toLevel: 当前层级（更细粒度，level更低）
func (tw *TimeWheel) cascadeMessages(fromLevel, toLevel *wheelLevel) {
	// 先获取锁，避免死锁（按层级顺序获取锁）
	if fromLevel.level > toLevel.level {
		fromLevel.mu.Lock()
		toLevel.mu.Lock()
	} else {
		toLevel.mu.Lock()
		fromLevel.mu.Lock()
	}

	// 推进上一层级的槽位
	oldSlot := fromLevel.currentSlot
	fromLevel.currentSlot = (fromLevel.currentSlot + 1) % fromLevel.wheelSize
	newSlot := fromLevel.currentSlot

	// 获取上一层级新到达槽位的消息（推进后的位置）
	// 这是关键修复：处理新到达槽位的消息，而不是旧槽位
	sourceList := fromLevel.slots[newSlot]
	now := time.Now().UnixMilli()

	logger.Debug("Cascading messages",
		"fromLevel", fromLevel.level,
		"toLevel", toLevel.level,
		"oldSlot", oldSlot,
		"newSlot", newSlot,
		"messagesInSlot", sourceList.Len())

	// 将消息重新分配到当前层级
	var messagesToCascade []*DelayMessageEntry
	for e := sourceList.Front(); e != nil; {
		entry := e.Value.(*DelayMessageEntry)
		next := e.Next()

		// 检查是否已经到期
		if entry.ExpireTime <= now {
			// 立即触发回调
			if tw.onExpire != nil {
				logger.Debug("Message expired during cascade, triggering callback",
					"fromLevel", fromLevel.level,
					"toLevel", toLevel.level,
					"slot", newSlot,
					"expireTime", entry.ExpireTime,
					"now", now,
					"offset", entry.CommitLogOffset)
				go tw.onExpire(entry.CommitLogOffset)
			}
			sourceList.Remove(e)
		} else {
			// 计算在当前层级应该放在哪个槽位
			delayMs := entry.ExpireTime - now
			delayDuration := time.Duration(delayMs) * time.Millisecond
			ticks := int(delayDuration / toLevel.tickDuration)

			// 如果延时时间在当前层级范围内，重新分配
			// 注意：使用 <= 而不是 <，允许剩余时间等于当前层级最大范围的消息降级
			if ticks <= toLevel.wheelSize {
				messagesToCascade = append(messagesToCascade, entry)
				sourceList.Remove(e)
			} else {
				// 延时时间仍然超过当前层级范围，保留在上一层级
				logger.Warn("Message delay exceeds lower level capacity, keeping in upper level",
					"fromLevel", fromLevel.level,
					"toLevel", toLevel.level,
					"delayMs", delayMs,
					"maxTicks", toLevel.wheelSize)
			}
		}

		e = next
	}

	// 释放锁，避免在递归调用时死锁
	fromLevel.mu.Unlock()
	toLevel.mu.Unlock()

	// 将消息添加到当前层级（在锁外执行，避免死锁）
	for _, entry := range messagesToCascade {
		delayMs := entry.ExpireTime - now
		delayDuration := time.Duration(delayMs) * time.Millisecond
		ticks := int(delayDuration / toLevel.tickDuration)

		var targetSlot int
		// 修复边界条件：当ticks正好是wheelSize的整数倍时，
		// 应该放在最后一个槽位，避免放在当前槽位导致立即执行
		if ticks%toLevel.wheelSize == 0 && ticks > 0 {
			targetSlot = toLevel.wheelSize - 1
		} else {
			targetSlot = (toLevel.currentSlot + ticks) % toLevel.wheelSize
		}

		toLevel.mu.Lock()
		toLevel.slots[targetSlot].PushBack(entry)
		toLevel.mu.Unlock()

		logger.Debug("Message cascaded to lower level",
			"fromLevel", fromLevel.level,
			"toLevel", toLevel.level,
			"targetSlot", targetSlot,
			"delayMs", delayMs)
	}

	// 如果上一层级也转完一圈，继续向上级联
	if fromLevel.currentSlot == 0 && fromLevel.prevLevel != nil {
		tw.cascadeMessages(fromLevel.prevLevel, fromLevel)
	}
}

// GetStats 获取时间轮统计信息
func (tw *TimeWheel) GetStats() map[string]interface{} {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	levelStats := make([]map[string]interface{}, len(tw.wheels))
	totalMessages := 0

	for i, wheel := range tw.wheels {
		wheel.mu.RLock()
		messagesInLevel := 0
		for _, slot := range wheel.slots {
			messagesInLevel += slot.Len()
		}
		totalMessages += messagesInLevel
		wheel.mu.RUnlock()

		levelStats[i] = map[string]interface{}{
			"level":        wheel.level,
			"currentSlot":  wheel.currentSlot,
			"messages":     messagesInLevel,
			"wheelSize":    wheel.wheelSize,
			"tickDuration": wheel.tickDuration.String(),
		}
	}

	return map[string]interface{}{
		"running":       tw.running,
		"totalMessages": totalMessages,
		"levels":        levelStats,
	}
}
