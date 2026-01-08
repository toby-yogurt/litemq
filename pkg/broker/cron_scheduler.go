package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// CronScheduler 定时消息调度器
// 基于Cron表达式实现定时消息的调度
type CronScheduler struct {
	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// Cron调度器
	cron *cron.Cron

	// 定时消息存储：messageID -> CronMessageInfo
	cronMessages map[string]*CronMessageInfo
	mu           sync.RWMutex

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// CronMessageInfo 定时消息信息
type CronMessageInfo struct {
	Message         *protocol.Message
	CommitLogOffset int64
	CronExpression  string
	CronEntryID     cron.EntryID
	StoreTime       int64
	NextRunTime     time.Time
	RunCount        int64 // 执行次数
	MaxRunCount     int64 // 最大执行次数（0表示不限制）
}

// NewCronScheduler 创建定时消息调度器
func NewCronScheduler(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *CronScheduler {
	// 创建Cron调度器，支持秒级精度
	c := cron.New(cron.WithSeconds())

	return &CronScheduler{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		cron:            c,
		cronMessages:    make(map[string]*CronMessageInfo),
		stopCh:          make(chan struct{}),
		running:         false,
	}
}

// Start 启动定时消息调度器
func (cs *CronScheduler) Start() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.running {
		return fmt.Errorf("cron scheduler is already running")
	}

	cs.running = true
	cs.cron.Start()

	logger.Info("Cron scheduler started")

	return nil
}

// Stop 停止定时消息调度器
func (cs *CronScheduler) Stop() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !cs.running {
		return
	}

	cs.running = false

	// 停止Cron调度器
	ctx := cs.cron.Stop()
	<-ctx.Done()

	close(cs.stopCh)
	cs.wg.Wait()

	logger.Info("Cron scheduler stopped")
}

// AddCronMessage 添加定时消息
func (cs *CronScheduler) AddCronMessage(msg *protocol.Message, commitLogOffset int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !cs.running {
		return fmt.Errorf("cron scheduler is not running")
	}

	// 获取Cron表达式
	cronExpression := msg.GetProperty("cron_expression")
	if cronExpression == "" {
		return fmt.Errorf("cron expression is required for cron message")
	}

	// 验证Cron表达式
	_, err := cron.ParseStandard(cronExpression)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %v", err)
	}

	// 获取最大执行次数（可选）
	maxRunCount := int64(0) // 0表示不限制
	maxRunCountStr := msg.GetProperty("max_run_count")
	if maxRunCountStr != "" {
		fmt.Sscanf(maxRunCountStr, "%d", &maxRunCount)
	}

	// 创建定时消息信息
	cronInfo := &CronMessageInfo{
		Message:         msg.Clone(),
		CommitLogOffset: commitLogOffset,
		CronExpression:  cronExpression,
		StoreTime:       time.Now().UnixMilli(),
		RunCount:        0,
		MaxRunCount:     maxRunCount,
	}

	// 添加Cron任务
	entryID, err := cs.cron.AddFunc(cronExpression, func() {
		cs.executeCronMessage(cronInfo)
	})
	if err != nil {
		return fmt.Errorf("failed to add cron job: %v", err)
	}

	cronInfo.CronEntryID = entryID

	// 计算下次执行时间
	entry := cs.cron.Entry(entryID)
	cronInfo.NextRunTime = entry.Next

	// 存储定时消息信息
	cs.cronMessages[msg.MessageID] = cronInfo

	logger.Info("Cron message added",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"cronExpression", cronExpression,
		"nextRunTime", cronInfo.NextRunTime,
		"maxRunCount", maxRunCount)

	return nil
}

// executeCronMessage 执行定时消息
func (cs *CronScheduler) executeCronMessage(cronInfo *CronMessageInfo) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// 检查是否超过最大执行次数
	if cronInfo.MaxRunCount > 0 && cronInfo.RunCount >= cronInfo.MaxRunCount {
		logger.Info("Cron message reached max run count, removing",
			"messageId", cronInfo.Message.MessageID,
			"runCount", cronInfo.RunCount,
			"maxRunCount", cronInfo.MaxRunCount)
		// 移除定时任务
		cs.cron.Remove(cronInfo.CronEntryID)
		delete(cs.cronMessages, cronInfo.Message.MessageID)
		return
	}

	// 增加执行次数
	cronInfo.RunCount++

	// 创建消息副本用于投递
	msg := cronInfo.Message.Clone()
	msg.MessageType = protocol.MessageTypeNormal // 转换为普通消息
	msg.SetProperty("cron_message_id", cronInfo.Message.MessageID)
	msg.SetProperty("cron_run_count", fmt.Sprintf("%d", cronInfo.RunCount))
	msg.SetProperty("cron_run_time", fmt.Sprintf("%d", time.Now().UnixMilli()))

	// 构建ConsumeQueue索引，使消息对消费者可见
	if err := cs.buildConsumeQueueIndex(msg); err != nil {
		logger.Warn("Failed to build consume queue index for cron message",
			"messageId", cronInfo.Message.MessageID,
			"runCount", cronInfo.RunCount,
			"error", err)
		return
	}

	// 更新下次执行时间
	entry := cs.cron.Entry(cronInfo.CronEntryID)
	cronInfo.NextRunTime = entry.Next

	logger.Info("Cron message executed",
		"messageId", cronInfo.Message.MessageID,
		"topic", cronInfo.Message.Topic,
		"runCount", cronInfo.RunCount,
		"nextRunTime", cronInfo.NextRunTime)
}

// RemoveCronMessage 移除定时消息
func (cs *CronScheduler) RemoveCronMessage(messageID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cronInfo, exists := cs.cronMessages[messageID]
	if !exists {
		return fmt.Errorf("cron message not found: %s", messageID)
	}

	// 从Cron调度器中移除
	cs.cron.Remove(cronInfo.CronEntryID)

	// 从存储中删除
	delete(cs.cronMessages, messageID)

	logger.Info("Cron message removed",
		"messageId", messageID)

	return nil
}

// GetCronMessage 获取定时消息信息
func (cs *CronScheduler) GetCronMessage(messageID string) (*CronMessageInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	cronInfo, exists := cs.cronMessages[messageID]
	if !exists {
		return nil, false
	}

	// 返回副本
	return cronInfo.clone(), true
}

// GetAllCronMessages 获取所有定时消息
func (cs *CronScheduler) GetAllCronMessages() []*CronMessageInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	messages := make([]*CronMessageInfo, 0, len(cs.cronMessages))
	for _, cronInfo := range cs.cronMessages {
		messages = append(messages, cronInfo.clone())
	}

	return messages
}

// buildConsumeQueueIndex 构建ConsumeQueue索引
func (cs *CronScheduler) buildConsumeQueueIndex(msg *protocol.Message) error {
	// 获取或创建ConsumeQueue
	cq, err := cs.consumeQueueMgr.GetOrCreateConsumeQueue(msg.Topic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	// 追加索引项
	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	return nil
}

// GetStats 获取定时消息调度器统计信息
func (cs *CronScheduler) GetStats() map[string]interface{} {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	return map[string]interface{}{
		"running":       cs.running,
		"totalMessages": len(cs.cronMessages),
		"cronJobs":      len(cs.cron.Entries()),
	}
}

// clone 克隆定时消息信息
func (cmi *CronMessageInfo) clone() *CronMessageInfo {
	return &CronMessageInfo{
		Message:         cmi.Message.Clone(),
		CommitLogOffset: cmi.CommitLogOffset,
		CronExpression:  cmi.CronExpression,
		CronEntryID:     cmi.CronEntryID,
		StoreTime:       cmi.StoreTime,
		NextRunTime:     cmi.NextRunTime,
		RunCount:        cmi.RunCount,
		MaxRunCount:     cmi.MaxRunCount,
	}
}
