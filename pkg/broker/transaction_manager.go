package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TransactionStatusUnknown    TransactionStatus = 0 // 未知状态
	TransactionStatusPrepared   TransactionStatus = 1 // 预提交状态（Half消息）
	TransactionStatusCommitted  TransactionStatus = 2 // 已提交
	TransactionStatusRollbacked TransactionStatus = 3 // 已回滚
)

// TransactionRecord 事务记录
type TransactionRecord struct {
	TransactionID   string            `json:"transaction_id"`
	MessageID       string            `json:"message_id"`
	Topic           string            `json:"topic"`
	QueueID         int               `json:"queue_id"`
	Status          TransactionStatus `json:"status"`
	CreatedTime     int64             `json:"created_time"`
	UpdatedTime     int64             `json:"updated_time"`
	CheckTimes      int               `json:"check_times"`
	MaxCheckTimes   int               `json:"max_check_times"`
	CommitLogOffset int64             `json:"commit_log_offset"` // Half消息在CommitLog中的偏移量
}

// TransactionManager 事务管理器
// 负责管理事务消息的整个生命周期：预提交、提交、回滚
type TransactionManager struct {
	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 事务记录存储: transactionID -> TransactionRecord
	transactionTable map[string]*TransactionRecord
	mutex            sync.RWMutex

	// Half消息存储（预提交状态的消息）
	halfMessageStore *HalfMessageStore

	// Op消息存储（操作消息，记录提交/回滚操作）
	opMessageStore *OpMessageStore

	// 回查服务
	checkbackService *CheckbackService

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *TransactionManager {
	tm := &TransactionManager{
		commitLog:        commitLog,
		consumeQueueMgr:  consumeQueueMgr,
		transactionTable: make(map[string]*TransactionRecord),
		halfMessageStore: NewHalfMessageStore(),
		opMessageStore:   NewOpMessageStore(),
		stopCh:           make(chan struct{}),
		running:          false,
	}

	// 创建回查服务
	tm.checkbackService = NewCheckbackService(tm)

	return tm
}

// Start 启动事务管理器
func (tm *TransactionManager) Start() error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.running {
		return fmt.Errorf("transaction manager is already running")
	}

	tm.running = true

	// 启动回查服务
	if err := tm.checkbackService.Start(); err != nil {
		tm.running = false
		return fmt.Errorf("failed to start checkback service: %v", err)
	}

	logger.Info("Transaction manager started")

	return nil
}

// Stop 停止事务管理器
func (tm *TransactionManager) Stop() {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if !tm.running {
		return
	}

	tm.running = false
	close(tm.stopCh)

	// 停止回查服务
	tm.checkbackService.Stop()

	// 等待所有goroutine结束
	tm.wg.Wait()

	logger.Info("Transaction manager stopped")
}

// PrepareTransaction 预提交事务消息（存储Half消息）
// 返回事务ID
func (tm *TransactionManager) PrepareTransaction(msg *protocol.Message, commitLogOffset int64) (string, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if !tm.running {
		return "", fmt.Errorf("transaction manager is not running")
	}

	// 生成事务ID
	transactionID := tm.generateTransactionID(msg.MessageID)

	// 创建事务记录
	record := &TransactionRecord{
		TransactionID:   transactionID,
		MessageID:       msg.MessageID,
		Topic:           msg.Topic,
		QueueID:         msg.QueueID,
		Status:          TransactionStatusPrepared,
		CreatedTime:     time.Now().Unix(),
		UpdatedTime:     time.Now().Unix(),
		CheckTimes:      0,
		MaxCheckTimes:   15, // 最大回查15次
		CommitLogOffset: commitLogOffset,
	}

	// 存储事务记录
	tm.transactionTable[transactionID] = record

	// 存储Half消息
	if err := tm.halfMessageStore.PutMessage(msg, transactionID, commitLogOffset); err != nil {
		delete(tm.transactionTable, transactionID)
		return "", fmt.Errorf("failed to store half message: %v", err)
	}

	logger.Info("Transaction prepared",
		"transactionId", transactionID,
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"commitLogOffset", commitLogOffset)

	return transactionID, nil
}

// CommitTransaction 提交事务
// 将Half消息转换为普通消息，使其对消费者可见
func (tm *TransactionManager) CommitTransaction(transactionID string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	record, exists := tm.transactionTable[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	if record.Status != TransactionStatusPrepared {
		return fmt.Errorf("transaction status is not prepared: %d", record.Status)
	}

	// 更新事务状态
	record.Status = TransactionStatusCommitted
	record.UpdatedTime = time.Now().Unix()

	// 存储Op消息（记录提交操作）
	if err := tm.opMessageStore.PutOpMessage(transactionID, "COMMIT", record.CommitLogOffset); err != nil {
		logger.Warn("Failed to store commit op message",
			"transactionId", transactionID,
			"error", err)
		// 不影响主流程，继续执行
	}

	// 从Half消息存储中获取消息
	halfMsg, exists := tm.halfMessageStore.GetMessage(transactionID)
	if !exists {
		return fmt.Errorf("half message not found: %s", transactionID)
	}

	// 将Half消息转换为普通消息（构建ConsumeQueue索引）
	// 恢复原始Topic
	halfMsg.Topic = record.Topic
	halfMsg.MessageStatus = protocol.MessageStatusCommit
	halfMsg.CommitLogOffset = record.CommitLogOffset

	// 构建ConsumeQueue索引，使消息对消费者可见
	if err := tm.buildConsumeQueueIndex(halfMsg); err != nil {
		// 回滚状态
		record.Status = TransactionStatusPrepared
		return fmt.Errorf("failed to build consume queue index: %v", err)
	}

	// 从Half消息存储中移除（已提交）
	tm.halfMessageStore.DeleteMessage(transactionID)

	logger.Info("Transaction committed",
		"transactionId", transactionID,
		"messageId", record.MessageID,
		"topic", record.Topic)

	return nil
}

// RollbackTransaction 回滚事务
// 删除Half消息，不构建ConsumeQueue索引
func (tm *TransactionManager) RollbackTransaction(transactionID string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	record, exists := tm.transactionTable[transactionID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	if record.Status != TransactionStatusPrepared {
		return fmt.Errorf("transaction status is not prepared: %d", record.Status)
	}

	// 更新事务状态
	record.Status = TransactionStatusRollbacked
	record.UpdatedTime = time.Now().Unix()

	// 存储Op消息（记录回滚操作）
	if err := tm.opMessageStore.PutOpMessage(transactionID, "ROLLBACK", record.CommitLogOffset); err != nil {
		logger.Warn("Failed to store rollback op message",
			"transactionId", transactionID,
			"error", err)
		// 不影响主流程，继续执行
	}

	// 删除Half消息
	if err := tm.halfMessageStore.DeleteMessage(transactionID); err != nil {
		logger.Warn("Failed to delete half message",
			"transactionId", transactionID,
			"error", err)
		// 不影响主流程，继续执行
	}

	logger.Info("Transaction rollbacked",
		"transactionId", transactionID,
		"messageId", record.MessageID,
		"topic", record.Topic)

	return nil
}

// CheckTransactionStatus 查询事务状态
func (tm *TransactionManager) CheckTransactionStatus(transactionID string) (TransactionStatus, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	record, exists := tm.transactionTable[transactionID]
	if !exists {
		return TransactionStatusUnknown, fmt.Errorf("transaction not found: %s", transactionID)
	}

	return record.Status, nil
}

// GetTransactionRecord 获取事务记录
func (tm *TransactionManager) GetTransactionRecord(transactionID string) (*TransactionRecord, bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	record, exists := tm.transactionTable[transactionID]
	return record, exists
}

// GetPendingTransactions 获取待处理的Half消息（用于回查）
func (tm *TransactionManager) GetPendingTransactions() []*TransactionRecord {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	var pending []*TransactionRecord
	now := time.Now().Unix()

	// 获取所有Prepared状态的事务，且超过60秒未处理
	for _, record := range tm.transactionTable {
		if record.Status == TransactionStatusPrepared {
			// 超过60秒未处理，需要回查
			if now-record.CreatedTime > 60 {
				pending = append(pending, record)
			}
		}
	}

	return pending
}

// IncrementCheckTimes 增加回查次数
func (tm *TransactionManager) IncrementCheckTimes(transactionID string) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	record, exists := tm.transactionTable[transactionID]
	if exists {
		record.CheckTimes++
		record.UpdatedTime = time.Now().Unix()
	}
}

// buildConsumeQueueIndex 构建ConsumeQueue索引
func (tm *TransactionManager) buildConsumeQueueIndex(msg *protocol.Message) error {
	// 如果 QueueID 未设置，默认使用 0
	if msg.QueueID == 0 {
		msg.QueueID = 0
	}

	// 获取或创建 ConsumeQueue
	cq, err := tm.consumeQueueMgr.GetOrCreateConsumeQueue(msg.Topic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	// 追加索引项
	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	return nil
}

// generateTransactionID 生成事务ID
func (tm *TransactionManager) generateTransactionID(messageID string) string {
	return fmt.Sprintf("TX-%s-%d", messageID, time.Now().UnixNano())
}

// GetTransactionStats 获取事务统计信息
func (tm *TransactionManager) GetTransactionStats() map[string]interface{} {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	stats := map[string]interface{}{
		"total_transactions": len(tm.transactionTable),
		"prepared_count":     0,
		"committed_count":    0,
		"rollbacked_count":   0,
	}

	for _, record := range tm.transactionTable {
		switch record.Status {
		case TransactionStatusPrepared:
			stats["prepared_count"] = stats["prepared_count"].(int) + 1
		case TransactionStatusCommitted:
			stats["committed_count"] = stats["committed_count"].(int) + 1
		case TransactionStatusRollbacked:
			stats["rollbacked_count"] = stats["rollbacked_count"].(int) + 1
		}
	}

	return stats
}
