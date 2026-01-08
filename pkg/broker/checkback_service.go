package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
)

// CheckbackService 回查服务
// 定期检查长时间未决的事务消息，主动向生产者查询事务状态
// 这是RocketMQ事务消息的核心机制，确保事务的最终一致性
type CheckbackService struct {
	transactionMgr *TransactionManager

	// 回查配置
	checkInterval time.Duration // 回查间隔（默认60秒）
	maxCheckTimes int           // 最大回查次数（默认15次）

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mutex   sync.RWMutex
}

// NewCheckbackService 创建回查服务
func NewCheckbackService(transactionMgr *TransactionManager) *CheckbackService {
	return &CheckbackService{
		transactionMgr: transactionMgr,
		checkInterval:  60 * time.Second, // 默认60秒回查一次
		maxCheckTimes:  15,               // 最多回查15次
		running:        false,
		stopCh:         make(chan struct{}),
	}
}

// Start 启动回查服务
func (cs *CheckbackService) Start() error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if cs.running {
		return fmt.Errorf("checkback service is already running")
	}

	cs.running = true

	cs.wg.Add(1)
	go cs.checkbackLoop()

	logger.Info("Checkback service started",
		"checkInterval", cs.checkInterval,
		"maxCheckTimes", cs.maxCheckTimes)

	return nil
}

// Stop 停止回查服务
func (cs *CheckbackService) Stop() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	if !cs.running {
		return
	}

	cs.running = false
	close(cs.stopCh)

	cs.wg.Wait()

	logger.Info("Checkback service stopped")
}

// checkbackLoop 回查循环
func (cs *CheckbackService) checkbackLoop() {
	defer cs.wg.Done()

	ticker := time.NewTicker(cs.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.stopCh:
			return
		case <-ticker.C:
			cs.checkPendingTransactions()
		}
	}
}

// checkPendingTransactions 检查待处理的事务
func (cs *CheckbackService) checkPendingTransactions() {
	// 获取所有待处理的事务（Prepared状态且超过60秒）
	pendingTransactions := cs.transactionMgr.GetPendingTransactions()

	if len(pendingTransactions) == 0 {
		return
	}

	logger.Info("Checking pending transactions",
		"count", len(pendingTransactions))

	for _, record := range pendingTransactions {
		// 检查是否超过最大回查次数
		if record.CheckTimes >= record.MaxCheckTimes {
			// 超过最大回查次数，自动回滚（避免消息长时间占用资源）
			logger.Warn("Transaction exceeded max check times, auto rollback",
				"transactionId", record.TransactionID,
				"checkTimes", record.CheckTimes,
				"maxCheckTimes", record.MaxCheckTimes)

			if err := cs.transactionMgr.RollbackTransaction(record.TransactionID); err != nil {
				logger.Error("Failed to auto rollback transaction",
					"transactionId", record.TransactionID,
					"error", err)
			}
			continue
		}

		// 执行回查
		cs.checkTransaction(record)
	}
}

// checkTransaction 回查单个事务
// 在实际实现中，这里应该向生产者发送回查请求
// 但为了简化，我们使用模拟逻辑
func (cs *CheckbackService) checkTransaction(record *TransactionRecord) {
	// 增加回查次数
	cs.transactionMgr.IncrementCheckTimes(record.TransactionID)

	logger.Debug("Checking transaction",
		"transactionId", record.TransactionID,
		"messageId", record.MessageID,
		"topic", record.Topic,
		"checkTimes", record.CheckTimes+1)

	// 实际实现中，这里应该：
	// 1. 向生产者发送回查请求（通过gRPC或HTTP）
	// 2. 生产者返回事务状态（COMMIT/ROLLBACK/UNKNOWN）
	// 3. 根据返回的状态执行相应的操作

	// 当前实现：由于无法直接访问生产者，使用保守策略
	// - 如果回查次数较少，继续等待（给生产者更多时间）
	// - 如果回查次数较多，自动回滚（避免消息长时间占用资源）

	// 注意：实际生产环境中，应该实现生产者的回查接口
	// 可以通过以下方式：
	// 1. 在消息的 Properties 中存储生产者的回调地址
	// 2. 通过 gRPC 或 HTTP 调用生产者的回查接口
	// 3. 生产者返回事务状态，Broker 根据状态执行相应操作

	// 当前简化实现：超过最大回查次数后自动回滚
	// 这个逻辑已经在 checkPendingTransactions 中处理
}

// SetCheckInterval 设置回查间隔
func (cs *CheckbackService) SetCheckInterval(interval time.Duration) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	cs.checkInterval = interval
}

// SetMaxCheckTimes 设置最大回查次数
func (cs *CheckbackService) SetMaxCheckTimes(maxTimes int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	cs.maxCheckTimes = maxTimes
}

// GetStats 获取回查服务统计信息
func (cs *CheckbackService) GetStats() map[string]interface{} {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	return map[string]interface{}{
		"running":       cs.running,
		"checkInterval": cs.checkInterval.String(),
		"maxCheckTimes": cs.maxCheckTimes,
	}
}
