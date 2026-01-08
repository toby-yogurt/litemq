package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
)

// OpType 操作类型
type OpType string

const (
	OpTypeCommit   OpType = "COMMIT"   // 提交操作
	OpTypeRollback OpType = "ROLLBACK" // 回滚操作
)

// OpMessage Op消息（操作消息）
// 用于记录事务的提交/回滚操作，用于故障恢复和审计
type OpMessage struct {
	TransactionID   string `json:"transaction_id"`
	OpType          OpType `json:"op_type"` // COMMIT or ROLLBACK
	Timestamp       int64  `json:"timestamp"`
	MessageID       string `json:"message_id,omitempty"`
	CommitLogOffset int64  `json:"commit_log_offset"`
}

// OpMessageStore Op消息存储
// 存储事务的提交/回滚操作记录
type OpMessageStore struct {
	// 内存存储: transactionID -> OpMessage
	opMessages map[string]*OpMessage
	mutex      sync.RWMutex

	// Op消息存储在特殊的Topic中（类似RocketMQ的RMQ_SYS_TRANS_OP_HALF_TOPIC）
	opTopic string
}

// NewOpMessageStore 创建Op消息存储
func NewOpMessageStore() *OpMessageStore {
	return &OpMessageStore{
		opMessages: make(map[string]*OpMessage),
		opTopic:    "RMQ_SYS_TRANS_OP_HALF_TOPIC", // 系统内部Topic
	}
}

// PutOpMessage 存储Op消息
func (oms *OpMessageStore) PutOpMessage(transactionID string, opType OpType, commitLogOffset int64) error {
	oms.mutex.Lock()
	defer oms.mutex.Unlock()

	opMsg := &OpMessage{
		TransactionID:   transactionID,
		OpType:          opType,
		Timestamp:       time.Now().Unix(),
		CommitLogOffset: commitLogOffset,
	}

	oms.opMessages[transactionID] = opMsg

	logger.Debug("Op message stored",
		"transactionId", transactionID,
		"opType", opType,
		"commitLogOffset", commitLogOffset)

	return nil
}

// GetOpMessage 获取Op消息
func (oms *OpMessageStore) GetOpMessage(transactionID string) (*OpMessage, bool) {
	oms.mutex.RLock()
	defer oms.mutex.RUnlock()

	opMsg, exists := oms.opMessages[transactionID]
	if !exists {
		return nil, false
	}

	// 返回副本
	return &OpMessage{
		TransactionID:   opMsg.TransactionID,
		OpType:          opMsg.OpType,
		Timestamp:       opMsg.Timestamp,
		MessageID:       opMsg.MessageID,
		CommitLogOffset: opMsg.CommitLogOffset,
	}, true
}

// DeleteOpMessage 删除Op消息
func (oms *OpMessageStore) DeleteOpMessage(transactionID string) error {
	oms.mutex.Lock()
	defer oms.mutex.Unlock()

	if _, exists := oms.opMessages[transactionID]; !exists {
		return fmt.Errorf("op message not found: %s", transactionID)
	}

	delete(oms.opMessages, transactionID)

	return nil
}

// GetAllOpMessages 获取所有Op消息
func (oms *OpMessageStore) GetAllOpMessages() []*OpMessage {
	oms.mutex.RLock()
	defer oms.mutex.RUnlock()

	messages := make([]*OpMessage, 0, len(oms.opMessages))
	for _, opMsg := range oms.opMessages {
		// 返回副本
		messages = append(messages, &OpMessage{
			TransactionID:   opMsg.TransactionID,
			OpType:          opMsg.OpType,
			Timestamp:       opMsg.Timestamp,
			MessageID:       opMsg.MessageID,
			CommitLogOffset: opMsg.CommitLogOffset,
		})
	}

	return messages
}

// Size 获取Op消息数量
func (oms *OpMessageStore) Size() int {
	oms.mutex.RLock()
	defer oms.mutex.RUnlock()

	return len(oms.opMessages)
}

// GetOpTopic 获取Op消息Topic名称
func (oms *OpMessageStore) GetOpTopic() string {
	return oms.opTopic
}
