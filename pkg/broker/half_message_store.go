package broker

import (
	"fmt"
	"sync"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

// HalfMessageStore Half消息存储
// Half消息是事务消息的预提交状态，存储在特殊的Topic中，消费者不可见
// 只有当事务提交后，才会将消息从Half消息存储移动到正常的Topic
type HalfMessageStore struct {
	// Half消息存储在特殊的Topic中（类似RocketMQ的RMQ_SYS_TRANS_HALF_TOPIC）
	halfTopic string

	// 内存存储: transactionID -> HalfMessageInfo
	messages map[string]*HalfMessageInfo
	mutex    sync.RWMutex
}

// HalfMessageInfo Half消息信息
type HalfMessageInfo struct {
	Message         *protocol.Message
	TransactionID   string
	CommitLogOffset int64
	StoreTime       int64
}

// NewHalfMessageStore 创建Half消息存储
func NewHalfMessageStore() *HalfMessageStore {
	return &HalfMessageStore{
		halfTopic: "RMQ_SYS_TRANS_HALF_TOPIC", // 系统内部Topic，类似RocketMQ
		messages:  make(map[string]*HalfMessageInfo),
	}
}

// PutMessage 存储Half消息
func (hms *HalfMessageStore) PutMessage(msg *protocol.Message, transactionID string, commitLogOffset int64) error {
	hms.mutex.Lock()
	defer hms.mutex.Unlock()

	// 创建Half消息副本（避免修改原始消息）
	halfMsg := msg.Clone()

	// 设置Half消息属性
	halfMsg.Topic = hms.halfTopic // 使用系统内部Topic
	halfMsg.MessageStatus = protocol.MessageStatusPrepared
	halfMsg.TransactionID = transactionID
	halfMsg.CommitLogOffset = commitLogOffset

	// 设置属性标记为Half消息
	if halfMsg.Properties == nil {
		halfMsg.Properties = make(map[string]string)
	}
	halfMsg.Properties["transaction_id"] = transactionID
	halfMsg.Properties["message_type"] = "half"
	halfMsg.Properties["original_topic"] = msg.Topic // 保存原始Topic

	// 存储Half消息信息
	hms.messages[transactionID] = &HalfMessageInfo{
		Message:         halfMsg,
		TransactionID:   transactionID,
		CommitLogOffset: commitLogOffset,
		StoreTime:       msg.StoreTimestamp,
	}

	logger.Debug("Half message stored",
		"transactionId", transactionID,
		"originalTopic", msg.Topic,
		"commitLogOffset", commitLogOffset)

	return nil
}

// GetMessage 获取Half消息
func (hms *HalfMessageStore) GetMessage(transactionID string) (*protocol.Message, bool) {
	hms.mutex.RLock()
	defer hms.mutex.RUnlock()

	info, exists := hms.messages[transactionID]
	if !exists {
		return nil, false
	}

	// 返回消息副本
	return info.Message.Clone(), true
}

// DeleteMessage 删除Half消息
func (hms *HalfMessageStore) DeleteMessage(transactionID string) error {
	hms.mutex.Lock()
	defer hms.mutex.Unlock()

	if _, exists := hms.messages[transactionID]; !exists {
		return fmt.Errorf("half message not found: %s", transactionID)
	}

	delete(hms.messages, transactionID)

	logger.Debug("Half message deleted",
		"transactionId", transactionID)

	return nil
}

// GetAllHalfMessages 获取所有Half消息
func (hms *HalfMessageStore) GetAllHalfMessages() []*HalfMessageInfo {
	hms.mutex.RLock()
	defer hms.mutex.RUnlock()

	messages := make([]*HalfMessageInfo, 0, len(hms.messages))
	for _, info := range hms.messages {
		messages = append(messages, info)
	}

	return messages
}

// GetExpiredHalfMessages 获取过期的Half消息（用于回查）
// expireTime: 过期时间戳（秒）
func (hms *HalfMessageStore) GetExpiredHalfMessages(expireTime int64) []*HalfMessageInfo {
	hms.mutex.RLock()
	defer hms.mutex.RUnlock()

	var expired []*HalfMessageInfo
	for _, info := range hms.messages {
		// 检查消息是否过期（超过一定时间未处理）
		if info.StoreTime/1000 < expireTime {
			expired = append(expired, info)
		}
	}

	return expired
}

// Size 获取Half消息数量
func (hms *HalfMessageStore) Size() int {
	hms.mutex.RLock()
	defer hms.mutex.RUnlock()

	return len(hms.messages)
}

// GetHalfTopic 获取Half消息Topic名称
func (hms *HalfMessageStore) GetHalfTopic() string {
	return hms.halfTopic
}
