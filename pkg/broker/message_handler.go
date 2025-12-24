package broker

import (
	"fmt"
	"time"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// MessageHandler 消息处理器
type MessageHandler struct {
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager
	config          *config.BrokerConfig
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(commitLog *storage.CommitLog, consumeQueueMgr *storage.ConsumeQueueManager, cfg *config.BrokerConfig) *MessageHandler {
	return &MessageHandler{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		config:          cfg,
	}
}

// HandleSendMessage 处理发送消息
func (mh *MessageHandler) HandleSendMessage(cmd *protocol.Command) *protocol.Command {
	// 解析消息
	msg, err := mh.parseMessageFromCommand(cmd)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseParameterError,
			fmt.Sprintf("failed to parse message: %v", err))
	}

	// 验证消息
	if err := mh.validateMessage(msg); err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseParameterError,
			fmt.Sprintf("invalid message: %v", err))
	}

	// 设置消息元数据
	mh.setMessageMetadata(msg)

	// 存储消息
	offset, err := mh.commitLog.AppendMessage(msg)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseStorageError,
			fmt.Sprintf("failed to store message: %v", err))
	}

	// 构建ConsumeQueue索引
	if err := mh.buildConsumeQueueIndex(msg); err != nil {
		// 记录警告但不影响消息发送成功
		logger.Warn("Failed to build consume queue index",
			"message_id", msg.MessageID,
			"error", err)
	}

	// 返回成功响应
	response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
	response.SetExtField("offset", offset)
	response.SetExtField("message_id", msg.MessageID)

	return response
}

// SendMessage 直接发送消息（供gRPC调用）
func (mh *MessageHandler) SendMessage(msg *protocol.Message) (int64, error) {
	// 验证消息
	if err := mh.validateMessage(msg); err != nil {
		return 0, fmt.Errorf("invalid message: %v", err)
	}

	// 设置消息元数据
	mh.setMessageMetadata(msg)

	// 如果 QueueID 未设置，默认使用 0
	if msg.QueueID == 0 {
		msg.QueueID = 0 // 确保是 0
	}

	// 存储消息
	offset, err := mh.commitLog.AppendMessage(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to store message: %v", err)
	}

	// 设置 CommitLogOffset（AppendMessage 返回的 offset）
	msg.CommitLogOffset = offset

	logger.Info("Message stored in commitlog",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"queueId", msg.QueueID,
		"commitLogOffset", offset)

	// 构建ConsumeQueue索引
	if err := mh.buildConsumeQueueIndex(msg); err != nil {
		// 记录警告但不影响消息发送成功
		logger.Warn("Failed to build consume queue index",
			"message_id", msg.MessageID,
			"error", err)
	}

	return offset, nil
}

// SendMessages 批量发送消息（供gRPC调用）
func (mh *MessageHandler) SendMessages(messages []*protocol.Message) ([]int64, error) {
	offsets := make([]int64, len(messages))

	for i, msg := range messages {
		offset, err := mh.SendMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to send message %d: %v", i, err)
		}
		offsets[i] = offset
	}

	return offsets, nil
}

// HandleSendMessages 批量处理发送消息
func (mh *MessageHandler) HandleSendMessages(cmd *protocol.Command) *protocol.Command {
	// 解析消息列表
	messages, err := mh.parseMessagesFromCommand(cmd)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseParameterError,
			fmt.Sprintf("failed to parse messages: %v", err))
	}

	if len(messages) == 0 {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseParameterError,
			"no messages to send")
	}

	// 验证所有消息
	for i, msg := range messages {
		if err := mh.validateMessage(msg); err != nil {
			return protocol.NewResponse(cmd.RequestID, protocol.ResponseParameterError,
				fmt.Sprintf("invalid message at index %d: %v", i, err))
		}
		mh.setMessageMetadata(msg)
	}

	// 批量存储消息
	offsets, err := mh.commitLog.AppendMessages(messages)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseStorageError,
			fmt.Sprintf("failed to store messages: %v", err))
	}

	// 批量构建ConsumeQueue索引
	for _, msg := range messages {
		if err := mh.buildConsumeQueueIndex(msg); err != nil {
			logger.Warn("Failed to build consume queue index",
				"message_id", msg.MessageID,
				"error", err)
		}
	}

	// 返回成功响应
	response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
	response.SetExtField("offsets", offsets)
	response.SetExtField("count", len(messages))

	return response
}

// parseMessageFromCommand 从命令解析消息
func (mh *MessageHandler) parseMessageFromCommand(cmd *protocol.Command) (*protocol.Message, error) {
	// 从命令体解析消息
	// 实际实现需要反序列化
	return nil, fmt.Errorf("not implemented")
}

// parseMessagesFromCommand 从命令解析消息列表
func (mh *MessageHandler) parseMessagesFromCommand(cmd *protocol.Command) ([]*protocol.Message, error) {
	// 从命令体解析消息列表
	// 实际实现需要反序列化
	return nil, fmt.Errorf("not implemented")
}

// validateMessage 验证消息
func (mh *MessageHandler) validateMessage(msg *protocol.Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	if msg.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if len(msg.Body) == 0 {
		return fmt.Errorf("message body is empty")
	}

	if len(msg.Body) > mh.config.MaxMessageSize {
		return fmt.Errorf("message size %d exceeds max size %d",
			len(msg.Body), mh.config.MaxMessageSize)
	}

	// 验证主题名格式
	if len(msg.Topic) > 255 {
		return fmt.Errorf("topic name too long: %s", msg.Topic)
	}

	// 验证消息ID（如果提供）
	if msg.MessageID != "" && len(msg.MessageID) > 255 {
		return fmt.Errorf("message ID too long: %s", msg.MessageID)
	}

	return nil
}

// setMessageMetadata 设置消息元数据
func (mh *MessageHandler) setMessageMetadata(msg *protocol.Message) {
	now := time.Now().UnixMilli()

	// 设置消息ID（如果为空）
	if msg.MessageID == "" {
		msg.MessageID = generateMessageID()
	}

	// 设置时间戳
	if msg.BornTimestamp == 0 {
		msg.BornTimestamp = now
	}

	// 设置存储主机信息
	if msg.BornHost == "" {
		msg.BornHost = fmt.Sprintf("%s:%d", mh.config.Host, mh.config.Port)
	}
	msg.StoreHost = msg.BornHost

	// 设置存储时间戳
	msg.StoreTimestamp = now

	// 根据消息类型设置默认值
	switch msg.MessageType {
	case protocol.MessageTypeDelay:
		if msg.DelayTime == 0 {
			// 默认延时5秒
			msg.DelayTime = now + 5000
		}
	case protocol.MessageTypeTransaction:
		if msg.MessageStatus == protocol.MessageStatusNormal {
			msg.MessageStatus = protocol.MessageStatusPrepared
		}
	}
}

// buildConsumeQueueIndex 构建ConsumeQueue索引
func (mh *MessageHandler) buildConsumeQueueIndex(msg *protocol.Message) error {
	// 如果 QueueID 未设置，默认使用 0
	if msg.QueueID == 0 {
		msg.QueueID = 0 // 确保是 0
	}

	// 获取或创建ConsumeQueue
	cq, err := mh.consumeQueueMgr.GetOrCreateConsumeQueue(msg.Topic, msg.QueueID)
	if err != nil {
		return fmt.Errorf("failed to get consume queue: %v", err)
	}

	// 追加索引项
	if err := cq.AppendMessage(msg); err != nil {
		return fmt.Errorf("failed to append message to consume queue: %v", err)
	}

	logger.Info("Built consume queue index",
		"topic", msg.Topic,
		"queueId", msg.QueueID,
		"commitLogOffset", msg.CommitLogOffset,
		"messageId", msg.MessageID)

	return nil
}

// GetMessageStats 获取消息统计信息
func (mh *MessageHandler) GetMessageStats() map[string]interface{} {
	return map[string]interface{}{
		"total_messages":   mh.commitLog.GetTotalMessages(),
		"total_bytes":      mh.commitLog.GetTotalBytes(),
		"wrote_offset":     mh.commitLog.GetWroteOffset(),
		"committed_offset": mh.commitLog.GetCommittedOffset(),
		"flushed_offset":   mh.commitLog.GetFlushedOffset(),
	}
}

// generateMessageID 生成消息ID (简化实现)
func generateMessageID() string {
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}
