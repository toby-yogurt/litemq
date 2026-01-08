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
	commitLog        *storage.CommitLog
	consumeQueueMgr  *storage.ConsumeQueueManager
	delayScheduler   *DelayMessageScheduler
	transactionMgr   *TransactionManager
	orderMessageMgr  *OrderMessageManager
	broadcastManager *BroadcastManager
	cronScheduler    *CronScheduler
	replicationMgr   *ReplicationManager
	flowControl      *FlowControl
	checkPoint       *storage.CheckPoint
	indexFile        *storage.IndexFile
	config           *config.BrokerConfig
}

// NewMessageHandler 创建消息处理器
func NewMessageHandler(commitLog *storage.CommitLog, consumeQueueMgr *storage.ConsumeQueueManager, cfg *config.BrokerConfig) *MessageHandler {
	return &MessageHandler{
		commitLog:       commitLog,
		consumeQueueMgr: consumeQueueMgr,
		config:          cfg,
		// delayScheduler 将在 Broker 初始化后设置
	}
}

// SetDelayScheduler 设置延时消息调度器
func (mh *MessageHandler) SetDelayScheduler(scheduler *DelayMessageScheduler) {
	mh.delayScheduler = scheduler
}

// SetTransactionManager 设置事务管理器
func (mh *MessageHandler) SetTransactionManager(tm *TransactionManager) {
	mh.transactionMgr = tm
}

// SetOrderMessageManager 设置顺序消息管理器
func (mh *MessageHandler) SetOrderMessageManager(omm *OrderMessageManager) {
	mh.orderMessageMgr = omm
}

// SetBroadcastManager 设置广播消息管理器
func (mh *MessageHandler) SetBroadcastManager(bm *BroadcastManager) {
	mh.broadcastManager = bm
}

// SetCronScheduler 设置定时消息调度器
func (mh *MessageHandler) SetCronScheduler(cs *CronScheduler) {
	mh.cronScheduler = cs
}

// SetReplicationManager 设置主从复制管理器
func (mh *MessageHandler) SetReplicationManager(rm *ReplicationManager) {
	mh.replicationMgr = rm
}

// SetFlowControl 设置流控管理器
func (mh *MessageHandler) SetFlowControl(fc *FlowControl) {
	mh.flowControl = fc
}

// SetCheckPoint 设置检查点管理器
func (mh *MessageHandler) SetCheckPoint(cp *storage.CheckPoint) {
	mh.checkPoint = cp
}

// SetIndexFile 设置索引文件
func (mh *MessageHandler) SetIndexFile(idx *storage.IndexFile) {
	mh.indexFile = idx
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

	// 设置 CommitLogOffset
	msg.CommitLogOffset = offset

	// 如果是事务消息，存储为Half消息，不立即构建索引
	if msg.MessageType == protocol.MessageTypeTransaction && mh.transactionMgr != nil {
		transactionID, err := mh.transactionMgr.PrepareTransaction(msg, offset)
		if err != nil {
			logger.Warn("Failed to prepare transaction",
				"message_id", msg.MessageID,
				"error", err)
			// 如果准备事务失败，回退到立即构建索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			logger.Info("Transaction message prepared",
				"messageId", msg.MessageID,
				"transactionId", transactionID)
			msg.TransactionID = transactionID
			response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
			response.SetExtField("offset", offset)
			response.SetExtField("message_id", msg.MessageID)
			response.SetExtField("transaction_id", transactionID)
			return response
		}
	}

	// 如果是广播消息，添加到广播管理器
	if (msg.MessageType == protocol.MessageTypeBroadcast || msg.Broadcast) && mh.broadcastManager != nil {
		if err := mh.broadcastManager.AddBroadcastMessage(msg, offset); err != nil {
			logger.Warn("Failed to add broadcast message",
				"message_id", msg.MessageID,
				"error", err)
			// 如果添加到广播管理器失败，回退到普通消息处理
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
			response.SetExtField("offset", offset)
			response.SetExtField("message_id", msg.MessageID)
			return response
		}
	}

	// 如果是延时消息，添加到延时消息调度器，不立即构建索引
	if msg.MessageType == protocol.MessageTypeDelay && mh.delayScheduler != nil {
		if err := mh.delayScheduler.AddDelayMessage(msg, offset); err != nil {
			logger.Warn("Failed to add delay message to scheduler",
				"message_id", msg.MessageID,
				"error", err)
			// 如果添加到调度器失败，回退到立即构建索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			response := protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
			response.SetExtField("offset", offset)
			response.SetExtField("message_id", msg.MessageID)
			return response
		}
	}

	// 普通消息立即构建ConsumeQueue索引
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

	// 流控检查：生产限流
	if mh.flowControl != nil {
		// 从消息属性中获取 producerID，如果没有则使用 BornHost
		producerID := msg.BornHost
		if id, ok := msg.Properties["producer_id"]; ok {
			producerID = id
		}
		if producerID == "" {
			producerID = "unknown"
		}

		allowed, err := mh.flowControl.CheckProducerLimit(producerID, len(msg.Body))
		if !allowed {
			return 0, fmt.Errorf("producer flow control limit exceeded: %v", err)
		}
	}

	// 设置消息元数据
	mh.setMessageMetadata(msg)

	// 如果是顺序消息，根据分片键路由到对应队列
	if msg.MessageType == protocol.MessageTypeOrder && mh.orderMessageMgr != nil {
		// 默认队列数量为4（顺序消息通常使用4个队列）
		queueCount := 4

		queueID, err := mh.orderMessageMgr.RouteOrderMessage(msg, queueCount)
		if err != nil {
			logger.Warn("Failed to route order message",
				"message_id", msg.MessageID,
				"error", err)
			// 如果路由失败，使用默认队列ID 0
			msg.QueueID = 0
		} else {
			msg.QueueID = queueID
		}
	} else if msg.QueueID == 0 {
		// 如果 QueueID 未设置，默认使用 0
		msg.QueueID = 0
	}

	// 存储消息
	offset, err := mh.commitLog.AppendMessage(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to store message: %v", err)
	}

	// 设置 CommitLogOffset（AppendMessage 返回的 offset）
	msg.CommitLogOffset = offset

	// 更新索引文件（从消息的 Keys 属性提取）
	if mh.indexFile != nil && len(msg.Keys) > 0 {
		for _, key := range msg.Keys {
			if err := mh.indexFile.PutIndex(key, offset, msg.StoreTimestamp); err != nil {
				logger.Warn("Failed to update index file",
					"key", key,
					"offset", offset,
					"error", err)
			}
		}
	}

	logger.Info("Message stored in commitlog",
		"messageId", msg.MessageID,
		"topic", msg.Topic,
		"queueId", msg.QueueID,
		"commitLogOffset", offset,
		"messageType", msg.MessageType)

	// 如果是事务消息，存储为Half消息，不立即构建索引
	if msg.MessageType == protocol.MessageTypeTransaction && mh.transactionMgr != nil {
		transactionID, err := mh.transactionMgr.PrepareTransaction(msg, offset)
		if err != nil {
			logger.Warn("Failed to prepare transaction",
				"message_id", msg.MessageID,
				"error", err)
			// 如果准备事务失败，回退到立即构建索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			logger.Info("Transaction message prepared",
				"messageId", msg.MessageID,
				"transactionId", transactionID)
			// 设置事务ID到消息中
			msg.TransactionID = transactionID
			return offset, nil
		}
	}

	// 如果是广播消息，添加到广播管理器
	if (msg.MessageType == protocol.MessageTypeBroadcast || msg.Broadcast) && mh.broadcastManager != nil {
		if err := mh.broadcastManager.AddBroadcastMessage(msg, offset); err != nil {
			logger.Warn("Failed to add broadcast message",
				"message_id", msg.MessageID,
				"error", err)
			// 如果添加到广播管理器失败，回退到普通消息处理
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			logger.Info("Broadcast message added",
				"messageId", msg.MessageID,
				"topic", msg.Topic)
			return offset, nil
		}
	}

	// 如果是定时消息，添加到定时消息调度器，不立即构建索引
	if msg.MessageType == protocol.MessageTypeCron && mh.cronScheduler != nil {
		if err := mh.cronScheduler.AddCronMessage(msg, offset); err != nil {
			logger.Warn("Failed to add cron message to scheduler",
				"message_id", msg.MessageID,
				"error", err)
			// 如果添加到调度器失败，回退到立即构建索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			logger.Info("Cron message added to scheduler",
				"messageId", msg.MessageID,
				"cronExpression", msg.GetProperty("cron_expression"))
			return offset, nil
		}
	}

	// 如果是延时消息，添加到延时消息调度器，不立即构建索引
	if msg.MessageType == protocol.MessageTypeDelay && mh.delayScheduler != nil {
		if err := mh.delayScheduler.AddDelayMessage(msg, offset); err != nil {
			logger.Warn("Failed to add delay message to scheduler",
				"message_id", msg.MessageID,
				"error", err)
			// 如果添加到调度器失败，回退到立即构建索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
		} else {
			logger.Info("Delay message added to scheduler",
				"messageId", msg.MessageID,
				"delayTime", msg.DelayTime)
			return offset, nil
		}
	} else {
		// 普通消息立即构建ConsumeQueue索引
		if err := mh.buildConsumeQueueIndex(msg); err != nil {
			// 记录警告但不影响消息发送成功
			logger.Warn("Failed to build consume queue index",
				"message_id", msg.MessageID,
				"error", err)
		}
	}

	// 流控检查：队列堆积限流（在构建索引后检查）
	if mh.flowControl != nil {
		// 获取队列当前消息数（从 ConsumeQueue 获取）
		cq, err := mh.consumeQueueMgr.GetOrCreateConsumeQueue(msg.Topic, msg.QueueID)
		if err == nil {
			// 获取队列消息数（索引项数 = 消息数）
			queueCount := cq.GetEntryCount()
			allowed, err := mh.flowControl.CheckQueueLimit(msg.Topic, msg.QueueID, queueCount)
			if !allowed {
				logger.Warn("Queue flow control limit exceeded",
					"topic", msg.Topic,
					"queueId", msg.QueueID,
					"messageCount", queueCount,
					"error", err)
				// 队列堆积限流只记录警告，不阻止消息发送
			}
		}
	}

	// 如果是Master，复制消息到Slave
	if mh.replicationMgr != nil {
		if err := mh.replicationMgr.ReplicateMessage(msg, offset); err != nil {
			logger.Warn("Failed to replicate message to slave",
				"message_id", msg.MessageID,
				"error", err)
			// 复制失败不影响消息发送成功（异步复制模式）
		}
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

	// 设置每个消息的 CommitLogOffset
	for i, msg := range messages {
		msg.CommitLogOffset = offsets[i]
	}

	// 处理消息索引：事务消息存储为Half消息，广播消息添加到广播管理器，延时消息添加到调度器，普通消息立即构建索引
	for i, msg := range messages {
		// 事务消息处理
		if msg.MessageType == protocol.MessageTypeTransaction && mh.transactionMgr != nil {
			transactionID, err := mh.transactionMgr.PrepareTransaction(msg, offsets[i])
			if err != nil {
				logger.Warn("Failed to prepare transaction",
					"message_id", msg.MessageID,
					"error", err)
				// 如果准备事务失败，回退到立即构建索引
				if err := mh.buildConsumeQueueIndex(msg); err != nil {
					logger.Warn("Failed to build consume queue index",
						"message_id", msg.MessageID,
						"error", err)
				}
			} else {
				msg.TransactionID = transactionID
				continue
			}
		}

		// 广播消息处理
		if (msg.MessageType == protocol.MessageTypeBroadcast || msg.Broadcast) && mh.broadcastManager != nil {
			if err := mh.broadcastManager.AddBroadcastMessage(msg, offsets[i]); err != nil {
				logger.Warn("Failed to add broadcast message",
					"message_id", msg.MessageID,
					"error", err)
				// 如果添加到广播管理器失败，回退到普通消息处理
				if err := mh.buildConsumeQueueIndex(msg); err != nil {
					logger.Warn("Failed to build consume queue index",
						"message_id", msg.MessageID,
						"error", err)
				}
			} else {
				continue
			}
		}

		// 定时消息处理
		if msg.MessageType == protocol.MessageTypeCron && mh.cronScheduler != nil {
			if err := mh.cronScheduler.AddCronMessage(msg, offsets[i]); err != nil {
				logger.Warn("Failed to add cron message to scheduler",
					"message_id", msg.MessageID,
					"error", err)
				// 如果添加到调度器失败，回退到立即构建索引
				if err := mh.buildConsumeQueueIndex(msg); err != nil {
					logger.Warn("Failed to build consume queue index",
						"message_id", msg.MessageID,
						"error", err)
				}
			} else {
				continue
			}
		}

		// 延时消息处理
		if msg.MessageType == protocol.MessageTypeDelay && mh.delayScheduler != nil {
			if err := mh.delayScheduler.AddDelayMessage(msg, offsets[i]); err != nil {
				logger.Warn("Failed to add delay message to scheduler",
					"message_id", msg.MessageID,
					"error", err)
				// 如果添加到调度器失败，回退到立即构建索引
				if err := mh.buildConsumeQueueIndex(msg); err != nil {
					logger.Warn("Failed to build consume queue index",
						"message_id", msg.MessageID,
						"error", err)
				}
			}
		} else {
			// 普通消息立即构建ConsumeQueue索引
			if err := mh.buildConsumeQueueIndex(msg); err != nil {
				logger.Warn("Failed to build consume queue index",
					"message_id", msg.MessageID,
					"error", err)
			}
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

// CommitTransaction 提交事务
func (mh *MessageHandler) CommitTransaction(transactionID string) error {
	if mh.transactionMgr == nil {
		return fmt.Errorf("transaction manager is not initialized")
	}

	return mh.transactionMgr.CommitTransaction(transactionID)
}

// RollbackTransaction 回滚事务
func (mh *MessageHandler) RollbackTransaction(transactionID string) error {
	if mh.transactionMgr == nil {
		return fmt.Errorf("transaction manager is not initialized")
	}

	return mh.transactionMgr.RollbackTransaction(transactionID)
}

// CheckTransactionStatus 查询事务状态
func (mh *MessageHandler) CheckTransactionStatus(transactionID string) (TransactionStatus, error) {
	if mh.transactionMgr == nil {
		return TransactionStatusUnknown, fmt.Errorf("transaction manager is not initialized")
	}

	return mh.transactionMgr.CheckTransactionStatus(transactionID)
}

// generateMessageID 生成消息ID (简化实现)
func generateMessageID() string {
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}
