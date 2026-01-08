package broker

import (
	"context"
	"log"

	"litemq/pkg/protocol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "litemq/api/proto"
)

// BrokerGRPCService 实现gRPC Broker服务
type BrokerGRPCService struct {
	pb.UnimplementedBrokerServiceServer
	broker *Broker
}

// NewBrokerGRPCService 创建Broker gRPC服务
func NewBrokerGRPCService(b *Broker) *BrokerGRPCService {
	return &BrokerGRPCService{
		broker: b,
	}
}

// SendMessage 发送消息
func (s *BrokerGRPCService) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	// 将proto消息转换为内部消息格式
	msg := convertProtoToMessage(req.Message)

	// 使用直接的消息发送方法
	offset, err := s.broker.GetMessageHandler().SendMessage(msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message: %v", err)
	}

	response := &pb.SendMessageResponse{
		MessageId: msg.MessageID,
		Offset:    offset,
		QueueId:   int32(msg.QueueID),
	}

	// 如果是事务消息，返回事务ID
	if msg.MessageType == protocol.MessageTypeTransaction {
		response.TransactionId = msg.TransactionID
	}

	return response, nil
}

// SendMessages 批量发送消息
func (s *BrokerGRPCService) SendMessages(ctx context.Context, req *pb.SendMessagesRequest) (*pb.SendMessagesResponse, error) {
	messages := make([]*protocol.Message, len(req.Messages))
	for i, protoMsg := range req.Messages {
		messages[i] = convertProtoToMessage(protoMsg)
	}

	// 使用直接的批量消息发送方法
	offsets, err := s.broker.GetMessageHandler().SendMessages(messages)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send messages: %v", err)
	}

	// 转换响应
	messageIds := make([]string, len(messages))
	queueIds := make([]int32, len(messages))

	for i, msg := range messages {
		messageIds[i] = msg.MessageID
		queueIds[i] = int32(msg.QueueID)
	}

	return &pb.SendMessagesResponse{
		MessageIds: messageIds,
		Offsets:    offsets,
		QueueIds:   queueIds,
	}, nil
}

// PullMessage 拉取消息
func (s *BrokerGRPCService) PullMessage(ctx context.Context, req *pb.PullMessageRequest) (*pb.PullMessageResponse, error) {
	// 使用直接的拉取消息方法
	// consumerID 从请求中获取，如果没有则使用 groupName 作为默认值
	consumerID := req.ConsumerId
	if consumerID == "" {
		consumerID = req.GroupName
	}
	messages, err := s.broker.GetConsumerManager().PullMessages(
		req.GroupName,
		req.Topic,
		int(req.QueueId),
		req.Offset,
		int(req.MaxCount),
		consumerID,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to pull messages: %v", err)
	}

	// 转换消息格式
	protoMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		protoMessages[i] = convertMessageToProto(msg)
	}

	return &pb.PullMessageResponse{
		Messages: protoMessages,
	}, nil
}

// AckMessage 消费确认
func (s *BrokerGRPCService) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*pb.AckMessageResponse, error) {
	// 使用直接的确认消息方法
	err := s.broker.GetConsumerManager().AckMessage(
		req.GroupName,
		req.Topic,
		int(req.QueueId),
		req.Offset,
	)
	if err != nil {
		return &pb.AckMessageResponse{
			Success: false,
		}, status.Errorf(codes.Internal, "failed to ack message: %v", err)
	}

	return &pb.AckMessageResponse{
		Success: true,
	}, nil
}

// RegisterConsumer 注册消费者
func (s *BrokerGRPCService) RegisterConsumer(ctx context.Context, req *pb.RegisterConsumerRequest) (*pb.RegisterConsumerResponse, error) {
	err := s.broker.GetConsumerManager().RegisterConsumer(req.GroupName, req.ConsumerId, req.Topics)
	if err != nil {
		return &pb.RegisterConsumerResponse{
			Success: false,
		}, status.Errorf(codes.Internal, "failed to register consumer: %v", err)
	}

	return &pb.RegisterConsumerResponse{
		Success: true,
	}, nil
}

// Heartbeat 心跳
func (s *BrokerGRPCService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.broker.GetConsumerManager().UpdateConsumerHeartbeat(req.GroupName, req.ConsumerId)
	if err != nil {
		log.Printf("Heartbeat failed for consumer %s in group %s: %v", req.ConsumerId, req.GroupName, err)
		return &pb.HeartbeatResponse{
			Success: false,
		}, status.Errorf(codes.Internal, "heartbeat failed: %v", err)
	}

	return &pb.HeartbeatResponse{
		Success: true,
	}, nil
}

// CommitTransaction 提交事务
func (s *BrokerGRPCService) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	if s.broker.GetTransactionManager() == nil {
		return nil, status.Errorf(codes.Internal, "transaction manager not initialized")
	}

	err := s.broker.GetTransactionManager().CommitTransaction(req.GetTransactionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
	}

	return &pb.CommitTransactionResponse{
		Success: true,
	}, nil
}

// RollbackTransaction 回滚事务
func (s *BrokerGRPCService) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {
	if s.broker.GetTransactionManager() == nil {
		return nil, status.Errorf(codes.Internal, "transaction manager not initialized")
	}

	err := s.broker.GetTransactionManager().RollbackTransaction(req.GetTransactionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
	}

	return &pb.RollbackTransactionResponse{
		Success: true,
	}, nil
}

// CheckTransactionStatus 查询事务状态
func (s *BrokerGRPCService) CheckTransactionStatus(ctx context.Context, req *pb.CheckTransactionStatusRequest) (*pb.CheckTransactionStatusResponse, error) {
	if s.broker.GetTransactionManager() == nil {
		return nil, status.Errorf(codes.Internal, "transaction manager not initialized")
	}

	txStatus, err := s.broker.GetTransactionManager().CheckTransactionStatus(req.GetTransactionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check transaction status: %v", err)
	}

	return &pb.CheckTransactionStatusResponse{
		Status: int32(txStatus),
	}, nil
}

// convertProtoToMessage 将proto消息转换为内部消息格式
func convertProtoToMessage(protoMsg *pb.Message) *protocol.Message {
	msg := protocol.NewMessage(protoMsg.Topic, protoMsg.Body)

	msg.MessageID = protoMsg.MessageId
	msg.Tags = protoMsg.Tags
	msg.Keys = protoMsg.Keys
	msg.Properties = protoMsg.Properties
	msg.MessageType = protocol.MessageType(protoMsg.MessageType)
	msg.Priority = int(protoMsg.Priority)
	msg.Reliability = int(protoMsg.Reliability)
	msg.DelayTime = protoMsg.DelayTime
	msg.TransactionID = protoMsg.TransactionId
	msg.MessageStatus = protocol.MessageStatus(protoMsg.MessageStatus)
	msg.ShardingKey = protoMsg.ShardingKey
	msg.Broadcast = protoMsg.Broadcast
	msg.BornTimestamp = protoMsg.BornTimestamp
	msg.BornHost = protoMsg.BornHost
	msg.QueueID = int(protoMsg.QueueId)

	return msg
}

// convertMessageToProto 将内部消息转换为proto格式
func convertMessageToProto(msg *protocol.Message) *pb.Message {
	return &pb.Message{
		MessageId:        msg.MessageID,
		Topic:            msg.Topic,
		Tags:             msg.Tags,
		Keys:             msg.Keys,
		Properties:       msg.Properties,
		Body:             msg.Body,
		MessageType:      pb.MessageType(msg.MessageType),
		Priority:         int32(msg.Priority),
		Reliability:      int32(msg.Reliability),
		DelayTime:        msg.DelayTime,
		TransactionId:    msg.TransactionID,
		MessageStatus:    pb.MessageStatus(msg.MessageStatus),
		ShardingKey:      msg.ShardingKey,
		Broadcast:        msg.Broadcast,
		BornTimestamp:    msg.BornTimestamp,
		BornHost:         msg.BornHost,
		StoreHost:        msg.StoreHost,
		QueueId:          int32(msg.QueueID),
		QueueOffset:      msg.QueueOffset,
		CommitLogOffset:  msg.CommitLogOffset,
		StoreSize:        int32(msg.StoreSize),
		StoreTimestamp:   msg.StoreTimestamp,
		ConsumeStartTime: msg.ConsumeStartTime,
		ConsumeEndTime:   msg.ConsumeEndTime,
		ConsumeCount:     int32(msg.ConsumeCount),
	}
}
