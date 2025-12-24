package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "litemq/api/proto"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

// Producer 消息生产者
type Producer struct {
	config          *config.ClientConfig
	nameServerAddrs []string
	currentBroker   string
	brokerClient    pb.BrokerServiceClient
	grpcConn        *grpc.ClientConn
	running         bool
	mutex           sync.RWMutex
	wg              sync.WaitGroup
}

// NewProducer 创建生产者
func NewProducer(cfg *config.ClientConfig) *Producer {
	return &Producer{
		config:          cfg,
		nameServerAddrs: cfg.NameServers,
		running:         false,
	}
}

// Start 启动生产者
func (p *Producer) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.running {
		return fmt.Errorf("producer is already running")
	}

	// 初始化连接
	if err := p.initConnections(); err != nil {
		return fmt.Errorf("failed to initialize connections: %v", err)
	}

	p.running = true
	logger.Info("Producer started", "nameservers", p.nameServerAddrs)

	return nil
}

// Shutdown 关闭生产者
func (p *Producer) Shutdown() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.running {
		return nil
	}

	p.running = false

	// 关闭gRPC连接
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}

	p.wg.Wait()

	logger.Info("Producer shutdown complete")
	return nil
}

// SendMessage 发送普通消息
func (p *Producer) SendMessage(topic string, body []byte) (*SendResult, error) {
	return p.SendMessageWithTags(topic, body, nil)
}

// SendMessageWithTags 发送带标签的消息
func (p *Producer) SendMessageWithTags(topic string, body []byte, tags []string) (*SendResult, error) {
	msg := protocol.NewMessage(topic, body)
	if len(tags) > 0 {
		for _, tag := range tags {
			msg.AddTag(tag)
		}
	}

	return p.send(msg)
}

// SendDelayMessage 发送延时消息
func (p *Producer) SendDelayMessage(topic string, body []byte, delayTime int64) (*SendResult, error) {
	msg := protocol.NewDelayMessage(topic, body, delayTime)
	return p.send(msg)
}

// SendTransactionMessage 发送事务消息
func (p *Producer) SendTransactionMessage(topic string, body []byte, transactionID string) (*SendResult, error) {
	msg := protocol.NewTransactionMessage(topic, body, transactionID)
	result, err := p.send(msg)
	if err != nil {
		return nil, err
	}

	// 事务消息发送成功后，需要等待事务完成
	// 这里简化处理，实际需要更复杂的两阶段提交逻辑

	return result, nil
}

// CommitTransaction 提交事务
func (p *Producer) CommitTransaction(transactionID string) error {
	// 发送事务提交命令
	return p.sendTransactionCommand(protocol.CommandEndTransaction, transactionID, protocol.MessageStatusCommit)
}

// RollbackTransaction 回滚事务
func (p *Producer) RollbackTransaction(transactionID string) error {
	// 发送事务回滚命令
	return p.sendTransactionCommand(protocol.CommandEndTransaction, transactionID, protocol.MessageStatusRollback)
}

// SendBroadcastMessage 发送广播消息
func (p *Producer) SendBroadcastMessage(topic string, body []byte) (*SendResult, error) {
	msg := protocol.NewBroadcastMessage(topic, body)
	return p.send(msg)
}

// SendOrderMessage 发送顺序消息
func (p *Producer) SendOrderMessage(topic string, body []byte, shardingKey string) (*SendResult, error) {
	msg := protocol.NewOrderMessage(topic, body, shardingKey)
	return p.send(msg)
}

// SendResult 发送结果
type SendResult struct {
	MessageID   string `json:"message_id"`
	Offset      int64  `json:"offset"`
	QueueID     int    `json:"queue_id"`
	BrokerAddr  string `json:"broker_addr"`
	SendTime    int64  `json:"send_time"`
	ElapsedTime int64  `json:"elapsed_time"`
}

// send 发送消息的核心逻辑
func (p *Producer) send(msg *protocol.Message) (*SendResult, error) {
	if !p.running {
		return nil, fmt.Errorf("producer is not running")
	}

	startTime := time.Now().UnixMilli()

	// 选择Broker
	brokerAddr, err := p.selectBroker(msg.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to select broker: %v", err)
	}

	// 发送消息到Broker
	result, err := p.sendToBroker(brokerAddr, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to send message to broker %s: %v", brokerAddr, err)
	}

	elapsedTime := time.Now().UnixMilli() - startTime

	sendResult := &SendResult{
		MessageID:   msg.MessageID,
		Offset:      result.Offset,
		QueueID:     msg.QueueID,
		BrokerAddr:  brokerAddr,
		SendTime:    startTime,
		ElapsedTime: elapsedTime,
	}

	return sendResult, nil
}

// selectBroker 选择合适的Broker
func (p *Producer) selectBroker(topic string) (string, error) {
	// 从NameServer获取路由信息
	routeInfo, err := p.fetchRouteInfo(topic)
	if err != nil {
		return "", fmt.Errorf("failed to fetch route info: %v", err)
	}

	if len(routeInfo.BrokerAddrs) == 0 {
		return "", fmt.Errorf("no broker available for topic %s", topic)
	}

	// 简单的负载均衡：轮询选择
	if p.currentBroker == "" {
		p.currentBroker = routeInfo.BrokerAddrs[0]
	} else {
		// 找到当前Broker的下一个
		currentIndex := -1
		for i, addr := range routeInfo.BrokerAddrs {
			if addr == p.currentBroker {
				currentIndex = i
				break
			}
		}

		nextIndex := (currentIndex + 1) % len(routeInfo.BrokerAddrs)
		p.currentBroker = routeInfo.BrokerAddrs[nextIndex]
	}

	return p.currentBroker, nil
}

// fetchRouteInfo 从NameServer获取路由信息
func (p *Producer) fetchRouteInfo(topic string) (*TopicRouteInfo, error) {
	// 选择NameServer
	nsAddr := p.selectNameServer()

	// 发送获取路由信息请求
	// 实际实现需要网络通信
	logger.Info("Fetching route info",
		"topic", topic,
		"nameserver", nsAddr)

	// 模拟返回路由信息
	return &TopicRouteInfo{
		TopicName:   topic,
		BrokerAddrs: []string{"localhost:10911"}, // 简化实现
	}, nil
}

// selectNameServer 选择NameServer
func (p *Producer) selectNameServer() string {
	if len(p.nameServerAddrs) == 0 {
		return ""
	}

	// 简单选择第一个
	return p.nameServerAddrs[0]
}

// sendToBroker 发送消息到Broker
func (p *Producer) sendToBroker(brokerAddr string, msg *protocol.Message) (*SendResponse, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 如果Broker地址改变，重新建立连接
	if p.currentBroker != brokerAddr || p.grpcConn == nil {
		if err := p.connectToBroker(brokerAddr); err != nil {
			return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddr, err)
		}
	}

	// 确保 QueueID 被设置（如果未设置，默认为 0）
	if msg.QueueID == 0 {
		msg.QueueID = 0 // 明确设置为 0
	}

	logger.Info("Sending message to broker",
		"broker", brokerAddr,
		"topic", msg.Topic,
		"messageId", msg.MessageID,
		"queueId", msg.QueueID,
		"bodyLength", len(msg.Body))

	// 转换消息格式
	protoMsg := convertMessageToProto(msg)

	// 发送消息
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := p.brokerClient.SendMessage(ctx, &pb.SendMessageRequest{
		Message: protoMsg,
	})
	if err != nil {
		logger.Warn("Failed to send message to broker",
			"broker", brokerAddr,
			"topic", msg.Topic,
			"error", err)
		return nil, fmt.Errorf("failed to send message: %v", err)
	}

	logger.Info("Message sent successfully",
		"broker", brokerAddr,
		"topic", msg.Topic,
		"messageId", resp.MessageId,
		"offset", resp.Offset,
		"queueId", resp.QueueId)

	return &SendResponse{
		MessageID: resp.MessageId,
		Offset:    resp.Offset,
		QueueID:   int(resp.QueueId),
	}, nil
}

// connectToBroker 连接到Broker
func (p *Producer) connectToBroker(brokerAddr string) error {
	// 关闭现有连接
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}

	// 创建新连接
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	p.grpcConn = conn
	p.brokerClient = pb.NewBrokerServiceClient(conn)
	p.currentBroker = brokerAddr

	logger.Info("Connected to broker", "broker", brokerAddr)
	return nil
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

// sendTransactionCommand 发送事务命令
func (p *Producer) sendTransactionCommand(cmdType protocol.CommandType, transactionID string, status protocol.MessageStatus) error {
	// 实现事务命令发送逻辑
	logger.Info("Sending transaction command",
		"command_type", cmdType,
		"transaction_id", transactionID)
	return nil
}

// initConnections 初始化连接
func (p *Producer) initConnections() error {
	// 初始化到NameServer的连接
	// 实际实现需要建立网络连接
	logger.Info("Initializing connections to nameservers", "nameservers", p.nameServerAddrs)
	return nil
}

// SendResponse 发送响应
type SendResponse struct {
	MessageID string `json:"message_id"`
	Offset    int64  `json:"offset"`
	QueueID   int    `json:"queue_id"`
}

// TopicRouteInfo 主题路由信息
type TopicRouteInfo struct {
	TopicName   string   `json:"topic_name"`
	BrokerAddrs []string `json:"broker_addrs"`
}

// IsRunning 检查生产者是否正在运行
func (p *Producer) IsRunning() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.running
}

// GetStats 获取统计信息
func (p *Producer) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"running":        p.running,
		"current_broker": p.currentBroker,
		"nameservers":    p.nameServerAddrs,
	}
}
