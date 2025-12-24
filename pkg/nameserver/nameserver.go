package nameserver

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "litemq/api/proto"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// BrokerInfo Broker信息
type BrokerInfo struct {
	BrokerID    string            `json:"broker_id"`
	BrokerName  string            `json:"broker_name"`
	BrokerAddr  string            `json:"broker_addr"`
	ClusterName string            `json:"cluster_name"`
	Role        config.Role       `json:"role"`
	LastUpdate  time.Time         `json:"last_update"`
	Topics      map[string][]int  `json:"topics"`     // topic -> queue ids
	Properties  map[string]string `json:"properties"` // 扩展属性
}

// TopicRouteInfo 主题路由信息
type TopicRouteInfo struct {
	TopicName     string   `json:"topic_name"`
	QueueCount    int      `json:"queue_count"`
	BrokerAddrs   []string `json:"broker_addrs"` // Broker地址列表
	ReadQueueIds  []int    `json:"read_queue_ids"`
	WriteQueueIds []int    `json:"write_queue_ids"`
}

// NameServer NameServer核心
type NameServer struct {
	config           *config.NameServerConfig
	routeInfoManager *RouteInfoManager
	heartbeatManager *HeartbeatManager

	// gRPC服务
	grpcServer   *grpc.Server
	grpcListener net.Listener

	// 网络服务
	listener net.Listener
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mutex sync.RWMutex
}

// NewNameServer 创建NameServer
func NewNameServer(cfg *config.NameServerConfig) *NameServer {
	ns := &NameServer{
		config:           cfg,
		routeInfoManager: NewRouteInfoManager(),
		heartbeatManager: NewHeartbeatManager(cfg.HeartbeatTimeout),
		stopCh:           make(chan struct{}),
		running:          false,
	}

	// 设置心跳管理器的回调
	ns.heartbeatManager.SetBrokerOfflineCallback(ns.onBrokerOffline)

	return ns
}

// Start 启动NameServer
func (ns *NameServer) Start() error {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	if ns.running {
		return fmt.Errorf("nameserver is already running")
	}

	// 启动心跳管理器
	if err := ns.heartbeatManager.Start(); err != nil {
		return fmt.Errorf("failed to start heartbeat manager: %v", err)
	}

	// 启动网络服务
	if err := ns.startNetworkService(); err != nil {
		ns.heartbeatManager.Stop()
		return fmt.Errorf("failed to start network service: %v", err)
	}

	ns.running = true
	logger.Info("NameServer started successfully",
		"host", ns.config.Host,
		"port", ns.config.Port)

	return nil
}

// Shutdown 关闭NameServer
func (ns *NameServer) Shutdown() error {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	if !ns.running {
		return nil
	}

	ns.running = false
	close(ns.stopCh)

	// 停止gRPC服务器
	if ns.grpcServer != nil {
		ns.grpcServer.GracefulStop()
	}

	// 停止网络服务
	if ns.listener != nil {
		ns.listener.Close()
	}

	// 停止心跳管理器
	ns.heartbeatManager.Stop()

	// 等待所有goroutine结束
	ns.wg.Wait()

	logger.Info("NameServer shutdown complete")
	return nil
}

// RegisterBroker 注册Broker (实现NameServerInterface)
func (ns *NameServer) RegisterBroker(brokerInfo interface{}) error {
	if info, ok := brokerInfo.(map[string]interface{}); ok {
		broker := &BrokerInfo{
			BrokerID:    info["broker_id"].(string),
			BrokerName:  info["broker_name"].(string),
			BrokerAddr:  info["broker_addr"].(string),
			ClusterName: info["cluster_name"].(string),
			Role:        config.Role(info["role"].(string)),
			LastUpdate:  time.Now(),
			Topics:      make(map[string][]int),
		}

		// 转换topics
		if topics, ok := info["topics"].(map[string]interface{}); ok {
			for topic, queueIds := range topics {
				if ids, ok := queueIds.([]interface{}); ok {
					queueIdsList := make([]int, len(ids))
					for i, id := range ids {
						queueIdsList[i] = int(id.(float64)) // JSON unmarshal gives float64
					}
					broker.Topics[topic] = queueIdsList
				}
			}
		}

		ns.routeInfoManager.RegisterBroker(broker)
		ns.heartbeatManager.UpdateHeartbeat(broker.BrokerID)
		return nil
	}
	return fmt.Errorf("invalid broker info format")
}

// UnregisterBroker 注销Broker (实现NameServerInterface)
func (ns *NameServer) UnregisterBroker(brokerId string) error {
	ns.routeInfoManager.UnregisterBroker(brokerId)
	ns.heartbeatManager.UnregisterBroker(brokerId)
	return nil
}

// GetTopicRouteInfo 获取主题路由信息 (实现NameServerInterface)
func (ns *NameServer) GetTopicRouteInfo(topic string) (interface{}, error) {
	routeInfo, err := ns.routeInfoManager.GetTopicRouteInfo(topic)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"topic_name":   routeInfo.TopicName,
		"queue_count":  int32(routeInfo.QueueCount),
		"broker_addrs": routeInfo.BrokerAddrs,
	}, nil
}

// GetAllBrokerInfos 获取所有Broker信息 (实现NameServerInterface)
func (ns *NameServer) GetAllBrokerInfos() []interface{} {
	brokers := ns.routeInfoManager.GetAllBrokerInfos()
	result := make([]interface{}, len(brokers))
	for i, broker := range brokers {
		topics := make(map[string]interface{})
		for topic, queueIds := range broker.Topics {
			queueIdsList := make([]interface{}, len(queueIds))
			for j, id := range queueIds {
				queueIdsList[j] = int32(id)
			}
			topics[topic] = queueIdsList
		}

		result[i] = map[string]interface{}{
			"broker_id":    broker.BrokerID,
			"broker_name":  broker.BrokerName,
			"broker_addr":  broker.BrokerAddr,
			"cluster_name": broker.ClusterName,
			"role":         string(broker.Role),
			"last_update":  broker.LastUpdate.UnixMilli(),
			"topics":       topics,
		}
	}
	return result
}

// UpdateBrokerHeartbeat 更新Broker心跳 (实现NameServerInterface)
func (ns *NameServer) UpdateBrokerHeartbeat(brokerId string) {
	ns.heartbeatManager.UpdateHeartbeat(brokerId)
}

// startNetworkService 启动网络服务
func (ns *NameServer) startNetworkService() error {
	// 创建gRPC服务器
	ns.grpcServer = grpc.NewServer()

	// 创建NameServer gRPC服务实例
	nameServerService := &nameServerGRPCService{nameserver: ns}

	// 注册gRPC服务
	pb.RegisterNameServerServiceServer(ns.grpcServer, nameServerService)

	// 启动gRPC监听
	addr := fmt.Sprintf("%s:%d", ns.config.Host, ns.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	ns.grpcListener = listener

	// 启动gRPC服务器
	ns.wg.Add(1)
	go func() {
		defer ns.wg.Done()

		// 在服务器启动前启用反射
		if ns.grpcServer != nil {
			reflection.Register(ns.grpcServer)
		}

		logger.Info("NameServer gRPC server starting", "addr", addr)

		err := ns.grpcServer.Serve(listener)

		// 检查是否是正常关闭
		if err == grpc.ErrServerStopped {
			logger.Info("NameServer gRPC server stopped gracefully")
			return
		}

		// 检查运行状态，避免竞态条件
		ns.mutex.RLock()
		isRunning := ns.running
		ns.mutex.RUnlock()

		if !isRunning {
			logger.Info("NameServer gRPC server stopped (not running)")
			return
		}

		// 真正的错误
		logger.Error("NameServer gRPC server error", "error", err)
	}()

	return nil
}

// acceptLoop 接受连接循环
func (ns *NameServer) acceptLoop() {
	defer ns.wg.Done()

	for {
		select {
		case <-ns.stopCh:
			return
		default:
			conn, err := ns.listener.Accept()
			if err != nil {
				if !ns.running {
					return // 正常关闭
				}
				logger.Warn("Accept connection error", "error", err)
				continue
			}

			// 处理连接
			ns.wg.Add(1)
			go ns.handleConnection(conn)
		}
	}
}

// handleConnection 处理连接
func (ns *NameServer) handleConnection(conn net.Conn) {
	defer ns.wg.Done()
	defer conn.Close()

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		select {
		case <-ns.stopCh:
			return
		default:
			// 读取命令
			cmd, err := ns.readCommand(conn)
			if err != nil {
				if !ns.isTemporaryError(err) {
					logger.Warn("Read command error", "error", err)
				}
				return
			}

			// 处理命令
			response := ns.handleCommand(cmd)

			// 发送响应
			if err := ns.writeCommand(conn, response); err != nil {
				logger.Warn("Write response error", "error", err)
				return
			}

			// 如果是单次请求，关闭连接
			if !ns.isPersistentCommand(cmd) {
				return
			}
		}
	}
}

// readCommand 读取命令
func (ns *NameServer) readCommand(conn net.Conn) (*protocol.Command, error) {
	// 简单实现，实际应该处理网络协议
	// 这里假设使用某种序列化协议
	return nil, fmt.Errorf("not implemented")
}

// writeCommand 写入命令
func (ns *NameServer) writeCommand(conn net.Conn, cmd *protocol.Command) error {
	// 简单实现，实际应该处理网络协议
	return fmt.Errorf("not implemented")
}

// handleCommand 处理命令
func (ns *NameServer) handleCommand(cmd *protocol.Command) *protocol.Command {
	switch cmd.CommandType {
	case protocol.CommandRegisterBroker:
		return ns.handleRegisterBroker(cmd)
	case protocol.CommandHeartbeat:
		return ns.handleHeartbeat(cmd)
	case protocol.CommandGetRouteInfo:
		return ns.handleGetRouteInfo(cmd)
	case protocol.CommandGetNameServerList:
		return ns.handleGetNameServerList(cmd)
	default:
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseUnknownCommand,
			fmt.Sprintf("unknown command: %d", cmd.CommandType))
	}
}

// handleRegisterBroker 处理Broker注册
func (ns *NameServer) handleRegisterBroker(cmd *protocol.Command) *protocol.Command {
	// 解析Broker信息
	brokerInfo := &BrokerInfo{}
	// 从命令体解析Broker信息...

	// 注册Broker
	if err := ns.routeInfoManager.RegisterBroker(brokerInfo); err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseSystemError,
			fmt.Sprintf("register broker failed: %v", err))
	}

	return protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
}

// handleHeartbeat 处理心跳
func (ns *NameServer) handleHeartbeat(cmd *protocol.Command) *protocol.Command {
	// 解析心跳信息
	var brokerID string
	// 从命令体解析brokerID...

	// 更新心跳
	ns.heartbeatManager.UpdateHeartbeat(brokerID)

	return protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
}

// handleGetRouteInfo 处理获取路由信息
func (ns *NameServer) handleGetRouteInfo(cmd *protocol.Command) *protocol.Command {
	// 解析主题名
	var topicName string
	// 从命令体解析topicName...

	// 获取路由信息
	routeInfo, err := ns.routeInfoManager.GetTopicRouteInfo(topicName)
	if err != nil {
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseTopicNotExist,
			fmt.Sprintf("topic not found: %s", topicName))
	}

	// 序列化路由信息到响应体
	_ = routeInfo // 临时使用，实际需要序列化

	return protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
}

// handleGetNameServerList 处理获取NameServer列表
func (ns *NameServer) handleGetNameServerList(cmd *protocol.Command) *protocol.Command {
	// 获取所有NameServer地址
	nameServers := []string{
		fmt.Sprintf("%s:%d", ns.config.Host, ns.config.Port),
		// 实际应该从集群配置获取所有NameServer地址
	}

	// 序列化NameServer列表到响应体
	_ = nameServers // 临时使用，实际需要序列化

	return protocol.NewResponse(cmd.RequestID, protocol.ResponseSuccess, "")
}

// onBrokerOffline Broker离线回调
func (ns *NameServer) onBrokerOffline(brokerID string) {
	logger.Warn("Broker offline", "broker_id", brokerID)

	// 从路由信息中移除Broker
	ns.routeInfoManager.UnregisterBroker(brokerID)
}

// isTemporaryError 判断是否为临时错误
func (ns *NameServer) isTemporaryError(err error) bool {
	// 判断网络超时等临时错误
	return false
}

// isPersistentCommand 判断是否为持久连接命令
func (ns *NameServer) isPersistentCommand(cmd *protocol.Command) bool {
	// 心跳命令需要保持连接
	return cmd.CommandType == protocol.CommandHeartbeat
}

// GetBrokerInfo 获取Broker信息
func (ns *NameServer) GetBrokerInfo(brokerID string) (*BrokerInfo, bool) {
	return ns.routeInfoManager.GetBrokerInfo(brokerID)
}

// GetAllTopicRouteInfos 获取所有主题路由信息
func (ns *NameServer) GetAllTopicRouteInfos() []*TopicRouteInfo {
	return ns.routeInfoManager.GetAllTopicRouteInfos()
}

// GetBrokerCount 获取Broker数量
func (ns *NameServer) GetBrokerCount() int {
	return ns.routeInfoManager.GetBrokerCount()
}

// GetTopicCount 获取主题数量
func (ns *NameServer) GetTopicCount() int {
	return ns.routeInfoManager.GetTopicCount()
}

// IsRunning 检查NameServer是否正在运行
func (ns *NameServer) IsRunning() bool {
	ns.mutex.RLock()
	defer ns.mutex.RUnlock()
	return ns.running
}

// nameServerGRPCService 内嵌的gRPC服务实现
type nameServerGRPCService struct {
	pb.UnimplementedNameServerServiceServer
	nameserver *NameServer
}

// RegisterBroker 注册Broker
func (s *nameServerGRPCService) RegisterBroker(ctx context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	brokerInfo := map[string]interface{}{
		"broker_id":    req.BrokerId,
		"broker_name":  req.BrokerName,
		"broker_addr":  req.BrokerAddr,
		"cluster_name": req.ClusterName,
		"role":         req.Role,
		"topics":       convertProtoTopicsToMap(req.Topics),
	}

	err := s.nameserver.RegisterBroker(brokerInfo)
	if err != nil {
		return &pb.RegisterBrokerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.RegisterBrokerResponse{
		Success: true,
	}, nil
}

// UnregisterBroker 注销Broker
func (s *nameServerGRPCService) UnregisterBroker(ctx context.Context, req *pb.UnregisterBrokerRequest) (*pb.UnregisterBrokerResponse, error) {
	err := s.nameserver.UnregisterBroker(req.BrokerId)
	if err != nil {
		return &pb.UnregisterBrokerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.UnregisterBrokerResponse{
		Success: true,
	}, nil
}

// GetRouteInfo 获取路由信息
func (s *nameServerGRPCService) GetRouteInfo(ctx context.Context, req *pb.GetRouteInfoRequest) (*pb.GetRouteInfoResponse, error) {
	routeInfo, err := s.nameserver.GetTopicRouteInfo(req.Topic)
	if err != nil {
		return &pb.GetRouteInfoResponse{
			Error: err.Error(),
		}, nil
	}

	protoRouteInfo := convertRouteInfoToProto(routeInfo)
	if protoRouteInfo == nil {
		return &pb.GetRouteInfoResponse{
			Error: "invalid route info format",
		}, nil
	}

	return &pb.GetRouteInfoResponse{
		RouteInfo: protoRouteInfo,
	}, nil
}

// GetAllBrokers 获取所有Broker
func (s *nameServerGRPCService) GetAllBrokers(ctx context.Context, req *pb.GetAllBrokersRequest) (*pb.GetAllBrokersResponse, error) {
	brokers := s.nameserver.GetAllBrokerInfos()
	protoBrokers := make([]*pb.BrokerInfo, len(brokers))
	for i, broker := range brokers {
		protoBrokers[i] = convertBrokerInfoToProto(broker)
		if protoBrokers[i] == nil {
			continue
		}
	}

	return &pb.GetAllBrokersResponse{
		Brokers: protoBrokers,
	}, nil
}

// Heartbeat 心跳
func (s *nameServerGRPCService) Heartbeat(ctx context.Context, req *pb.BrokerHeartbeatRequest) (*pb.BrokerHeartbeatResponse, error) {
	// 防御性检查
	if s == nil {
		return &pb.BrokerHeartbeatResponse{
			Success: false,
			Error:   "gRPC service not initialized",
		}, nil
	}

	if s.nameserver == nil {
		return &pb.BrokerHeartbeatResponse{
			Success: false,
			Error:   "nameserver service not initialized",
		}, nil
	}

	if req == nil {
		return &pb.BrokerHeartbeatResponse{
			Success: false,
			Error:   "invalid heartbeat request: request is nil",
		}, nil
	}

	// 获取broker ID（安全访问）
	var brokerID string
	if req != nil {
		brokerID = req.BrokerId
	}

	if brokerID == "" {
		return &pb.BrokerHeartbeatResponse{
			Success: false,
			Error:   "invalid heartbeat request: broker_id is required",
		}, nil
	}

	// 更新心跳（添加错误处理）
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("Heartbeat panic recovered", "panic", r)
		}
	}()

	s.nameserver.UpdateBrokerHeartbeat(brokerID)

	return &pb.BrokerHeartbeatResponse{
		Success: true,
	}, nil
}

// convertProtoTopicsToMap 将proto topics转换为map
func convertProtoTopicsToMap(protoTopics map[string]*pb.QueueIds) map[string]interface{} {
	result := make(map[string]interface{})
	for topic, queueIds := range protoTopics {
		queueIdsList := make([]interface{}, len(queueIds.QueueIds))
		for i, id := range queueIds.QueueIds {
			queueIdsList[i] = id
		}
		result[topic] = queueIdsList
	}
	return result
}

// convertRouteInfoToProto 将路由信息转换为proto格式
func convertRouteInfoToProto(routeInfo interface{}) *pb.TopicRouteInfo {
	if info, ok := routeInfo.(map[string]interface{}); ok {
		return &pb.TopicRouteInfo{
			TopicName:   info["topic_name"].(string),
			QueueCount:  info["queue_count"].(int32),
			BrokerAddrs: info["broker_addrs"].([]string),
		}
	}
	return nil
}

// convertBrokerInfoToProto 将broker信息转换为proto格式
func convertBrokerInfoToProto(brokerInfo interface{}) *pb.BrokerInfo {
	if info, ok := brokerInfo.(map[string]interface{}); ok {
		topics := make(map[string]*pb.QueueIds)
		if topicsMap, ok := info["topics"].(map[string]interface{}); ok {
			for topic, queueIds := range topicsMap {
				if ids, ok := queueIds.([]interface{}); ok {
					queueIdsList := make([]int32, len(ids))
					for i, id := range ids {
						queueIdsList[i] = id.(int32)
					}
					topics[topic] = &pb.QueueIds{QueueIds: queueIdsList}
				}
			}
		}

		return &pb.BrokerInfo{
			BrokerId:    info["broker_id"].(string),
			BrokerName:  info["broker_name"].(string),
			BrokerAddr:  info["broker_addr"].(string),
			ClusterName: info["cluster_name"].(string),
			Role:        info["role"].(string),
			LastUpdate:  info["last_update"].(int64),
			Topics:      topics,
		}
	}
	return nil
}
