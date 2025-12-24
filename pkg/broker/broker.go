package broker

import (
	"fmt"
	"net"
	"sync"
	"time"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/monitoring"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
	"litemq/web"

	pb "litemq/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Broker MQ Broker核心
type Broker struct {
	config *config.BrokerConfig

	// 核心组件
	commitLog        *storage.CommitLog
	consumeQueueMgr  *storage.ConsumeQueueManager
	flushService     *storage.FlushService
	messageHandler   *MessageHandler
	consumerMgr      *ConsumerManager
	nameServerClient *NameServerClient
	heartbeatService *HeartbeatService

	// Web和监控
	webServer *web.WebServer
	metrics   *monitoring.MetricsCollector

	// gRPC服务
	grpcServer    *grpc.Server
	grpcListener  net.Listener
	brokerService *BrokerGRPCService

	// 网络服务
	listener net.Listener
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mutex sync.RWMutex
}

// NewBroker 创建新的Broker
func NewBroker(cfg *config.BrokerConfig) (*Broker, error) {
	// 初始化存储引擎
	commitLog, err := storage.NewCommitLog(cfg.DataPath+"/commitlog", cfg.CommitLog.FileSize, cfg.CommitLog.MaxFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to create commit log: %v", err)
	}

	consumeQueueMgr := storage.NewConsumeQueueManager(cfg.DataPath+"/consumequeue", cfg.ConsumeQueue.FileSize, cfg.ConsumeQueue.MaxFiles)

	// 初始化刷新服务
	flushMode := storage.FlushModeAsync
	if cfg.FlushMode == config.FlushModeSync {
		flushMode = storage.FlushModeSync
	}
	flushService := storage.NewFlushService("broker-flush", flushMode,
		cfg.FlushInterval, commitLog, consumeQueueMgr)

	// 初始化消息处理器
	messageHandler := NewMessageHandler(commitLog, consumeQueueMgr, cfg)

	// 初始化消费者管理器
	consumerMgr := NewConsumerManager(commitLog, consumeQueueMgr, cfg)

	// 初始化NameServer客户端
	nameServerClient := NewNameServerClient(cfg.NameServers, cfg.BrokerID)

	// 初始化心跳服务
	heartbeatService := NewHeartbeatService(nameServerClient, cfg.Heartbeat.Interval)

	// 创建Broker实例（先创建实例，再初始化gRPC服务）
	broker := &Broker{
		config:           cfg,
		commitLog:        commitLog,
		consumeQueueMgr:  consumeQueueMgr,
		flushService:     flushService,
		messageHandler:   messageHandler,
		consumerMgr:      consumerMgr,
		nameServerClient: nameServerClient,
		heartbeatService: heartbeatService,
		stopCh:           make(chan struct{}),
		running:          false,
	}

	// 初始化gRPC服务
	grpcServer := grpc.NewServer()
	brokerService := NewBrokerGRPCService(broker)

	// 注册gRPC服务
	pb.RegisterBrokerServiceServer(grpcServer, brokerService)

	// 启用反射（可选，用于调试）
	reflection.Register(grpcServer)

	// 设置gRPC相关字段
	broker.grpcServer = grpcServer
	broker.brokerService = brokerService

	// 初始化Web服务器和监控
	if err := broker.initWebAndMonitoring(); err != nil {
		return nil, fmt.Errorf("failed to initialize web and monitoring: %v", err)
	}

	return broker, nil
}

// Start 启动Broker
func (b *Broker) Start() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.running {
		return fmt.Errorf("broker is already running")
	}

	logger.Info("Starting Broker...", "broker_id", b.config.BrokerID)

	// 启动刷新服务
	if err := b.flushService.Start(); err != nil {
		return fmt.Errorf("failed to start flush service: %v", err)
	}

	// 启动消费者管理器
	if err := b.consumerMgr.Start(); err != nil {
		b.flushService.Stop()
		return fmt.Errorf("failed to start consumer manager: %v", err)
	}

	// 注册到NameServer
	if err := b.registerToNameServer(); err != nil {
		b.consumerMgr.Stop()
		b.flushService.Stop()
		return fmt.Errorf("failed to register to nameserver: %v", err)
	}

	// 启动心跳服务
	if err := b.heartbeatService.Start(); err != nil {
		b.consumerMgr.Stop()
		b.flushService.Stop()
		return fmt.Errorf("failed to start heartbeat service: %v", err)
	}

	// 启动网络服务
	if err := b.startNetworkService(); err != nil {
		b.heartbeatService.Stop()
		b.consumerMgr.Stop()
		b.flushService.Stop()
		return fmt.Errorf("failed to start network service: %v", err)
	}

	b.running = true
	logger.Info("Broker started successfully",
		"broker_id", b.config.BrokerID,
		"host", b.config.Host,
		"port", b.config.Port)

	return nil
}

// Shutdown 关闭Broker
func (b *Broker) Shutdown() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if !b.running {
		return nil
	}

	logger.Info("Shutting down Broker...", "broker_id", b.config.BrokerID)
	b.running = false
	close(b.stopCh)

	// 停止gRPC服务器
	b.stopGRPCServer()

	// 停止网络服务
	if b.listener != nil {
		b.listener.Close()
	}

	// 停止心跳服务
	b.heartbeatService.Stop()

	// 停止消费者管理器
	b.consumerMgr.Stop()

	// 停止刷新服务
	b.flushService.Stop()

	// 等待所有goroutine结束
	b.wg.Wait()

	// 关闭存储引擎
	if err := b.commitLog.Close(); err != nil {
		logger.Warn("Failed to close commit log", "error", err)
	}

	if err := b.consumeQueueMgr.Close(); err != nil {
		logger.Warn("Failed to close consume queue manager", "error", err)
	}

	logger.Info("Broker shutdown complete", "broker_id", b.config.BrokerID)
	return nil
}

// startNetworkService 启动网络服务
func (b *Broker) startNetworkService() error {
	// 启动gRPC服务器
	return b.startGRPCServer()
}

// startGRPCServer 启动gRPC服务器
func (b *Broker) startGRPCServer() error {
	// 监听地址
	addr := fmt.Sprintf("%s:%d", b.config.Host, b.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}
	b.grpcListener = listener

	// 启动gRPC服务器
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		logger.Info("Broker gRPC server listening", "addr", addr)
		if err := b.grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	logger.Info("Broker gRPC server started", "addr", addr)
	return nil
}

// stopGRPCServer 停止gRPC服务器
func (b *Broker) stopGRPCServer() {
	if b.grpcServer != nil {
		logger.Info("Stopping gRPC server...")
		b.grpcServer.GracefulStop()
	}

	if b.grpcListener != nil {
		b.grpcListener.Close()
	}
}

// acceptLoop 接受连接循环
func (b *Broker) acceptLoop() {
	defer b.wg.Done()

	for {
		select {
		case <-b.stopCh:
			return
		default:
			conn, err := b.listener.Accept()
			if err != nil {
				if !b.running {
					return // 正常关闭
				}
				logger.Warn("Accept connection error", "error", err)
				continue
			}

			// 处理连接
			b.wg.Add(1)
			go b.handleConnection(conn)
		}
	}
}

// handleConnection 处理连接
func (b *Broker) handleConnection(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()

	// 设置连接参数
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	clientType := "unknown"
	defer func() {
		logger.Info("Connection closed",
			"client_type", clientType,
			"remote_addr", conn.RemoteAddr())
	}()

	for {
		select {
		case <-b.stopCh:
			return
		default:
			// 读取命令
			cmd, err := b.readCommand(conn)
			if err != nil {
				if !b.isTemporaryError(err) {
					logger.Warn("Read command error",
						"remote_addr", conn.RemoteAddr(),
						"error", err)
				}
				return
			}

			// 处理命令
			response := b.handleCommand(cmd, &clientType)

			// 发送响应
			if err := b.writeCommand(conn, response); err != nil {
				logger.Warn("Write response error",
					"remote_addr", conn.RemoteAddr(),
					"error", err)
				return
			}
		}
	}
}

// readCommand 读取命令
func (b *Broker) readCommand(conn net.Conn) (*protocol.Command, error) {
	// 简单实现，实际应该处理网络协议
	return nil, fmt.Errorf("not implemented")
}

// writeCommand 写入命令
func (b *Broker) writeCommand(conn net.Conn, cmd *protocol.Command) error {
	// 简单实现，实际应该处理网络协议
	return fmt.Errorf("not implemented")
}

// handleCommand 处理命令
func (b *Broker) handleCommand(cmd *protocol.Command, clientType *string) *protocol.Command {
	switch cmd.CommandType {
	case protocol.CommandSendMessage:
		*clientType = "producer"
		return b.messageHandler.HandleSendMessage(cmd)
	case protocol.CommandPullMessage:
		*clientType = "consumer"
		return b.consumerMgr.HandlePullMessage(cmd)
	case protocol.CommandConsumeAck:
		*clientType = "consumer"
		return b.consumerMgr.HandleConsumeAck(cmd)
	default:
		return protocol.NewResponse(cmd.RequestID, protocol.ResponseUnknownCommand,
			fmt.Sprintf("unknown command: %d", cmd.CommandType))
	}
}

// registerToNameServer 注册到NameServer
func (b *Broker) registerToNameServer() error {
	brokerInfo := map[string]interface{}{
		"broker_id":    b.config.BrokerID,
		"broker_name":  b.config.BrokerName,
		"broker_addr":  fmt.Sprintf("%s:%d", b.config.Host, b.config.Port),
		"cluster_name": "default", // TODO: 从配置获取
		"role":         b.config.Role,
		"topics":       make(map[string][]int), // 暂时为空
	}

	return b.nameServerClient.RegisterBroker(brokerInfo)
}

// isTemporaryError 判断是否为临时错误
func (b *Broker) isTemporaryError(err error) bool {
	// 判断网络超时等临时错误
	return false
}

// GetBrokerInfo 获取Broker信息
func (b *Broker) GetBrokerInfo() map[string]interface{} {
	return map[string]interface{}{
		"broker_id":   b.config.BrokerID,
		"broker_name": b.config.BrokerName,
		"address":     fmt.Sprintf("%s:%d", b.config.Host, b.config.Port),
		"role":        b.config.Role,
		"status":      b.getStatus(),
	}
}

// GetStats 获取统计信息
func (b *Broker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"broker_id":        b.config.BrokerID,
		"total_messages":   b.commitLog.GetTotalMessages(),
		"total_bytes":      b.commitLog.GetTotalBytes(),
		"wrote_offset":     b.commitLog.GetWroteOffset(),
		"committed_offset": b.commitLog.GetCommittedOffset(),
		"flushed_offset":   b.commitLog.GetFlushedOffset(),
		"consumers":        b.consumerMgr.GetConsumerCount(),
		"topics":           b.consumerMgr.GetTopicCount(),
	}
}

// getStatus 获取Broker状态
func (b *Broker) getStatus() string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.running {
		return "running"
	}
	return "stopped"
}

// IsRunning 检查Broker是否正在运行
func (b *Broker) IsRunning() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.running
}

// GetConfig 获取配置
func (b *Broker) GetConfig() *config.BrokerConfig {
	return b.config
}

// ForceFlush 强制刷新
func (b *Broker) ForceFlush() error {
	return b.flushService.Flush()
}

// GetCommitLog 获取CommitLog
func (b *Broker) GetCommitLog() *storage.CommitLog {
	return b.commitLog
}

// GetConsumeQueueManager 获取ConsumeQueue管理器
func (b *Broker) GetConsumeQueueManager() *storage.ConsumeQueueManager {
	return b.consumeQueueMgr
}

// GetMessageHandler 获取消息处理器
func (b *Broker) GetMessageHandler() *MessageHandler {
	return b.messageHandler
}

// GetConsumerManager 获取消费者管理器
func (b *Broker) GetConsumerManager() *ConsumerManager {
	return b.consumerMgr
}

// initWebAndMonitoring 初始化Web服务器和监控
func (b *Broker) initWebAndMonitoring() error {
	// 初始化监控收集器
	b.metrics = monitoring.NewMetricsCollector()

	// 初始化Web服务器，传入MetricsCollector
	brokerAddr := fmt.Sprintf("%s:%d", b.config.Host, b.config.Port)
	webAddr := fmt.Sprintf("%s:%d", b.config.Host, b.config.MetricsPort)
	webServer, err := web.NewWebServer(webAddr, brokerAddr, "localhost:9876", b.metrics)
	if err != nil {
		return fmt.Errorf("failed to create web server: %v", err)
	}
	b.webServer = webServer

	return nil
}

// GetWebServer 获取Web服务器
func (b *Broker) GetWebServer() *web.WebServer {
	return b.webServer
}

// GetMetrics 获取监控收集器
func (b *Broker) GetMetrics() *monitoring.MetricsCollector {
	return b.metrics
}
