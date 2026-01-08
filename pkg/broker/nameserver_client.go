package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "litemq/api/proto"
	"litemq/pkg/common/logger"
)

// NameServerClient NameServer客户端
type NameServerClient struct {
	nameServers []string
	brokerID    string
	currentNS   string
	nsClient    pb.NameServerServiceClient
	grpcConn    *grpc.ClientConn
	mutex       sync.RWMutex
}

// NewNameServerClient 创建NameServer客户端
func NewNameServerClient(nameServers []string, brokerID string) *NameServerClient {
	return &NameServerClient{
		nameServers: nameServers,
		brokerID:    brokerID,
		currentNS:   "",
	}
}

// RegisterBroker 注册Broker到NameServer
func (nsc *NameServerClient) RegisterBroker(brokerInfo map[string]interface{}) error {
	// 确保已连接
	if err := nsc.ensureConnected(); err != nil {
		return fmt.Errorf("failed to connect to nameserver: %v", err)
	}

	// 提取broker信息
	brokerID, _ := brokerInfo["broker_id"].(string)
	brokerName, _ := brokerInfo["broker_name"].(string)
	brokerAddr, _ := brokerInfo["broker_addr"].(string)
	clusterName, _ := brokerInfo["cluster_name"].(string)
	role, _ := brokerInfo["role"].(string)
	topics, _ := brokerInfo["topics"].(map[string][]int)

	// 转换topics格式
	protoTopics := make(map[string]*pb.QueueIds)
	for topic, queueIDs := range topics {
		protoQueueIDs := make([]int32, len(queueIDs))
		for i, qid := range queueIDs {
			protoQueueIDs[i] = int32(qid)
		}
		protoTopics[topic] = &pb.QueueIds{QueueIds: protoQueueIDs}
	}

	// 发送注册请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nsc.mutex.RLock()
	client := nsc.nsClient
	nsc.mutex.RUnlock()

	resp, err := client.RegisterBroker(ctx, &pb.RegisterBrokerRequest{
		BrokerId:    brokerID,
		BrokerName:  brokerName,
		BrokerAddr:  brokerAddr,
		ClusterName: clusterName,
		Role:        role,
		Topics:      protoTopics,
	})
	if err != nil {
		return fmt.Errorf("failed to register broker: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("register broker failed: %s", resp.Error)
	}

	logger.Info("Broker registered to nameserver",
		"broker_id", nsc.brokerID,
		"nameserver", nsc.currentNS)

	return nil
}

// FetchRouteInfo 获取路由信息
func (nsc *NameServerClient) FetchRouteInfo(topic string) (map[string]interface{}, error) {
	// 确保已连接
	if err := nsc.ensureConnected(); err != nil {
		return nil, fmt.Errorf("failed to connect to nameserver: %v", err)
	}

	// 发送获取路由信息请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nsc.mutex.RLock()
	client := nsc.nsClient
	nsc.mutex.RUnlock()

	resp, err := client.GetRouteInfo(ctx, &pb.GetRouteInfoRequest{
		Topic: topic,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get route info: %v", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("nameserver error: %s", resp.Error)
	}

	if resp.RouteInfo == nil {
		return nil, fmt.Errorf("no route info for topic %s", topic)
	}

	// 转换为map格式
	routeInfo := map[string]interface{}{
		"topic_name":   resp.RouteInfo.TopicName,
		"queue_count":  resp.RouteInfo.QueueCount,
		"broker_addrs": resp.RouteInfo.BrokerAddrs,
	}

	logger.Debug("Route info fetched",
		"topic", topic,
		"broker_addrs", resp.RouteInfo.BrokerAddrs)

	return routeInfo, nil
}

// SendHeartbeat 发送心跳
func (nsc *NameServerClient) SendHeartbeat() error {
	// 确保已连接
	if err := nsc.ensureConnected(); err != nil {
		return fmt.Errorf("failed to connect to nameserver: %v", err)
	}

	// 发送心跳请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nsc.mutex.RLock()
	client := nsc.nsClient
	nsc.mutex.RUnlock()

	resp, err := client.Heartbeat(ctx, &pb.BrokerHeartbeatRequest{
		BrokerId: nsc.brokerID,
	})
	if err != nil {
		// 连接可能已断开，尝试重新连接
		logger.Warn("Heartbeat failed, reconnecting",
			"broker_id", nsc.brokerID,
			"error", err)
		if err := nsc.ensureConnected(); err != nil {
			return fmt.Errorf("failed to reconnect: %v", err)
		}

		// 重试一次
		nsc.mutex.RLock()
		client = nsc.nsClient
		nsc.mutex.RUnlock()

		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()

		resp, err = client.Heartbeat(ctx2, &pb.BrokerHeartbeatRequest{
			BrokerId: nsc.brokerID,
		})
		if err != nil {
			return fmt.Errorf("failed to send heartbeat: %v", err)
		}
	}

	if !resp.Success {
		return fmt.Errorf("heartbeat failed: %s", resp.Error)
	}

	logger.Debug("Heartbeat sent to nameserver",
		"broker_id", nsc.brokerID,
		"nameserver", nsc.currentNS)

	return nil
}

// selectNameServer 选择NameServer
func (nsc *NameServerClient) selectNameServer() string {
	if len(nsc.nameServers) == 0 {
		return ""
	}

	// 简单轮询选择
	nsc.mutex.RLock()
	current := nsc.currentNS
	nsc.mutex.RUnlock()

	// 如果已有连接，使用当前NameServer
	for _, ns := range nsc.nameServers {
		if ns == current {
			return ns
		}
	}

	// 否则选择第一个
	return nsc.nameServers[0]
}

// getCurrentNameServer 获取当前NameServer
func (nsc *NameServerClient) getCurrentNameServer() string {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()
	return nsc.currentNS
}

// ensureConnected 确保已连接到NameServer
func (nsc *NameServerClient) ensureConnected() error {
	nsc.mutex.RLock()
	conn := nsc.grpcConn
	client := nsc.nsClient
	nsc.mutex.RUnlock()

	// 如果已连接，直接返回
	if conn != nil && client != nil {
		return nil
	}

	// 尝试连接
	return nsc.reconnect()
}

// reconnect 重新连接到NameServer
func (nsc *NameServerClient) reconnect() error {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()

	// 关闭现有连接
	if nsc.grpcConn != nil {
		nsc.grpcConn.Close()
		nsc.grpcConn = nil
		nsc.nsClient = nil
	}

	// 尝试连接每个NameServer
	for _, nsAddr := range nsc.nameServers {
		conn, err := grpc.Dial(nsAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second))
		if err != nil {
			logger.Warn("Failed to connect to nameserver",
				"nameserver", nsAddr,
				"error", err)
			continue
		}

		nsc.grpcConn = conn
		nsc.nsClient = pb.NewNameServerServiceClient(conn)
		nsc.currentNS = nsAddr

		logger.Info("Connected to nameserver", "nameserver", nsAddr)
		return nil
	}

	return fmt.Errorf("failed to connect to any nameserver")
}

// GetNameServers 获取NameServer列表
func (nsc *NameServerClient) GetNameServers() []string {
	servers := make([]string, len(nsc.nameServers))
	copy(servers, nsc.nameServers)
	return servers
}

// GetCurrentNameServer 获取当前连接的NameServer
func (nsc *NameServerClient) GetCurrentNameServer() string {
	return nsc.getCurrentNameServer()
}

// IsConnected 检查是否已连接
func (nsc *NameServerClient) IsConnected() bool {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()
	return nsc.grpcConn != nil && nsc.nsClient != nil
}

// Close 关闭连接
func (nsc *NameServerClient) Close() error {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()

	if nsc.grpcConn != nil {
		err := nsc.grpcConn.Close()
		nsc.grpcConn = nil
		nsc.nsClient = nil
		nsc.currentNS = ""
		return err
	}

	return nil
}
