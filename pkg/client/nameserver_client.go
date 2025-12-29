package client

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
	nameServerAddrs []string
	currentNS       string
	nsClient        pb.NameServerServiceClient
	grpcConn        *grpc.ClientConn
	mutex           sync.RWMutex
	lastUpdate      time.Time
	routeCache      map[string]*TopicRouteInfo // topic -> routeInfo
	cacheExpiry     time.Duration
}

// NewNameServerClient 创建NameServer客户端
func NewNameServerClient(nameServerAddrs []string) *NameServerClient {
	return &NameServerClient{
		nameServerAddrs: nameServerAddrs,
		routeCache:      make(map[string]*TopicRouteInfo),
		cacheExpiry:     30 * time.Second, // 路由信息缓存30秒
	}
}

// Connect 连接到NameServer
func (nsc *NameServerClient) Connect() error {
	nsc.mutex.Lock()
	defer nsc.mutex.Unlock()

	if len(nsc.nameServerAddrs) == 0 {
		return fmt.Errorf("no nameserver addresses provided")
	}

	// 尝试连接每个NameServer，直到成功
	for _, nsAddr := range nsc.nameServerAddrs {
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

// GetRouteInfo 获取路由信息（带缓存）
func (nsc *NameServerClient) GetRouteInfo(topic string) (*TopicRouteInfo, error) {
	nsc.mutex.RLock()
	cached, exists := nsc.routeCache[topic]
	lastUpdate := nsc.lastUpdate
	nsc.mutex.RUnlock()

	// 检查缓存是否有效
	if exists && time.Since(lastUpdate) < nsc.cacheExpiry {
		return cached, nil
	}

	// 缓存过期或不存在，从NameServer获取
	routeInfo, err := nsc.fetchRouteInfoFromServer(topic)
	if err != nil {
		// 如果获取失败，尝试使用缓存（即使过期）
		if exists {
			logger.Warn("Failed to fetch route info, using stale cache",
				"topic", topic,
				"error", err)
			return cached, nil
		}
		return nil, err
	}

	// 更新缓存
	nsc.mutex.Lock()
	nsc.routeCache[topic] = routeInfo
	nsc.lastUpdate = time.Now()
	nsc.mutex.Unlock()

	return routeInfo, nil
}

// fetchRouteInfoFromServer 从NameServer服务器获取路由信息
func (nsc *NameServerClient) fetchRouteInfoFromServer(topic string) (*TopicRouteInfo, error) {
	nsc.mutex.RLock()
	client := nsc.nsClient
	conn := nsc.grpcConn
	nsc.mutex.RUnlock()

	// 检查连接
	if client == nil || conn == nil {
		// 尝试重新连接
		if err := nsc.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect to nameserver: %v", err)
		}
		nsc.mutex.RLock()
		client = nsc.nsClient
		nsc.mutex.RUnlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetRouteInfo(ctx, &pb.GetRouteInfoRequest{
		Topic: topic,
	})
	if err != nil {
		// 连接可能已断开，尝试重新连接
		logger.Warn("Failed to get route info, reconnecting",
			"topic", topic,
			"error", err)
		if err := nsc.Connect(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %v", err)
		}

		// 重试一次
		nsc.mutex.RLock()
		client = nsc.nsClient
		nsc.mutex.RUnlock()

		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()

		resp, err = client.GetRouteInfo(ctx2, &pb.GetRouteInfoRequest{
			Topic: topic,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get route info: %v", err)
		}
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("nameserver error: %s", resp.Error)
	}

	if resp.RouteInfo == nil {
		return nil, fmt.Errorf("no route info for topic %s", topic)
	}

	// 转换proto格式到内部格式
	brokerAddrs := make([]string, 0)
	if resp.RouteInfo.BrokerAddrs != nil {
		brokerAddrs = resp.RouteInfo.BrokerAddrs
	}

	return &TopicRouteInfo{
		TopicName:   resp.RouteInfo.TopicName,
		QueueCount:  int(resp.RouteInfo.QueueCount),
		BrokerAddrs: brokerAddrs,
	}, nil
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

// IsConnected 检查是否已连接
func (nsc *NameServerClient) IsConnected() bool {
	nsc.mutex.RLock()
	defer nsc.mutex.RUnlock()
	return nsc.grpcConn != nil && nsc.nsClient != nil
}
