package broker

import (
	"fmt"
	"sync"

	"litemq/pkg/common/logger"
)

// NameServerClient NameServer客户端
type NameServerClient struct {
	nameServers []string
	brokerID    string
	currentNS   string
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
	// 选择NameServer
	nsAddr := nsc.selectNameServer()
	if nsAddr == "" {
		return fmt.Errorf("no available nameserver")
	}

	// 发送注册请求
	// 实际实现需要网络通信
	logger.Info("Registering broker to nameserver",
		"broker_id", nsc.brokerID,
		"nameserver", nsAddr)

	nsc.mutex.Lock()
	nsc.currentNS = nsAddr
	nsc.mutex.Unlock()

	return nil
}

// FetchRouteInfo 获取路由信息
func (nsc *NameServerClient) FetchRouteInfo(topic string) (map[string]interface{}, error) {
	nsAddr := nsc.getCurrentNameServer()
	if nsAddr == "" {
		return nil, fmt.Errorf("no connected nameserver")
	}

	// 发送获取路由信息请求
	// 实际实现需要网络通信
	logger.Info("Fetching route info",
		"topic", topic,
		"nameserver", nsAddr)

	return nil, fmt.Errorf("not implemented")
}

// SendHeartbeat 发送心跳
func (nsc *NameServerClient) SendHeartbeat() error {
	nsAddr := nsc.getCurrentNameServer()
	if nsAddr == "" {
		// 尝试重新连接
		if err := nsc.reconnect(); err != nil {
			return fmt.Errorf("failed to reconnect to nameserver: %v", err)
		}
		nsAddr = nsc.getCurrentNameServer()
		if nsAddr == "" {
			return fmt.Errorf("no available nameserver")
		}
	}

	// 发送心跳请求
	// 实际实现需要网络通信
	logger.Info("Sending heartbeat to nameserver", "nameserver", nsAddr)

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

// reconnect 重新连接到NameServer
func (nsc *NameServerClient) reconnect() error {
	for _, nsAddr := range nsc.nameServers {
		// 尝试连接
		// 实际实现需要网络连接测试
		logger.Info("Trying to connect to nameserver", "nameserver", nsAddr)

		// 模拟连接成功
		nsc.mutex.Lock()
		nsc.currentNS = nsAddr
		nsc.mutex.Unlock()

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
	return nsc.getCurrentNameServer() != ""
}
