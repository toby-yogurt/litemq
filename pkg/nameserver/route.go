package nameserver

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
)

// RouteInfoManager 路由信息管理器
type RouteInfoManager struct {
	// Broker信息: brokerID -> BrokerInfo
	brokers map[string]*BrokerInfo

	// 主题路由: topic -> TopicRouteInfo
	topicRoutes map[string]*TopicRouteInfo

	// 集群信息: cluster -> brokerIDs
	clusters map[string][]string

	mutex sync.RWMutex
}

// NewRouteInfoManager 创建路由信息管理器
func NewRouteInfoManager() *RouteInfoManager {
	return &RouteInfoManager{
		brokers:     make(map[string]*BrokerInfo),
		topicRoutes: make(map[string]*TopicRouteInfo),
		clusters:    make(map[string][]string),
	}
}

// RegisterBroker 注册Broker
func (rim *RouteInfoManager) RegisterBroker(brokerInfo *BrokerInfo) error {
	rim.mutex.Lock()
	defer rim.mutex.Unlock()

	brokerID := brokerInfo.BrokerID

	// 检查是否已存在
	if existing, exists := rim.brokers[brokerID]; exists {
		// 更新现有信息
		existing.BrokerAddr = brokerInfo.BrokerAddr
		existing.ClusterName = brokerInfo.ClusterName
		existing.Role = brokerInfo.Role
		existing.LastUpdate = time.Now()
		existing.Topics = brokerInfo.Topics
		existing.Properties = brokerInfo.Properties
	} else {
		// 添加新Broker
		brokerInfo.LastUpdate = time.Now()
		rim.brokers[brokerID] = brokerInfo
	}

	// 更新集群信息
	clusterName := brokerInfo.ClusterName
	if clusterName != "" {
		rim.addBrokerToCluster(clusterName, brokerID)
	}

	// 更新主题路由信息
	rim.updateTopicRoutes(brokerInfo)

	logger.Info("Broker registered",
		"broker_id", brokerID,
		"addr", brokerInfo.BrokerAddr)
	return nil
}

// UnregisterBroker 注销Broker
func (rim *RouteInfoManager) UnregisterBroker(brokerID string) error {
	rim.mutex.Lock()
	defer rim.mutex.Unlock()

	brokerInfo, exists := rim.brokers[brokerID]
	if !exists {
		return fmt.Errorf("broker not found: %s", brokerID)
	}

	// 从集群中移除
	if brokerInfo.ClusterName != "" {
		rim.removeBrokerFromCluster(brokerInfo.ClusterName, brokerID)
	}

	// 移除主题路由中的Broker
	rim.removeBrokerFromTopicRoutes(brokerID)

	// 删除Broker信息
	delete(rim.brokers, brokerID)

	logger.Info("Broker unregistered", "broker_id", brokerID)
	return nil
}

// GetBrokerInfo 获取Broker信息
func (rim *RouteInfoManager) GetBrokerInfo(brokerID string) (*BrokerInfo, bool) {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	broker, exists := rim.brokers[brokerID]
	return broker, exists
}

// GetAllBrokerInfos 获取所有Broker信息
func (rim *RouteInfoManager) GetAllBrokerInfos() []*BrokerInfo {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	brokers := make([]*BrokerInfo, 0, len(rim.brokers))
	for _, broker := range rim.brokers {
		brokers = append(brokers, broker)
	}

	return brokers
}

// GetTopicRouteInfo 获取主题路由信息
func (rim *RouteInfoManager) GetTopicRouteInfo(topicName string) (*TopicRouteInfo, error) {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	route, exists := rim.topicRoutes[topicName]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topicName)
	}

	return route, nil
}

// GetAllTopicRouteInfos 获取所有主题路由信息
func (rim *RouteInfoManager) GetAllTopicRouteInfos() []*TopicRouteInfo {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	routes := make([]*TopicRouteInfo, 0, len(rim.topicRoutes))
	for _, route := range rim.topicRoutes {
		routes = append(routes, route)
	}

	return routes
}

// GetBrokersByTopic 获取主题相关的Broker列表
func (rim *RouteInfoManager) GetBrokersByTopic(topicName string) ([]*BrokerInfo, error) {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	route, exists := rim.topicRoutes[topicName]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topicName)
	}

	brokers := make([]*BrokerInfo, 0, len(route.BrokerAddrs))
	for _, addr := range route.BrokerAddrs {
		// 根据地址查找Broker
		for _, broker := range rim.brokers {
			if broker.BrokerAddr == addr {
				brokers = append(brokers, broker)
				break
			}
		}
	}

	return brokers, nil
}

// GetTopicsByBroker 获取Broker上的主题列表
func (rim *RouteInfoManager) GetTopicsByBroker(brokerID string) ([]string, error) {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	broker, exists := rim.brokers[brokerID]
	if !exists {
		return nil, fmt.Errorf("broker not found: %s", brokerID)
	}

	topics := make([]string, 0, len(broker.Topics))
	for topic := range broker.Topics {
		topics = append(topics, topic)
	}

	return topics, nil
}

// GetBrokersByCluster 获取集群中的Broker列表
func (rim *RouteInfoManager) GetBrokersByCluster(clusterName string) ([]*BrokerInfo, error) {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	brokerIDs, exists := rim.clusters[clusterName]
	if !exists {
		return nil, fmt.Errorf("cluster not found: %s", clusterName)
	}

	brokers := make([]*BrokerInfo, 0, len(brokerIDs))
	for _, brokerID := range brokerIDs {
		if broker, exists := rim.brokers[brokerID]; exists {
			brokers = append(brokers, broker)
		}
	}

	return brokers, nil
}

// GetBrokerCount 获取Broker数量
func (rim *RouteInfoManager) GetBrokerCount() int {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()
	return len(rim.brokers)
}

// GetTopicCount 获取主题数量
func (rim *RouteInfoManager) GetTopicCount() int {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()
	return len(rim.topicRoutes)
}

// GetClusterCount 获取集群数量
func (rim *RouteInfoManager) GetClusterCount() int {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()
	return len(rim.clusters)
}

// addBrokerToCluster 添加Broker到集群
func (rim *RouteInfoManager) addBrokerToCluster(clusterName, brokerID string) {
	brokerIDs := rim.clusters[clusterName]
	// 检查是否已存在
	for _, id := range brokerIDs {
		if id == brokerID {
			return
		}
	}
	rim.clusters[clusterName] = append(brokerIDs, brokerID)
}

// removeBrokerFromCluster 从集群中移除Broker
func (rim *RouteInfoManager) removeBrokerFromCluster(clusterName, brokerID string) {
	brokerIDs := rim.clusters[clusterName]
	for i, id := range brokerIDs {
		if id == brokerID {
			rim.clusters[clusterName] = append(brokerIDs[:i], brokerIDs[i+1:]...)
			break
		}
	}

	// 如果集群为空，删除集群
	if len(rim.clusters[clusterName]) == 0 {
		delete(rim.clusters, clusterName)
	}
}

// updateTopicRoutes 更新主题路由信息
func (rim *RouteInfoManager) updateTopicRoutes(brokerInfo *BrokerInfo) {
	for topicName, queueIDs := range brokerInfo.Topics {
		route, exists := rim.topicRoutes[topicName]
		if !exists {
			route = &TopicRouteInfo{
				TopicName:   topicName,
				BrokerAddrs: make([]string, 0),
			}
			rim.topicRoutes[topicName] = route
		}

		// 添加Broker地址
		addrExists := false
		for _, addr := range route.BrokerAddrs {
			if addr == brokerInfo.BrokerAddr {
				addrExists = true
				break
			}
		}
		if !addrExists {
			route.BrokerAddrs = append(route.BrokerAddrs, brokerInfo.BrokerAddr)
		}

		// 更新队列数量
		if len(queueIDs) > route.QueueCount {
			route.QueueCount = len(queueIDs)
		}

		// 根据Broker角色设置读写队列
		if brokerInfo.Role == config.RoleMaster {
			route.WriteQueueIds = queueIDs
		}
		route.ReadQueueIds = queueIDs // 所有Broker都可以读
	}
}

// removeBrokerFromTopicRoutes 从主题路由中移除Broker
func (rim *RouteInfoManager) removeBrokerFromTopicRoutes(brokerID string) {
	brokerInfo, exists := rim.brokers[brokerID]
	if !exists {
		return
	}

	for topicName, route := range rim.topicRoutes {
		// 从Broker地址列表中移除
		for i, addr := range route.BrokerAddrs {
			if addr == brokerInfo.BrokerAddr {
				route.BrokerAddrs = append(route.BrokerAddrs[:i], route.BrokerAddrs[i+1:]...)
				break
			}
		}

		// 如果没有Broker地址了，删除主题路由
		if len(route.BrokerAddrs) == 0 {
			delete(rim.topicRoutes, topicName)
		}
	}
}

// CleanExpiredBrokers 清理过期Broker
func (rim *RouteInfoManager) CleanExpiredBrokers(expiryTime time.Duration) []string {
	rim.mutex.Lock()
	defer rim.mutex.Unlock()

	now := time.Now()
	expiredBrokers := make([]string, 0)

	for brokerID, broker := range rim.brokers {
		if now.Sub(broker.LastUpdate) > expiryTime {
			expiredBrokers = append(expiredBrokers, brokerID)
		}
	}

	// 移除过期Broker
	for _, brokerID := range expiredBrokers {
		rim.UnregisterBroker(brokerID)
	}

	return expiredBrokers
}

// GetRouteStats 获取路由统计信息
type RouteStats struct {
	BrokerCount  int            `json:"broker_count"`
	TopicCount   int            `json:"topic_count"`
	ClusterCount int            `json:"cluster_count"`
	Clusters     map[string]int `json:"clusters"` // cluster -> broker count
}

// GetRouteStats 获取路由统计信息
func (rim *RouteInfoManager) GetRouteStats() *RouteStats {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	stats := &RouteStats{
		BrokerCount:  len(rim.brokers),
		TopicCount:   len(rim.topicRoutes),
		ClusterCount: len(rim.clusters),
		Clusters:     make(map[string]int),
	}

	for cluster, brokerIDs := range rim.clusters {
		stats.Clusters[cluster] = len(brokerIDs)
	}

	return stats
}

// PrintRouteInfo 打印路由信息（调试用）
func (rim *RouteInfoManager) PrintRouteInfo() {
	rim.mutex.RLock()
	defer rim.mutex.RUnlock()

	logger.Info("Route Information ---")
	logger.Info("Brokers", "count", len(rim.brokers))
	for id, broker := range rim.brokers {
		logger.Info("Broker detail", "broker_id", id, "addr", broker.BrokerAddr, "role", broker.Role)
	}

	logger.Info("Topics", "count", len(rim.topicRoutes))
	for name, route := range rim.topicRoutes {
		logger.Info("Topic detail",
			"topic", name,
			"brokers", len(route.BrokerAddrs),
			"queues", route.QueueCount)
	}

	logger.Info("Clusters", "count", len(rim.clusters))
	for name, brokerIDs := range rim.clusters {
		logger.Info("Cluster detail", "cluster", name, "brokers", len(brokerIDs))
	}
	logger.Info("Route Information --- end ---")
}
