package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIServer REST API服务器
type APIServer struct {
	engine          *gin.Engine
	brokerAddr      string
	nameserverAddr  string
}

// NewAPIServer 创建API服务器
func NewAPIServer(brokerAddr, nameserverAddr string) *APIServer {
	// 设置Gin模式
	gin.SetMode(gin.ReleaseMode)

	server := &APIServer{
		engine:         gin.New(),
		brokerAddr:     brokerAddr,
		nameserverAddr: nameserverAddr,
	}

	server.setupRoutes()
	server.setupMiddleware()

	return server
}

// setupMiddleware 设置中间件
func (s *APIServer) setupMiddleware() {
	// 恢复中间件
	s.engine.Use(gin.Recovery())

	// 日志中间件
	s.engine.Use(gin.Logger())

	// CORS中间件
	s.engine.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})
}

// setupRoutes 设置路由
func (s *APIServer) setupRoutes() {
	api := s.engine.Group("/api/v1")
	{
		// 健康检查
		api.GET("/health", s.healthCheck)

		// Broker API
		broker := api.Group("/broker")
		{
			broker.GET("/info", s.getBrokerInfo)
			broker.GET("/stats", s.getBrokerStats)
			broker.GET("/topics", s.getTopics)
			broker.GET("/consumers", s.getConsumers)

			// 主题管理
			broker.POST("/topics/:topic", s.createTopic)
			broker.DELETE("/topics/:topic", s.deleteTopic)
		}

		// NameServer API
		nameserver := api.Group("/nameserver")
		{
			nameserver.GET("/brokers", s.getBrokers)
			nameserver.GET("/topics", s.getAllTopics)
			nameserver.GET("/stats", s.getNameServerStats)
		}
	}
}

// ServeHTTP 实现http.Handler接口
func (s *APIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.engine.ServeHTTP(w, r)
}

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status   string                 `json:"status"`
	Services map[string]interface{} `json:"services"`
	Timestamp int64                 `json:"timestamp"`
	Version  string                 `json:"version"`
}

// healthCheck 健康检查
func (s *APIServer) healthCheck(c *gin.Context) {
	response := HealthResponse{
		Status: "ok",
		Services: map[string]interface{}{
			"broker": map[string]interface{}{
				"address": s.brokerAddr,
				"status":  "unknown", // 这里可以添加实际的健康检查
			},
			"nameserver": map[string]interface{}{
				"address": s.nameserverAddr,
				"status":  "unknown", // 这里可以添加实际的健康检查
			},
		},
		Timestamp: 0, // 可以设置为当前时间戳
		Version:   "1.0.0",
	}

	c.JSON(200, response)
}

// BrokerInfoResponse Broker信息响应
type BrokerInfoResponse struct {
	BrokerID   string `json:"broker_id"`
	BrokerName string `json:"broker_name"`
	Address    string `json:"address"`
	Role       string `json:"role"`
	Status     string `json:"status"`
	StartTime  int64  `json:"start_time"`
}

// getBrokerInfo 获取Broker信息
func (s *APIServer) getBrokerInfo(c *gin.Context) {
	info := BrokerInfoResponse{
		BrokerID:   "broker-1",
		BrokerName: "broker-1",
		Address:    s.brokerAddr,
		Role:       "master",
		Status:     "running",
		StartTime:  0, // 可以设置为实际启动时间
	}

	c.JSON(200, info)
}

// BrokerStatsResponse Broker统计信息响应
type BrokerStatsResponse struct {
	TotalMessages     int64 `json:"total_messages"`
	TotalBytes        int64 `json:"total_bytes"`
	ActiveConnections int64 `json:"active_connections"`
	Topics            int   `json:"topics"`
	Consumers         int   `json:"consumers"`
	WroteOffset       int64 `json:"wrote_offset"`
	CommittedOffset   int64 `json:"committed_offset"`
	FlushedOffset     int64 `json:"flushed_offset"`
}

// getBrokerStats 获取Broker统计信息
func (s *APIServer) getBrokerStats(c *gin.Context) {
	stats := BrokerStatsResponse{
		TotalMessages:     1000,
		TotalBytes:        1024000,
		ActiveConnections: 5,
		Topics:            3,
		Consumers:         2,
		WroteOffset:       1024,
		CommittedOffset:   1024,
		FlushedOffset:     1024,
	}

	c.JSON(200, stats)
}

// TopicInfo 主题信息
type TopicInfo struct {
	Name      string `json:"name"`
	Partitions int   `json:"partitions"`
	Messages  int64  `json:"messages"`
	Consumers int    `json:"consumers"`
}

// TopicsResponse 主题列表响应
type TopicsResponse struct {
	Topics []TopicInfo `json:"topics"`
	Count  int         `json:"count"`
}

// getTopics 获取主题列表
func (s *APIServer) getTopics(c *gin.Context) {
	topics := []TopicInfo{
		{Name: "test_topic", Partitions: 4, Messages: 100, Consumers: 2},
		{Name: "order_topic", Partitions: 8, Messages: 200, Consumers: 1},
	}

	response := TopicsResponse{
		Topics: topics,
		Count:  len(topics),
	}

	c.JSON(200, response)
}

// ConsumerGroupInfo 消费者组信息
type ConsumerGroupInfo struct {
	GroupName string   `json:"group_name"`
	Consumers int      `json:"consumers"`
	Topics    []string `json:"topics"`
	Status    string   `json:"status"`
	Lag       int64    `json:"lag"`
}

// ConsumersResponse 消费者列表响应
type ConsumersResponse struct {
	ConsumerGroups []ConsumerGroupInfo `json:"consumer_groups"`
	Count          int                 `json:"count"`
}

// getConsumers 获取消费者列表
func (s *APIServer) getConsumers(c *gin.Context) {
	consumerGroups := []ConsumerGroupInfo{
		{
			GroupName: "test_group",
			Consumers: 2,
			Topics:    []string{"test_topic"},
			Status:    "active",
			Lag:       0,
		},
		{
			GroupName: "order_group",
			Consumers: 1,
			Topics:    []string{"order_topic"},
			Status:    "active",
			Lag:       5,
		},
	}

	response := ConsumersResponse{
		ConsumerGroups: consumerGroups,
		Count:          len(consumerGroups),
	}

	c.JSON(200, response)
}

// CreateTopicRequest 创建主题请求
type CreateTopicRequest struct {
	Partitions int `json:"partitions" binding:"required,min=1,max=100"`
}

// TopicResponse 主题操作响应
type TopicResponse struct {
	Topic   string `json:"topic"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// createTopic 创建主题
func (s *APIServer) createTopic(c *gin.Context) {
	topic := c.Param("topic")

	var req CreateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request body", "details": err.Error()})
		return
	}

	// 这里应该实现主题创建逻辑
	// 目前只是返回成功响应
	response := TopicResponse{
		Topic:   topic,
		Status:  "created",
		Message: "Topic created successfully",
	}

	c.JSON(201, response)
}

// deleteTopic 删除主题
func (s *APIServer) deleteTopic(c *gin.Context) {
	topic := c.Param("topic")

	// 这里应该实现主题删除逻辑
	response := TopicResponse{
		Topic:   topic,
		Status:  "deleted",
		Message: "Topic deleted successfully",
	}

	c.JSON(200, response)
}

// BrokerInfo NameServer中的Broker信息
type BrokerInfo struct {
	BrokerID   string `json:"broker_id"`
	BrokerAddr string `json:"broker_addr"`
	Role       string `json:"role"`
	Status     string `json:"status"`
	Topics     int    `json:"topics"`
}

// BrokersResponse Brokers列表响应
type BrokersResponse struct {
	Brokers []BrokerInfo `json:"brokers"`
	Count   int          `json:"count"`
}

// getBrokers 获取所有Broker
func (s *APIServer) getBrokers(c *gin.Context) {
	brokers := []BrokerInfo{
		{
			BrokerID:   "broker-1",
			BrokerAddr: s.brokerAddr,
			Role:       "master",
			Status:     "online",
			Topics:     3,
		},
		{
			BrokerID:   "broker-2",
			BrokerAddr: "localhost:10912",
			Role:       "slave",
			Status:     "online",
			Topics:     2,
		},
	}

	response := BrokersResponse{
		Brokers: brokers,
		Count:   len(brokers),
	}

	c.JSON(200, response)
}

// TopicRouteInfo 主题路由信息
type TopicRouteInfo struct {
	TopicName  string   `json:"topic_name"`
	QueueCount int      `json:"queue_count"`
	BrokerAddrs []string `json:"broker_addrs"`
}

// TopicsRouteResponse 主题路由列表响应
type TopicsRouteResponse struct {
	Topics []TopicRouteInfo `json:"topics"`
	Count  int              `json:"count"`
}

// getAllTopics 获取所有主题
func (s *APIServer) getAllTopics(c *gin.Context) {
	topics := []TopicRouteInfo{
		{
			TopicName:  "test_topic",
			QueueCount: 4,
			BrokerAddrs: []string{s.brokerAddr, "localhost:10912"},
		},
		{
			TopicName:  "order_topic",
			QueueCount: 8,
			BrokerAddrs: []string{s.brokerAddr},
		},
	}

	response := TopicsRouteResponse{
		Topics: topics,
		Count:  len(topics),
	}

	c.JSON(200, response)
}

// NameServerStatsResponse NameServer统计信息响应
type NameServerStatsResponse struct {
	Brokers int  `json:"brokers"`
	Topics  int  `json:"topics"`
	Running bool `json:"running"`
	Uptime  int64 `json:"uptime"`
}

// getNameServerStats 获取NameServer统计信息
func (s *APIServer) getNameServerStats(c *gin.Context) {
	stats := NameServerStatsResponse{
		Brokers: 2,
		Topics:  2,
		Running: true,
		Uptime:  3600, // 模拟1小时运行时间
	}

	c.JSON(200, stats)
}
