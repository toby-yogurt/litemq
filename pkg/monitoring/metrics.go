package monitoring

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsCollector 指标收集器
type MetricsCollector struct {
	// 消息相关指标
	messagesSent     prometheus.Counter
	messagesReceived prometheus.Counter
	messageSize      prometheus.Histogram

	// 连接相关指标
	activeConnections prometheus.Gauge
	totalConnections  prometheus.Counter

	// 存储相关指标
	storageUsed   prometheus.Gauge
	commitLogSize prometheus.Gauge
	queueSize     prometheus.Gauge

	// 性能相关指标
	requestDuration prometheus.Histogram
	requestCount    prometheus.Counter

	// 错误相关指标
	errorsTotal prometheus.Counter

	// 业务指标
	topicsTotal    prometheus.Gauge
	consumersTotal prometheus.Gauge
	brokersTotal   prometheus.Gauge

	// 用于非Prometheus指标的互斥锁
	mutex sync.RWMutex

	// 用于跟踪消息总数的上次值（用于计算增量）
	lastTotalMessages int64
}

// 全局变量，用于单例模式
var (
	globalMetricsCollector *MetricsCollector
	metricsOnce            sync.Once
)

// NewMetricsCollector 创建指标收集器（单例模式）
func NewMetricsCollector() *MetricsCollector {
	metricsOnce.Do(func() {
		mc := &MetricsCollector{
			messagesSent: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "litemq_messages_sent_total",
				Help: "Total number of messages sent",
			}),
			messagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "litemq_messages_received_total",
				Help: "Total number of messages received",
			}),
			messageSize: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "litemq_message_size_bytes",
				Help:    "Message size distribution",
				Buckets: prometheus.DefBuckets,
			}),
			activeConnections: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_active_connections",
				Help: "Number of active connections",
			}),
			totalConnections: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "litemq_connections_total",
				Help: "Total number of connections",
			}),
			storageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_storage_used_bytes",
				Help: "Storage space used in bytes",
			}),
			commitLogSize: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_commitlog_size_bytes",
				Help: "Commit log size in bytes",
			}),
			queueSize: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_queue_size",
				Help: "Number of messages in queues",
			}),
			requestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "litemq_request_duration_seconds",
				Help:    "Request duration distribution",
				Buckets: prometheus.DefBuckets,
			}),
			requestCount: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "litemq_requests_total",
				Help: "Total number of requests",
			}),
			errorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "litemq_errors_total",
				Help: "Total number of errors",
			}),
			topicsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_topics_total",
				Help: "Total number of topics",
			}),
			consumersTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_consumers_total",
				Help: "Total number of consumers",
			}),
			brokersTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "litemq_brokers_total",
				Help: "Total number of brokers",
			}),
		}

		// 注册指标
		prometheus.MustRegister(
			mc.messagesSent,
			mc.messagesReceived,
			mc.messageSize,
			mc.activeConnections,
			mc.totalConnections,
			mc.storageUsed,
			mc.commitLogSize,
			mc.queueSize,
			mc.requestDuration,
			mc.requestCount,
			mc.errorsTotal,
			mc.topicsTotal,
			mc.consumersTotal,
			mc.brokersTotal,
		)

		globalMetricsCollector = mc
	})

	return globalMetricsCollector
}

// Handler 返回 Prometheus 指标处理器
func (mc *MetricsCollector) Handler() http.Handler {
	return promhttp.Handler()
}

// RecordMessageSent 记录发送的消息
func (mc *MetricsCollector) RecordMessageSent(size int) {
	mc.messagesSent.Inc()
	mc.messageSize.Observe(float64(size))
}

// RecordMessageReceived 记录接收的消息
func (mc *MetricsCollector) RecordMessageReceived() {
	mc.messagesReceived.Inc()
}

// RecordConnection 记录新连接
func (mc *MetricsCollector) RecordConnection() {
	mc.activeConnections.Inc()
	mc.totalConnections.Inc()
}

// RecordDisconnection 记录断开连接
func (mc *MetricsCollector) RecordDisconnection() {
	mc.activeConnections.Dec()
}

// UpdateStorageMetrics 更新存储指标
func (mc *MetricsCollector) UpdateStorageMetrics(used, commitLog, queue int64) {
	mc.storageUsed.Set(float64(used))
	mc.commitLogSize.Set(float64(commitLog))
	mc.queueSize.Set(float64(queue))
}

// RecordRequest 记录请求
func (mc *MetricsCollector) RecordRequest(duration time.Duration) {
	mc.requestCount.Inc()
	mc.requestDuration.Observe(duration.Seconds())
}

// RecordError 记录错误
func (mc *MetricsCollector) RecordError() {
	mc.errorsTotal.Inc()
}

// RecordMessageSize 记录消息大小
func (mc *MetricsCollector) RecordMessageSize(size int) {
	mc.messageSize.Observe(float64(size))
}

// UpdateTopicsCount 更新主题数量
func (mc *MetricsCollector) UpdateTopicsCount(count int) {
	mc.topicsTotal.Set(float64(count))
}

// UpdateConsumersCount 更新消费者数量
func (mc *MetricsCollector) UpdateConsumersCount(count int) {
	mc.consumersTotal.Set(float64(count))
}

// UpdateBrokersCount 更新Broker数量
func (mc *MetricsCollector) UpdateBrokersCount(count int) {
	mc.brokersTotal.Set(float64(count))
}

// GetMetricsSummary 获取指标摘要
func (mc *MetricsCollector) GetMetricsSummary() map[string]interface{} {
	return map[string]interface{}{
		"messages_sent":      1000, // 示例值
		"messages_received":  950,  // 示例值
		"active_connections": 5,    // 示例值
		"total_connections":  100,  // 示例值
		"topics_total":       10,   // 示例值
		"consumers_total":    20,   // 示例值
		"brokers_total":      3,    // 示例值
		"errors_total":       2,    // 示例值
	}
}

// MetricsServer 指标服务器
type MetricsServer struct {
	collector *MetricsCollector
	server    *http.Server
}

// NewMetricsServer 创建指标服务器
func NewMetricsServer(addr string, collector *MetricsCollector) *MetricsServer {

	mux := http.NewServeMux()
	mux.Handle("/metrics", collector.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return &MetricsServer{
		collector: collector,
		server:    server,
	}
}

// Start 启动指标服务器
func (ms *MetricsServer) Start() error {
	go func() {
		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return nil
}

// Stop 停止指标服务器
func (ms *MetricsServer) Stop() error {
	return ms.server.Close()
}

// GetCollector 获取指标收集器
func (ms *MetricsServer) GetCollector() *MetricsCollector {
	return ms.collector
}

// Handler 获取HTTP处理器
func (ms *MetricsServer) Handler() http.Handler {
	return ms.server.Handler
}

// Middleware 监控中间件
type Middleware struct {
	collector *MetricsCollector
}

// NewMiddleware 创建监控中间件
func NewMiddleware(collector *MetricsCollector) *Middleware {
	return &Middleware{collector: collector}
}

// WrapHandler 包装HTTP处理器
func (m *Middleware) WrapHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 包装ResponseWriter以捕获状态码
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// 调用原始处理器
		handler.ServeHTTP(rw, r)

		// 记录指标
		duration := time.Since(start)
		m.collector.RecordRequest(duration)
	})
}

// responseWriter 包装的ResponseWriter
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// GetStatusCode 获取状态码
func (rw *responseWriter) GetStatusCode() int {
	return rw.statusCode
}

// UpdateMetrics 更新所有指标
func (mc *MetricsCollector) UpdateMetrics(stats map[string]interface{}) {
	if val, ok := stats["topics"].(int); ok {
		mc.UpdateTopicsCount(val)
	}
	if val, ok := stats["consumers"].(int); ok {
		mc.UpdateConsumersCount(val)
	}
	if val, ok := stats["brokers"].(int); ok {
		mc.UpdateBrokersCount(val)
	}
	if val, ok := stats["total_messages"].(int64); ok {
		// 更新消息计数器（基于存储的总消息数）
		mc.mutex.Lock()
		// 计算增量
		if val > mc.lastTotalMessages {
			delta := val - mc.lastTotalMessages
			// 将增量添加到接收消息计数器（因为这些都是已存储的消息）
			for i := int64(0); i < delta; i++ {
				mc.messagesReceived.Inc()
			}
			mc.lastTotalMessages = val
		}
		mc.mutex.Unlock()
	}
}
