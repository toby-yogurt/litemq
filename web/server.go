package web

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	v1 "litemq/api/v1"
	"litemq/pkg/common/logger"
	"litemq/pkg/monitoring"
)

//go:embed static/css/custom.css static/js/app.js templates/*.html
var webFS embed.FS

// WebServer Web管理服务器
type WebServer struct {
	mux       *http.ServeMux
	apiServer *v1.APIServer
	metrics   *monitoring.MetricsServer
	templates *template.Template
	server    *http.Server
}

// NewWebServer 创建Web服务器
func NewWebServer(addr, brokerAddr, nameserverAddr string, metricsCollector *monitoring.MetricsCollector) (*WebServer, error) {
	// 创建模板函数映射
	funcMap := template.FuncMap{
		"formatTime": func(t int64) string {
			if t == 0 {
				return "Never"
			}
			return time.Unix(t/1000, 0).Format("2006-01-02 15:04:05")
		},
		"formatBytes": func(bytes int64) string {
			const unit = 1024
			if bytes < unit {
				return fmt.Sprintf("%d B", bytes)
			}
			div, exp := int64(unit), 0
			for n := bytes / unit; n >= unit; n /= unit {
				div *= unit
				exp++
			}
			return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
		},
		"add": func(a, b int) int {
			return a + b
		},
	}

	// 解析模板 - 先解析 base.html，然后解析其他模板
	templates := template.New("").Funcs(funcMap)

	// 先解析 base.html
	baseContent, err := webFS.ReadFile("templates/base.html")
	if err != nil {
		return nil, fmt.Errorf("failed to read base.html: %v", err)
	}
	templates, err = templates.Parse(string(baseContent))
	if err != nil {
		return nil, fmt.Errorf("failed to parse base.html: %v", err)
	}

	// 解析其他模板
	templates, err = templates.ParseFS(webFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse templates: %v", err)
	}

	// 创建API服务器
	apiServer := v1.NewAPIServer(brokerAddr, nameserverAddr)

	// 创建指标服务器，使用传入的 MetricsCollector
	metricsServer := monitoring.NewMetricsServer("0.0.0.0:9090", metricsCollector)

	ws := &WebServer{
		mux:       http.NewServeMux(),
		apiServer: apiServer,
		metrics:   metricsServer,
		templates: templates,
		server:    &http.Server{Addr: addr, Handler: nil},
	}

	ws.setupRoutes()
	return ws, nil
}

// setupRoutes 设置路由
func (ws *WebServer) setupRoutes() {
	// API路由
	ws.mux.Handle("/api/", ws.apiServer)

	// 指标路由
	ws.mux.Handle("/metrics", ws.metrics.Handler())

	// Web界面路由
	ws.mux.HandleFunc("/", ws.dashboard)
	ws.mux.HandleFunc("/brokers", ws.brokers)
	ws.mux.HandleFunc("/topics", ws.topics)
	ws.mux.HandleFunc("/consumers", ws.consumers)
	ws.mux.HandleFunc("/monitoring", ws.monitoring)

	// 静态文件
	ws.mux.Handle("/static/", http.FileServer(http.FS(webFS)))
}

// ServeHTTP 实现http.Handler接口
func (ws *WebServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws.mux.ServeHTTP(w, r)
}

// dashboard 仪表板页面
func (ws *WebServer) dashboard(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":           "LiteMQ Dashboard",
		"ContentTemplate": "dashboard.html",
		"Stats": map[string]interface{}{
			"Brokers":   2,
			"Topics":    5,
			"Messages":  1000,
			"Consumers": 3,
		},
	}

	ws.renderTemplate(w, "dashboard.html", data)
}

// brokers Broker管理页面
func (ws *WebServer) brokers(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":           "Brokers",
		"ContentTemplate": "brokers.html",
		"Brokers": []map[string]interface{}{
			{
				"ID":       "broker-1",
				"Address":  "localhost:10911",
				"Status":   "online",
				"Topics":   3,
				"Messages": 500,
			},
			{
				"ID":       "broker-2",
				"Address":  "localhost:10912",
				"Status":   "online",
				"Topics":   2,
				"Messages": 300,
			},
		},
	}

	ws.renderTemplate(w, "brokers.html", data)
}

// topics 主题管理页面
func (ws *WebServer) topics(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":           "Topics",
		"ContentTemplate": "topics.html",
		"Topics": []map[string]interface{}{
			{
				"Name":        "test_topic",
				"Partitions":  4,
				"Messages":    100,
				"Consumers":   2,
				"LastUpdated": "2024-01-01 12:00:00",
			},
			{
				"Name":        "order_topic",
				"Partitions":  8,
				"Messages":    200,
				"Consumers":   1,
				"LastUpdated": "2024-01-01 12:30:00",
			},
		},
	}

	ws.renderTemplate(w, "topics.html", data)
}

// consumers 消费者管理页面
func (ws *WebServer) consumers(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":           "Consumers",
		"ContentTemplate": "consumers.html",
		"ConsumerGroups": []map[string]interface{}{
			{
				"GroupName": "test_group",
				"Consumers": 2,
				"Topics":    []string{"test_topic"},
				"Status":    "active",
				"Lag":       0,
			},
			{
				"GroupName": "order_group",
				"Consumers": 1,
				"Topics":    []string{"order_topic"},
				"Status":    "active",
				"Lag":       5,
			},
		},
	}

	ws.renderTemplate(w, "consumers.html", data)
}

// monitoring 监控页面
func (ws *WebServer) monitoring(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Title":           "Monitoring",
		"ContentTemplate": "monitoring.html",
		"MetricsURL":      "/metrics",
		"Stats":           ws.metrics.GetCollector().GetMetricsSummary(),
	}

	ws.renderTemplate(w, "monitoring.html", data)
}

// renderTemplate 渲染模板
func (ws *WebServer) renderTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// 如果模板名不包含 .html，添加它
	if !strings.HasSuffix(tmpl, ".html") {
		tmpl = tmpl + ".html"
	}

	// 先尝试执行 base.html（它会包含 content 模板）
	// 如果失败，尝试直接执行指定的模板
	if err := ws.templates.ExecuteTemplate(w, "base.html", data); err != nil {
		// 如果 base.html 执行失败，尝试直接执行模板
		if err2 := ws.templates.ExecuteTemplate(w, tmpl, data); err2 != nil {
			logger.Error("Failed to render template",
				"template", tmpl,
				"error1", err,
				"error2", err2)
			http.Error(w, fmt.Sprintf("Template error: %v (fallback: %v)", err, err2), http.StatusInternalServerError)
			return
		}
	}
}

// Start 启动Web服务器
func (ws *WebServer) Start() error {
	ws.server.Handler = ws

	go func() {
		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Start 启动Web服务器异常", "error", err)
			panic(err)
		}
	}()

	return nil
}

// GetAPIServer 获取API服务器
func (ws *WebServer) GetAPIServer() *v1.APIServer {
	return ws.apiServer
}

// GetMetricsServer 获取指标服务器
func (ws *WebServer) GetMetricsServer() *monitoring.MetricsServer {
	return ws.metrics
}
