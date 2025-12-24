package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"litemq/pkg/common/logger"
	"litemq/pkg/monitoring"
	"litemq/web"
)

var (
	brokerAddr     = flag.String("broker", "localhost:10911", "Broker address")
	nameserverAddr = flag.String("nameserver", "localhost:9876", "NameServer address")
	host           = flag.String("host", "0.0.0.0", "Web server host")
	port           = flag.Int("port", 8088, "Web server port")
)

func main() {
	flag.Parse()

	logger.Info("=== LiteMQ Web Server ===")

	// 创建Web服务器
	serverAddr := fmt.Sprintf("%s:%d", *host, *port)

	// 创建独立的MetricsCollector
	metricsCollector := monitoring.NewMetricsCollector()
	webServer, err := web.NewWebServer(serverAddr, *brokerAddr, *nameserverAddr, metricsCollector)
	if err != nil {
		log.Fatalf("Failed to create web server: %v", err)
	}

	// 启动Web服务器
	if err := webServer.Start(); err != nil {
		log.Fatalf("Failed to start web server: %v", err)
	}

	logger.Info("Web management interface started", "url", fmt.Sprintf("http://%s", serverAddr))
	logger.Info("Available endpoints",
		"dashboard", fmt.Sprintf("http://%s/", serverAddr),
		"brokers", fmt.Sprintf("http://%s/brokers", serverAddr),
		"topics", fmt.Sprintf("http://%s/topics", serverAddr),
		"consumers", fmt.Sprintf("http://%s/consumers", serverAddr),
		"monitoring", fmt.Sprintf("http://%s/monitoring", serverAddr),
		"metrics", fmt.Sprintf("http://%s/metrics", serverAddr),
		"health", fmt.Sprintf("http://%s/health", serverAddr),
	)
	logger.Info("Press Ctrl+C to stop...")

	// 等待中断信号
	waitForShutdown()
}

func connectToService(addr string) (interface{}, error) {
	// 简单的连接测试
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	conn.Close()

	// 这里应该返回实际的服务连接
	// 暂时返回nil，表示连接成功但不返回实际对象
	return nil, nil
}

func waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Received shutdown signal, stopping web server...")

	logger.Info("Web server stopped successfully")
}
