package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/monitoring"
	"litemq/pkg/nameserver"
	"litemq/web"
)

var (
	configFile = flag.String("config", "./configs/nameserver.toml", "Path to config file")
	host       = flag.String("host", "0.0.0.0", "NameServer host")
	port       = flag.Int("port", 9876, "NameServer port")
)

func main() {
	flag.Parse()

	logger.Info("=== LiteMQ NameServer ===")

	// 加载配置
	cfg := config.DefaultNameServerConfig()

	// 如果指定了配置文件，尝试加载
	if *configFile != "" {
		if loadedCfg, err := config.LoadNameServerConfig(*configFile); err != nil {
			log.Printf("Warning: failed to load config from %s: %v, using default config", *configFile, err)
		} else {
			cfg = loadedCfg
		}
	}

	// 命令行参数覆盖配置文件
	if *host != "0.0.0.0" {
		cfg.Host = *host
	}
	if *port != 9876 {
		cfg.Port = *port
	}

	// 验证配置
	if err := config.ValidateNameServerConfig(cfg); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// 初始化日志器
	if err := logger.InitDefaultLogger(&cfg.Log); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	logger.Info("Starting LiteMQ NameServer",
		"host", cfg.Host,
		"port", cfg.Port,
		"data_path", cfg.DataPath)

	// 创建NameServer实例
	ns := nameserver.NewNameServer(cfg)

	// 启动NameServer
	if err := ns.Start(); err != nil {
		log.Fatalf("Failed to start NameServer: %v", err)
	}

	// 启动Web管理界面（避免与 Broker 默认 8080 冲突，使用 8081）
	nameserverAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	webAddr := ":8081"

	// 创建NameServer专用的MetricsCollector
	metricsCollector := monitoring.NewMetricsCollector()
	webServer, err := web.NewWebServer(webAddr, "localhost:10911", nameserverAddr, metricsCollector)
	if err != nil {
		log.Printf("Warning: Failed to create web server: %v", err)
	} else {
		if err := webServer.Start(); err != nil {
			log.Printf("Warning: Failed to start web server: %v", err)
		} else {
			logger.Info("Web management interface started",
				"addr", webAddr)
		}
	}

	logger.Info("NameServer started successfully",
		"host", cfg.Host,
		"port", cfg.Port)
	logger.Info("Press Ctrl+C to stop...")

	// 等待中断信号
	waitForShutdown(ns)
}

func waitForShutdown(ns *nameserver.NameServer) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Received shutdown signal, stopping NameServer...")

	if err := ns.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
		os.Exit(1)
	}

	logger.Info("NameServer stopped successfully")
}
