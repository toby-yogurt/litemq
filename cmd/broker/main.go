package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"litemq/pkg/broker"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/monitoring"
	"litemq/web"
)

var (
	configFile  = flag.String("config", "./configs/broker.toml", "Path to config file")
	brokerID    = flag.String("id", "", "Broker ID")
	brokerName  = flag.String("name", "", "Broker name")
	host        = flag.String("host", "0.0.0.0", "Broker host")
	port        = flag.Int("port", 10911, "Broker port")
	nameServers = flag.String("nameservers", "localhost:9876", "NameServer addresses (comma separated)")
	role        = flag.String("role", "", "Broker role (master/slave)")
	dataPath    = flag.String("data-path", "", "Data storage path")
	flushMode   = flag.String("flush-mode", "", "Flush mode (sync/async)")
)

func main() {
	flag.Parse()

	logger.Info("=== LiteMQ Broker ===")

	// 加载配置
	cfg := config.DefaultBrokerConfig()

	// 如果指定了配置文件，尝试加载
	if *configFile != "" {
		if loadedCfg, err := config.LoadBrokerConfig(*configFile); err != nil {
			log.Printf("Warning: failed to load config from %s: %v, using default config", *configFile, err)
		} else {
			cfg = loadedCfg
		}
	}

	// 命令行参数覆盖配置文件
	if *brokerID != "" {
		cfg.BrokerID = *brokerID
	}
	if *brokerName != "" {
		cfg.BrokerName = *brokerName
	}
	if *host != "0.0.0.0" {
		cfg.Host = *host
	}
	if *port != 10911 {
		cfg.Port = *port
	}
	if *nameServers != "localhost:9876" {
		cfg.NameServers = strings.Split(*nameServers, ",")
		for i := range cfg.NameServers {
			cfg.NameServers[i] = strings.TrimSpace(cfg.NameServers[i])
		}
	}
	if *role != "" {
		if *role == "master" {
			cfg.Role = config.RoleMaster
		} else if *role == "slave" {
			cfg.Role = config.RoleSlave
		}
	}
	if *dataPath != "" {
		cfg.DataPath = *dataPath
	}
	if *flushMode != "" {
		if *flushMode == "sync" {
			cfg.FlushMode = config.FlushModeSync
		} else if *flushMode == "async" {
			cfg.FlushMode = config.FlushModeAsync
		}
	}

	// 验证配置
	if err := config.ValidateBrokerConfig(cfg); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	logger.Info("Starting Broker with config",
		"broker_id", cfg.BrokerID,
		"host", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		"role", cfg.Role,
		"nameservers", cfg.NameServers,
		"data_path", cfg.DataPath,
		"flush_mode", cfg.FlushMode)

	// 创建Broker实例
	b, err := broker.NewBroker(cfg)
	if err != nil {
		log.Fatalf("Failed to create Broker: %v", err)
	}

	// 启动Broker
	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start Broker: %v", err)
	}

	// 启动Web管理界面
	brokerAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	webAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.MetricsPort)

	// 创建独立的MetricsCollector
	metricsCollector := monitoring.NewMetricsCollector()
	webServer, err := web.NewWebServer(webAddr, brokerAddr, "localhost:9876", metricsCollector)
	if err != nil {
		log.Printf("Warning: Failed to create web server: %v", err)
	} else {
		if err := webServer.Start(); err != nil {
			log.Printf("Warning: Failed to start web server: %v", err)
		} else {
			logger.Info("Web management interface started", "addr", webAddr)
		}
	}

	logger.Info("Broker started successfully",
		"broker_id", cfg.BrokerID,
		"host", cfg.Host,
		"port", cfg.Port)
	logger.Info("API available", "url", fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port))
	logger.Info("Press Ctrl+C to stop...")

	// 等待中断信号
	waitForShutdown(b)
}

func waitForShutdown(b *broker.Broker) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Received shutdown signal, stopping Broker...")

	if err := b.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
		os.Exit(1)
	}

	logger.Info("Broker stopped successfully")
}
