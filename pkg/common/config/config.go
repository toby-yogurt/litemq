package config

import (
	"fmt"
	"io"
	"os"
	"time"

	"litemq/pkg/common/logger"

	"github.com/pelletier/go-toml/v2"
)

// Role 定义Broker角色
type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

// ReplicationMode 定义复制模式
type ReplicationMode string

const (
	ReplicationModeSync  ReplicationMode = "sync"
	ReplicationModeAsync ReplicationMode = "async"
)

// FlushMode 定义刷新模式
type FlushMode string

const (
	FlushModeSync  FlushMode = "sync"
	FlushModeAsync FlushMode = "async"
)

// LogConfig 日志配置（与 logger.Config 保持一致）
type LogConfig = logger.Config

// NameServerConfig NameServer配置
type NameServerConfig struct {
	// 网络配置
	Host string `toml:"host"`
	Port int    `toml:"port"`

	// 心跳配置
	HeartbeatInterval time.Duration `toml:"heartbeat_interval"` // Broker心跳间隔
	HeartbeatTimeout  time.Duration `toml:"heartbeat_timeout"`  // 心跳超时时间

	// 路由配置
	RouteUpdateInterval time.Duration `toml:"route_update_interval"` // 路由更新间隔

	// 存储配置
	DataPath string `toml:"data_path"` // 数据存储路径

	// 日志配置
	Log LogConfig `toml:"log"`
}

// BrokerConfig Broker配置
type BrokerConfig struct {
	// 基本配置
	BrokerID   string `toml:"broker_id"`
	BrokerName string `toml:"broker_name"`

	// 网络配置
	Host string `toml:"host"`
	Port int    `toml:"port"`

	// NameServer配置
	NameServers []string `toml:"nameservers"`

	// 主从配置
	Role            Role            `toml:"role"`
	MasterAddr      string          `toml:"master_addr"`
	SlaveID         int             `toml:"slave_id"`
	ReplicationMode ReplicationMode `toml:"replication_mode"`

	// 存储配置
	DataPath      string        `toml:"data_path"`
	FlushMode     FlushMode     `toml:"flush_mode"`
	FlushInterval time.Duration `toml:"flush_interval"`
	SyncFlush     bool          `toml:"sync_flush"`
	SyncReplicas  int           `toml:"sync_replicas"`

	// CommitLog配置
	CommitLog struct {
		FileSize int64 `toml:"file_size"` // 文件大小，默认1GB
		MaxFiles int   `toml:"max_files"` // 最大文件数
	} `toml:"commitlog"`

	// ConsumeQueue配置
	ConsumeQueue struct {
		FileSize int64 `toml:"file_size"` // 文件大小，默认600MB
		MaxFiles int   `toml:"max_files"` // 最大文件数
	} `toml:"consumequeue"`

	// 消息配置
	MaxMessageSize int `toml:"max_message_size"` // 最大消息大小，默认4MB

	// 心跳配置
	Heartbeat struct {
		Interval time.Duration `toml:"interval"` // 心跳间隔
	} `toml:"heartbeat"`

	// 消费者配置
	MaxConsumerConnections int `toml:"max_consumer_connections"` // 最大消费者连接数

	// 监控配置
	MetricsEnabled bool   `toml:"metrics_enabled"`
	MetricsPort    int    `toml:"metrics_port"`
	MetricsPath    string `toml:"metrics_path"`

	// 日志配置
	Log LogConfig `toml:"log"`
}

// ClientConfig 客户端配置
type ClientConfig struct {
	// NameServer配置
	NameServers []string `toml:"nameservers"`

	// 连接配置
	ConnectTimeout time.Duration `toml:"connect_timeout"`
	ReadTimeout    time.Duration `toml:"read_timeout"`
	WriteTimeout   time.Duration `toml:"write_timeout"`

	// 重试配置
	MaxRetries    int           `toml:"max_retries"`
	RetryInterval time.Duration `toml:"retry_interval"`

	// 批量配置
	BatchSize    int           `toml:"batch_size"`
	BatchTimeout time.Duration `toml:"batch_timeout"`

	// 消费者配置
	GroupName        string        `toml:"group_name"`
	ConsumeFromWhere string        `toml:"consume_from_where"` // first, last, timestamp
	ConsumeTimestamp int64         `toml:"consume_timestamp"`
	PullBatchSize    int           `toml:"pull_batch_size"`
	PullInterval     time.Duration `toml:"pull_interval"`
	ConsumeTimeout   time.Duration `toml:"consume_timeout"`
}

// DefaultNameServerConfig 返回默认NameServer配置
func DefaultNameServerConfig() *NameServerConfig {
	return &NameServerConfig{
		Host:                "0.0.0.0",
		Port:                9876,
		HeartbeatInterval:   30 * time.Second,
		HeartbeatTimeout:    90 * time.Second,
		RouteUpdateInterval: 30 * time.Second,
		DataPath:            "./data/nameserver",
		Log: LogConfig{
			Level:      "info",
			Format:     "text",
			Output:     "stdout",
			FilePath:   "./logs/nameserver.log",
			MaxSize:    100,
			MaxAge:     30,
			MaxBackups: 10,
			Compress:   true,
		},
	}
}

// DefaultBrokerConfig 返回默认Broker配置
func DefaultBrokerConfig() *BrokerConfig {
	config := &BrokerConfig{
		BrokerID:        "broker-1",
		BrokerName:      "broker-1",
		Host:            "0.0.0.0",
		Port:            10911,
		NameServers:     []string{"localhost:9876"},
		Role:            RoleMaster,
		MasterAddr:      "",
		SlaveID:         1,
		ReplicationMode: ReplicationModeAsync,
		DataPath:        "./data/broker",
		FlushMode:       FlushModeAsync,
		FlushInterval:   1 * time.Second,
		SyncFlush:       false,
		SyncReplicas:    1,
		MaxMessageSize:  4 * 1024 * 1024, // 4MB
		Heartbeat: struct {
			Interval time.Duration `toml:"interval"`
		}{
			Interval: 30 * time.Second,
		},
		MaxConsumerConnections: 1000,
		MetricsEnabled:         true,
		MetricsPort:            8080,
		MetricsPath:            "/metrics",
		Log: LogConfig{
			Level:      "info",
			Format:     "text",
			Output:     "stdout",
			FilePath:   "./logs/broker.log",
			MaxSize:    100,
			MaxAge:     30,
			MaxBackups: 10,
			Compress:   true,
		},
	}

	// CommitLog默认配置
	config.CommitLog.FileSize = 1 * 1024 * 1024 * 1024 // 1GB
	config.CommitLog.MaxFiles = 100

	// ConsumeQueue默认配置
	config.ConsumeQueue.FileSize = 600 * 1024 * 1024 // 600MB
	config.ConsumeQueue.MaxFiles = 100

	return config
}

// DefaultClientConfig 返回默认客户端配置
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		NameServers:      []string{"localhost:9876"},
		ConnectTimeout:   3 * time.Second,
		ReadTimeout:      3 * time.Second,
		WriteTimeout:     3 * time.Second,
		MaxRetries:       3,
		RetryInterval:    1 * time.Second,
		BatchSize:        100,
		BatchTimeout:     1 * time.Second,
		GroupName:        "default_group",
		ConsumeFromWhere: "last",
		ConsumeTimestamp: 0,
		PullBatchSize:    32,
		PullInterval:     0,
		ConsumeTimeout:   15 * time.Second,
	}
}

// LoadNameServerConfig 加载NameServer配置
func LoadNameServerConfig(filename string) (*NameServerConfig, error) {
	config := DefaultNameServerConfig()
	if err := loadConfig(filename, config); err != nil {
		return nil, err
	}
	return config, nil
}

// LoadBrokerConfig 加载Broker配置
func LoadBrokerConfig(filename string) (*BrokerConfig, error) {
	config := DefaultBrokerConfig()
	if err := loadConfig(filename, config); err != nil {
		return nil, err
	}
	return config, nil
}

// LoadClientConfig 加载客户端配置
func LoadClientConfig(filename string) (*ClientConfig, error) {
	config := DefaultClientConfig()
	if err := loadConfig(filename, config); err != nil {
		return nil, err
	}
	return config, nil
}

// loadConfig 通用配置加载函数
func loadConfig(filename string, config interface{}) error {
	if filename == "" {
		return nil // 使用默认配置
	}

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open config file %s: %v", filename, err)
	}
	defer file.Close()

	// 使用toml解析配置
	if err := loadFromTOML(file, config); err != nil {
		return fmt.Errorf("failed to parse config file %s: %v", filename, err)
	}

	return nil
}

// loadFromTOML 从TOML文件加载配置
func loadFromTOML(file *os.File, config interface{}) error {
	// 读取文件内容
	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// 使用toml库解析配置文件
	if err := toml.Unmarshal(data, config); err != nil {
		return fmt.Errorf("failed to decode TOML: %v", err)
	}
	return nil
}

// ValidateNameServerConfig 验证NameServer配置
func ValidateNameServerConfig(config *NameServerConfig) error {
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d", config.Port)
	}
	if config.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	if config.HeartbeatTimeout <= 0 {
		return fmt.Errorf("heartbeat_timeout must be positive")
	}
	if config.HeartbeatTimeout <= config.HeartbeatInterval {
		return fmt.Errorf("heartbeat_timeout must be greater than heartbeat_interval")
	}
	return nil
}

// ValidateBrokerConfig 验证Broker配置
func ValidateBrokerConfig(config *BrokerConfig) error {
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d", config.Port)
	}
	if len(config.NameServers) == 0 {
		return fmt.Errorf("nameservers cannot be empty")
	}
	if config.Role == RoleSlave && config.MasterAddr == "" {
		return fmt.Errorf("master_addr is required for slave broker")
	}
	if config.MaxMessageSize <= 0 {
		return fmt.Errorf("max_message_size must be positive")
	}
	if config.Heartbeat.Interval <= 0 {
		return fmt.Errorf("heartbeat.interval must be positive")
	}
	return nil
}

// ValidateClientConfig 验证客户端配置
func ValidateClientConfig(config *ClientConfig) error {
	if len(config.NameServers) == 0 {
		return fmt.Errorf("nameservers cannot be empty")
	}
	if config.ConnectTimeout <= 0 {
		return fmt.Errorf("connect_timeout must be positive")
	}
	return nil
}
