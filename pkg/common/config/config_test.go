package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadBrokerConfig(t *testing.T) {
	// 创建临时TOML配置文件
	tomlContent := `
broker_id = "test-broker"
broker_name = "test-broker"

[network]
host = "127.0.0.1"
port = 9092

nameservers = ["localhost:9876"]

[replication]
role = "master"
master_addr = ""
slave_id = 1
mode = "async"

[storage]
data_path = "./data/test"
flush_mode = "async"
flush_interval = "2s"

[commitlog]
file_size = 536870912
max_files = 50

[consumequeue]
file_size = 314572800
max_files = 50

max_message_size = 2097152

[heartbeat]
interval = "45s"

max_consumer_connections = 500

[metrics]
enabled = false
port = 9090
path = "/test-metrics"
`

	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "broker_config_*.toml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(tomlContent)
	assert.NoError(t, err)
	tmpFile.Close()

	// 加载配置
	config, err := LoadBrokerConfig(tmpFile.Name())
	assert.NoError(t, err)

	// 验证配置值
	assert.Equal(t, "test-broker", config.BrokerID)
	assert.Equal(t, "test-broker", config.BrokerName)
	assert.Equal(t, "127.0.0.1", config.Host)
	assert.Equal(t, 9092, config.Port)
	assert.Equal(t, []string{"localhost:9876"}, config.NameServers)
	assert.Equal(t, RoleMaster, config.Role)
	assert.Equal(t, ReplicationModeAsync, config.ReplicationMode)
	assert.Equal(t, "./data/test", config.DataPath)
	assert.Equal(t, FlushModeAsync, config.FlushMode)
	assert.Equal(t, 2*time.Second, config.FlushInterval)
	assert.Equal(t, int64(536870912), config.CommitLog.FileSize)
	assert.Equal(t, 50, config.CommitLog.MaxFiles)
	assert.Equal(t, int64(314572800), config.ConsumeQueue.FileSize)
	assert.Equal(t, 50, config.ConsumeQueue.MaxFiles)
	assert.Equal(t, 2097152, config.MaxMessageSize)
	assert.Equal(t, 45*time.Second, config.Heartbeat.Interval)
	assert.Equal(t, 500, config.MaxConsumerConnections)
	assert.Equal(t, false, config.MetricsEnabled)
	assert.Equal(t, 9090, config.MetricsPort)
	assert.Equal(t, "/test-metrics", config.MetricsPath)
}

func TestLoadNameServerConfig(t *testing.T) {
	// 创建临时TOML配置文件
	tomlContent := `
host = "127.0.0.1"
port = 9876
heartbeat_interval = "40s"
heartbeat_timeout = "120s"
route_update_interval = "40s"
data_path = "./data/nameserver-test"
`

	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "nameserver_config_*.toml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(tomlContent)
	assert.NoError(t, err)
	tmpFile.Close()

	// 加载配置
	config, err := LoadNameServerConfig(tmpFile.Name())
	assert.NoError(t, err)

	// 验证配置值
	assert.Equal(t, "127.0.0.1", config.Host)
	assert.Equal(t, 9876, config.Port)
	assert.Equal(t, 40*time.Second, config.HeartbeatInterval)
	assert.Equal(t, 120*time.Second, config.HeartbeatTimeout)
	assert.Equal(t, 40*time.Second, config.RouteUpdateInterval)
	assert.Equal(t, "./data/nameserver-test", config.DataPath)
}

func TestLoadClientConfig(t *testing.T) {
	// 创建临时TOML配置文件
	tomlContent := `
nameservers = ["localhost:9876", "localhost:9877"]
connect_timeout = "5s"
read_timeout = "10s"
write_timeout = "5s"
max_retries = 5
retry_interval = "2s"
batch_size = 200
batch_timeout = "3s"
group_name = "test-group"
consume_from_where = "last"
consume_timestamp = 1234567890
pull_batch_size = 64
pull_interval = "1s"
consume_timeout = "30s"
`

	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "client_config_*.toml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(tomlContent)
	assert.NoError(t, err)
	tmpFile.Close()

	// 加载配置
	config, err := LoadClientConfig(tmpFile.Name())
	assert.NoError(t, err)

	// 验证配置值
	assert.Equal(t, []string{"localhost:9876", "localhost:9877"}, config.NameServers)
	assert.Equal(t, 5*time.Second, config.ConnectTimeout)
	assert.Equal(t, 10*time.Second, config.ReadTimeout)
	assert.Equal(t, 5*time.Second, config.WriteTimeout)
	assert.Equal(t, 5, config.MaxRetries)
	assert.Equal(t, 2*time.Second, config.RetryInterval)
	assert.Equal(t, 200, config.BatchSize)
	assert.Equal(t, 3*time.Second, config.BatchTimeout)
	assert.Equal(t, "test-group", config.GroupName)
	assert.Equal(t, "last", config.ConsumeFromWhere)
	assert.Equal(t, int64(1234567890), config.ConsumeTimestamp)
	assert.Equal(t, 64, config.PullBatchSize)
	assert.Equal(t, time.Second, config.PullInterval)
	assert.Equal(t, 30*time.Second, config.ConsumeTimeout)
}

func TestDefaultConfigs(t *testing.T) {
	// 测试默认配置
	brokerCfg := DefaultBrokerConfig()
	assert.NotNil(t, brokerCfg)
	assert.Equal(t, "broker-1", brokerCfg.BrokerID)
	assert.Equal(t, "0.0.0.0", brokerCfg.Host)
	assert.Equal(t, 10911, brokerCfg.Port)

	nameServerCfg := DefaultNameServerConfig()
	assert.NotNil(t, nameServerCfg)
	assert.Equal(t, "0.0.0.0", nameServerCfg.Host)
	assert.Equal(t, 9876, nameServerCfg.Port)

	clientCfg := DefaultClientConfig()
	assert.NotNil(t, clientCfg)
	assert.Equal(t, []string{"localhost:9876"}, clientCfg.NameServers)
	assert.Equal(t, "default_group", clientCfg.GroupName)
}
