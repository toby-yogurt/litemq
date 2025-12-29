package nameserver

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
)

// HeartbeatInfo 心跳信息
type HeartbeatInfo struct {
	BrokerID       string    `json:"broker_id"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	HeartbeatCount int64     `json:"heartbeat_count"`
	Status         string    `json:"status"` // online, offline, timeout
}

// HeartbeatManager 心跳管理器
type HeartbeatManager struct {
	heartbeatTimeout time.Duration
	checkInterval    time.Duration

	// 心跳信息: brokerID -> HeartbeatInfo
	heartbeats map[string]*HeartbeatInfo

	// 回调函数
	brokerOfflineCallback func(brokerID string)

	running bool
	stopCh  chan struct{}
	mutex   sync.RWMutex
	wg      sync.WaitGroup
}

// NewHeartbeatManager 创建心跳管理器
func NewHeartbeatManager(heartbeatTimeout time.Duration) *HeartbeatManager {
	return &HeartbeatManager{
		heartbeatTimeout: heartbeatTimeout,
		checkInterval:    heartbeatTimeout / 3, // 检查间隔为超时的1/3
		heartbeats:       make(map[string]*HeartbeatInfo),
		running:          false,
		stopCh:           make(chan struct{}),
	}
}

// Start 启动心跳管理器
func (hm *HeartbeatManager) Start() error {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.running {
		return fmt.Errorf("heartbeat manager is already running")
	}

	hm.running = true

	// 启动心跳检查循环
	hm.wg.Add(1)
	go hm.checkLoop()

	logger.Info("Heartbeat manager started",
		"timeout", hm.heartbeatTimeout,
		"check_interval", hm.checkInterval)

	return nil
}

// Stop 停止心跳管理器
func (hm *HeartbeatManager) Stop() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if !hm.running {
		return
	}

	hm.running = false
	close(hm.stopCh)

	hm.wg.Wait()
	logger.Info("Heartbeat manager stopped")
}

// checkLoop 心跳检查循环
func (hm *HeartbeatManager) checkLoop() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.stopCh:
			return
		case <-ticker.C:
			hm.checkHeartbeats()
		}
	}
}

// checkHeartbeats 检查心跳超时
func (hm *HeartbeatManager) checkHeartbeats() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	now := time.Now()
	timeoutBrokers := make([]string, 0)

	for brokerID, info := range hm.heartbeats {
		if info.Status == "online" && now.Sub(info.LastHeartbeat) > hm.heartbeatTimeout {
			// 心跳超时
			info.Status = "timeout"
			timeoutBrokers = append(timeoutBrokers, brokerID)
			logger.Info("Broker heartbeat timeout",
				"broker_id", brokerID,
				"last_heartbeat", info.LastHeartbeat,
				"now", now)
		}
	}

	// 处理超时Broker
	for _, brokerID := range timeoutBrokers {
		hm.handleBrokerTimeout(brokerID)
	}
}

// UpdateHeartbeat 更新心跳
func (hm *HeartbeatManager) UpdateHeartbeat(brokerID string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	info, exists := hm.heartbeats[brokerID]
	if !exists {
		// 新Broker
		info = &HeartbeatInfo{
			BrokerID:       brokerID,
			LastHeartbeat:  time.Now(),
			HeartbeatCount: 1,
			Status:         "online",
		}
		hm.heartbeats[brokerID] = info
		logger.Info("New broker heartbeat", "broker_id", brokerID)
	} else {
		// 现有Broker
		if info.Status != "online" {
			logger.Info("Broker back online", "broker_id", brokerID)
		}
		info.LastHeartbeat = time.Now()
		info.HeartbeatCount++
		info.Status = "online"
	}
}

// RegisterBroker 注册Broker
func (hm *HeartbeatManager) RegisterBroker(brokerID string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if _, exists := hm.heartbeats[brokerID]; !exists {
		info := &HeartbeatInfo{
			BrokerID:       brokerID,
			LastHeartbeat:  time.Now(),
			HeartbeatCount: 0,
			Status:         "registered",
		}
		hm.heartbeats[brokerID] = info
		logger.Info("Broker registered for heartbeat", "broker_id", brokerID)
	}
}

// UnregisterBroker 注销Broker
func (hm *HeartbeatManager) UnregisterBroker(brokerID string) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if info, exists := hm.heartbeats[brokerID]; exists {
		info.Status = "offline"
		logger.Info("Broker unregistered from heartbeat", "broker_id", brokerID)
	}
}

// GetHeartbeatInfo 获取心跳信息
func (hm *HeartbeatManager) GetHeartbeatInfo(brokerID string) (*HeartbeatInfo, bool) {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	info, exists := hm.heartbeats[brokerID]
	return info, exists
}

// GetAllHeartbeatInfos 获取所有心跳信息
func (hm *HeartbeatManager) GetAllHeartbeatInfos() []*HeartbeatInfo {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	infos := make([]*HeartbeatInfo, 0, len(hm.heartbeats))
	for _, info := range hm.heartbeats {
		infos = append(infos, info)
	}

	return infos
}

// GetOnlineBrokers 获取在线Broker列表
func (hm *HeartbeatManager) GetOnlineBrokers() []string {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	online := make([]string, 0)
	for brokerID, info := range hm.heartbeats {
		if info.Status == "online" {
			online = append(online, brokerID)
		}
	}

	return online
}

// GetOfflineBrokers 获取离线Broker列表
func (hm *HeartbeatManager) GetOfflineBrokers() []string {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	offline := make([]string, 0)
	for brokerID, info := range hm.heartbeats {
		if info.Status == "offline" || info.Status == "timeout" {
			offline = append(offline, brokerID)
		}
	}

	return offline
}

// handleBrokerTimeout 处理Broker超时
func (hm *HeartbeatManager) handleBrokerTimeout(brokerID string) {
	logger.Warn("Handling broker timeout", "broker_id", brokerID)

	// 调用离线回调
	if hm.brokerOfflineCallback != nil {
		go hm.brokerOfflineCallback(brokerID)
	}
}

// SetBrokerOfflineCallback 设置Broker离线回调
func (hm *HeartbeatManager) SetBrokerOfflineCallback(callback func(brokerID string)) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.brokerOfflineCallback = callback
}

// CleanExpiredHeartbeats 清理过期心跳信息
func (hm *HeartbeatManager) CleanExpiredHeartbeats(maxAge time.Duration) []string {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	now := time.Now()
	expired := make([]string, 0)

	for brokerID, info := range hm.heartbeats {
		if info.Status == "offline" && now.Sub(info.LastHeartbeat) > maxAge {
			expired = append(expired, brokerID)
		}
	}

	// 删除过期心跳信息
	for _, brokerID := range expired {
		delete(hm.heartbeats, brokerID)
		logger.Info("Cleaned expired heartbeat", "broker_id", brokerID)
	}

	return expired
}

// GetHeartbeatStats 获取心跳统计信息
type HeartbeatStats struct {
	TotalBrokers   int `json:"total_brokers"`
	OnlineBrokers  int `json:"online_brokers"`
	OfflineBrokers int `json:"offline_brokers"`
	TimeoutBrokers int `json:"timeout_brokers"`
}

// GetHeartbeatStats 获取心跳统计信息
func (hm *HeartbeatManager) GetHeartbeatStats() *HeartbeatStats {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	stats := &HeartbeatStats{}
	for _, info := range hm.heartbeats {
		stats.TotalBrokers++
		switch info.Status {
		case "online":
			stats.OnlineBrokers++
		case "offline":
			stats.OfflineBrokers++
		case "timeout":
			stats.TimeoutBrokers++
		}
	}

	return stats
}

// IsRunning 检查心跳管理器是否正在运行
func (hm *HeartbeatManager) IsRunning() bool {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return hm.running
}

// SetHeartbeatTimeout 设置心跳超时时间
func (hm *HeartbeatManager) SetHeartbeatTimeout(timeout time.Duration) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if timeout <= 0 {
		timeout = 90 * time.Second // 默认90秒
	}
	hm.heartbeatTimeout = timeout
	hm.checkInterval = timeout / 3
}

// GetHeartbeatTimeout 获取心跳超时时间
func (hm *HeartbeatManager) GetHeartbeatTimeout() time.Duration {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return hm.heartbeatTimeout
}

// PrintHeartbeatInfo 打印心跳信息（调试用）
func (hm *HeartbeatManager) PrintHeartbeatInfo() {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	logger.Info("Heartbeat Information ---")
	logger.Info("Heartbeat totals", "total_brokers", len(hm.heartbeats))

	stats := hm.GetHeartbeatStats()
	logger.Info("Heartbeat status",
		"online", stats.OnlineBrokers,
		"offline", stats.OfflineBrokers,
		"timeout", stats.TimeoutBrokers)

	for brokerID, info := range hm.heartbeats {
		logger.Info("Heartbeat detail",
			"broker_id", brokerID,
			"status", info.Status,
			"last", info.LastHeartbeat,
			"count", info.HeartbeatCount)
	}
	logger.Info("Heartbeat Information --- end ---")
}
