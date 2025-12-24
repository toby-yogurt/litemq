package broker

import (
	"fmt"
	"sync"
	"time"

	"litemq/pkg/common/logger"
)

// HeartbeatService 心跳服务
type HeartbeatService struct {
	nameServerClient *NameServerClient
	interval         time.Duration
	running          bool
	stopCh           chan struct{}
	mutex            sync.RWMutex
	wg               sync.WaitGroup
}

// NewHeartbeatService 创建心跳服务
func NewHeartbeatService(nameServerClient *NameServerClient, interval time.Duration) *HeartbeatService {
	return &HeartbeatService{
		nameServerClient: nameServerClient,
		interval:         interval,
		running:          false,
		stopCh:           make(chan struct{}),
	}
}

// Start 启动心跳服务
func (hs *HeartbeatService) Start() error {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	if hs.running {
		return fmt.Errorf("heartbeat service is already running")
	}

	hs.running = true

	// 启动心跳循环
	hs.wg.Add(1)
	go hs.heartbeatLoop()

	logger.Info("Heartbeat service started", "interval", hs.interval)
	return nil
}

// Stop 停止心跳服务
func (hs *HeartbeatService) Stop() {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	if !hs.running {
		return
	}

	hs.running = false
	close(hs.stopCh)

	hs.wg.Wait()
	logger.Info("Heartbeat service stopped")
}

// heartbeatLoop 心跳循环
func (hs *HeartbeatService) heartbeatLoop() {
	defer hs.wg.Done()

	ticker := time.NewTicker(hs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hs.stopCh:
			return
		case <-ticker.C:
			if err := hs.sendHeartbeat(); err != nil {
				logger.Warn("Failed to send heartbeat", "error", err)
			}
		}
	}
}

// sendHeartbeat 发送心跳
func (hs *HeartbeatService) sendHeartbeat() error {
	return hs.nameServerClient.SendHeartbeat()
}

// IsRunning 检查是否正在运行
func (hs *HeartbeatService) IsRunning() bool {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()
	return hs.running
}

// GetInterval 获取心跳间隔
func (hs *HeartbeatService) GetInterval() time.Duration {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()
	return hs.interval
}

// SetInterval 设置心跳间隔
func (hs *HeartbeatService) SetInterval(interval time.Duration) {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	if interval <= 0 {
		interval = 30 * time.Second // 默认30秒
	}
	hs.interval = interval
}
