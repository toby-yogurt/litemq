package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"litemq/pkg/common/logger"
)

// FlushMode 刷新模式
type FlushMode int

const (
	FlushModeSync  FlushMode = 0 // 同步刷新
	FlushModeAsync FlushMode = 1 // 异步刷新
)

// FlushService 刷新服务
type FlushService struct {
	name         string
	mode         FlushMode
	interval     time.Duration
	commitLog    *CommitLog
	consumeQueue *ConsumeQueueManager

	// 异步刷新相关
	running int32
	stopCh  chan struct{}
	flushCh chan FlushRequest
	wg      sync.WaitGroup

	// 统计信息
	flushCount    int64
	lastFlushTime time.Time
	mutex         sync.RWMutex
}

// FlushRequest 刷新请求
type FlushRequest struct {
	Force    bool          // 是否强制刷新
	Timeout  time.Duration // 超时时间
	ResultCh chan error    // 结果通道
}

// NewFlushService 创建刷新服务
func NewFlushService(name string, mode FlushMode, interval time.Duration,
	commitLog *CommitLog, consumeQueue *ConsumeQueueManager) *FlushService {

	fs := &FlushService{
		name:          name,
		mode:          mode,
		interval:      interval,
		commitLog:     commitLog,
		consumeQueue:  consumeQueue,
		running:       0,
		stopCh:        make(chan struct{}),
		flushCh:       make(chan FlushRequest, 1000), // 缓冲区大小
		flushCount:    0,
		lastFlushTime: time.Now(),
	}

	return fs
}

// Start 启动刷新服务
func (fs *FlushService) Start() error {
	if !atomic.CompareAndSwapInt32(&fs.running, 0, 1) {
		return fmt.Errorf("flush service %s is already running", fs.name)
	}

	if fs.mode == FlushModeAsync {
		fs.wg.Add(1)
		go fs.asyncFlushLoop()
	}

	return nil
}

// Stop 停止刷新服务
func (fs *FlushService) Stop() error {
	if !atomic.CompareAndSwapInt32(&fs.running, 1, 0) {
		return nil // 已经停止
	}

	close(fs.stopCh)

	// 等待异步刷新循环结束
	fs.wg.Wait()

	// 执行最后一次强制刷新
	return fs.flush(true)
}

// Flush 执行刷新
func (fs *FlushService) Flush() error {
	return fs.FlushWithTimeout(30 * time.Second)
}

// FlushWithTimeout 带超时的刷新
func (fs *FlushService) FlushWithTimeout(timeout time.Duration) error {
	if fs.mode == FlushModeSync {
		return fs.flush(true)
	}

	// 异步模式
	req := FlushRequest{
		Force:    true,
		Timeout:  timeout,
		ResultCh: make(chan error, 1),
	}

	select {
	case fs.flushCh <- req:
		// 发送请求成功
	case <-time.After(timeout):
		return fmt.Errorf("flush request timeout")
	case <-fs.stopCh:
		return fmt.Errorf("flush service stopped")
	}

	// 等待结果
	select {
	case err := <-req.ResultCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("flush result timeout")
	case <-fs.stopCh:
		return fmt.Errorf("flush service stopped")
	}
}

// asyncFlushLoop 异步刷新循环
func (fs *FlushService) asyncFlushLoop() {
	defer fs.wg.Done()

	ticker := time.NewTicker(fs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-fs.stopCh:
			return
		case req := <-fs.flushCh:
			// 处理刷新请求
			err := fs.flush(req.Force)
			if req.ResultCh != nil {
				select {
				case req.ResultCh <- err:
				case <-time.After(req.Timeout):
				case <-fs.stopCh:
				}
			}
		case <-ticker.C:
			// 定时刷新
			if err := fs.flush(false); err != nil {
				// 记录错误但不停止服务
				logger.Warn("Scheduled flush failed", "error", err)
			}
		}
	}
}

// flush 执行实际的刷新操作
func (fs *FlushService) flush(force bool) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// 刷新CommitLog
	if fs.commitLog != nil {
		if err := fs.commitLog.Flush(); err != nil {
			return fmt.Errorf("failed to flush commit log: %v", err)
		}
	}

	// 刷新ConsumeQueue
	if fs.consumeQueue != nil {
		if err := fs.consumeQueue.Flush(); err != nil {
			return fmt.Errorf("failed to flush consume queue: %v", err)
		}
	}

	// 更新统计信息
	atomic.AddInt64(&fs.flushCount, 1)
	fs.lastFlushTime = time.Now()

	return nil
}

// GetFlushCount 获取刷新次数
func (fs *FlushService) GetFlushCount() int64 {
	return atomic.LoadInt64(&fs.flushCount)
}

// GetLastFlushTime 获取最后刷新时间
func (fs *FlushService) GetLastFlushTime() time.Time {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	return fs.lastFlushTime
}

// IsRunning 检查服务是否正在运行
func (fs *FlushService) IsRunning() bool {
	return atomic.LoadInt32(&fs.running) == 1
}

// GetMode 获取刷新模式
func (fs *FlushService) GetMode() FlushMode {
	return fs.mode
}

// GetInterval 获取刷新间隔
func (fs *FlushService) GetInterval() time.Duration {
	return fs.interval
}

// FlushManager 刷新管理器
type FlushManager struct {
	services map[string]*FlushService
	mutex    sync.RWMutex
}

// NewFlushManager 创建刷新管理器
func NewFlushManager() *FlushManager {
	return &FlushManager{
		services: make(map[string]*FlushService),
	}
}

// RegisterService 注册刷新服务
func (fm *FlushManager) RegisterService(name string, service *FlushService) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.services[name] = service
}

// UnregisterService 注销刷新服务
func (fm *FlushManager) UnregisterService(name string) {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	delete(fm.services, name)
}

// GetService 获取刷新服务
func (fm *FlushManager) GetService(name string) (*FlushService, bool) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	service, exists := fm.services[name]
	return service, exists
}

// StartAll 启动所有刷新服务
func (fm *FlushManager) StartAll() error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	for name, service := range fm.services {
		if err := service.Start(); err != nil {
			return fmt.Errorf("failed to start flush service %s: %v", name, err)
		}
	}

	return nil
}

// StopAll 停止所有刷新服务
func (fm *FlushManager) StopAll() error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	var lastErr error
	for name, service := range fm.services {
		if err := service.Stop(); err != nil {
			lastErr = err
			logger.Warn("Failed to stop flush service", "service", name, "error", err)
		}
	}

	return lastErr
}

// FlushAll 刷新所有服务
func (fm *FlushManager) FlushAll() error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	for name, service := range fm.services {
		if err := service.Flush(); err != nil {
			return fmt.Errorf("failed to flush service %s: %v", name, err)
		}
	}

	return nil
}

// GetAllServices 获取所有刷新服务
func (fm *FlushManager) GetAllServices() map[string]*FlushService {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	services := make(map[string]*FlushService)
	for name, service := range fm.services {
		services[name] = service
	}

	return services
}

// FlushStats 刷新统计信息
type FlushStats struct {
	ServiceName   string        `json:"service_name"`
	Mode          string        `json:"mode"`
	Interval      time.Duration `json:"interval"`
	Running       bool          `json:"running"`
	FlushCount    int64         `json:"flush_count"`
	LastFlushTime time.Time     `json:"last_flush_time"`
}

// GetStats 获取统计信息
func (fs *FlushService) GetStats() *FlushStats {
	modeStr := "sync"
	if fs.mode == FlushModeAsync {
		modeStr = "async"
	}

	return &FlushStats{
		ServiceName:   fs.name,
		Mode:          modeStr,
		Interval:      fs.interval,
		Running:       fs.IsRunning(),
		FlushCount:    fs.GetFlushCount(),
		LastFlushTime: fs.GetLastFlushTime(),
	}
}

// GetAllStats 获取所有服务的统计信息
func (fm *FlushManager) GetAllStats() []*FlushStats {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	stats := make([]*FlushStats, 0, len(fm.services))
	for _, service := range fm.services {
		stats = append(stats, service.GetStats())
	}

	return stats
}

// ForceFlush 强制刷新指定服务
func (fm *FlushManager) ForceFlush(serviceName string) error {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	service, exists := fm.services[serviceName]
	if !exists {
		return fmt.Errorf("flush service %s not found", serviceName)
	}

	return service.Flush()
}

// ForceFlushAll 强制刷新所有服务
func (fm *FlushManager) ForceFlushAll() error {
	return fm.FlushAll()
}

// SetFlushInterval 设置刷新间隔
func (fs *FlushService) SetFlushInterval(interval time.Duration) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if interval <= 0 {
		interval = time.Second // 默认1秒
	}
	fs.interval = interval
}

// SetFlushMode 设置刷新模式
func (fs *FlushService) SetFlushMode(mode FlushMode) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if fs.IsRunning() {
		return fmt.Errorf("cannot change flush mode while service is running")
	}

	fs.mode = mode
	return nil
}
