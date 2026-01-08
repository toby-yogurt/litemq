package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
	"litemq/pkg/storage"
)

// ReplicationManager 主从复制管理器
// 负责管理Master-Slave之间的数据复制
type ReplicationManager struct {
	// 配置
	config *config.BrokerConfig

	// 存储组件
	commitLog       *storage.CommitLog
	consumeQueueMgr *storage.ConsumeQueueManager

	// 角色和状态
	role            config.Role
	isMaster        bool
	isSlave         bool
	masterAddr      string
	slaveID         int
	replicationMode config.ReplicationMode

	// Master端：管理所有Slave连接
	slaveConnections map[string]*ReplicationConnection // slaveID -> connection
	slaveMutex       sync.RWMutex

	// Slave端：连接到Master
	masterConnection *ReplicationConnection

	// 控制
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// ReplicationConnection 复制连接
type ReplicationConnection struct {
	ID            string
	Conn          net.Conn
	RemoteAddr    string
	LastHeartbeat time.Time
	SyncOffset    int64 // 已同步的偏移量
	mu            sync.RWMutex
}

// NewReplicationManager 创建复制管理器
func NewReplicationManager(
	cfg *config.BrokerConfig,
	commitLog *storage.CommitLog,
	consumeQueueMgr *storage.ConsumeQueueManager,
) *ReplicationManager {
	rm := &ReplicationManager{
		config:           cfg,
		commitLog:        commitLog,
		consumeQueueMgr:  consumeQueueMgr,
		role:             cfg.Role,
		isMaster:         cfg.Role == config.RoleMaster,
		isSlave:          cfg.Role == config.RoleSlave,
		masterAddr:       cfg.MasterAddr,
		slaveID:          cfg.SlaveID,
		replicationMode:  cfg.ReplicationMode,
		slaveConnections: make(map[string]*ReplicationConnection),
		stopCh:           make(chan struct{}),
		running:          false,
	}

	return rm
}

// Start 启动复制管理器
func (rm *ReplicationManager) Start() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.running {
		return fmt.Errorf("replication manager is already running")
	}

	rm.running = true

	if rm.isMaster {
		// Master模式：启动Slave连接监听
		rm.wg.Add(1)
		go rm.masterLoop()
		logger.Info("Replication manager started as Master")
	} else if rm.isSlave {
		// Slave模式：连接到Master
		rm.wg.Add(1)
		go rm.slaveLoop()
		logger.Info("Replication manager started as Slave",
			"masterAddr", rm.masterAddr)
	}

	return nil
}

// Stop 停止复制管理器
func (rm *ReplicationManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.running {
		return
	}

	rm.running = false
	close(rm.stopCh)
	rm.wg.Wait()

	// 关闭所有连接
	if rm.isMaster {
		rm.slaveMutex.Lock()
		for _, conn := range rm.slaveConnections {
			if conn.Conn != nil {
				conn.Conn.Close()
			}
		}
		rm.slaveConnections = make(map[string]*ReplicationConnection)
		rm.slaveMutex.Unlock()
	} else if rm.isSlave && rm.masterConnection != nil {
		if rm.masterConnection.Conn != nil {
			rm.masterConnection.Conn.Close()
		}
		rm.masterConnection = nil
	}

	logger.Info("Replication manager stopped")
}

// masterLoop Master端主循环
func (rm *ReplicationManager) masterLoop() {
	defer rm.wg.Done()

	// 启动心跳检查
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// 启动Slave消息处理循环
	rm.wg.Add(1)
	go rm.handleSlaveMessages()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			rm.checkSlaveHealth()
		}
	}
}

// handleSlaveMessages 处理来自Slave的消息（注册、心跳、同步请求等）
func (rm *ReplicationManager) handleSlaveMessages() {
	defer rm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			rm.processSlaveMessages()
		}
	}
}

// processSlaveMessages 处理所有Slave连接的消息
func (rm *ReplicationManager) processSlaveMessages() {
	rm.slaveMutex.RLock()
	slaves := make([]*ReplicationConnection, 0, len(rm.slaveConnections))
	for _, conn := range rm.slaveConnections {
		slaves = append(slaves, conn)
	}
	rm.slaveMutex.RUnlock()

	for _, slave := range slaves {
		slave.mu.RLock()
		conn := slave.Conn
		slave.mu.RUnlock()

		if conn == nil {
			continue
		}

		// 非阻塞读取消息
		msg, err := rm.receiveReplicationMessage(conn, 100*time.Millisecond)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // 超时，继续下一个
			}
			logger.Debug("Failed to read message from slave",
				"slaveID", slave.ID,
				"error", err)
			continue
		}

		// 处理消息
		rm.handleSlaveMessage(slave, msg)
	}
}

// handleSlaveMessage 处理单个Slave消息
func (rm *ReplicationManager) handleSlaveMessage(slave *ReplicationConnection, msg *protocol.Message) {
	msgType := msg.GetProperty("type")

	switch msgType {
	case "register":
		// 注册消息已在RegisterSlave中处理
		logger.Debug("Received register message from slave",
			"slaveID", slave.ID)

	case "heartbeat":
		// 更新心跳时间
		slave.mu.Lock()
		slave.LastHeartbeat = time.Now()
		slave.mu.Unlock()
		logger.Debug("Received heartbeat from slave",
			"slaveID", slave.ID)

	case "sync_request":
		// 处理同步请求
		lastOffsetStr := msg.GetProperty("last_offset")
		var lastOffset int64
		if _, err := fmt.Sscanf(lastOffsetStr, "%d", &lastOffset); err != nil {
			logger.Warn("Invalid last_offset in sync request",
				"slaveID", slave.ID,
				"lastOffsetStr", lastOffsetStr)
			return
		}

		// 获取增量消息
		messages, err := rm.commitLog.ReadMessages(lastOffset+1, 100) // 最多100条
		if err != nil {
			logger.Warn("Failed to read messages for sync",
				"slaveID", slave.ID,
				"lastOffset", lastOffset,
				"error", err)
			return
		}

		if len(messages) == 0 {
			// 没有新消息，发送数量0（4字节）
			countBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(countBytes, 0)
			if _, err := slave.Conn.Write(countBytes); err != nil {
				logger.Warn("Failed to send empty message count",
					"slaveID", slave.ID,
					"error", err)
			}
			return
		}

		// 发送消息数量（4字节）
		countBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(countBytes, uint32(len(messages)))
		if _, err := slave.Conn.Write(countBytes); err != nil {
			logger.Warn("Failed to send message count",
				"slaveID", slave.ID,
				"error", err)
			return
		}

		// 发送所有消息
		for _, commitLogMsg := range messages {
			if err := rm.sendReplicationMessage(slave.Conn, commitLogMsg.Message); err != nil {
				logger.Warn("Failed to send sync message",
					"slaveID", slave.ID,
					"messageId", commitLogMsg.Message.MessageID,
					"error", err)
				return
			}
		}

		logger.Debug("Sent sync messages to slave",
			"slaveID", slave.ID,
			"messageCount", len(messages))

	default:
		logger.Debug("Unknown message type from slave",
			"slaveID", slave.ID,
			"type", msgType)
	}
}

// slaveLoop Slave端主循环
func (rm *ReplicationManager) slaveLoop() {
	defer rm.wg.Done()

	// 重连间隔
	reconnectInterval := 5 * time.Second

	for {
		select {
		case <-rm.stopCh:
			return
		default:
			// 连接到Master
			if err := rm.connectToMaster(); err != nil {
				logger.Warn("Failed to connect to master, retrying...",
					"masterAddr", rm.masterAddr,
					"error", err)
				time.Sleep(reconnectInterval)
				continue
			}

			// 保持连接并同步数据
			rm.syncFromMaster()
		}
	}
}

// connectToMaster Slave连接到Master
func (rm *ReplicationManager) connectToMaster() error {
	conn, err := net.DialTimeout("tcp", rm.masterAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to dial master: %v", err)
	}

	rm.masterConnection = &ReplicationConnection{
		ID:            fmt.Sprintf("slave-%d", rm.slaveID),
		Conn:          conn,
		RemoteAddr:    rm.masterAddr,
		LastHeartbeat: time.Now(),
		SyncOffset:    0,
	}

	// 发送注册消息
	registerMsg := &protocol.Message{
		MessageID: fmt.Sprintf("register-%d", time.Now().UnixNano()),
		Topic:     "__REPLICATION__",
		Properties: map[string]string{
			"type":      "register",
			"slave_id":  fmt.Sprintf("%d", rm.slaveID),
			"broker_id": rm.config.BrokerID,
		},
	}

	// 发送注册消息到Master
	if err := rm.sendReplicationMessage(conn, registerMsg); err != nil {
		conn.Close()
		return fmt.Errorf("failed to send register message: %v", err)
	}

	logger.Info("Connected to master",
		"masterAddr", rm.masterAddr,
		"slaveID", rm.slaveID)

	return nil
}

// syncFromMaster 从Master同步数据
func (rm *ReplicationManager) syncFromMaster() {
	if rm.masterConnection == nil || rm.masterConnection.Conn == nil {
		return
	}

	// 心跳间隔
	heartbeatTicker := time.NewTicker(5 * time.Second)
	defer heartbeatTicker.Stop()

	// 同步间隔
	syncTicker := time.NewTicker(1 * time.Second)
	defer syncTicker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-heartbeatTicker.C:
			// 发送心跳
			rm.sendHeartbeat()
		case <-syncTicker.C:
			// 请求同步
			rm.requestSync()
		}
	}
}

// sendHeartbeat 发送心跳
func (rm *ReplicationManager) sendHeartbeat() {
	if rm.masterConnection == nil || rm.masterConnection.Conn == nil {
		return
	}

	rm.masterConnection.mu.Lock()
	rm.masterConnection.LastHeartbeat = time.Now()
	conn := rm.masterConnection.Conn
	rm.masterConnection.mu.Unlock()

	// 发送心跳消息
	heartbeatMsg := &protocol.Message{
		MessageID: fmt.Sprintf("heartbeat-%d", time.Now().UnixNano()),
		Topic:     "__REPLICATION__",
		Properties: map[string]string{
			"type":      "heartbeat",
			"slave_id":  fmt.Sprintf("%d", rm.slaveID),
			"broker_id": rm.config.BrokerID,
		},
	}

	if err := rm.sendReplicationMessage(conn, heartbeatMsg); err != nil {
		logger.Warn("Failed to send heartbeat to master", "error", err)
		// 心跳失败，连接可能已断开，会在下次循环中重连
		return
	}

	logger.Debug("Heartbeat sent to master")
}

// requestSync 请求同步数据
func (rm *ReplicationManager) requestSync() {
	if rm.masterConnection == nil || rm.masterConnection.Conn == nil {
		return
	}

	// 获取当前同步偏移量
	rm.masterConnection.mu.RLock()
	lastOffset := rm.masterConnection.SyncOffset
	conn := rm.masterConnection.Conn
	rm.masterConnection.mu.RUnlock()

	// 发送同步请求
	syncRequest := &protocol.Message{
		MessageID: fmt.Sprintf("sync-request-%d", time.Now().UnixNano()),
		Topic:     "__REPLICATION__",
		Properties: map[string]string{
			"type":        "sync_request",
			"slave_id":    fmt.Sprintf("%d", rm.slaveID),
			"last_offset": fmt.Sprintf("%d", lastOffset),
		},
	}

	if err := rm.sendReplicationMessage(conn, syncRequest); err != nil {
		logger.Warn("Failed to send sync request", "error", err)
		return
	}

	// 接收Master返回的消息数据
	messages, err := rm.receiveReplicationMessages(conn, 10*time.Second)
	if err != nil {
		if err != io.EOF {
			logger.Warn("Failed to receive sync messages", "error", err)
		}
		return
	}

	// 写入本地CommitLog并更新SyncOffset
	maxOffset := int64(0)
	for _, msg := range messages {
		offset, err := rm.commitLog.AppendMessage(msg)
		if err != nil {
			logger.Warn("Failed to append message to commitlog",
				"messageId", msg.MessageID,
				"error", err)
			continue
		}

		if offset > maxOffset {
			maxOffset = offset
		}

		logger.Debug("Message synced from master",
			"messageId", msg.MessageID,
			"offset", offset)
	}

	// 更新同步偏移量
	if maxOffset > 0 {
		rm.masterConnection.mu.Lock()
		if maxOffset > rm.masterConnection.SyncOffset {
			rm.masterConnection.SyncOffset = maxOffset
		}
		rm.masterConnection.mu.Unlock()
	}

	// 发送ACK确认（用于同步复制模式）
	ackMsg := &protocol.Message{
		MessageID: fmt.Sprintf("ack-%d", time.Now().UnixNano()),
		Topic:     "__REPLICATION__",
		Properties: map[string]string{
			"type":        "ack",
			"slave_id":    fmt.Sprintf("%d", rm.slaveID),
			"last_offset": fmt.Sprintf("%d", maxOffset),
		},
	}
	if err := rm.sendReplicationMessage(conn, ackMsg); err != nil {
		logger.Warn("Failed to send ack to master", "error", err)
	}

	logger.Debug("Sync completed from master",
		"lastOffset", lastOffset,
		"newOffset", rm.masterConnection.SyncOffset,
		"messageCount", len(messages))
}

// checkSlaveHealth 检查Slave健康状态
func (rm *ReplicationManager) checkSlaveHealth() {
	rm.slaveMutex.Lock()
	defer rm.slaveMutex.Unlock()

	now := time.Now()
	timeout := 30 * time.Second

	for slaveID, conn := range rm.slaveConnections {
		conn.mu.RLock()
		lastHeartbeat := conn.LastHeartbeat
		conn.mu.RUnlock()

		if now.Sub(lastHeartbeat) > timeout {
			// Slave超时，关闭连接
			logger.Warn("Slave heartbeat timeout, closing connection",
				"slaveID", slaveID,
				"lastHeartbeat", lastHeartbeat)

			if conn.Conn != nil {
				conn.Conn.Close()
			}
			delete(rm.slaveConnections, slaveID)
		}
	}
}

// ReplicateMessage 复制消息到Slave
// Master端调用，将消息复制到所有Slave
func (rm *ReplicationManager) ReplicateMessage(msg *protocol.Message, offset int64) error {
	if !rm.isMaster {
		return nil // Slave不进行复制
	}

	rm.slaveMutex.RLock()
	slaves := make([]*ReplicationConnection, 0, len(rm.slaveConnections))
	for _, conn := range rm.slaveConnections {
		slaves = append(slaves, conn)
	}
	rm.slaveMutex.RUnlock()

	if len(slaves) == 0 {
		return nil // 没有Slave，直接返回
	}

	// 根据复制模式处理
	if rm.replicationMode == config.ReplicationModeSync {
		// 同步复制：等待所有Slave确认
		return rm.syncReplicate(msg, offset, slaves)
	} else {
		// 异步复制：不等待确认
		go rm.asyncReplicate(msg, offset, slaves)
		return nil
	}
}

// syncReplicate 同步复制
func (rm *ReplicationManager) syncReplicate(
	msg *protocol.Message,
	offset int64,
	slaves []*ReplicationConnection,
) error {
	if len(slaves) == 0 {
		return nil
	}

	// 设置消息的CommitLogOffset
	msg.CommitLogOffset = offset

	// 使用WaitGroup等待所有Slave确认
	var wg sync.WaitGroup
	errorCh := make(chan error, len(slaves))
	successCount := 0
	var successMu sync.Mutex

	// 发送消息到所有Slave
	for _, slave := range slaves {
		wg.Add(1)
		go func(slaveConn *ReplicationConnection) {
			defer wg.Done()

			slaveConn.mu.RLock()
			conn := slaveConn.Conn
			slaveConn.mu.RUnlock()

			if conn == nil {
				errorCh <- fmt.Errorf("slave connection is nil")
				return
			}

			// 发送消息
			if err := rm.sendReplicationMessage(conn, msg); err != nil {
				errorCh <- fmt.Errorf("failed to send message to slave %s: %v", slaveConn.ID, err)
				return
			}

			// 等待确认（读取ACK）
			ackMsg, err := rm.receiveReplicationMessage(conn, 5*time.Second)
			if err != nil {
				errorCh <- fmt.Errorf("failed to receive ack from slave %s: %v", slaveConn.ID, err)
				return
			}

			// 检查ACK消息
			if ackType := ackMsg.GetProperty("type"); ackType != "ack" {
				errorCh <- fmt.Errorf("invalid ack type from slave %s: %s", slaveConn.ID, ackType)
				return
			}

			// 更新Slave的同步偏移量
			slaveConn.mu.Lock()
			if offset > slaveConn.SyncOffset {
				slaveConn.SyncOffset = offset
			}
			slaveConn.LastHeartbeat = time.Now()
			slaveConn.mu.Unlock()

			successMu.Lock()
			successCount++
			successMu.Unlock()

			logger.Debug("Message replicated to slave",
				"messageId", msg.MessageID,
				"slaveID", slaveConn.ID,
				"offset", offset)
		}(slave)
	}

	// 等待所有Slave响应（带超时）
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有Slave都响应了
	case <-time.After(10 * time.Second):
		// 超时
		return fmt.Errorf("sync replication timeout after 10 seconds")
	}

	// 检查错误
	close(errorCh)
	var errors []error
	for err := range errorCh {
		errors = append(errors, err)
	}

	// 如果所有Slave都成功，返回nil
	if successCount == len(slaves) {
		logger.Debug("Sync replication completed",
			"messageId", msg.MessageID,
			"offset", offset,
			"slaveCount", len(slaves),
			"successCount", successCount)
		return nil
	}

	// 部分失败
	return fmt.Errorf("sync replication partially failed: %d/%d succeeded, errors: %v",
		successCount, len(slaves), errors)
}

// asyncReplicate 异步复制
func (rm *ReplicationManager) asyncReplicate(
	msg *protocol.Message,
	offset int64,
	slaves []*ReplicationConnection,
) {
	if len(slaves) == 0 {
		return
	}

	// 设置消息的CommitLogOffset
	msg.CommitLogOffset = offset

	// 异步发送消息到所有Slave，不等待确认
	for _, slave := range slaves {
		go func(slaveConn *ReplicationConnection) {
			slaveConn.mu.RLock()
			conn := slaveConn.Conn
			slaveConn.mu.RUnlock()

			if conn == nil {
				logger.Warn("Slave connection is nil",
					"slaveID", slaveConn.ID)
				return
			}

			// 发送消息
			if err := rm.sendReplicationMessage(conn, msg); err != nil {
				logger.Warn("Failed to send message to slave",
					"slaveID", slaveConn.ID,
					"messageId", msg.MessageID,
					"error", err)
				return
			}

			// 异步接收ACK（不阻塞）
			go func() {
				ackMsg, err := rm.receiveReplicationMessage(conn, 5*time.Second)
				if err != nil {
					logger.Debug("Failed to receive ack from slave (async)",
						"slaveID", slaveConn.ID,
						"error", err)
					return
				}

				// 更新Slave的同步偏移量
				if ackType := ackMsg.GetProperty("type"); ackType == "ack" {
					slaveConn.mu.Lock()
					if offset > slaveConn.SyncOffset {
						slaveConn.SyncOffset = offset
					}
					slaveConn.LastHeartbeat = time.Now()
					slaveConn.mu.Unlock()

					logger.Debug("Message replicated to slave (async)",
						"messageId", msg.MessageID,
						"slaveID", slaveConn.ID,
						"offset", offset)
				}
			}()
		}(slave)
	}

	logger.Debug("Async replication initiated",
		"messageId", msg.MessageID,
		"offset", offset,
		"slaveCount", len(slaves))
}

// RegisterSlave 注册Slave连接（Master端调用）
func (rm *ReplicationManager) RegisterSlave(slaveID string, conn net.Conn) error {
	if !rm.isMaster {
		return fmt.Errorf("only master can register slaves")
	}

	rm.slaveMutex.Lock()
	defer rm.slaveMutex.Unlock()

	replicationConn := &ReplicationConnection{
		ID:            slaveID,
		Conn:          conn,
		RemoteAddr:    conn.RemoteAddr().String(),
		LastHeartbeat: time.Now(),
		SyncOffset:    0,
	}

	rm.slaveConnections[slaveID] = replicationConn

	logger.Info("Slave registered",
		"slaveID", slaveID,
		"remoteAddr", conn.RemoteAddr().String())

	return nil
}

// GetStats 获取复制管理器统计信息
func (rm *ReplicationManager) GetStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rm.slaveMutex.RLock()
	slaveCount := len(rm.slaveConnections)
	rm.slaveMutex.RUnlock()

	stats := map[string]interface{}{
		"running":         rm.running,
		"role":            string(rm.role),
		"isMaster":        rm.isMaster,
		"isSlave":         rm.isSlave,
		"replicationMode": string(rm.replicationMode),
		"slaveCount":      slaveCount,
	}

	if rm.isSlave && rm.masterConnection != nil {
		rm.masterConnection.mu.RLock()
		stats["masterConnected"] = rm.masterConnection.Conn != nil
		stats["lastHeartbeat"] = rm.masterConnection.LastHeartbeat
		stats["syncOffset"] = rm.masterConnection.SyncOffset
		rm.masterConnection.mu.RUnlock()
	}

	return stats
}

// sendReplicationMessage 发送复制消息
func (rm *ReplicationManager) sendReplicationMessage(conn net.Conn, msg *protocol.Message) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// 编码消息
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}

	// 先发送消息长度（4字节）
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))
	if _, err := conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to write message length: %v", err)
	}

	// 发送消息数据
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write message data: %v", err)
	}

	return nil
}

// receiveReplicationMessage 接收单个复制消息
func (rm *ReplicationManager) receiveReplicationMessage(conn net.Conn, timeout time.Duration) (*protocol.Message, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// 设置读取超时
	if timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(timeout))
	}

	// 读取消息长度（4字节）
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read message length: %v", err)
	}

	length := binary.BigEndian.Uint32(lengthBytes)
	if length == 0 {
		return nil, fmt.Errorf("invalid message length: 0")
	}

	// 限制消息大小（防止内存溢出）
	if length > 10*1024*1024 { // 10MB
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	// 读取消息数据
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, fmt.Errorf("failed to read message data: %v", err)
	}

	// 解码消息
	msg, err := protocol.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message: %v", err)
	}

	return msg, nil
}

// receiveReplicationMessages 接收多个复制消息（用于同步）
func (rm *ReplicationManager) receiveReplicationMessages(conn net.Conn, timeout time.Duration) ([]*protocol.Message, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// 设置读取超时
	if timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(timeout))
	}

	// 先读取消息数量（4字节）
	countBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, countBytes); err != nil {
		if err == io.EOF {
			return []*protocol.Message{}, nil // 没有消息，返回空列表
		}
		return nil, fmt.Errorf("failed to read message count: %v", err)
	}

	count := binary.BigEndian.Uint32(countBytes)
	if count == 0 {
		return []*protocol.Message{}, nil
	}

	// 限制批量大小（防止内存溢出）
	if count > 1000 {
		return nil, fmt.Errorf("message count too large: %d", count)
	}

	messages := make([]*protocol.Message, 0, count)

	// 读取所有消息
	for i := uint32(0); i < count; i++ {
		msg, err := rm.receiveReplicationMessage(conn, timeout)
		if err != nil {
			if err == io.EOF {
				break // 提前结束
			}
			return messages, fmt.Errorf("failed to receive message %d: %v", i, err)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
