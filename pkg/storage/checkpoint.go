package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CheckPoint 检查点管理器
// 用于记录关键状态信息，确保数据一致性和恢复
type CheckPoint struct {
	storePath string
	filePath  string

	// 检查点数据
	commitLogFlushedOffset    int64                               // CommitLog 已刷新偏移量
	consumeQueueFlushedOffset map[string]map[int]int64            // Topic -> QueueID -> 已刷新偏移量
	consumerOffset            map[string]map[string]map[int]int64 // ConsumerGroup -> Topic -> QueueID -> 消费偏移量

	// 时间戳
	lastFlushTime  int64 // 最后刷新时间（毫秒）
	lastUpdateTime int64 // 最后更新时间（毫秒）

	mutex sync.RWMutex
}

// CheckPointData 检查点数据结构
type CheckPointData struct {
	MagicNumber              uint32 // 魔数，用于验证文件格式
	Version                  uint16 // 版本号
	CommitLogFlushedOffset   int64  // CommitLog 已刷新偏移量
	ConsumeQueueFlushedCount int32  // ConsumeQueue 刷新记录数量
	ConsumerOffsetCount      int32  // 消费者偏移量记录数量
	LastFlushTime            int64  // 最后刷新时间（毫秒）
	LastUpdateTime           int64  // 最后更新时间（毫秒）
}

const (
	checkPointMagicNumber = 0x4C4D5103 // "LMQ" + checkpoint
	checkPointVersion     = 1
	checkPointFileName    = "checkpoint"
)

// NewCheckPoint 创建检查点管理器
func NewCheckPoint(storePath string) (*CheckPoint, error) {
	// 确保目录存在
	if err := os.MkdirAll(storePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory %s: %v", storePath, err)
	}

	cp := &CheckPoint{
		storePath:                 storePath,
		filePath:                  filepath.Join(storePath, checkPointFileName),
		consumeQueueFlushedOffset: make(map[string]map[int]int64),
		consumerOffset:            make(map[string]map[string]map[int]int64),
		lastFlushTime:             time.Now().UnixMilli(),
		lastUpdateTime:            time.Now().UnixMilli(),
	}

	// 恢复检查点数据
	if err := cp.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover checkpoint: %v", err)
	}

	return cp, nil
}

// UpdateCommitLogFlushedOffset 更新 CommitLog 已刷新偏移量
func (cp *CheckPoint) UpdateCommitLogFlushedOffset(offset int64) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.commitLogFlushedOffset = offset
	cp.lastUpdateTime = time.Now().UnixMilli()

	return nil
}

// GetCommitLogFlushedOffset 获取 CommitLog 已刷新偏移量
func (cp *CheckPoint) GetCommitLogFlushedOffset() int64 {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	return cp.commitLogFlushedOffset
}

// UpdateConsumeQueueFlushedOffset 更新 ConsumeQueue 已刷新偏移量
func (cp *CheckPoint) UpdateConsumeQueueFlushedOffset(topic string, queueID int, offset int64) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if cp.consumeQueueFlushedOffset[topic] == nil {
		cp.consumeQueueFlushedOffset[topic] = make(map[int]int64)
	}
	cp.consumeQueueFlushedOffset[topic][queueID] = offset
	cp.lastUpdateTime = time.Now().UnixMilli()

	return nil
}

// GetConsumeQueueFlushedOffset 获取 ConsumeQueue 已刷新偏移量
func (cp *CheckPoint) GetConsumeQueueFlushedOffset(topic string, queueID int) int64 {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	if cp.consumeQueueFlushedOffset[topic] == nil {
		return 0
	}
	return cp.consumeQueueFlushedOffset[topic][queueID]
}

// UpdateConsumerOffset 更新消费者偏移量
func (cp *CheckPoint) UpdateConsumerOffset(consumerGroup, topic string, queueID int, offset int64) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if cp.consumerOffset[consumerGroup] == nil {
		cp.consumerOffset[consumerGroup] = make(map[string]map[int]int64)
	}
	if cp.consumerOffset[consumerGroup][topic] == nil {
		cp.consumerOffset[consumerGroup][topic] = make(map[int]int64)
	}
	cp.consumerOffset[consumerGroup][topic][queueID] = offset
	cp.lastUpdateTime = time.Now().UnixMilli()

	return nil
}

// GetConsumerOffset 获取消费者偏移量
func (cp *CheckPoint) GetConsumerOffset(consumerGroup, topic string, queueID int) int64 {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	if cp.consumerOffset[consumerGroup] == nil {
		return 0
	}
	if cp.consumerOffset[consumerGroup][topic] == nil {
		return 0
	}
	return cp.consumerOffset[consumerGroup][topic][queueID]
}

// Flush 刷新检查点到磁盘
func (cp *CheckPoint) Flush() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.lastFlushTime = time.Now().UnixMilli()

	// 创建临时文件
	tmpFilePath := cp.filePath + ".tmp"
	file, err := os.Create(tmpFilePath)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint file: %v", err)
	}
	defer file.Close()

	// 写入检查点数据
	data := &CheckPointData{
		MagicNumber:              checkPointMagicNumber,
		Version:                  checkPointVersion,
		CommitLogFlushedOffset:   cp.commitLogFlushedOffset,
		ConsumeQueueFlushedCount: int32(cp.getConsumeQueueCount()),
		ConsumerOffsetCount:      int32(cp.getConsumerOffsetCount()),
		LastFlushTime:            cp.lastFlushTime,
		LastUpdateTime:           cp.lastUpdateTime,
	}

	// 写入头部
	if err := cp.writeHeader(file, data); err != nil {
		return fmt.Errorf("failed to write checkpoint header: %v", err)
	}

	// 写入 ConsumeQueue 刷新偏移量
	if err := cp.writeConsumeQueueOffsets(file); err != nil {
		return fmt.Errorf("failed to write consume queue offsets: %v", err)
	}

	// 写入消费者偏移量
	if err := cp.writeConsumerOffsets(file); err != nil {
		return fmt.Errorf("failed to write consumer offsets: %v", err)
	}

	// 同步到磁盘
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint file: %v", err)
	}

	// 原子替换文件
	if err := os.Rename(tmpFilePath, cp.filePath); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %v", err)
	}

	return nil
}

// writeHeader 写入检查点头部
func (cp *CheckPoint) writeHeader(file *os.File, data *CheckPointData) error {
	// Magic number (4 bytes)
	if err := binary.Write(file, binary.BigEndian, data.MagicNumber); err != nil {
		return err
	}

	// Version (2 bytes)
	if err := binary.Write(file, binary.BigEndian, data.Version); err != nil {
		return err
	}

	// CommitLog flushed offset (8 bytes)
	if err := binary.Write(file, binary.BigEndian, data.CommitLogFlushedOffset); err != nil {
		return err
	}

	// ConsumeQueue flushed count (4 bytes)
	if err := binary.Write(file, binary.BigEndian, data.ConsumeQueueFlushedCount); err != nil {
		return err
	}

	// Consumer offset count (4 bytes)
	if err := binary.Write(file, binary.BigEndian, data.ConsumerOffsetCount); err != nil {
		return err
	}

	// Last flush time (8 bytes)
	if err := binary.Write(file, binary.BigEndian, data.LastFlushTime); err != nil {
		return err
	}

	// Last update time (8 bytes)
	if err := binary.Write(file, binary.BigEndian, data.LastUpdateTime); err != nil {
		return err
	}

	return nil
}

// writeConsumeQueueOffsets 写入 ConsumeQueue 刷新偏移量
func (cp *CheckPoint) writeConsumeQueueOffsets(file *os.File) error {
	for topic, queueOffsets := range cp.consumeQueueFlushedOffset {
		// Topic length (2 bytes) + Topic (variable)
		topicBytes := []byte(topic)
		if err := binary.Write(file, binary.BigEndian, uint16(len(topicBytes))); err != nil {
			return err
		}
		if _, err := file.Write(topicBytes); err != nil {
			return err
		}

		// Queue count (4 bytes)
		if err := binary.Write(file, binary.BigEndian, int32(len(queueOffsets))); err != nil {
			return err
		}

		// Queue offsets
		for queueID, offset := range queueOffsets {
			// Queue ID (4 bytes)
			if err := binary.Write(file, binary.BigEndian, int32(queueID)); err != nil {
				return err
			}
			// Offset (8 bytes)
			if err := binary.Write(file, binary.BigEndian, offset); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeConsumerOffsets 写入消费者偏移量
func (cp *CheckPoint) writeConsumerOffsets(file *os.File) error {
	for consumerGroup, topicOffsets := range cp.consumerOffset {
		// Consumer group length (2 bytes) + Consumer group (variable)
		groupBytes := []byte(consumerGroup)
		if err := binary.Write(file, binary.BigEndian, uint16(len(groupBytes))); err != nil {
			return err
		}
		if _, err := file.Write(groupBytes); err != nil {
			return err
		}

		// Topic count (4 bytes)
		if err := binary.Write(file, binary.BigEndian, int32(len(topicOffsets))); err != nil {
			return err
		}

		for topic, queueOffsets := range topicOffsets {
			// Topic length (2 bytes) + Topic (variable)
			topicBytes := []byte(topic)
			if err := binary.Write(file, binary.BigEndian, uint16(len(topicBytes))); err != nil {
				return err
			}
			if _, err := file.Write(topicBytes); err != nil {
				return err
			}

			// Queue count (4 bytes)
			if err := binary.Write(file, binary.BigEndian, int32(len(queueOffsets))); err != nil {
				return err
			}

			// Queue offsets
			for queueID, offset := range queueOffsets {
				// Queue ID (4 bytes)
				if err := binary.Write(file, binary.BigEndian, int32(queueID)); err != nil {
					return err
				}
				// Offset (8 bytes)
				if err := binary.Write(file, binary.BigEndian, offset); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// recover 恢复检查点数据
func (cp *CheckPoint) recover() error {
	// 检查文件是否存在
	if _, err := os.Stat(cp.filePath); os.IsNotExist(err) {
		// 文件不存在，使用默认值
		return nil
	}

	file, err := os.Open(cp.filePath)
	if err != nil {
		return fmt.Errorf("failed to open checkpoint file: %v", err)
	}
	defer file.Close()

	// 读取头部
	data, err := cp.readHeader(file)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint header: %v", err)
	}

	// 验证魔数
	if data.MagicNumber != checkPointMagicNumber {
		return fmt.Errorf("invalid checkpoint magic number: %x", data.MagicNumber)
	}

	// 验证版本
	if data.Version != checkPointVersion {
		return fmt.Errorf("unsupported checkpoint version: %d", data.Version)
	}

	// 恢复数据
	cp.commitLogFlushedOffset = data.CommitLogFlushedOffset
	cp.lastFlushTime = data.LastFlushTime
	cp.lastUpdateTime = data.LastUpdateTime

	// 读取 ConsumeQueue 刷新偏移量
	if err := cp.readConsumeQueueOffsets(file, data.ConsumeQueueFlushedCount); err != nil {
		return fmt.Errorf("failed to read consume queue offsets: %v", err)
	}

	// 读取消费者偏移量
	if err := cp.readConsumerOffsets(file, data.ConsumerOffsetCount); err != nil {
		return fmt.Errorf("failed to read consumer offsets: %v", err)
	}

	return nil
}

// readHeader 读取检查点头部
func (cp *CheckPoint) readHeader(file *os.File) (*CheckPointData, error) {
	data := &CheckPointData{}

	// Magic number (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.MagicNumber); err != nil {
		return nil, err
	}

	// Version (2 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.Version); err != nil {
		return nil, err
	}

	// CommitLog flushed offset (8 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.CommitLogFlushedOffset); err != nil {
		return nil, err
	}

	// ConsumeQueue flushed count (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.ConsumeQueueFlushedCount); err != nil {
		return nil, err
	}

	// Consumer offset count (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.ConsumerOffsetCount); err != nil {
		return nil, err
	}

	// Last flush time (8 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.LastFlushTime); err != nil {
		return nil, err
	}

	// Last update time (8 bytes)
	if err := binary.Read(file, binary.BigEndian, &data.LastUpdateTime); err != nil {
		return nil, err
	}

	return data, nil
}

// readConsumeQueueOffsets 读取 ConsumeQueue 刷新偏移量
func (cp *CheckPoint) readConsumeQueueOffsets(file *os.File, count int32) error {
	for i := int32(0); i < count; i++ {
		// Topic length (2 bytes)
		var topicLen uint16
		if err := binary.Read(file, binary.BigEndian, &topicLen); err != nil {
			return err
		}

		// Topic (variable)
		topicBytes := make([]byte, topicLen)
		if _, err := file.Read(topicBytes); err != nil {
			return err
		}
		topic := string(topicBytes)

		// Queue count (4 bytes)
		var queueCount int32
		if err := binary.Read(file, binary.BigEndian, &queueCount); err != nil {
			return err
		}

		if cp.consumeQueueFlushedOffset[topic] == nil {
			cp.consumeQueueFlushedOffset[topic] = make(map[int]int64)
		}

		// Queue offsets
		for j := int32(0); j < queueCount; j++ {
			// Queue ID (4 bytes)
			var queueID int32
			if err := binary.Read(file, binary.BigEndian, &queueID); err != nil {
				return err
			}

			// Offset (8 bytes)
			var offset int64
			if err := binary.Read(file, binary.BigEndian, &offset); err != nil {
				return err
			}

			cp.consumeQueueFlushedOffset[topic][int(queueID)] = offset
		}
	}

	return nil
}

// readConsumerOffsets 读取消费者偏移量
func (cp *CheckPoint) readConsumerOffsets(file *os.File, count int32) error {
	for i := int32(0); i < count; i++ {
		// Consumer group length (2 bytes)
		var groupLen uint16
		if err := binary.Read(file, binary.BigEndian, &groupLen); err != nil {
			return err
		}

		// Consumer group (variable)
		groupBytes := make([]byte, groupLen)
		if _, err := file.Read(groupBytes); err != nil {
			return err
		}
		consumerGroup := string(groupBytes)

		// Topic count (4 bytes)
		var topicCount int32
		if err := binary.Read(file, binary.BigEndian, &topicCount); err != nil {
			return err
		}

		if cp.consumerOffset[consumerGroup] == nil {
			cp.consumerOffset[consumerGroup] = make(map[string]map[int]int64)
		}

		for j := int32(0); j < topicCount; j++ {
			// Topic length (2 bytes)
			var topicLen uint16
			if err := binary.Read(file, binary.BigEndian, &topicLen); err != nil {
				return err
			}

			// Topic (variable)
			topicBytes := make([]byte, topicLen)
			if _, err := file.Read(topicBytes); err != nil {
				return err
			}
			topic := string(topicBytes)

			// Queue count (4 bytes)
			var queueCount int32
			if err := binary.Read(file, binary.BigEndian, &queueCount); err != nil {
				return err
			}

			if cp.consumerOffset[consumerGroup][topic] == nil {
				cp.consumerOffset[consumerGroup][topic] = make(map[int]int64)
			}

			// Queue offsets
			for k := int32(0); k < queueCount; k++ {
				// Queue ID (4 bytes)
				var queueID int32
				if err := binary.Read(file, binary.BigEndian, &queueID); err != nil {
					return err
				}

				// Offset (8 bytes)
				var offset int64
				if err := binary.Read(file, binary.BigEndian, &offset); err != nil {
					return err
				}

				cp.consumerOffset[consumerGroup][topic][int(queueID)] = offset
			}
		}
	}

	return nil
}

// getConsumeQueueCount 获取 ConsumeQueue 记录数量
func (cp *CheckPoint) getConsumeQueueCount() int {
	count := 0
	for _, queueOffsets := range cp.consumeQueueFlushedOffset {
		count += len(queueOffsets)
	}
	return count
}

// getConsumerOffsetCount 获取消费者偏移量记录数量
func (cp *CheckPoint) getConsumerOffsetCount() int {
	count := 0
	for _, topicOffsets := range cp.consumerOffset {
		for _, queueOffsets := range topicOffsets {
			count += len(queueOffsets)
		}
	}
	return count
}

// GetStats 获取检查点统计信息
func (cp *CheckPoint) GetStats() map[string]interface{} {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	return map[string]interface{}{
		"commit_log_flushed_offset":   cp.commitLogFlushedOffset,
		"consume_queue_flushed_count": cp.getConsumeQueueCount(),
		"consumer_offset_count":       cp.getConsumerOffsetCount(),
		"last_flush_time":             cp.lastFlushTime,
		"last_update_time":            cp.lastUpdateTime,
	}
}
