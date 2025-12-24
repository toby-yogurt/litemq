package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"litemq/pkg/protocol"
)

// CommitLog CommitLog存储引擎
type CommitLog struct {
	storePath       string
	fileSize        int64
	maxFileCount    int
	mappedFileQueue *MappedFileQueue

	// 位置管理
	wroteOffset     int64 // 写偏移量
	committedOffset int64 // 提交偏移量
	flushedOffset   int64 // 刷新偏移量

	// 文件管理
	fileIndex int // 当前写文件索引
	mutex     sync.RWMutex

	// 统计信息
	totalMessages int64 // 总消息数
	totalBytes    int64 // 总字节数
}

// CommitLogMessage CommitLog中的消息结构
type CommitLogMessage struct {
	Offset    int64             // 在CommitLog中的偏移量
	Size      int32             // 消息大小
	Message   *protocol.Message // 消息对象
	Timestamp int64             // 存储时间戳
}

// NewCommitLog 创建新的CommitLog
func NewCommitLog(storePath string, fileSize int64, maxFileCount int) (*CommitLog, error) {
	// 确保目录存在
	if err := os.MkdirAll(storePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create commitlog directory %s: %v", storePath, err)
	}

	cl := &CommitLog{
		storePath:       storePath,
		fileSize:        fileSize,
		maxFileCount:    maxFileCount,
		mappedFileQueue: NewMappedFileQueue(storePath, fileSize, maxFileCount),
		wroteOffset:     0,
		committedOffset: 0,
		flushedOffset:   0,
		fileIndex:       0,
		totalMessages:   0,
		totalBytes:      0,
	}

	// 恢复现有文件
	if err := cl.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover commitlog: %v", err)
	}

	return cl, nil
}

// AppendMessage 追加消息到CommitLog
func (cl *CommitLog) AppendMessage(msg *protocol.Message) (int64, error) {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	// 编码消息
	data, err := msg.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode message: %v", err)
	}

	// 计算消息总大小（包含长度前缀）
	totalSize := 4 + len(data) // 4字节长度 + 数据

	// 获取当前写入文件
	mappedFile, err := cl.mappedFileQueue.GetMappedFile()
	if err != nil {
		return 0, fmt.Errorf("failed to get mapped file: %v", err)
	}

	// 检查是否有足够空间
	if mappedFile.RemainingSpace() < int64(totalSize) {
		// 当前文件空间不足，获取新的文件
		mappedFile, err = cl.mappedFileQueue.GetMappedFile()
		if err != nil {
			return 0, fmt.Errorf("failed to get new mapped file: %v", err)
		}

		if mappedFile.RemainingSpace() < int64(totalSize) {
			return 0, fmt.Errorf("message too large: %d bytes, remaining space: %d bytes",
				totalSize, mappedFile.RemainingSpace())
		}
	}

	// 记录当前偏移量
	offset := cl.wroteOffset

	// 写入长度前缀
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(data)))
	if _, err := mappedFile.Write(sizeBytes); err != nil {
		return 0, fmt.Errorf("failed to write message size: %v", err)
	}

	// 写入消息数据
	if _, err := mappedFile.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write message data: %v", err)
	}

	// 更新位置和统计信息
	cl.wroteOffset += int64(totalSize)
	cl.totalMessages++
	cl.totalBytes += int64(totalSize)

	// 设置消息的存储相关字段
	msg.CommitLogOffset = offset
	msg.StoreSize = totalSize
	msg.StoreTimestamp = time.Now().UnixMilli()

	return offset, nil
}

// AppendMessages 批量追加消息
func (cl *CommitLog) AppendMessages(messages []*protocol.Message) ([]int64, error) {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if len(messages) == 0 {
		return []int64{}, nil
	}

	offsets := make([]int64, len(messages))

	// 预计算总大小
	totalSize := int64(0)
	encodedMessages := make([][]byte, len(messages))

	for i, msg := range messages {
		data, err := msg.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode message %d: %v", i, err)
		}
		encodedMessages[i] = data
		totalSize += 4 + int64(len(data)) // 4字节长度 + 数据
	}

	// 检查是否有足够空间
	mappedFile, err := cl.mappedFileQueue.GetMappedFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped file: %v", err)
	}

	if mappedFile.RemainingSpace() < totalSize {
		// 如果当前文件空间不足，可能需要多个文件
		remaining := mappedFile.RemainingSpace()
		if remaining < 4 { // 连长度前缀都写不下
			mappedFile, err = cl.mappedFileQueue.GetMappedFile()
			if err != nil {
				return nil, fmt.Errorf("failed to get new mapped file: %v", err)
			}
		}
	}

	// 逐个写入消息
	currentOffset := cl.wroteOffset
	for i, data := range encodedMessages {
		msgSize := 4 + len(data)

		// 检查当前文件是否有足够空间
		if mappedFile.RemainingSpace() < int64(msgSize) {
			mappedFile, err = cl.mappedFileQueue.GetMappedFile()
			if err != nil {
				return nil, fmt.Errorf("failed to get new mapped file for message %d: %v", i, err)
			}
		}

		offsets[i] = currentOffset

		// 写入长度前缀
		sizeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBytes, uint32(len(data)))
		if _, err := mappedFile.Write(sizeBytes); err != nil {
			return nil, fmt.Errorf("failed to write message size for message %d: %v", i, err)
		}

		// 写入消息数据
		if _, err := mappedFile.Write(data); err != nil {
			return nil, fmt.Errorf("failed to write message data for message %d: %v", i, err)
		}

		currentOffset += int64(msgSize)

		// 设置消息的存储相关字段
		messages[i].CommitLogOffset = offsets[i]
		messages[i].StoreSize = msgSize
		messages[i].StoreTimestamp = time.Now().UnixMilli()
	}

	// 更新位置和统计信息
	cl.wroteOffset = currentOffset
	cl.totalMessages += int64(len(messages))
	cl.totalBytes += totalSize

	return offsets, nil
}

// ReadMessage 读取指定偏移量的消息
func (cl *CommitLog) ReadMessage(offset int64) (*CommitLogMessage, error) {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()

	// 获取对应的文件和文件内偏移量
	mappedFile, fileOffset, err := cl.mappedFileQueue.GetMappedFileByOffset(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped file for offset %d: %v", offset, err)
	}

	// 读取消息长度
	sizeBytes, err := mappedFile.Read(fileOffset, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size at offset %d: %v", offset, err)
	}

	if len(sizeBytes) < 4 {
		return nil, fmt.Errorf("incomplete message size at offset %d", offset)
	}

	msgSize := binary.BigEndian.Uint32(sizeBytes)

	// 读取消息数据
	msgData, err := mappedFile.Read(fileOffset+4, int64(msgSize))
	if err != nil {
		return nil, fmt.Errorf("failed to read message data at offset %d: %v", offset, err)
	}

	if len(msgData) < int(msgSize) {
		return nil, fmt.Errorf("incomplete message data at offset %d, expected %d, got %d",
			offset, msgSize, len(msgData))
	}

	// 解码消息
	msg, err := protocol.Decode(msgData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode message at offset %d: %v", offset, err)
	}

	commitLogMsg := &CommitLogMessage{
		Offset:    offset,
		Size:      int32(4 + msgSize),
		Message:   msg,
		Timestamp: msg.StoreTimestamp,
	}

	return commitLogMsg, nil
}

// ReadMessages 批量读取消息
func (cl *CommitLog) ReadMessages(startOffset int64, maxCount int) ([]*CommitLogMessage, error) {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()

	messages := make([]*CommitLogMessage, 0, maxCount)
	currentOffset := startOffset

	for i := 0; i < maxCount && currentOffset < cl.wroteOffset; i++ {
		msg, err := cl.ReadMessage(currentOffset)
		if err != nil {
			return messages, err // 返回已读取的消息
		}

		messages = append(messages, msg)
		currentOffset += int64(msg.Size)
	}

	return messages, nil
}

// Commit 提交数据
func (cl *CommitLog) Commit(commitOffset int64) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if commitOffset < cl.committedOffset || commitOffset > cl.wroteOffset {
		return fmt.Errorf("invalid commit offset: %d, committed: %d, wrote: %d",
			commitOffset, cl.committedOffset, cl.wroteOffset)
	}

	cl.committedOffset = commitOffset

	// 提交所有相关文件
	for _, mf := range cl.mappedFileQueue.GetMappedFiles() {
		fileStartOffset := int64(cl.mappedFileQueue.CalculateFileIndex(0)) * cl.fileSize

		// 计算在这个文件中的提交位置
		commitInFile := commitOffset - fileStartOffset
		if commitInFile > 0 {
			if commitInFile > mf.WrotePosition() {
				commitInFile = mf.WrotePosition()
			}
			if err := mf.Commit(commitInFile); err != nil {
				return fmt.Errorf("failed to commit mapped file %s: %v", mf.FileName(), err)
			}
		}
	}

	return nil
}

// Flush 刷新数据到磁盘
func (cl *CommitLog) Flush() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if err := cl.mappedFileQueue.Flush(); err != nil {
		return fmt.Errorf("failed to flush mapped file queue: %v", err)
	}

	cl.flushedOffset = cl.committedOffset
	return nil
}

// FlushToOffset 刷新到指定偏移量
func (cl *CommitLog) FlushToOffset(offset int64) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if offset < cl.flushedOffset || offset > cl.committedOffset {
		return fmt.Errorf("invalid flush offset: %d, flushed: %d, committed: %d",
			offset, cl.flushedOffset, cl.committedOffset)
	}

	// 计算需要刷新的文件范围
	startFileIndex := cl.mappedFileQueue.CalculateFileIndex(cl.flushedOffset)
	endFileIndex := cl.mappedFileQueue.CalculateFileIndex(offset)

	for i := startFileIndex; i <= endFileIndex; i++ {
		mf, exists := cl.mappedFileQueue.GetMappedFileByIndex(i)
		if !exists {
			continue
		}

		fileStartOffset := int64(i) * cl.fileSize

		// 计算在这个文件中需要刷新的范围
		flushStart := cl.flushedOffset - fileStartOffset
		if flushStart < 0 {
			flushStart = 0
		}

		flushEnd := offset - fileStartOffset
		if flushEnd > mf.CommittedPosition() {
			flushEnd = mf.CommittedPosition()
		}

		if flushStart < flushEnd {
			if err := mf.FlushRange(flushStart, flushEnd); err != nil {
				return fmt.Errorf("failed to flush range in file %s: %v", mf.FileName(), err)
			}
		}
	}

	cl.flushedOffset = offset
	return nil
}

// GetWroteOffset 获取写偏移量
func (cl *CommitLog) GetWroteOffset() int64 {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()
	return cl.wroteOffset
}

// GetCommittedOffset 获取提交偏移量
func (cl *CommitLog) GetCommittedOffset() int64 {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()
	return cl.committedOffset
}

// GetFlushedOffset 获取刷新偏移量
func (cl *CommitLog) GetFlushedOffset() int64 {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()
	return cl.flushedOffset
}

// GetTotalMessages 获取总消息数
func (cl *CommitLog) GetTotalMessages() int64 {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()
	return cl.totalMessages
}

// GetTotalBytes 获取总字节数
func (cl *CommitLog) GetTotalBytes() int64 {
	cl.mutex.RLock()
	defer cl.mutex.RUnlock()
	return cl.totalBytes
}

// Close 关闭CommitLog
func (cl *CommitLog) Close() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	return cl.mappedFileQueue.Close()
}

// recover 恢复现有文件
func (cl *CommitLog) recover() error {
	// 扫描目录中的所有文件
	pattern := filepath.Join(cl.storePath, "*.data")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob files: %v", err)
	}

	if len(files) == 0 {
		return nil // 没有文件需要恢复
	}

	// 排序文件
	sort.Strings(files)

	// 恢复每个文件
	for _, file := range files {
		// 解析文件名获取文件索引
		baseName := filepath.Base(file)
		if len(baseName) < 20 {
			continue // 文件名格式不正确
		}

		// 假设文件名格式为 00000000000000000000.data
		fileIndex := 0
		for _, mf := range cl.mappedFileQueue.GetMappedFiles() {
			if mf.FileName() == file {
				break
			}
			fileIndex++
		}

		// 创建对应的MappedFile
		mf, err := NewMappedFile(file, cl.fileSize)
		if err != nil {
			return fmt.Errorf("failed to recover mapped file %s: %v", file, err)
		}

		// 添加到队列
		cl.mappedFileQueue.mappedFiles = append(cl.mappedFileQueue.mappedFiles, mf)

		// 更新写位置
		if mf.WrotePosition() > 0 {
			fileStartOffset := int64(fileIndex) * cl.fileSize
			cl.wroteOffset = fileStartOffset + mf.WrotePosition()
			cl.fileIndex = fileIndex
		}
	}

	// 重新验证和重建索引
	if err := cl.rebuildIndex(); err != nil {
		return fmt.Errorf("failed to rebuild index: %v", err)
	}

	return nil
}

// rebuildIndex 重建索引
func (cl *CommitLog) rebuildIndex() error {
	cl.totalMessages = 0
	cl.totalBytes = 0

	currentOffset := int64(0)
	for {
		if currentOffset >= cl.wroteOffset {
			break
		}

		// 读取消息大小
		sizeBytes, err := cl.readRawBytes(currentOffset, 4)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read message size at offset %d: %v", currentOffset, err)
		}

		msgSize := binary.BigEndian.Uint32(sizeBytes)
		totalSize := 4 + msgSize

		cl.totalMessages++
		cl.totalBytes += int64(totalSize)

		currentOffset += int64(totalSize)
	}

	// 确保计算一致
	if currentOffset != cl.wroteOffset {
		return fmt.Errorf("offset mismatch after rebuild: calculated %d, stored %d",
			currentOffset, cl.wroteOffset)
	}

	return nil
}

// readRawBytes 读取原始字节
func (cl *CommitLog) readRawBytes(offset int64, length int64) ([]byte, error) {
	mappedFile, fileOffset, err := cl.mappedFileQueue.GetMappedFileByOffset(offset)
	if err != nil {
		return nil, err
	}

	return mappedFile.Read(fileOffset, length)
}

// Truncate 截断CommitLog到指定偏移量（用于故障恢复）
func (cl *CommitLog) Truncate(offset int64) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if offset < 0 || offset > cl.wroteOffset {
		return fmt.Errorf("invalid truncate offset: %d", offset)
	}

	// 计算需要截断的文件
	fileIndex := cl.mappedFileQueue.CalculateFileIndex(offset)
	fileOffset := cl.mappedFileQueue.CalculateFileOffset(offset)

	// 截断文件
	mf, exists := cl.mappedFileQueue.GetMappedFileByIndex(fileIndex)
	if exists {
		if err := mf.file.Truncate(fileOffset); err != nil {
			return fmt.Errorf("failed to truncate file: %v", err)
		}
		mf.wrotePos = fileOffset
		mf.committedPos = fileOffset
		mf.flushedPos = fileOffset
	}

	// 删除后续文件
	for i := fileIndex + 1; i < len(cl.mappedFileQueue.mappedFiles); i++ {
		mf := cl.mappedFileQueue.mappedFiles[i]
		if err := os.Remove(mf.FileName()); err != nil {
			return fmt.Errorf("failed to remove file %s: %v", mf.FileName(), err)
		}
	}

	// 更新队列
	cl.mappedFileQueue.mappedFiles = cl.mappedFileQueue.mappedFiles[:fileIndex+1]
	cl.wroteOffset = offset
	cl.committedOffset = offset
	cl.flushedOffset = offset

	// 重新计算统计信息
	cl.rebuildIndex()

	return nil
}
