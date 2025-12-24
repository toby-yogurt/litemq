package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"litemq/pkg/protocol"
)

// ConsumeQueueEntry ConsumeQueue索引项
type ConsumeQueueEntry struct {
	Offset      int64  // 消息在CommitLog中的偏移量
	MessageSize int32  // 消息大小
	TagHash     uint32 // Tag的哈希值
	Timestamp   int64  // 消息时间戳
}

// Size 返回索引项大小
func (entry *ConsumeQueueEntry) Size() int {
	return 8 + 4 + 4 + 8 // offset + size + tagHash + timestamp
}

// Encode 编码索引项
func (entry *ConsumeQueueEntry) Encode() []byte {
	data := make([]byte, entry.Size())

	binary.BigEndian.PutUint64(data[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint32(data[8:12], uint32(entry.MessageSize))
	binary.BigEndian.PutUint32(data[12:16], entry.TagHash)
	binary.BigEndian.PutUint64(data[16:24], uint64(entry.Timestamp))

	return data
}

// Decode 解码索引项
func DecodeConsumeQueueEntry(data []byte) (*ConsumeQueueEntry, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("data too short for consume queue entry")
	}

	entry := &ConsumeQueueEntry{
		Offset:      int64(binary.BigEndian.Uint64(data[0:8])),
		MessageSize: int32(binary.BigEndian.Uint32(data[8:12])),
		TagHash:     binary.BigEndian.Uint32(data[12:16]),
		Timestamp:   int64(binary.BigEndian.Uint64(data[16:24])),
	}

	return entry, nil
}

// ConsumeQueue ConsumeQueue索引
type ConsumeQueue struct {
	topic           string
	queueID         int
	storePath       string
	fileSize        int64
	maxFileCount    int
	mappedFileQueue *MappedFileQueue

	// 位置管理
	wroteOffset     int64 // 写偏移量
	committedOffset int64 // 提交偏移量

	mutex sync.RWMutex

	// 统计信息
	totalEntries int64 // 总索引项数
}

// NewConsumeQueue 创建新的ConsumeQueue
func NewConsumeQueue(topic string, queueID int, storePath string, fileSize int64, maxFileCount int) (*ConsumeQueue, error) {
	queuePath := filepath.Join(storePath, topic, fmt.Sprintf("%d", queueID))

	// 确保目录存在
	if err := os.MkdirAll(queuePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create consume queue directory %s: %v", queuePath, err)
	}

	cq := &ConsumeQueue{
		topic:           topic,
		queueID:         queueID,
		storePath:       queuePath,
		fileSize:        fileSize,
		maxFileCount:    maxFileCount,
		mappedFileQueue: NewMappedFileQueue(queuePath, fileSize, maxFileCount),
		wroteOffset:     0,
		committedOffset: 0,
		totalEntries:    0,
	}

	// 恢复现有文件
	if err := cq.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover consume queue: %v", err)
	}

	return cq, nil
}

// AppendEntry 追加索引项
func (cq *ConsumeQueue) AppendEntry(offset int64, messageSize int32, tags []string, timestamp int64) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	// 计算Tag哈希
	tagHash := cq.calculateTagHash(tags)

	entry := &ConsumeQueueEntry{
		Offset:      offset,
		MessageSize: messageSize,
		TagHash:     tagHash,
		Timestamp:   timestamp,
	}

	// 获取当前写入文件
	mappedFile, err := cq.mappedFileQueue.GetMappedFile()
	if err != nil {
		return fmt.Errorf("failed to get mapped file: %v", err)
	}

	// 检查空间是否足够
	entrySize := int64(entry.Size())
	if mappedFile.RemainingSpace() < entrySize {
		// 获取新文件
		mappedFile, err = cq.mappedFileQueue.GetMappedFile()
		if err != nil {
			return fmt.Errorf("failed to get new mapped file: %v", err)
		}

		if mappedFile.RemainingSpace() < entrySize {
			return fmt.Errorf("entry too large: %d bytes, remaining space: %d bytes",
				entrySize, mappedFile.RemainingSpace())
		}
	}

	// 写入索引项
	data := entry.Encode()
	if _, err := mappedFile.Write(data); err != nil {
		return fmt.Errorf("failed to write consume queue entry: %v", err)
	}

	// 更新位置和统计信息
	cq.wroteOffset += entrySize
	cq.totalEntries++

	return nil
}

// AppendMessage 为消息创建索引项
func (cq *ConsumeQueue) AppendMessage(msg *protocol.Message) error {
	if msg.QueueID != cq.queueID {
		return fmt.Errorf("queue ID mismatch: expected %d, got %d", cq.queueID, msg.QueueID)
	}

	// 检查消息是否已正确存储（StoreSize > 0 表示消息已存储）
	// CommitLogOffset 为 0 是合法的（表示第一个消息），所以不能只检查 offset
	if msg.StoreSize == 0 {
		return fmt.Errorf("message store size is 0, message may not be stored in commitlog (offset: %d, messageId: %s)", msg.CommitLogOffset, msg.MessageID)
	}

	// 追加索引项
	if err := cq.AppendEntry(msg.CommitLogOffset, int32(msg.StoreSize), msg.Tags, msg.StoreTimestamp); err != nil {
		return fmt.Errorf("failed to append entry to consume queue: %v", err)
	}

	return nil
}

// GetEntry 获取指定位置的索引项
func (cq *ConsumeQueue) GetEntry(index int64) (*ConsumeQueueEntry, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	offset := index * 24 // 每个索引项24字节
	if offset >= cq.wroteOffset {
		return nil, fmt.Errorf("index out of range: %d", index)
	}

	// 获取对应的文件
	mappedFile, fileOffset, err := cq.mappedFileQueue.GetMappedFileByOffset(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapped file for offset %d: %v", offset, err)
	}

	// 读取索引项数据
	data, err := mappedFile.Read(fileOffset, 24)
	if err != nil {
		return nil, fmt.Errorf("failed to read consume queue entry at offset %d: %v", offset, err)
	}

	return DecodeConsumeQueueEntry(data)
}

// GetEntries 批量获取索引项
func (cq *ConsumeQueue) GetEntries(startIndex int64, count int) ([]*ConsumeQueueEntry, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	entries := make([]*ConsumeQueueEntry, 0, count)
	currentIndex := startIndex
	totalEntries := cq.GetEntryCount()

	// 如果起始索引超出范围，返回空列表
	if startIndex >= totalEntries {
		return entries, nil
	}

	for i := 0; i < count && currentIndex < totalEntries; i++ {
		entry, err := cq.GetEntry(currentIndex)
		if err != nil {
			// 如果获取失败，返回已读取的条目（不返回错误，允许部分读取）
			return entries, nil
		}

		entries = append(entries, entry)
		currentIndex++
	}

	return entries, nil
}

// GetEntriesByTag 根据Tag获取索引项
func (cq *ConsumeQueue) GetEntriesByTag(tag string, startIndex int64, count int) ([]*ConsumeQueueEntry, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	tagHash := cq.calculateTagHash([]string{tag})
	entries := make([]*ConsumeQueueEntry, 0, count)

	currentIndex := startIndex
	found := 0

	for found < count {
		if currentIndex >= cq.GetEntryCount() {
			break
		}

		entry, err := cq.GetEntry(currentIndex)
		if err != nil {
			return entries, err
		}

		// 检查Tag哈希是否匹配
		if entry.TagHash == tagHash {
			entries = append(entries, entry)
			found++
		}

		currentIndex++
	}

	return entries, nil
}

// GetEntriesByTime 根据时间范围获取索引项
func (cq *ConsumeQueue) GetEntriesByTime(startTime, endTime int64, startIndex int64, count int) ([]*ConsumeQueueEntry, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	entries := make([]*ConsumeQueueEntry, 0, count)
	currentIndex := startIndex

	for len(entries) < count {
		if currentIndex >= cq.GetEntryCount() {
			break
		}

		entry, err := cq.GetEntry(currentIndex)
		if err != nil {
			return entries, err
		}

		// 检查时间是否在范围内
		if entry.Timestamp >= startTime && entry.Timestamp <= endTime {
			entries = append(entries, entry)
		}

		currentIndex++
	}

	return entries, nil
}

// GetEntryCount 获取索引项总数
func (cq *ConsumeQueue) GetEntryCount() int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return cq.totalEntries
}

// GetWroteOffset 获取写偏移量
func (cq *ConsumeQueue) GetWroteOffset() int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return cq.wroteOffset
}

// GetCommittedOffset 获取提交偏移量
func (cq *ConsumeQueue) GetCommittedOffset() int64 {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return cq.committedOffset
}

// Commit 提交数据
func (cq *ConsumeQueue) Commit(commitOffset int64) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	if commitOffset < cq.committedOffset || commitOffset > cq.wroteOffset {
		return fmt.Errorf("invalid commit offset: %d, committed: %d, wrote: %d",
			commitOffset, cq.committedOffset, cq.wroteOffset)
	}

	cq.committedOffset = commitOffset

	// 提交所有相关文件
	for _, mf := range cq.mappedFileQueue.GetMappedFiles() {
		fileStartOffset := int64(cq.mappedFileQueue.CalculateFileIndex(0)) * cq.fileSize

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
func (cq *ConsumeQueue) Flush() error {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	return cq.mappedFileQueue.Flush()
}

// Close 关闭ConsumeQueue
func (cq *ConsumeQueue) Close() error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	return cq.mappedFileQueue.Close()
}

// GetTopic 获取主题名
func (cq *ConsumeQueue) GetTopic() string {
	return cq.topic
}

// GetQueueID 获取队列ID
func (cq *ConsumeQueue) GetQueueID() int {
	return cq.queueID
}

// calculateTagHash 计算Tag哈希值
func (cq *ConsumeQueue) calculateTagHash(tags []string) uint32 {
	if len(tags) == 0 {
		return 0
	}

	// 使用FNV-1a哈希算法
	hasher := fnv.New32a()

	for _, tag := range tags {
		hasher.Write([]byte(tag))
		hasher.Write([]byte{0}) // 分隔符
	}

	return hasher.Sum32()
}

// recover 恢复现有文件
func (cq *ConsumeQueue) recover() error {
	// 扫描目录中的所有文件
	pattern := filepath.Join(cq.storePath, "*.data")
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
		// 创建对应的MappedFile
		mf, err := NewMappedFile(file, cq.fileSize)
		if err != nil {
			return fmt.Errorf("failed to recover mapped file %s: %v", file, err)
		}

		// 添加到队列
		cq.mappedFileQueue.mappedFiles = append(cq.mappedFileQueue.mappedFiles, mf)

		// 更新写位置
		if mf.WrotePosition() > 0 {
			fileIndex := len(cq.mappedFileQueue.mappedFiles) - 1
			fileStartOffset := int64(fileIndex) * cq.fileSize
			cq.wroteOffset = fileStartOffset + mf.WrotePosition()
		}
	}

	// 重新计算统计信息
	cq.rebuildStats()

	return nil
}

// rebuildStats 重新计算统计信息
func (cq *ConsumeQueue) rebuildStats() {
	cq.totalEntries = cq.wroteOffset / 24 // 每个索引项24字节
}

// Truncate 截断ConsumeQueue到指定偏移量
func (cq *ConsumeQueue) Truncate(offset int64) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	if offset < 0 || offset > cq.wroteOffset {
		return fmt.Errorf("invalid truncate offset: %d", offset)
	}

	// 计算需要截断的文件
	fileIndex := cq.mappedFileQueue.CalculateFileIndex(offset)
	fileOffset := cq.mappedFileQueue.CalculateFileOffset(offset)

	// 截断文件
	mf, exists := cq.mappedFileQueue.GetMappedFileByIndex(fileIndex)
	if exists {
		if err := mf.file.Truncate(fileOffset); err != nil {
			return fmt.Errorf("failed to truncate file: %v", err)
		}
		mf.wrotePos = fileOffset
		mf.committedPos = fileOffset
		mf.flushedPos = fileOffset
	}

	// 删除后续文件
	for i := fileIndex + 1; i < len(cq.mappedFileQueue.mappedFiles); i++ {
		mf := cq.mappedFileQueue.mappedFiles[i]
		if err := os.Remove(mf.FileName()); err != nil {
			return fmt.Errorf("failed to remove file %s: %v", mf.FileName(), err)
		}
	}

	// 更新队列
	cq.mappedFileQueue.mappedFiles = cq.mappedFileQueue.mappedFiles[:fileIndex+1]
	cq.wroteOffset = offset
	cq.committedOffset = offset

	// 重新计算统计信息
	cq.rebuildStats()

	return nil
}

// ConsumeQueueManager ConsumeQueue管理器
type ConsumeQueueManager struct {
	storePath    string
	fileSize     int64
	maxFileCount int
	queues       map[string]map[int]*ConsumeQueue // topic -> queueID -> ConsumeQueue
	mutex        sync.RWMutex
}

// NewConsumeQueueManager 创建ConsumeQueue管理器
func NewConsumeQueueManager(storePath string, fileSize int64, maxFileCount int) *ConsumeQueueManager {
	return &ConsumeQueueManager{
		storePath:    storePath,
		fileSize:     fileSize,
		maxFileCount: maxFileCount,
		queues:       make(map[string]map[int]*ConsumeQueue),
	}
}

// GetOrCreateConsumeQueue 获取或创建ConsumeQueue
func (cqm *ConsumeQueueManager) GetOrCreateConsumeQueue(topic string, queueID int) (*ConsumeQueue, error) {
	cqm.mutex.Lock()
	defer cqm.mutex.Unlock()

	// 获取主题的队列映射
	topicQueues, exists := cqm.queues[topic]
	if !exists {
		topicQueues = make(map[int]*ConsumeQueue)
		cqm.queues[topic] = topicQueues
	}

	// 获取或创建队列
	cq, exists := topicQueues[queueID]
	if !exists {
		var err error
		cq, err = NewConsumeQueue(topic, queueID, cqm.storePath, cqm.fileSize, cqm.maxFileCount)
		if err != nil {
			return nil, err
		}
		topicQueues[queueID] = cq
	}

	return cq, nil
}

// GetConsumeQueue 获取ConsumeQueue
func (cqm *ConsumeQueueManager) GetConsumeQueue(topic string, queueID int) (*ConsumeQueue, bool) {
	cqm.mutex.RLock()
	defer cqm.mutex.RUnlock()

	topicQueues, exists := cqm.queues[topic]
	if !exists {
		return nil, false
	}

	cq, exists := topicQueues[queueID]
	return cq, exists
}

// GetAllConsumeQueues 获取所有ConsumeQueue
func (cqm *ConsumeQueueManager) GetAllConsumeQueues() []*ConsumeQueue {
	cqm.mutex.RLock()
	defer cqm.mutex.RUnlock()

	queues := make([]*ConsumeQueue, 0)
	for _, topicQueues := range cqm.queues {
		for _, cq := range topicQueues {
			queues = append(queues, cq)
		}
	}

	return queues
}

// Flush 刷新所有ConsumeQueue
func (cqm *ConsumeQueueManager) Flush() error {
	cqm.mutex.RLock()
	defer cqm.mutex.RUnlock()

	for _, topicQueues := range cqm.queues {
		for _, cq := range topicQueues {
			if err := cq.Flush(); err != nil {
				return fmt.Errorf("failed to flush consume queue for topic %s queue %d: %v",
					cq.GetTopic(), cq.GetQueueID(), err)
			}
		}
	}

	return nil
}

// Close 关闭所有ConsumeQueue
func (cqm *ConsumeQueueManager) Close() error {
	cqm.mutex.Lock()
	defer cqm.mutex.Unlock()

	var lastErr error
	for _, topicQueues := range cqm.queues {
		for _, cq := range topicQueues {
			if err := cq.Close(); err != nil {
				lastErr = err
			}
		}
	}

	cqm.queues = nil
	return lastErr
}
