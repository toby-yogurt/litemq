package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// IndexFile 消息索引文件
// 支持按Key或时间范围查询消息
type IndexFile struct {
	storePath string
	filePath  string

	// 索引结构
	hashSlots    []int32       // Hash槽位，每个槽位指向索引链表的第一个节点
	indexEntries []*IndexEntry // 索引条目链表

	// 文件管理
	fileSize        int64
	maxHashSlots    int // 最大Hash槽位数（默认500万）
	maxIndexEntries int // 最大索引条目数（默认2000万）

	// 统计信息
	totalEntries int // 当前索引条目数
	indexCount   int // 当前索引数量

	mutex sync.RWMutex
}

// IndexEntry 索引条目（20字节）
type IndexEntry struct {
	KeyHash   uint32 // Key的Hash值（4字节）
	PhyOffset int64  // 物理偏移量（8字节）
	TimeDiff  int32  // 时间差（相对于文件创建时间，秒）（4字节）
	Next      int32  // 下一个索引条目的位置（4字节）
}

// IndexHeader 索引文件头部（40字节）
type IndexHeader struct {
	MagicNumber    uint32   // 魔数（4字节）
	Version        uint16   // 版本号（2字节）
	CreateTime     int64    // 创建时间（毫秒）（8字节）
	LastUpdateTime int64    // 最后更新时间（毫秒）（8字节）
	HashSlotCount  int32    // Hash槽位数量（4字节）
	IndexCount     int32    // 索引条目数量（4字节）
	Reserved       [10]byte // 保留字段（10字节）
}

const (
	indexFileMagicNumber = 0x4C4D5104 // "LMQ" + index
	indexFileVersion     = 1
	indexEntrySize       = 20       // 每个索引条目20字节
	indexHeaderSize      = 40       // 头部40字节
	defaultHashSlots     = 5000000  // 默认500万个Hash槽位
	defaultIndexEntries  = 20000000 // 默认2000万个索引条目
)

// NewIndexFile 创建新的索引文件
func NewIndexFile(storePath string, fileSize int64) (*IndexFile, error) {
	// 确保目录存在
	if err := os.MkdirAll(storePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory %s: %v", storePath, err)
	}

	// 生成文件名（基于时间戳）
	now := time.Now()
	fileName := fmt.Sprintf("%s", now.Format("20060102150405"))
	filePath := filepath.Join(storePath, fileName)

	idx := &IndexFile{
		storePath:       storePath,
		filePath:        filePath,
		fileSize:        fileSize,
		maxHashSlots:    defaultHashSlots,
		maxIndexEntries: defaultIndexEntries,
		hashSlots:       make([]int32, defaultHashSlots),
		indexEntries:    make([]*IndexEntry, 0, defaultIndexEntries),
		totalEntries:    0,
		indexCount:      0,
	}

	// 初始化Hash槽位（全部设为-1，表示空）
	for i := range idx.hashSlots {
		idx.hashSlots[i] = -1
	}

	// 如果文件已存在，恢复索引
	if _, err := os.Stat(filePath); err == nil {
		if err := idx.recover(); err != nil {
			return nil, fmt.Errorf("failed to recover index file: %v", err)
		}
	}

	return idx, nil
}

// PutIndex 添加索引
func (idx *IndexFile) PutIndex(key string, phyOffset int64, storeTime int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// 检查是否超过最大索引条目数
	if idx.totalEntries >= idx.maxIndexEntries {
		return fmt.Errorf("index file is full, max entries: %d", idx.maxIndexEntries)
	}

	// 计算Key的Hash值
	keyHash := idx.hashKey(key)

	// 计算Hash槽位索引
	slotIndex := int(keyHash) % idx.maxHashSlots

	// 创建索引条目
	entry := &IndexEntry{
		KeyHash:   keyHash,
		PhyOffset: phyOffset,
		TimeDiff:  int32(storeTime - time.Now().Unix()), // 简化实现，实际应该使用文件创建时间
		Next:      -1,                                   // 初始化为-1
	}

	// 添加到索引条目数组
	entryIndex := idx.totalEntries
	idx.indexEntries = append(idx.indexEntries, entry)
	idx.totalEntries++

	// 插入到Hash槽位的链表中
	if idx.hashSlots[slotIndex] == -1 {
		// 槽位为空，直接插入
		idx.hashSlots[slotIndex] = int32(entryIndex)
	} else {
		// 槽位已有条目，插入到链表头部
		entry.Next = idx.hashSlots[slotIndex]
		idx.hashSlots[slotIndex] = int32(entryIndex)
	}

	idx.indexCount++

	return nil
}

// GetIndexByKey 根据Key查询索引
func (idx *IndexFile) GetIndexByKey(key string) ([]*IndexEntry, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// 计算Key的Hash值
	keyHash := idx.hashKey(key)

	// 计算Hash槽位索引
	slotIndex := int(keyHash) % idx.maxHashSlots

	// 获取槽位的第一个条目
	entryIndex := idx.hashSlots[slotIndex]
	if entryIndex == -1 {
		// 槽位为空，没有匹配的索引
		return []*IndexEntry{}, nil
	}

	// 遍历链表，查找匹配的索引
	results := make([]*IndexEntry, 0)
	currentIndex := entryIndex

	for currentIndex != -1 && currentIndex < int32(len(idx.indexEntries)) {
		entry := idx.indexEntries[currentIndex]
		if entry.KeyHash == keyHash {
			// 创建副本返回
			result := &IndexEntry{
				KeyHash:   entry.KeyHash,
				PhyOffset: entry.PhyOffset,
				TimeDiff:  entry.TimeDiff,
				Next:      entry.Next,
			}
			results = append(results, result)
		}
		currentIndex = entry.Next
	}

	return results, nil
}

// GetIndexByTimeRange 根据时间范围查询索引
func (idx *IndexFile) GetIndexByTimeRange(startTime, endTime int64) ([]*IndexEntry, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	results := make([]*IndexEntry, 0)

	// 遍历所有索引条目
	for _, entry := range idx.indexEntries {
		if entry == nil {
			continue
		}

		// 计算实际时间（简化实现，实际应该使用文件创建时间）
		actualTime := time.Now().Unix() + int64(entry.TimeDiff)

		// 检查是否在时间范围内
		if actualTime >= startTime && actualTime <= endTime {
			// 创建副本返回
			result := &IndexEntry{
				KeyHash:   entry.KeyHash,
				PhyOffset: entry.PhyOffset,
				TimeDiff:  entry.TimeDiff,
				Next:      entry.Next,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// Flush 刷新索引到磁盘
func (idx *IndexFile) Flush() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// 创建临时文件
	tmpFilePath := idx.filePath + ".tmp"
	file, err := os.Create(tmpFilePath)
	if err != nil {
		return fmt.Errorf("failed to create index file: %v", err)
	}
	defer file.Close()

	// 写入头部
	header := &IndexHeader{
		MagicNumber:    indexFileMagicNumber,
		Version:        indexFileVersion,
		CreateTime:     time.Now().UnixMilli(), // 简化实现
		LastUpdateTime: time.Now().UnixMilli(),
		HashSlotCount:  int32(idx.maxHashSlots),
		IndexCount:     int32(idx.indexCount),
	}

	if err := idx.writeHeader(file, header); err != nil {
		return fmt.Errorf("failed to write index header: %v", err)
	}

	// 写入Hash槽位
	if err := idx.writeHashSlots(file); err != nil {
		return fmt.Errorf("failed to write hash slots: %v", err)
	}

	// 写入索引条目
	if err := idx.writeIndexEntries(file); err != nil {
		return fmt.Errorf("failed to write index entries: %v", err)
	}

	// 同步到磁盘
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %v", err)
	}

	// 原子替换文件
	if err := os.Rename(tmpFilePath, idx.filePath); err != nil {
		return fmt.Errorf("failed to rename index file: %v", err)
	}

	return nil
}

// writeHeader 写入索引文件头部
func (idx *IndexFile) writeHeader(file *os.File, header *IndexHeader) error {
	// Magic number (4 bytes)
	if err := binary.Write(file, binary.BigEndian, header.MagicNumber); err != nil {
		return err
	}

	// Version (2 bytes)
	if err := binary.Write(file, binary.BigEndian, header.Version); err != nil {
		return err
	}

	// Create time (8 bytes)
	if err := binary.Write(file, binary.BigEndian, header.CreateTime); err != nil {
		return err
	}

	// Last update time (8 bytes)
	if err := binary.Write(file, binary.BigEndian, header.LastUpdateTime); err != nil {
		return err
	}

	// Hash slot count (4 bytes)
	if err := binary.Write(file, binary.BigEndian, header.HashSlotCount); err != nil {
		return err
	}

	// Index count (4 bytes)
	if err := binary.Write(file, binary.BigEndian, header.IndexCount); err != nil {
		return err
	}

	// Reserved (10 bytes)
	if _, err := file.Write(header.Reserved[:]); err != nil {
		return err
	}

	return nil
}

// writeHashSlots 写入Hash槽位
func (idx *IndexFile) writeHashSlots(file *os.File) error {
	for _, slot := range idx.hashSlots {
		if err := binary.Write(file, binary.BigEndian, slot); err != nil {
			return err
		}
	}
	return nil
}

// writeIndexEntries 写入索引条目
func (idx *IndexFile) writeIndexEntries(file *os.File) error {
	for _, entry := range idx.indexEntries {
		if entry == nil {
			continue
		}

		// KeyHash (4 bytes)
		if err := binary.Write(file, binary.BigEndian, entry.KeyHash); err != nil {
			return err
		}

		// PhyOffset (8 bytes)
		if err := binary.Write(file, binary.BigEndian, entry.PhyOffset); err != nil {
			return err
		}

		// TimeDiff (4 bytes)
		if err := binary.Write(file, binary.BigEndian, entry.TimeDiff); err != nil {
			return err
		}

		// Next (4 bytes)
		if err := binary.Write(file, binary.BigEndian, entry.Next); err != nil {
			return err
		}
	}
	return nil
}

// recover 恢复索引文件
func (idx *IndexFile) recover() error {
	file, err := os.Open(idx.filePath)
	if err != nil {
		return fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	// 读取头部
	header, err := idx.readHeader(file)
	if err != nil {
		return fmt.Errorf("failed to read index header: %v", err)
	}

	// 验证魔数
	if header.MagicNumber != indexFileMagicNumber {
		return fmt.Errorf("invalid index file magic number: %x", header.MagicNumber)
	}

	// 验证版本
	if header.Version != indexFileVersion {
		return fmt.Errorf("unsupported index file version: %d", header.Version)
	}

	// 读取Hash槽位
	if err := idx.readHashSlots(file, int(header.HashSlotCount)); err != nil {
		return fmt.Errorf("failed to read hash slots: %v", err)
	}

	// 读取索引条目
	if err := idx.readIndexEntries(file, int(header.IndexCount)); err != nil {
		return fmt.Errorf("failed to read index entries: %v", err)
	}

	idx.indexCount = int(header.IndexCount)
	idx.totalEntries = int(header.IndexCount)

	return nil
}

// readHeader 读取索引文件头部
func (idx *IndexFile) readHeader(file *os.File) (*IndexHeader, error) {
	header := &IndexHeader{}

	// Magic number (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.MagicNumber); err != nil {
		return nil, err
	}

	// Version (2 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.Version); err != nil {
		return nil, err
	}

	// Create time (8 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.CreateTime); err != nil {
		return nil, err
	}

	// Last update time (8 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.LastUpdateTime); err != nil {
		return nil, err
	}

	// Hash slot count (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.HashSlotCount); err != nil {
		return nil, err
	}

	// Index count (4 bytes)
	if err := binary.Read(file, binary.BigEndian, &header.IndexCount); err != nil {
		return nil, err
	}

	// Reserved (10 bytes)
	if _, err := file.Read(header.Reserved[:]); err != nil {
		return nil, err
	}

	return header, nil
}

// readHashSlots 读取Hash槽位
func (idx *IndexFile) readHashSlots(file *os.File, count int) error {
	idx.hashSlots = make([]int32, count)
	for i := 0; i < count; i++ {
		if err := binary.Read(file, binary.BigEndian, &idx.hashSlots[i]); err != nil {
			return err
		}
	}
	return nil
}

// readIndexEntries 读取索引条目
func (idx *IndexFile) readIndexEntries(file *os.File, count int) error {
	idx.indexEntries = make([]*IndexEntry, 0, count)
	for i := 0; i < count; i++ {
		entry := &IndexEntry{}

		// KeyHash (4 bytes)
		if err := binary.Read(file, binary.BigEndian, &entry.KeyHash); err != nil {
			return err
		}

		// PhyOffset (8 bytes)
		if err := binary.Read(file, binary.BigEndian, &entry.PhyOffset); err != nil {
			return err
		}

		// TimeDiff (4 bytes)
		if err := binary.Read(file, binary.BigEndian, &entry.TimeDiff); err != nil {
			return err
		}

		// Next (4 bytes)
		if err := binary.Read(file, binary.BigEndian, &entry.Next); err != nil {
			return err
		}

		idx.indexEntries = append(idx.indexEntries, entry)
	}
	return nil
}

// hashKey 计算Key的Hash值
func (idx *IndexFile) hashKey(key string) uint32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}

// GetStats 获取索引文件统计信息
func (idx *IndexFile) GetStats() map[string]interface{} {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	return map[string]interface{}{
		"file_path":         idx.filePath,
		"total_entries":     idx.totalEntries,
		"index_count":       idx.indexCount,
		"max_hash_slots":    idx.maxHashSlots,
		"max_index_entries": idx.maxIndexEntries,
	}
}
