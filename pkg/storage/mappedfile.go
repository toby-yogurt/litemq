package storage

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"litemq/pkg/common/logger"
)

// MappedFile 内存映射文件
type MappedFile struct {
	fileName     string
	file         *os.File
	data         []byte
	size         int64
	wrotePos     int64
	committedPos int64
	flushedPos   int64
	mutex        sync.RWMutex
}

// MappedFileQueue 内存映射文件队列
type MappedFileQueue struct {
	storePath    string
	fileSize     int64
	maxFileCount int
	mappedFiles  []*MappedFile
	writeIndex   int
	flushIndex   int
	mutex        sync.RWMutex
}

// NewMappedFile 创建新的内存映射文件
func NewMappedFile(fileName string, fileSize int64) (*MappedFile, error) {
	// 确保目录存在
	dir := fileName[:len(fileName)-len(getFileName(fileName))]
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	// 打开或创建文件
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
	}

	// 获取文件信息
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %v", fileName, err)
	}

	// 如果文件大小为0，需要扩展文件
	if stat.Size() == 0 {
		if err := file.Truncate(fileSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to truncate file %s: %v", fileName, err)
		}
	} else if stat.Size() != fileSize {
		file.Close()
		return nil, fmt.Errorf("file %s size mismatch, expected %d, got %d", fileName, fileSize, stat.Size())
	}

	// 内存映射
	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %v", fileName, err)
	}

	mf := &MappedFile{
		fileName:     fileName,
		file:         file,
		data:         data,
		size:         fileSize,
		wrotePos:     0,
		committedPos: 0,
		flushedPos:   0,
	}

	return mf, nil
}

// Write 写入数据
func (mf *MappedFile) Write(data []byte) (int, error) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	dataLen := int64(len(data))
	if mf.wrotePos+dataLen > mf.size {
		return 0, fmt.Errorf("not enough space in mapped file, wrotePos: %d, dataLen: %d, size: %d",
			mf.wrotePos, dataLen, mf.size)
	}

	copy(mf.data[mf.wrotePos:], data)
	mf.wrotePos += dataLen

	return len(data), nil
}

// WriteAt 在指定位置写入数据
func (mf *MappedFile) WriteAt(offset int64, data []byte) (int, error) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	dataLen := int64(len(data))
	if offset+dataLen > mf.size {
		return 0, fmt.Errorf("write beyond file size, offset: %d, dataLen: %d, size: %d",
			offset, dataLen, mf.size)
	}

	copy(mf.data[offset:], data)

	// 更新写位置
	if offset+dataLen > mf.wrotePos {
		mf.wrotePos = offset + dataLen
	}

	return len(data), nil
}

// Read 读取数据
func (mf *MappedFile) Read(offset, length int64) ([]byte, error) {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()

	if offset < 0 || offset >= mf.size {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	if offset+length > mf.size {
		length = mf.size - offset
	}

	if length <= 0 {
		return []byte{}, nil
	}

	data := make([]byte, length)
	copy(data, mf.data[offset:offset+length])

	return data, nil
}

// ReadAt 读取指定位置的数据
func (mf *MappedFile) ReadAt(offset, length int64) ([]byte, error) {
	return mf.Read(offset, length)
}

// Commit 提交数据（更新提交位置）
func (mf *MappedFile) Commit(commitPos int64) error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if commitPos < 0 || commitPos > mf.wrotePos {
		return fmt.Errorf("invalid commit position: %d, wrotePos: %d", commitPos, mf.wrotePos)
	}

	mf.committedPos = commitPos
	return nil
}

// Flush 刷新数据到磁盘
func (mf *MappedFile) Flush() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	// 使用msync刷新到磁盘
	if mf.committedPos > mf.flushedPos {
		_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
			uintptr(unsafe.Pointer(&mf.data[mf.flushedPos])),
			uintptr(mf.committedPos-mf.flushedPos),
			syscall.MS_SYNC)
		if err != 0 {
			return fmt.Errorf("failed to msync: %v", err)
		}
		mf.flushedPos = mf.committedPos
	}

	return nil
}

// FlushRange 刷新指定范围的数据
func (mf *MappedFile) FlushRange(start, end int64) error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if start < 0 || end > mf.committedPos || start >= end {
		return fmt.Errorf("invalid flush range: start=%d, end=%d, committedPos=%d",
			start, end, mf.committedPos)
	}

	// 使用msync刷新指定范围
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mf.data[start])),
		uintptr(end-start),
		syscall.MS_SYNC)
	if err != 0 {
		return fmt.Errorf("failed to msync range: %v", err)
	}

	if end > mf.flushedPos {
		mf.flushedPos = end
	}

	return nil
}

// Close 关闭文件
func (mf *MappedFile) Close() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	// 刷新所有数据
	if err := mf.Flush(); err != nil {
		// 记录错误但不阻止关闭
		logger.Warn("Failed to flush data before close", "error", err)
	}

	// 取消内存映射
	if mf.data != nil {
		if err := syscall.Munmap(mf.data); err != nil {
			return fmt.Errorf("failed to munmap: %v", err)
		}
		mf.data = nil
	}

	// 关闭文件
	if mf.file != nil {
		if err := mf.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
		mf.file = nil
	}

	return nil
}

// Size 获取文件大小
func (mf *MappedFile) Size() int64 {
	return mf.size
}

// WrotePosition 获取写位置
func (mf *MappedFile) WrotePosition() int64 {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.wrotePos
}

// CommittedPosition 获取提交位置
func (mf *MappedFile) CommittedPosition() int64 {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.committedPos
}

// FlushedPosition 获取刷新位置
func (mf *MappedFile) FlushedPosition() int64 {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.flushedPos
}

// RemainingSpace 获取剩余空间
func (mf *MappedFile) RemainingSpace() int64 {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.size - mf.wrotePos
}

// IsFull 检查文件是否已满
func (mf *MappedFile) IsFull() bool {
	mf.mutex.RLock()
	defer mf.mutex.RUnlock()
	return mf.wrotePos >= mf.size
}

// FileName 获取文件名
func (mf *MappedFile) FileName() string {
	return mf.fileName
}

// NewMappedFileQueue 创建新的内存映射文件队列
func NewMappedFileQueue(storePath string, fileSize int64, maxFileCount int) *MappedFileQueue {
	return &MappedFileQueue{
		storePath:    storePath,
		fileSize:     fileSize,
		maxFileCount: maxFileCount,
		mappedFiles:  make([]*MappedFile, 0),
		writeIndex:   0,
		flushIndex:   0,
	}
}

// GetMappedFile 获取当前写入的文件
func (mfq *MappedFileQueue) GetMappedFile() (*MappedFile, error) {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()

	// 如果没有文件或当前文件已满，创建新文件
	if len(mfq.mappedFiles) == 0 || mfq.mappedFiles[mfq.writeIndex].IsFull() {
		if err := mfq.createNewMappedFile(); err != nil {
			return nil, err
		}
	}

	return mfq.mappedFiles[mfq.writeIndex], nil
}

// GetMappedFileByIndex 根据索引获取文件
func (mfq *MappedFileQueue) GetMappedFileByIndex(index int) (*MappedFile, bool) {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()

	if index < 0 || index >= len(mfq.mappedFiles) {
		return nil, false
	}

	return mfq.mappedFiles[index], true
}

// GetMappedFiles 获取所有文件
func (mfq *MappedFileQueue) GetMappedFiles() []*MappedFile {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()

	files := make([]*MappedFile, len(mfq.mappedFiles))
	copy(files, mfq.mappedFiles)
	return files
}

// Flush 刷新所有文件
func (mfq *MappedFileQueue) Flush() error {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()

	for _, mf := range mfq.mappedFiles {
		if err := mf.Flush(); err != nil {
			return fmt.Errorf("failed to flush mapped file %s: %v", mf.FileName(), err)
		}
	}

	return nil
}

// Close 关闭所有文件
func (mfq *MappedFileQueue) Close() error {
	mfq.mutex.Lock()
	defer mfq.mutex.Unlock()

	var lastErr error
	for _, mf := range mfq.mappedFiles {
		if err := mf.Close(); err != nil {
			lastErr = err
		}
	}

	mfq.mappedFiles = nil
	return lastErr
}

// GetTotalSize 获取总大小
func (mfq *MappedFileQueue) GetTotalSize() int64 {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()

	return int64(len(mfq.mappedFiles)) * mfq.fileSize
}

// GetFileCount 获取文件数量
func (mfq *MappedFileQueue) GetFileCount() int {
	mfq.mutex.RLock()
	defer mfq.mutex.RUnlock()

	return len(mfq.mappedFiles)
}

// createNewMappedFile 创建新的内存映射文件
func (mfq *MappedFileQueue) createNewMappedFile() error {
	// 检查是否超过最大文件数
	if len(mfq.mappedFiles) >= mfq.maxFileCount {
		return fmt.Errorf("exceed max file count: %d", mfq.maxFileCount)
	}

	// 生成文件名
	fileIndex := len(mfq.mappedFiles)
	fileName := fmt.Sprintf("%s/%020d", mfq.storePath, fileIndex)

	// 创建内存映射文件
	mf, err := NewMappedFile(fileName, mfq.fileSize)
	if err != nil {
		return fmt.Errorf("failed to create mapped file %s: %v", fileName, err)
	}

	mfq.mappedFiles = append(mfq.mappedFiles, mf)
	mfq.writeIndex = len(mfq.mappedFiles) - 1

	return nil
}

// getFileName 从路径中提取文件名
func getFileName(filePath string) string {
	for i := len(filePath) - 1; i >= 0; i-- {
		if filePath[i] == '/' {
			return filePath[i+1:]
		}
	}
	return filePath
}

// CalculateFileIndex 根据偏移量计算文件索引
func (mfq *MappedFileQueue) CalculateFileIndex(offset int64) int {
	if mfq.fileSize == 0 {
		return 0
	}
	return int(offset / mfq.fileSize)
}

// CalculateFileOffset 根据偏移量计算文件内偏移
func (mfq *MappedFileQueue) CalculateFileOffset(offset int64) int64 {
	if mfq.fileSize == 0 {
		return offset
	}
	return offset % mfq.fileSize
}

// GetMappedFileByOffset 根据全局偏移量获取对应的文件
func (mfq *MappedFileQueue) GetMappedFileByOffset(offset int64) (*MappedFile, int64, error) {
	fileIndex := mfq.CalculateFileIndex(offset)
	fileOffset := mfq.CalculateFileOffset(offset)

	mf, exists := mfq.GetMappedFileByIndex(fileIndex)
	if !exists {
		return nil, 0, fmt.Errorf("mapped file not found for offset %d, fileIndex: %d", offset, fileIndex)
	}

	return mf, fileOffset, nil
}
