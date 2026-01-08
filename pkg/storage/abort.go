package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AbortFile 异常标记文件管理器
// 用于标记系统是否正常关闭，在异常关闭时保留标记，正常关闭时删除标记
type AbortFile struct {
	storePath string
	filePath  string
	exists    bool
	mutex     sync.RWMutex
}

const (
	abortFileName = "abort"
)

// NewAbortFile 创建异常标记文件管理器
func NewAbortFile(storePath string) (*AbortFile, error) {
	// 确保目录存在
	if err := os.MkdirAll(storePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create abort directory %s: %v", storePath, err)
	}

	filePath := filepath.Join(storePath, abortFileName)
	af := &AbortFile{
		storePath: storePath,
		filePath:  filePath,
		exists:    false,
	}

	// 检查文件是否存在
	if _, err := os.Stat(filePath); err == nil {
		af.exists = true
	}

	return af, nil
}

// Create 创建异常标记文件
// 在系统启动时创建，标记系统正在运行
func (af *AbortFile) Create() error {
	af.mutex.Lock()
	defer af.mutex.Unlock()

	// 如果文件已存在，先删除
	if af.exists {
		if err := os.Remove(af.filePath); err != nil {
			return fmt.Errorf("failed to remove existing abort file: %v", err)
		}
	}

	// 创建新文件
	file, err := os.Create(af.filePath)
	if err != nil {
		return fmt.Errorf("failed to create abort file: %v", err)
	}
	defer file.Close()

	// 写入时间戳（可选，用于记录创建时间）
	timestamp := time.Now().UnixMilli()
	if _, err := file.WriteString(fmt.Sprintf("%d", timestamp)); err != nil {
		return fmt.Errorf("failed to write abort file: %v", err)
	}

	// 同步到磁盘
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync abort file: %v", err)
	}

	af.exists = true
	return nil
}

// Remove 删除异常标记文件
// 在系统正常关闭时删除，表示正常关闭
func (af *AbortFile) Remove() error {
	af.mutex.Lock()
	defer af.mutex.Unlock()

	if !af.exists {
		return nil // 文件不存在，无需删除
	}

	if err := os.Remove(af.filePath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove abort file: %v", err)
		}
	}

	af.exists = false
	return nil
}

// Exists 检查异常标记文件是否存在
// 如果存在，说明上次是异常关闭
func (af *AbortFile) Exists() bool {
	af.mutex.RLock()
	defer af.mutex.RUnlock()

	// 双重检查，确保文件状态是最新的
	if _, err := os.Stat(af.filePath); err == nil {
		af.exists = true
		return true
	}

	af.exists = false
	return false
}

// IsAbnormalShutdown 检查上次是否异常关闭
func (af *AbortFile) IsAbnormalShutdown() bool {
	return af.Exists()
}

// GetFilePath 获取异常标记文件路径
func (af *AbortFile) GetFilePath() string {
	return af.filePath
}

// GetStorePath 获取存储路径
func (af *AbortFile) GetStorePath() string {
	return af.storePath
}
