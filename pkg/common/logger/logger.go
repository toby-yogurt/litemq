package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Level 日志级别
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String 返回日志级别的字符串表示
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger LiteMQ日志器
type Logger struct {
	logger *slog.Logger
	level  Level
}

// Config 日志配置
type Config struct {
	Level      string `toml:"level"`       // 日志级别: debug, info, warn, error
	Format     string `toml:"format"`      // 输出格式: text, json
	Output     string `toml:"output"`      // 输出目标: stdout, stderr, file
	FilePath   string `toml:"file_path"`   // 日志文件路径
	MaxSize    int    `toml:"max_size"`    // 单个日志文件最大大小(MB)
	MaxAge     int    `toml:"max_age"`     // 日志文件保留天数
	MaxBackups int    `toml:"max_backups"` // 最大备份文件数
	Compress   bool   `toml:"compress"`    // 是否压缩旧日志文件
}

// DefaultConfig 返回默认日志配置
func DefaultConfig() *Config {
	return &Config{
		Level:      "info",
		Format:     "text",
		Output:     "stdout",
		FilePath:   "./logs/litemq.log",
		MaxSize:    100, // 100MB
		MaxAge:     30,  // 30天
		MaxBackups: 10,  // 10个备份
		Compress:   true,
	}
}

// NewLogger 创建新的日志器
func NewLogger(config *Config) (*Logger, error) {
	level, err := parseLevel(config.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %v", err)
	}

	var writer io.Writer
	switch config.Output {
	case "stdout":
		writer = os.Stdout
	case "stderr":
		writer = os.Stderr
	case "file":
		// 确保日志目录存在
		dir := filepath.Dir(config.FilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}

		writer = &lumberjack.Logger{
			Filename:   config.FilePath,
			MaxSize:    config.MaxSize,
			MaxAge:     config.MaxAge,
			MaxBackups: config.MaxBackups,
			Compress:   config.Compress,
		}
	default:
		return nil, fmt.Errorf("unsupported output: %s", config.Output)
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: slog.Level(level),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// 自定义时间格式
			if a.Key == slog.TimeKey {
				return slog.Attr{
					Key:   "time",
					Value: slog.StringValue(a.Value.Time().Format("2006-01-02 15:04:05.000")),
				}
			}
			return a
		},
	}

	switch config.Format {
	case "json":
		handler = slog.NewJSONHandler(writer, opts)
	case "text":
		handler = slog.NewTextHandler(writer, opts)
	default:
		return nil, fmt.Errorf("unsupported format: %s", config.Format)
	}

	logger := &Logger{
		logger: slog.New(handler),
		level:  level,
	}

	return logger, nil
}

// parseLevel 解析日志级别字符串
func parseLevel(level string) (Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	default:
		return LevelInfo, fmt.Errorf("unknown level: %s", level)
	}
}

// Debug 记录调试日志
func (l *Logger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

// Info 记录信息日志
func (l *Logger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

// Warn 记录警告日志
func (l *Logger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

// Error 记录错误日志
func (l *Logger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

// WithContext 返回带有上下文的日志器
func (l *Logger) WithContext(ctx context.Context) *Logger {
	return &Logger{
		logger: l.logger.With(),
		level:  l.level,
	}
}

// WithFields 返回带有额外字段的日志器
func (l *Logger) WithFields(args ...any) *Logger {
	return &Logger{
		logger: l.logger.With(args...),
		level:  l.level,
	}
}

// SetLevel 设置日志级别
func (l *Logger) SetLevel(level Level) {
	l.level = level
	// Note: slog 不支持动态改变级别，这里只是保存状态
}

// GetLevel 获取当前日志级别
func (l *Logger) GetLevel() Level {
	return l.level
}

// Flush 刷新日志缓冲区（对于文件日志）
func (l *Logger) Flush() error {
	// 对于 lumberjack，它会自动处理刷新
	return nil
}

// Close 关闭日志器
func (l *Logger) Close() error {
	// 清理资源
	return nil
}

// 全局日志器实例
var defaultLogger *Logger

// InitDefaultLogger 初始化默认日志器
func InitDefaultLogger(config *Config) error {
	logger, err := NewLogger(config)
	if err != nil {
		return err
	}
	defaultLogger = logger
	return nil
}

// GetDefaultLogger 获取默认日志器
func GetDefaultLogger() *Logger {
	if defaultLogger == nil {
		// 如果没有初始化，使用默认配置
		config := DefaultConfig()
		if err := InitDefaultLogger(config); err != nil {
			// 最后的后备方案：使用标准输出
			defaultLogger = &Logger{
				logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{})),
				level:  LevelInfo,
			}
		}
	}
	return defaultLogger
}

// 便捷函数
func Debug(msg string, args ...any) { GetDefaultLogger().Debug(msg, args...) }
func Info(msg string, args ...any)  { GetDefaultLogger().Info(msg, args...) }
func Warn(msg string, args ...any)  { GetDefaultLogger().Warn(msg, args...) }
func Error(msg string, args ...any) { GetDefaultLogger().Error(msg, args...) }
