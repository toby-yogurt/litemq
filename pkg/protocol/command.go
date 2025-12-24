package protocol

import (
	"encoding/binary"
	"fmt"
)

// CommandType 命令类型
type CommandType int16

const (
	// 生产者命令
	CommandSendMessage         CommandType = 1001 // 发送消息
	CommandSendMessageBatch    CommandType = 1002 // 批量发送消息
	CommandSendTransactionMsg  CommandType = 1003 // 发送事务消息
	CommandCommitTransaction   CommandType = 1004 // 提交事务
	CommandRollbackTransaction CommandType = 1005 // 回滚事务

	// 消费者命令
	CommandPullMessage      CommandType = 2001 // 拉取消息
	CommandConsumeAck       CommandType = 2002 // 消费确认
	CommandConsumeFail      CommandType = 2003 // 消费失败
	CommandConsumeRetry     CommandType = 2004 // 重试消费
	CommandSubscribeTopic   CommandType = 2005 // 订阅主题
	CommandUnsubscribeTopic CommandType = 2006 // 取消订阅

	// 管理命令
	CommandCreateTopic     CommandType = 3001 // 创建主题
	CommandDeleteTopic     CommandType = 3002 // 删除主题
	CommandGetTopicInfo    CommandType = 3003 // 获取主题信息
	CommandGetBrokerInfo   CommandType = 3004 // 获取Broker信息
	CommandGetConsumerList CommandType = 3005 // 获取消费者列表
	CommandGetProducerList CommandType = 3006 // 获取生产者列表

	// NameServer命令
	CommandRegisterBroker    CommandType = 4001 // Broker注册
	CommandUnregisterBroker  CommandType = 4002 // Broker注销
	CommandHeartbeat         CommandType = 4003 // 心跳
	CommandGetRouteInfo      CommandType = 4004 // 获取路由信息
	CommandGetNameServerList CommandType = 4005 // 获取NameServer列表

	// 响应命令
	CommandSuccess CommandType = 5001 // 成功响应
	CommandError   CommandType = 5002 // 错误响应
	CommandTimeout CommandType = 5003 // 超时响应

	// 特殊命令
	CommandCheckTransaction CommandType = 6001 // 检查事务状态
	CommandEndTransaction   CommandType = 6002 // 结束事务
)

// ResponseCode 响应码
type ResponseCode int16

const (
	ResponseSuccess          ResponseCode = 0  // 成功
	ResponseSystemError      ResponseCode = 1  // 系统错误
	ResponseTopicNotExist    ResponseCode = 2  // 主题不存在
	ResponseTopicExist       ResponseCode = 3  // 主题已存在
	ResponseBrokerNotExist   ResponseCode = 4  // Broker不存在
	ResponseNoMessage        ResponseCode = 5  // 没有消息
	ResponseMessageTooLarge  ResponseCode = 6  // 消息过大
	ResponseTimeout          ResponseCode = 7  // 超时
	ResponseParameterError   ResponseCode = 8  // 参数错误
	ResponsePermissionDenied ResponseCode = 9  // 权限拒绝
	ResponseTransactionError ResponseCode = 10 // 事务错误
	ResponseConsumerNotExist ResponseCode = 11 // 消费者不存在
	ResponseProducerNotExist ResponseCode = 12 // 生产者不存在
	ResponseDuplicateMessage ResponseCode = 13 // 重复消息
	ResponseNetworkError     ResponseCode = 14 // 网络错误
	ResponseStorageError     ResponseCode = 15 // 存储错误
	ResponseUnknownCommand   ResponseCode = 16 // 未知命令
)

// Command 命令结构
type Command struct {
	// 命令头
	CommandType CommandType `json:"command_type"` // 命令类型
	RequestID   string      `json:"request_id"`   // 请求ID
	Version     int16       `json:"version"`      // 协议版本
	Compress    bool        `json:"compress"`     // 是否压缩
	Timeout     int32       `json:"timeout"`      // 超时时间(毫秒)

	// 命令体
	Headers map[string]string `json:"headers"` // 请求头
	Body    []byte            `json:"body"`    // 请求体

	// 响应相关
	ResponseCode ResponseCode `json:"response_code,omitempty"` // 响应码
	ErrorMsg     string       `json:"error_msg,omitempty"`     // 错误信息

	// 扩展字段
	ExtFields map[string]interface{} `json:"ext_fields,omitempty"` // 扩展字段
}

// NewCommand 创建新命令
func NewCommand(cmdType CommandType) *Command {
	return &Command{
		CommandType: cmdType,
		RequestID:   generateRequestID(),
		Version:     1,
		Compress:    false,
		Timeout:     30000, // 默认30秒
		Headers:     make(map[string]string),
		ExtFields:   make(map[string]interface{}),
	}
}

// NewResponse 创建响应命令
func NewResponse(requestID string, responseCode ResponseCode, errorMsg string) *Command {
	cmd := NewCommand(CommandSuccess)
	cmd.RequestID = requestID
	cmd.ResponseCode = responseCode
	if errorMsg != "" {
		cmd.ErrorMsg = errorMsg
	}
	return cmd
}

// SetHeader 设置请求头
func (c *Command) SetHeader(key, value string) {
	if c.Headers == nil {
		c.Headers = make(map[string]string)
	}
	c.Headers[key] = value
}

// GetHeader 获取请求头
func (c *Command) GetHeader(key string) string {
	if c.Headers == nil {
		return ""
	}
	return c.Headers[key]
}

// SetExtField 设置扩展字段
func (c *Command) SetExtField(key string, value interface{}) {
	if c.ExtFields == nil {
		c.ExtFields = make(map[string]interface{})
	}
	c.ExtFields[key] = value
}

// GetExtField 获取扩展字段
func (c *Command) GetExtField(key string) interface{} {
	if c.ExtFields == nil {
		return nil
	}
	return c.ExtFields[key]
}

// IsSuccess 是否成功响应
func (c *Command) IsSuccess() bool {
	return c.ResponseCode == ResponseSuccess
}

// IsError 是否错误响应
func (c *Command) IsError() bool {
	return c.ResponseCode != ResponseSuccess
}

// Size 计算命令大小
func (c *Command) Size() int {
	size := 2 + // command type
		2 + len(c.RequestID) +
		2 + // version
		1 + // compress
		4 + // timeout
		4 + c.calculateHeadersSize() +
		4 + len(c.Body) +
		2 + // response code
		2 + len(c.ErrorMsg) +
		4 + c.calculateExtFieldsSize()

	return size
}

// calculateHeadersSize 计算请求头大小
func (c *Command) calculateHeadersSize() int {
	size := 0
	for key, value := range c.Headers {
		size += 2 + len(key) + 2 + len(value)
	}
	return size
}

// calculateExtFieldsSize 计算扩展字段大小 (简化实现，实际应序列化为JSON)
func (c *Command) calculateExtFieldsSize() int {
	size := 0
	for key, value := range c.ExtFields {
		size += 2 + len(key) + 4 // key + value size (简化)
		_ = value                // 实际需要计算value的大小
	}
	return size
}

// Encode 编码命令为字节数组
func (c *Command) Encode() ([]byte, error) {
	data := make([]byte, c.Size())
	offset := 0

	// Magic number
	binary.BigEndian.PutUint32(data[offset:], 0x4C4D5102) // "LMQ" + command version
	offset += 4

	// Command type
	binary.BigEndian.PutUint16(data[offset:], uint16(c.CommandType))
	offset += 2

	// Request ID
	requestIDBytes := []byte(c.RequestID)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(requestIDBytes)))
	offset += 2
	copy(data[offset:], requestIDBytes)
	offset += len(requestIDBytes)

	// Version
	binary.BigEndian.PutUint16(data[offset:], uint16(c.Version))
	offset += 2

	// Compress
	if c.Compress {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++

	// Timeout
	binary.BigEndian.PutUint32(data[offset:], uint32(c.Timeout))
	offset += 4

	// Headers
	binary.BigEndian.PutUint32(data[offset:], uint32(len(c.Headers)))
	offset += 4
	for key, value := range c.Headers {
		keyBytes := []byte(key)
		binary.BigEndian.PutUint16(data[offset:], uint16(len(keyBytes)))
		offset += 2
		copy(data[offset:], keyBytes)
		offset += len(keyBytes)

		valueBytes := []byte(value)
		binary.BigEndian.PutUint16(data[offset:], uint16(len(valueBytes)))
		offset += 2
		copy(data[offset:], valueBytes)
		offset += len(valueBytes)
	}

	// Body
	binary.BigEndian.PutUint32(data[offset:], uint32(len(c.Body)))
	offset += 4
	copy(data[offset:], c.Body)
	offset += len(c.Body)

	// Response code
	binary.BigEndian.PutUint16(data[offset:], uint16(c.ResponseCode))
	offset += 2

	// Error message
	errorMsgBytes := []byte(c.ErrorMsg)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(errorMsgBytes)))
	offset += 2
	copy(data[offset:], errorMsgBytes)
	offset += len(errorMsgBytes)

	// Ext fields (简化实现，实际应使用JSON序列化)
	binary.BigEndian.PutUint32(data[offset:], uint32(len(c.ExtFields)))
	offset += 4
	for key, value := range c.ExtFields {
		keyBytes := []byte(key)
		binary.BigEndian.PutUint16(data[offset:], uint16(len(keyBytes)))
		offset += 2
		copy(data[offset:], keyBytes)
		offset += len(keyBytes)

		// 简化处理，实际需要根据类型序列化
		valueStr := fmt.Sprintf("%v", value)
		valueBytes := []byte(valueStr)
		binary.BigEndian.PutUint32(data[offset:], uint32(len(valueBytes)))
		offset += 4
		copy(data[offset:], valueBytes)
		offset += len(valueBytes)
	}

	return data, nil
}

// DecodeCommand 从字节数组解码命令
func DecodeCommand(data []byte) (*Command, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("command data too short")
	}

	// Check magic number
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != 0x4C4D5102 {
		return nil, fmt.Errorf("invalid magic number: %x", magic)
	}

	cmd := &Command{
		Headers:   make(map[string]string),
		ExtFields: make(map[string]interface{}),
	}
	offset := 4

	// Command type
	if offset+2 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.CommandType = CommandType(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Request ID
	if offset+2 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	requestIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+requestIDLen > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.RequestID = string(data[offset : offset+requestIDLen])
	offset += requestIDLen

	// Version
	if offset+2 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.Version = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Compress
	if offset+1 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.Compress = data[offset] == 1
	offset++

	// Timeout
	if offset+4 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.Timeout = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Headers
	if offset+4 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	headersCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	for i := 0; i < headersCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+2 > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		valueLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+valueLen > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		value := string(data[offset : offset+valueLen])
		offset += valueLen

		cmd.Headers[key] = value
	}

	// Body
	if offset+4 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	bodyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+bodyLen > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.Body = make([]byte, bodyLen)
	copy(cmd.Body, data[offset:offset+bodyLen])
	offset += bodyLen

	// Response code
	if offset+2 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.ResponseCode = ResponseCode(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Error message
	if offset+2 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	errorMsgLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+errorMsgLen > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	cmd.ErrorMsg = string(data[offset : offset+errorMsgLen])
	offset += errorMsgLen

	// Ext fields (简化实现)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("command data corrupted")
	}
	extFieldsCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	for i := 0; i < extFieldsCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+4 > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		valueLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+int(valueLen) > len(data) {
			return nil, fmt.Errorf("command data corrupted")
		}
		valueStr := string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)

		cmd.ExtFields[key] = valueStr // 简化处理，实际应反序列化
	}

	return cmd, nil
}

// String 返回命令的字符串表示
func (c *Command) String() string {
	return fmt.Sprintf("Command{Type:%d, RequestID:%s, ResponseCode:%d}",
		c.CommandType, c.RequestID, c.ResponseCode)
}

// generateRequestID 生成请求ID (简化实现)
func generateRequestID() string {
	return fmt.Sprintf("req-%d", 0) // 实际应该使用更复杂的ID生成算法
}

// GetCommandTypeName 获取命令类型名称
func (ct CommandType) String() string {
	switch ct {
	case CommandSendMessage:
		return "SendMessage"
	case CommandSendMessageBatch:
		return "SendMessageBatch"
	case CommandPullMessage:
		return "PullMessage"
	case CommandConsumeAck:
		return "ConsumeAck"
	case CommandHeartbeat:
		return "Heartbeat"
	case CommandSuccess:
		return "Success"
	case CommandError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}

// GetResponseCodeName 获取响应码名称
func (rc ResponseCode) String() string {
	switch rc {
	case ResponseSuccess:
		return "Success"
	case ResponseSystemError:
		return "SystemError"
	case ResponseTopicNotExist:
		return "TopicNotExist"
	case ResponseTimeout:
		return "Timeout"
	default:
		return fmt.Sprintf("Unknown(%d)", rc)
	}
}
