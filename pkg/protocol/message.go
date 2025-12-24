package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

// MessageType 消息类型
type MessageType int

const (
	MessageTypeNormal      MessageType = 0 // 普通消息
	MessageTypeDelay       MessageType = 1 // 延时消息
	MessageTypeTransaction MessageType = 2 // 事务消息
	MessageTypeBroadcast   MessageType = 3 // 广播消息
	MessageTypeOrder       MessageType = 4 // 顺序消息
)

// MessageStatus 消息状态
type MessageStatus int

const (
	MessageStatusNormal   MessageStatus = 0 // 正常
	MessageStatusPrepared MessageStatus = 1 // 事务消息准备状态
	MessageStatusRollback MessageStatus = 2 // 事务消息回滚
	MessageStatusCommit   MessageStatus = 3 // 事务消息提交
	MessageStatusDead     MessageStatus = 4 // 死信消息
)

// ConsumeStatus 消费状态
type ConsumeStatus int

const (
	ConsumeStatusSuccess ConsumeStatus = 0 // 消费成功
	ConsumeStatusRetry   ConsumeStatus = 1 // 需要重试
	ConsumeStatusFail    ConsumeStatus = 2 // 消费失败
)

// Message 消息结构
type Message struct {
	// 消息头
	MessageID  string            `json:"message_id"`           // 消息ID，全局唯一
	Topic      string            `json:"topic"`                // 主题
	Tags       []string          `json:"tags,omitempty"`       // 标签，用于消息过滤
	Keys       []string          `json:"keys,omitempty"`       // 键，用于消息去重
	Properties map[string]string `json:"properties,omitempty"` // 扩展属性

	// 消息体
	Body       []byte `json:"body"` // 消息内容
	BodyLength int    `json:"-"`    // 消息体长度（不序列化）

	// 消息控制
	MessageType MessageType `json:"message_type"` // 消息类型
	Priority    int         `json:"priority"`     // 消息优先级 0-255
	Reliability int         `json:"reliability"`  // 可靠性等级 0-3

	// 延时消息
	DelayTime int64 `json:"delay_time,omitempty"` // 延时时间戳（毫秒）

	// 事务消息
	TransactionID string        `json:"transaction_id,omitempty"` // 事务ID
	MessageStatus MessageStatus `json:"message_status"`           // 消息状态

	// 顺序消息
	ShardingKey string `json:"sharding_key,omitempty"` // 分片键

	// 广播消息
	Broadcast bool `json:"broadcast,omitempty"` // 是否广播消息

	// 消息元数据
	BornTimestamp int64  `json:"born_timestamp"` // 消息创建时间戳
	BornHost      string `json:"born_host"`      // 消息创建主机
	StoreHost     string `json:"store_host"`     // 消息存储主机

	// 存储相关（运行时字段）
	QueueID         int   `json:"-"` // 队列ID
	QueueOffset     int64 `json:"-"` // 队列偏移量
	CommitLogOffset int64 `json:"-"` // CommitLog偏移量
	StoreSize       int   `json:"-"` // 存储大小
	StoreTimestamp  int64 `json:"-"` // 存储时间戳

	// 消费相关（运行时字段）
	ConsumeStartTime int64 `json:"-"` // 开始消费时间
	ConsumeEndTime   int64 `json:"-"` // 结束消费时间
	ConsumeCount     int   `json:"-"` // 消费次数
}

// NewMessage 创建新消息
func NewMessage(topic string, body []byte) *Message {
	now := time.Now().UnixMilli()
	return &Message{
		MessageID:      generateMessageID(),
		Topic:          topic,
		Body:           body,
		BodyLength:     len(body),
		MessageType:    MessageTypeNormal,
		Priority:       3, // 默认优先级
		Reliability:    1, // 默认可靠性
		MessageStatus:  MessageStatusNormal,
		BornTimestamp:  now,
		StoreTimestamp: now,
		ConsumeCount:   0,
	}
}

// NewDelayMessage 创建延时消息
func NewDelayMessage(topic string, body []byte, delayTime int64) *Message {
	msg := NewMessage(topic, body)
	msg.MessageType = MessageTypeDelay
	msg.DelayTime = delayTime
	return msg
}

// NewTransactionMessage 创建事务消息
func NewTransactionMessage(topic string, body []byte, transactionID string) *Message {
	msg := NewMessage(topic, body)
	msg.MessageType = MessageTypeTransaction
	msg.TransactionID = transactionID
	msg.MessageStatus = MessageStatusPrepared
	return msg
}

// NewBroadcastMessage 创建广播消息
func NewBroadcastMessage(topic string, body []byte) *Message {
	msg := NewMessage(topic, body)
	msg.MessageType = MessageTypeBroadcast
	msg.Broadcast = true
	return msg
}

// NewOrderMessage 创建顺序消息
func NewOrderMessage(topic string, body []byte, shardingKey string) *Message {
	msg := NewMessage(topic, body)
	msg.MessageType = MessageTypeOrder
	msg.ShardingKey = shardingKey
	return msg
}

// SetProperty 设置消息属性
func (m *Message) SetProperty(key, value string) {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	m.Properties[key] = value
}

// GetProperty 获取消息属性
func (m *Message) GetProperty(key string) string {
	if m.Properties == nil {
		return ""
	}
	return m.Properties[key]
}

// AddTag 添加标签
func (m *Message) AddTag(tag string) {
	m.Tags = append(m.Tags, tag)
}

// AddKey 添加键
func (m *Message) AddKey(key string) {
	m.Keys = append(m.Keys, key)
}

// Size 返回消息大小
func (m *Message) Size() int {
	return 4 + // magic number
		8 + len(m.MessageID) +
		2 + len(m.Topic) +
		4 + m.calculateTagsSize() +
		4 + m.calculateKeysSize() +
		4 + m.calculatePropertiesSize() +
		4 + len(m.Body) +
		1 + // message type
		1 + // priority
		1 + // reliability
		8 + // delay time
		2 + len(m.TransactionID) +
		1 + // message status
		2 + len(m.ShardingKey) +
		1 + // broadcast
		8 + // born timestamp
		2 + len(m.BornHost) +
		2 + len(m.StoreHost) +
		4 + // queue id
		8 + // queue offset
		8 + // commit log offset
		4 + // store size
		8 + // store timestamp
		8 + // consume start time
		8 + // consume end time
		4 // consume count
}

// calculateTagsSize 计算标签大小
func (m *Message) calculateTagsSize() int {
	size := 0
	for _, tag := range m.Tags {
		size += 2 + len(tag)
	}
	return size
}

// calculateKeysSize 计算键大小
func (m *Message) calculateKeysSize() int {
	size := 0
	for _, key := range m.Keys {
		size += 2 + len(key)
	}
	return size
}

// calculatePropertiesSize 计算属性大小
func (m *Message) calculatePropertiesSize() int {
	size := 0
	for key, value := range m.Properties {
		size += 2 + len(key) + 2 + len(value)
	}
	return size
}

// Encode 编码消息为字节数组
func (m *Message) Encode() ([]byte, error) {
	data := make([]byte, m.Size())
	offset := 0

	// Magic number
	binary.BigEndian.PutUint32(data[offset:], 0x4C4D5101) // "LMQ" + version
	offset += 4

	// Message ID
	messageIDBytes := []byte(m.MessageID)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(messageIDBytes)))
	offset += 2
	copy(data[offset:], messageIDBytes)
	offset += len(messageIDBytes)

	// Topic
	topicBytes := []byte(m.Topic)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(topicBytes)))
	offset += 2
	copy(data[offset:], topicBytes)
	offset += len(topicBytes)

	// Tags
	binary.BigEndian.PutUint32(data[offset:], uint32(len(m.Tags)))
	offset += 4
	for _, tag := range m.Tags {
		tagBytes := []byte(tag)
		binary.BigEndian.PutUint16(data[offset:], uint16(len(tagBytes)))
		offset += 2
		copy(data[offset:], tagBytes)
		offset += len(tagBytes)
	}

	// Keys
	binary.BigEndian.PutUint32(data[offset:], uint32(len(m.Keys)))
	offset += 4
	for _, key := range m.Keys {
		keyBytes := []byte(key)
		binary.BigEndian.PutUint16(data[offset:], uint16(len(keyBytes)))
		offset += 2
		copy(data[offset:], keyBytes)
		offset += len(keyBytes)
	}

	// Properties
	binary.BigEndian.PutUint32(data[offset:], uint32(len(m.Properties)))
	offset += 4
	for key, value := range m.Properties {
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
	binary.BigEndian.PutUint32(data[offset:], uint32(len(m.Body)))
	offset += 4
	copy(data[offset:], m.Body)
	offset += len(m.Body)

	// Message type, priority, reliability
	data[offset] = byte(m.MessageType)
	offset++
	data[offset] = byte(m.Priority)
	offset++
	data[offset] = byte(m.Reliability)
	offset++

	// Delay time
	binary.BigEndian.PutUint64(data[offset:], uint64(m.DelayTime))
	offset += 8

	// Transaction ID
	transactionIDBytes := []byte(m.TransactionID)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(transactionIDBytes)))
	offset += 2
	copy(data[offset:], transactionIDBytes)
	offset += len(transactionIDBytes)

	// Message status
	data[offset] = byte(m.MessageStatus)
	offset++

	// Sharding key
	shardingKeyBytes := []byte(m.ShardingKey)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(shardingKeyBytes)))
	offset += 2
	copy(data[offset:], shardingKeyBytes)
	offset += len(shardingKeyBytes)

	// Broadcast
	if m.Broadcast {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++

	// Timestamps
	binary.BigEndian.PutUint64(data[offset:], uint64(m.BornTimestamp))
	offset += 8

	// Hosts
	bornHostBytes := []byte(m.BornHost)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(bornHostBytes)))
	offset += 2
	copy(data[offset:], bornHostBytes)
	offset += len(bornHostBytes)

	storeHostBytes := []byte(m.StoreHost)
	binary.BigEndian.PutUint16(data[offset:], uint16(len(storeHostBytes)))
	offset += 2
	copy(data[offset:], storeHostBytes)
	offset += len(storeHostBytes)

	// Storage fields
	binary.BigEndian.PutUint32(data[offset:], uint32(m.QueueID))
	offset += 4
	binary.BigEndian.PutUint64(data[offset:], uint64(m.QueueOffset))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:], uint64(m.CommitLogOffset))
	offset += 8
	binary.BigEndian.PutUint32(data[offset:], uint32(m.StoreSize))
	offset += 4
	binary.BigEndian.PutUint64(data[offset:], uint64(m.StoreTimestamp))
	offset += 8

	// Consume fields
	binary.BigEndian.PutUint64(data[offset:], uint64(m.ConsumeStartTime))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:], uint64(m.ConsumeEndTime))
	offset += 8
	binary.BigEndian.PutUint32(data[offset:], uint32(m.ConsumeCount))
	offset += 4

	return data, nil
}

// Decode 从字节数组解码消息
func Decode(data []byte) (*Message, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("message data too short")
	}

	// Check magic number
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != 0x4C4D5101 {
		return nil, fmt.Errorf("invalid magic number: %x", magic)
	}

	msg := &Message{}
	offset := 4

	// Message ID
	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	messageIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+messageIDLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.MessageID = string(data[offset : offset+messageIDLen])
	offset += messageIDLen

	// Topic
	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	// Tags
	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	tagsCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	msg.Tags = make([]string, 0, tagsCount)
	for i := 0; i < tagsCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		tagLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+tagLen > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		msg.Tags = append(msg.Tags, string(data[offset:offset+tagLen]))
		offset += tagLen
	}

	// Keys
	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	keysCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	msg.Keys = make([]string, 0, keysCount)
	for i := 0; i < keysCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		msg.Keys = append(msg.Keys, string(data[offset:offset+keyLen]))
		offset += keyLen
	}

	// Properties
	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	propsCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	msg.Properties = make(map[string]string)
	for i := 0; i < propsCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+2 > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		valueLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+valueLen > len(data) {
			return nil, fmt.Errorf("message data corrupted")
		}
		value := string(data[offset : offset+valueLen])
		offset += valueLen

		msg.Properties[key] = value
	}

	// Body
	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	bodyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+bodyLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.Body = make([]byte, bodyLen)
	copy(msg.Body, data[offset:offset+bodyLen])
	msg.BodyLength = bodyLen
	offset += bodyLen

	// Message type, priority, reliability
	if offset+3 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.MessageType = MessageType(data[offset])
	offset++
	msg.Priority = int(data[offset])
	offset++
	msg.Reliability = int(data[offset])
	offset++

	// Delay time
	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.DelayTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Transaction ID
	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	transactionIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+transactionIDLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.TransactionID = string(data[offset : offset+transactionIDLen])
	offset += transactionIDLen

	// Message status
	if offset+1 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.MessageStatus = MessageStatus(data[offset])
	offset++

	// Sharding key
	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	shardingKeyLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+shardingKeyLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.ShardingKey = string(data[offset : offset+shardingKeyLen])
	offset += shardingKeyLen

	// Broadcast
	if offset+1 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.Broadcast = data[offset] == 1
	offset++

	// Timestamps
	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.BornTimestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Hosts
	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	bornHostLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+bornHostLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.BornHost = string(data[offset : offset+bornHostLen])
	offset += bornHostLen

	if offset+2 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	storeHostLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+storeHostLen > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.StoreHost = string(data[offset : offset+storeHostLen])
	offset += storeHostLen

	// Storage fields
	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.QueueID = int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.QueueOffset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.CommitLogOffset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.StoreSize = int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.StoreTimestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Consume fields
	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.ConsumeStartTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	if offset+8 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.ConsumeEndTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	if offset+4 > len(data) {
		return nil, fmt.Errorf("message data corrupted")
	}
	msg.ConsumeCount = int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	return msg, nil
}

// ToJSON 转换为JSON字符串
func (m *Message) ToJSON() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// FromJSON 从JSON字符串解析
func FromJSON(jsonStr string) (*Message, error) {
	var msg Message
	err := json.Unmarshal([]byte(jsonStr), &msg)
	if err != nil {
		return nil, err
	}
	msg.BodyLength = len(msg.Body)
	return &msg, nil
}

// Clone 克隆消息
func (m *Message) Clone() *Message {
	clone := &Message{
		MessageID:        m.MessageID,
		Topic:            m.Topic,
		Tags:             make([]string, len(m.Tags)),
		Keys:             make([]string, len(m.Keys)),
		Properties:       make(map[string]string),
		Body:             make([]byte, len(m.Body)),
		BodyLength:       m.BodyLength,
		MessageType:      m.MessageType,
		Priority:         m.Priority,
		Reliability:      m.Reliability,
		DelayTime:        m.DelayTime,
		TransactionID:    m.TransactionID,
		MessageStatus:    m.MessageStatus,
		ShardingKey:      m.ShardingKey,
		Broadcast:        m.Broadcast,
		BornTimestamp:    m.BornTimestamp,
		BornHost:         m.BornHost,
		StoreHost:        m.StoreHost,
		QueueID:          m.QueueID,
		QueueOffset:      m.QueueOffset,
		CommitLogOffset:  m.CommitLogOffset,
		StoreSize:        m.StoreSize,
		StoreTimestamp:   m.StoreTimestamp,
		ConsumeStartTime: m.ConsumeStartTime,
		ConsumeEndTime:   m.ConsumeEndTime,
		ConsumeCount:     m.ConsumeCount,
	}

	copy(clone.Tags, m.Tags)
	copy(clone.Keys, m.Keys)
	for k, v := range m.Properties {
		clone.Properties[k] = v
	}
	copy(clone.Body, m.Body)

	return clone
}

// generateMessageID 生成消息ID (简化实现)
func generateMessageID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), 0) // 实际应该使用更复杂的ID生成算法
}

// String 返回消息的字符串表示
func (m *Message) String() string {
	return fmt.Sprintf("Message{ID:%s, Topic:%s, Type:%d, Size:%d}",
		m.MessageID, m.Topic, m.MessageType, m.Size())
}
