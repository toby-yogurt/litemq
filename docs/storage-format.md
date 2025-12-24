# LiteMQ 存储格式文档

本文档详细说明 LiteMQ 的存储文件格式，包括 CommitLog 和 ConsumeQueue 的二进制格式。

## 目录

- [CommitLog 文件格式](#commitlog-文件格式)
- [消息编码格式](#消息编码格式)
- [ConsumeQueue 文件格式](#consumequeue-文件格式)
- [工具使用](#工具使用)

---

## CommitLog 文件格式

CommitLog 是 LiteMQ 的核心存储文件，所有消息都顺序写入到 CommitLog 文件中。

### 文件结构

```
[4字节消息长度][消息数据][4字节消息长度][消息数据]...
```

- **消息长度**：4 字节，BigEndian 格式的 uint32，表示后续消息数据的字节数
- **消息数据**：可变长度，通过 `Message.Encode()` 编码后的二进制数据

### 文件命名规则

CommitLog 文件按照写入顺序命名，格式为：`{20位数字}.log`

例如：
- `00000000000000000000` - 第一个文件
- `00000000000000000001` - 第二个文件
- `00000000000000000002` - 第三个文件

### 读取示例

```go
// 读取 CommitLog 文件中的消息
file, _ := os.Open("data/broker/commitlog/00000000000000000000")
defer file.Close()

offset := int64(0)
for offset < fileSize {
    // 读取消息长度
    sizeBytes := make([]byte, 4)
    file.ReadAt(sizeBytes, offset)
    messageSize := binary.BigEndian.Uint32(sizeBytes)
    
    // 读取消息数据
    messageData := make([]byte, messageSize)
    file.ReadAt(messageData, offset+4)
    
    // 解码消息
    msg, _ := protocol.Decode(messageData)
    
    offset += 4 + int64(messageSize)
}
```

---

## 消息编码格式

每条消息在 CommitLog 中的存储格式如下（所有多字节整数使用 BigEndian 编码）：

### 消息数据格式

| 偏移量 | 长度 | 字段 | 说明 |
|--------|------|------|------|
| 0 | 4 字节 | Magic Number | 固定值 `0x4C4D5101` ("LMQ" + version) |
| 4 | 2 字节 | MessageID 长度 | MessageID 字符串的字节数 |
| 6 | N 字节 | MessageID | 消息唯一标识符 |
| 6+N | 2 字节 | Topic 长度 | Topic 字符串的字节数 |
| 8+N | M 字节 | Topic | 主题名称 |
| 8+N+M | 4 字节 | Tags 数量 | Tag 数组的长度 |
| 12+N+M | ... | Tags 数据 | 每个 Tag: 2字节长度 + 字符串数据 |
| ... | 4 字节 | Keys 数量 | Key 数组的长度 |
| ... | ... | Keys 数据 | 每个 Key: 2字节长度 + 字符串数据 |
| ... | 4 字节 | Properties 数量 | Properties map 的键值对数量 |
| ... | ... | Properties 数据 | 每个属性: 2字节key长度 + key + 2字节value长度 + value |
| ... | 4 字节 | Body 长度 | 消息体的字节数 |
| ... | B 字节 | Body | 消息内容 |
| ... | 1 字节 | MessageType | 消息类型 (0=Normal, 1=Delay, 2=Transaction, 3=Broadcast, 4=Order) |
| ... | 1 字节 | Priority | 消息优先级 (0-255) |
| ... | 1 字节 | Reliability | 可靠性等级 (0-3) |
| ... | 8 字节 | DelayTime | 延时时间戳（毫秒） |
| ... | 2 字节 | TransactionID 长度 | TransactionID 字符串的字节数 |
| ... | T 字节 | TransactionID | 事务ID |
| ... | 1 字节 | MessageStatus | 消息状态 |
| ... | 2 字节 | ShardingKey 长度 | ShardingKey 字符串的字节数 |
| ... | S 字节 | ShardingKey | 分片键（用于顺序消息） |
| ... | 1 字节 | Broadcast | 是否广播消息 (0=false, 1=true) |
| ... | 8 字节 | BornTimestamp | 消息创建时间戳（毫秒） |
| ... | 2 字节 | BornHost 长度 | BornHost 字符串的字节数 |
| ... | H1 字节 | BornHost | 消息创建主机 |
| ... | 2 字节 | StoreHost 长度 | StoreHost 字符串的字节数 |
| ... | H2 字节 | StoreHost | 消息存储主机 |
| ... | 4 字节 | QueueID | 队列ID |
| ... | 8 字节 | QueueOffset | 队列偏移量 |
| ... | 8 字节 | CommitLogOffset | CommitLog 偏移量 |
| ... | 4 字节 | StoreSize | 存储大小（包含长度前缀） |
| ... | 8 字节 | StoreTimestamp | 存储时间戳（毫秒） |
| ... | 8 字节 | ConsumeStartTime | 开始消费时间戳（毫秒） |
| ... | 8 字节 | ConsumeEndTime | 结束消费时间戳（毫秒） |
| ... | 4 字节 | ConsumeCount | 消费次数 |

### 详细字段说明

#### 1. Magic Number (4 字节)
- **值**: `0x4C4D5101`
- **说明**: 固定魔数，用于验证消息格式，"LMQ" (0x4C4D51) + 版本号 (0x01)

#### 2. MessageID (2+N 字节)
- **长度字段**: 2 字节，uint16，表示 MessageID 字符串的字节数
- **数据**: N 字节，UTF-8 编码的字符串

#### 3. Topic (2+M 字节)
- **长度字段**: 2 字节，uint16，表示 Topic 字符串的字节数
- **数据**: M 字节，UTF-8 编码的字符串

#### 4. Tags (4+... 字节)
- **数量字段**: 4 字节，uint32，表示 Tag 数组的长度
- **每个 Tag**: 
  - 2 字节：Tag 字符串长度（uint16）
  - N 字节：Tag 字符串数据

#### 5. Keys (4+... 字节)
- **数量字段**: 4 字节，uint32，表示 Key 数组的长度
- **每个 Key**:
  - 2 字节：Key 字符串长度（uint16）
  - N 字节：Key 字符串数据

#### 6. Properties (4+... 字节)
- **数量字段**: 4 字节，uint32，表示 Properties map 的键值对数量
- **每个属性**:
  - 2 字节：Key 字符串长度（uint16）
  - N 字节：Key 字符串数据
  - 2 字节：Value 字符串长度（uint16）
  - M 字节：Value 字符串数据

#### 7. Body (4+B 字节)
- **长度字段**: 4 字节，uint32，表示消息体的字节数
- **数据**: B 字节，原始字节数据

#### 8. 消息控制字段
- **MessageType** (1 字节): 消息类型
  - `0` = Normal (普通消息)
  - `1` = Delay (延时消息)
  - `2` = Transaction (事务消息)
  - `3` = Broadcast (广播消息)
  - `4` = Order (顺序消息)
- **Priority** (1 字节): 消息优先级，范围 0-255
- **Reliability** (1 字节): 可靠性等级，范围 0-3

#### 9. 延时和事务字段
- **DelayTime** (8 字节): uint64，延时时间戳（毫秒）
- **TransactionID** (2+T 字节): 
  - 2 字节：长度（uint16）
  - T 字节：事务ID 字符串
- **MessageStatus** (1 字节): 消息状态
  - `0` = Normal
  - `1` = Prepared
  - `2` = Committed
  - `3` = Rollback

#### 10. 顺序消息字段
- **ShardingKey** (2+S 字节):
  - 2 字节：长度（uint16）
  - S 字节：分片键字符串

#### 11. 广播标志 (1 字节)
- `0` = false
- `1` = true

#### 12. 时间戳字段
- **BornTimestamp** (8 字节): uint64，消息创建时间戳（毫秒）
- **StoreTimestamp** (8 字节): uint64，消息存储时间戳（毫秒）
- **ConsumeStartTime** (8 字节): uint64，开始消费时间戳（毫秒）
- **ConsumeEndTime** (8 字节): uint64，结束消费时间戳（毫秒）

#### 13. 主机信息
- **BornHost** (2+H1 字节):
  - 2 字节：长度（uint16）
  - H1 字节：创建主机字符串
- **StoreHost** (2+H2 字节):
  - 2 字节：长度（uint16）
  - H2 字节：存储主机字符串

#### 14. 存储相关字段
- **QueueID** (4 字节): uint32，队列ID
- **QueueOffset** (8 字节): int64，队列偏移量
- **CommitLogOffset** (8 字节): int64，CommitLog 偏移量
- **StoreSize** (4 字节): uint32，存储大小（包含4字节长度前缀）
- **ConsumeCount** (4 字节): uint32，消费次数

---

## ConsumeQueue 文件格式

ConsumeQueue 是 CommitLog 的索引文件，用于快速定位消息在 CommitLog 中的位置。

### 文件结构

```
[索引项][索引项][索引项]...
```

每个索引项固定 24 字节：

| 偏移量 | 长度 | 字段 | 说明 |
|--------|------|------|------|
| 0 | 8 字节 | Offset | CommitLog 中的偏移量（int64） |
| 8 | 4 字节 | MessageSize | 消息大小（int32） |
| 12 | 4 字节 | TagHash | Tag 哈希值（uint32） |
| 16 | 8 字节 | Timestamp | 存储时间戳（int64，毫秒） |

### 文件命名规则

ConsumeQueue 文件按照主题和队列ID组织：

```
data/broker/consumequeue/{topic}/{queueID}/{20位数字}
```

例如：
- `data/broker/consumequeue/test_topic/0/00000000000000000000`
- `data/broker/consumequeue/test_topic/0/00000000000000000001`

### 索引项计算

- **索引位置** = QueueOffset / 24
- **文件内偏移量** = (QueueOffset % 文件大小) / 24 * 24

---

## 工具使用

### 读取 CommitLog 文件

LiteMQ 提供了一个命令行工具来读取和查看 CommitLog 文件：

```bash
# 编译工具
go build -o bin/read_commitlog cmd/read_commitlog/main.go

# 查看 CommitLog 文件
./bin/read_commitlog data/broker/commitlog/00000000000000000000
```

工具会显示：
- 文件基本信息（路径、大小）
- 每条消息的详细信息
- 统计信息（总消息数、总大小）

### 示例输出

```
=== CommitLog File Reader ===
File: data/broker/commitlog/00000000000000000000
Size: 1024 bytes

=== Message #1 (offset: 0, size: 256 bytes) ===
MessageID:     1766547877259022000-0
Topic:         test_topic
QueueID:       0
QueueOffset:   0
CommitLogOffset: 0
StoreSize:     256
Body:          Hello LiteMQ! Message #1 (25 bytes)

=== Summary ===
Total messages: 1
Total size: 256 bytes
```

---

## 注意事项

1. **字节序**: 所有多字节整数使用 **BigEndian**（大端序）编码
2. **字符串编码**: 所有字符串使用 **UTF-8** 编码
3. **文件大小**: CommitLog 文件默认大小为 1GB，可通过配置修改
4. **顺序写入**: CommitLog 采用顺序写入，保证高性能
5. **索引更新**: ConsumeQueue 索引在消息写入 CommitLog 后异步更新

---

## 参考

- 消息编码实现: `pkg/protocol/message.go` - `Encode()` 方法
- 消息解码实现: `pkg/protocol/message.go` - `Decode()` 函数
- CommitLog 实现: `pkg/storage/commitlog.go`
- ConsumeQueue 实现: `pkg/storage/consumequeue.go`

