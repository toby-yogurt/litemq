# LiteMQ 技术栈文档

本文档详细说明 LiteMQ 项目使用的所有技术栈和依赖库。

## 核心语言与运行时

| 技术 | 版本 | 用途 |
|------|------|------|
| **Go** | 1.23+ | 主要开发语言 |
| **Go Toolchain** | 1.24.11 | Go 工具链版本 |

## 网络通信

### gRPC
- **库**: `google.golang.org/grpc` v1.56.0
- **用途**: 
  - Broker 与 Client 之间的高性能 RPC 通信
  - NameServer 与 Broker 之间的服务注册与发现
  - 支持双向流式通信
- **特点**: 
  - 基于 HTTP/2
  - 支持流式传输
  - 强类型接口定义

### Protocol Buffers
- **库**: `google.golang.org/protobuf` v1.30.0
- **用途**: 
  - 消息序列化
  - gRPC 接口定义
  - 跨语言数据交换
- **文件位置**: `api/proto/*.proto`

### HTTP/REST API
- **框架**: `github.com/gin-gonic/gin` v1.9.1
- **用途**: 
  - Web 管理界面后端 API
  - RESTful API 服务
  - 健康检查接口
- **特点**: 
  - 高性能 HTTP 框架
  - 支持中间件
  - JSON 序列化

## 存储引擎

### 内存映射文件 (MappedFile)
- **实现**: 自定义实现 (`pkg/storage/mappedfile.go`)
- **技术**: 
  - `syscall.Mmap` - 系统级内存映射
  - `unsafe.Pointer` - 零拷贝访问
- **用途**: 
  - CommitLog 顺序写入
  - ConsumeQueue 索引存储
  - 高性能磁盘 I/O

### 存储架构
- **CommitLog**: 顺序写入的消息日志
- **ConsumeQueue**: 消息索引队列
- **文件格式**: 自定义二进制格式（详见 [存储格式文档](storage-format.md)）

## 配置管理

### TOML 配置
- **库**: `github.com/pelletier/go-toml/v2` v2.1.0
- **用途**: 
  - Broker 配置 (`configs/broker.toml`)
  - NameServer 配置 (`configs/nameserver.toml`)
  - Client 配置 (`configs/client.toml`)
- **特点**: 
  - 人类可读的配置格式
  - 支持嵌套结构
  - 类型安全

## 日志系统

### 结构化日志
- **标准库**: `log/slog` (Go 1.21+)
- **日志轮转**: `gopkg.in/natefinch/lumberjack.v2` v2.2.1
- **用途**: 
  - 应用日志记录
  - 日志文件自动轮转
  - 日志级别控制
- **特点**: 
  - 支持日志文件大小限制
  - 自动压缩和清理
  - 多级别日志输出

## 监控与指标

### Prometheus
- **库**: `github.com/prometheus/client_golang` v1.16.0
- **用途**: 
  - 性能指标收集
  - 系统监控
  - 指标导出
- **指标类型**: 
  - Counter（计数器）
  - Gauge（仪表盘）
  - Histogram（直方图）
- **端点**: `/metrics`

## 测试框架

### 单元测试
- **库**: `github.com/stretchr/testify` v1.8.4
- **用途**: 
  - 单元测试
  - 断言库
  - Mock 支持
- **特点**: 
  - 丰富的断言方法
  - 测试套件支持
  - Mock 对象生成

## Web 前端

### 模板引擎
- **标准库**: `html/template`
- **用途**: 
  - Web 管理界面模板
  - 动态内容渲染
- **特点**: 
  - 自动转义防止 XSS
  - 模板继承
  - 函数支持

### 静态资源
- **技术**: Go 1.16+ `embed` 包
- **用途**: 
  - 嵌入静态文件（CSS、JS、图片）
  - 单文件部署
- **位置**: `web/static/`

## 序列化

### 自定义二进制格式
- **实现**: `pkg/protocol/message.go`
- **特点**: 
  - 高效的二进制编码
  - 固定长度字段 + 变长字段
  - BigEndian 字节序
- **用途**: 
  - 消息持久化
  - 网络传输（可选）

### JSON（用于 REST API）
- **库**: `github.com/gin-gonic/gin` 内置
- **用途**: 
  - REST API 响应
  - 配置解析
  - Web 界面数据交换

## 并发与同步

### 标准库
- **sync.Mutex**: 互斥锁
- **sync.RWMutex**: 读写锁
- **sync.WaitGroup**: 协程同步
- **sync.Map**: 并发安全的 Map（如需要）

### 协程
- **goroutine**: Go 原生协程
- **channel**: 协程间通信

## 系统调用

### 内存映射
- **syscall.Mmap**: 内存映射文件
- **syscall.Munmap**: 取消内存映射
- **unsafe.Pointer**: 零拷贝数据访问

## 工具与依赖

### 间接依赖
- **github.com/bytedance/sonic**: 高性能 JSON 编解码
- **github.com/go-playground/validator/v10**: 数据验证
- **golang.org/x/net**: 网络相关工具
- **golang.org/x/sys**: 系统调用封装
- **golang.org/x/text**: 文本处理
- **golang.org/x/crypto**: 加密相关

## 项目结构对应的技术

| 模块 | 主要技术 |
|------|---------|
| **Broker** | gRPC、内存映射文件、Prometheus |
| **NameServer** | gRPC、内存存储 |
| **Client SDK** | gRPC、Protocol Buffers |
| **Web 管理界面** | Gin、HTML Template、Embed |
| **存储引擎** | MappedFile、顺序写入 |
| **监控系统** | Prometheus、HTTP Metrics |
| **配置管理** | TOML、文件解析 |

## 技术选型理由

### 为什么选择 Go？
- ✅ 高性能：编译型语言，接近 C 的性能
- ✅ 并发友好：原生支持 goroutine，适合高并发场景
- ✅ 部署简单：单文件二进制，无需运行时环境
- ✅ 内存安全：自动垃圾回收，避免内存泄漏

### 为什么选择 gRPC？
- ✅ 高性能：基于 HTTP/2，支持多路复用
- ✅ 强类型：Protocol Buffers 提供类型安全
- ✅ 跨语言：支持多种编程语言
- ✅ 流式传输：支持双向流，适合实时通信

### 为什么选择内存映射文件？
- ✅ 零拷贝：直接访问文件数据，无需系统调用
- ✅ 高性能：操作系统级别的缓存管理
- ✅ 大文件支持：可以映射超大文件
- ✅ 顺序写入：充分利用磁盘顺序写入性能

### 为什么选择 Gin？
- ✅ 高性能：基于 httprouter，路由性能优秀
- ✅ 简单易用：API 设计简洁
- ✅ 中间件支持：丰富的中间件生态
- ✅ 活跃社区：维护活跃，文档完善

### 为什么选择 Prometheus？
- ✅ 标准协议：Prometheus 是监控领域的标准
- ✅ 丰富生态：与 Grafana 等工具完美集成
- ✅ 多维指标：支持标签和多维度查询
- ✅ 长期存储：支持时序数据存储

## 版本兼容性

- **Go 版本**: 1.23+（推荐 1.24+）
- **gRPC**: 兼容 gRPC-Go 1.56.0+
- **Protocol Buffers**: 兼容 protobuf 3.x

## 依赖管理

项目使用 Go Modules 进行依赖管理：
- **go.mod**: 依赖声明文件
- **go.sum**: 依赖校验和文件

## 构建工具

- **Makefile**: 构建脚本和任务自动化
- **Docker**: 容器化部署
- **Kubernetes**: 容器编排（可选）

## 开发工具推荐

- **IDE**: VS Code、GoLand
- **调试**: Delve (dlv)
- **性能分析**: pprof
- **代码检查**: golangci-lint
- **格式化**: gofmt、goimports

---

## 参考链接

- [Go 官方文档](https://golang.org/doc/)
- [gRPC 官方文档](https://grpc.io/docs/)
- [Protocol Buffers 文档](https://developers.google.com/protocol-buffers)
- [Gin 框架文档](https://gin-gonic.com/docs/)
- [Prometheus 文档](https://prometheus.io/docs/)

