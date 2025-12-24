# LiteMQ Web 管理界面使用指南

本文档说明如何启动和使用 LiteMQ 的 Web 管理界面。

## 启动方式

LiteMQ 的 Web 管理界面有两种启动方式：

### 方式 1：随 Broker 自动启动（推荐）

当启动 Broker 时，Web 管理界面会自动启动。

#### 步骤

1. **启动 NameServer**（如果还没有启动）
```bash
# 编译
go build -o bin/nameserver cmd/nameserver/main.go

# 启动
./bin/nameserver
```

2. **启动 Broker**
```bash
# 编译
go build -o bin/broker cmd/broker/main.go

# 启动（会自动启动 Web 服务器）
./bin/broker
```

3. **访问 Web 界面**

根据配置文件 `configs/broker.toml` 中的 `[metrics]` 配置：
- **默认地址**: `http://localhost:18080`
- **配置项**: `port = 18080`（可在配置文件中修改）

### 方式 2：独立启动 Web 服务器

如果需要单独运行 Web 服务器（例如，将 Web 服务器部署在不同的机器上）：

#### 步骤

1. **编译 Web 服务器**
```bash
go build -o bin/web cmd/web/main.go
```

2. **启动 Web 服务器**
```bash
# 使用默认配置（Broker: localhost:10911, NameServer: localhost:9876, Web: 0.0.0.0:8088）
./bin/web

# 或指定参数
./bin/web -broker localhost:10911 -nameserver localhost:9876 -host 0.0.0.0 -port 8088
```

3. **访问 Web 界面**
- **默认地址**: `http://localhost:8088`

#### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-broker` | Broker 地址 | `localhost:10911` |
| `-nameserver` | NameServer 地址 | `localhost:9876` |
| `-host` | Web 服务器监听地址 | `0.0.0.0` |
| `-port` | Web 服务器端口 | `8088` |

## 使用 Makefile 启动

### 快速启动

```bash
# 1. 初始化环境
make init

# 2. 构建所有组件
make build

# 3. 启动 NameServer（终端1）
make run-nameserver

# 4. 启动 Broker（终端2，会自动启动 Web 服务器）
make run-broker

# 或者单独启动 Web 服务器（终端3）
make run-web
```

## Web 界面功能

### 主要页面

访问 `http://localhost:18080`（或你配置的端口）后，可以看到以下页面：

#### 1. 仪表盘 (Dashboard)
- **路径**: `/`
- **功能**: 
  - 系统概览
  - 统计信息（Broker 数量、主题数量、消息数量、消费者数量）
  - 快速导航

#### 2. Broker 管理
- **路径**: `/brokers`
- **功能**:
  - 查看所有 Broker 列表
  - Broker 状态（在线/离线）
  - Broker 详细信息
  - 主题和消息统计

#### 3. 主题管理
- **路径**: `/topics`
- **功能**:
  - 查看所有主题列表
  - 主题分区信息
  - 消息统计
  - 创建/删除主题（通过 API）

#### 4. 消费者管理
- **路径**: `/consumers`
- **功能**:
  - 查看所有消费者组
  - 消费者状态
  - 消费进度
  - 消费偏移量

#### 5. 监控页面
- **路径**: `/monitoring`
- **功能**:
  - 性能指标图表
  - 系统资源使用情况
  - 消息吞吐量
  - 延迟统计

### API 端点

Web 服务器还提供了 REST API：

#### 健康检查
```bash
curl http://localhost:18080/api/v1/health
```

#### 获取 Broker 信息
```bash
curl http://localhost:18080/api/v1/broker/info
```

#### 获取统计信息
```bash
curl http://localhost:18080/api/v1/broker/stats
```

#### 获取主题列表
```bash
curl http://localhost:18080/api/v1/broker/topics
```

#### Prometheus 指标
```bash
curl http://localhost:18080/metrics
```

## 配置说明

### Broker 配置中的 Web 服务器设置

在 `configs/broker.toml` 中：

```toml
# 监控配置
[metrics]
enabled = true          # 是否启用监控
port = 18080           # Web 服务器端口
path = "/metrics"      # Prometheus 指标路径
```

### 修改端口

如果需要修改 Web 服务器端口：

1. **方式 1**：修改配置文件
```toml
[metrics]
port = 8080  # 改为你想要的端口
```

2. **方式 2**：独立启动时使用命令行参数
```bash
./bin/web -port 8080
```

## 静态资源

Web 界面的静态资源（CSS、JavaScript、图片）已经通过 Go 1.16+ 的 `embed` 功能嵌入到二进制文件中，无需单独部署。

静态资源位置：
- CSS: `web/static/css/custom.css`
- JavaScript: `web/static/js/app.js`
- 模板: `web/templates/*.html`

## 故障排查

### Web 服务器无法启动

1. **检查端口是否被占用**
```bash
# Linux/macOS
lsof -i :18080
# 或
netstat -an | grep 18080

# 如果端口被占用，修改配置文件中的端口
```

2. **检查 Broker 是否运行**
```bash
# Web 服务器需要连接到 Broker，确保 Broker 正在运行
# 检查 Broker 是否在监听
lsof -i :10911
```

3. **查看日志**
```bash
# 查看 Broker 日志（如果随 Broker 启动）
tail -f logs/broker.log

# 查看 Web 服务器日志（如果独立启动）
# 日志会输出到标准输出
```

### 无法访问 Web 界面

1. **检查防火墙设置**
   - 确保端口 18080（或你配置的端口）没有被防火墙阻止

2. **检查监听地址**
   - 如果配置为 `0.0.0.0`，可以从任何 IP 访问
   - 如果配置为 `127.0.0.1`，只能从本机访问

3. **检查浏览器控制台**
   - 打开浏览器开发者工具（F12）
   - 查看 Console 和 Network 标签页
   - 检查是否有 JavaScript 错误或 API 请求失败

### API 请求失败

1. **检查 Broker 连接**
   - Web 服务器需要连接到 Broker 的 gRPC 服务
   - 确保 Broker 地址配置正确

2. **检查 NameServer 连接**
   - 某些 API 需要从 NameServer 获取路由信息
   - 确保 NameServer 地址配置正确

## 开发模式

### 修改前端代码

1. **修改模板文件**
   - 位置: `web/templates/*.html`
   - 修改后需要重新编译

2. **修改静态资源**
   - CSS: `web/static/css/custom.css`
   - JavaScript: `web/static/js/app.js`
   - 修改后需要重新编译

3. **重新编译**
```bash
# 重新编译 Broker（如果使用方式1）
go build -o bin/broker cmd/broker/main.go

# 或重新编译 Web 服务器（如果使用方式2）
go build -o bin/web cmd/web/main.go
```

## 示例：完整启动流程

```bash
# 1. 编译所有组件
make build

# 2. 启动 NameServer（终端1）
./bin/nameserver

# 3. 启动 Broker（终端2，会自动启动 Web 服务器在 18080 端口）
./bin/broker

# 4. 在浏览器中访问
# http://localhost:18080
```

## 相关文档

- [技术栈文档](tech-stack.md) - 了解使用的技术
- [配置说明](../README.md#配置说明) - 详细配置选项

