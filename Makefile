# LiteMQ Makefile

.PHONY: all build clean test deps fmt vet check

# 默认目标
all: deps check build

# 依赖安装
deps:
	go mod tidy
	go mod download

# 代码格式化
fmt:
	go fmt ./...

# 代码检查
vet:
	go vet ./...

# 代码质量检查
check: fmt vet

# 构建所有组件
build: build-nameserver build-broker build-web build-examples

# 构建NameServer
build-nameserver:
	@echo "Building NameServer..."
	@cd cmd/nameserver && go build -o ../../bin/nameserver .

# 构建Broker
build-broker:
	@echo "Building Broker..."
	@cd cmd/broker && go build -o ../../bin/broker .

# 构建Web服务器
build-web:
	@echo "Building Web Server..."
	@cd cmd/web && go build -o ../../bin/web .

# 构建示例程序
build-examples:
	@echo "Building examples..."
	@cd examples/producer && go build -o ../../bin/producer .
	@cd examples/consumer && go build -o ../../bin/consumer .

# 运行测试
test:
	go test ./...

# 运行集成测试
test-integration:
	go test -tags=integration ./...

# 运行性能测试
test-benchmark:
	go test -bench=. ./...

# 生成覆盖率报告
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# 清理构建产物
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

# 运行NameServer
run-nameserver:
	./bin/nameserver $(if $(PORT),-port $(PORT))

# 运行Broker
run-broker:
	./bin/broker $(if $(PORT),-port $(PORT))

# 运行Web服务器
run-web:
	./bin/web

# 运行生产者示例
run-producer:
	./bin/producer

# 运行消费者示例
run-consumer:
	./bin/consumer

# Docker部署
docker-deploy:
	./scripts/deploy.sh

# Docker启动
docker-start:
	./scripts/start-cluster.sh

# Docker停止
docker-stop:
	./scripts/stop-cluster.sh

# Docker日志
docker-logs:
	./scripts/logs.sh $(service)

# Docker健康检查
docker-health:
	./scripts/health-check.sh

# Kubernetes部署
k8s-deploy:
	kubectl apply -f deployments/k8s/namespace.yaml
	kubectl apply -f deployments/k8s/nameserver.yaml
	kubectl apply -f deployments/k8s/broker.yaml

# Kubernetes删除
k8s-delete:
	kubectl delete -f deployments/k8s/broker.yaml
	kubectl delete -f deployments/k8s/nameserver.yaml
	kubectl delete -f deployments/k8s/namespace.yaml

# 生成Protocol Buffers代码
proto:
	@echo "Generating Protocol Buffers code..."
	@cd api/proto && protoc --proto_path=. \
		--go_out=.. --go_opt=paths=source_relative \
		--go-grpc_out=.. --go-grpc_opt=paths=source_relative \
		message.proto broker.proto nameserver.proto

# 初始化开发环境
init: deps proto
	mkdir -p bin
	mkdir -p data/nameserver
	mkdir -p data/broker

# 帮助信息
help:
	@echo "LiteMQ Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  all              - Run deps, check and build"
	@echo "  deps             - Install dependencies"
	@echo "  check            - Run code quality checks"
	@echo "  build            - Build all components"
	@echo "  build-nameserver - Build NameServer only"
	@echo "  build-broker     - Build Broker only"
	@echo "  build-web        - Build Web Server only"
	@echo "  build-examples   - Build example programs"
	@echo "  test             - Run unit tests"
	@echo "  test-integration - Run integration tests"
	@echo "  test-benchmark   - Run benchmark tests"
	@echo "  test-coverage    - Generate coverage report"
	@echo "  clean            - Clean build artifacts"
	@echo "  run-nameserver   - Run NameServer"
	@echo "  run-broker       - Run Broker"
	@echo "  run-web          - Run Web Server"
	@echo "  run-producer     - Run producer example"
	@echo "  run-consumer     - Run consumer example"
	@echo "  docker-deploy    - Deploy with Docker Compose"
	@echo "  docker-start     - Start Docker cluster"
	@echo "  docker-stop      - Stop Docker cluster"
	@echo "  docker-logs      - View Docker logs (use service=xxx)"
	@echo "  docker-health    - Health check Docker cluster"
	@echo "  proto            - Generate Protocol Buffers code"
	@echo "  k8s-deploy       - Deploy to Kubernetes"
	@echo "  k8s-delete       - Delete from Kubernetes"
	@echo "  init             - Initialize development environment"
	@echo "  help             - Show this help message"
