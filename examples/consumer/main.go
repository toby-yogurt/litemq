package main

import (
	"fmt"
	"log"
	"time"

	"litemq/pkg/client"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

func main() {
	logger.Info("=== LiteMQ Consumer Example ===")
	fmt.Println("=== LiteMQ Consumer Example ===")

	// 创建客户端配置
	cfg := config.DefaultClientConfig()

	// 创建消费者
	consumer := client.NewConsumer(cfg, "test_group")

	// 定义消息处理函数
	messageHandler := func(msg *protocol.Message) protocol.ConsumeStatus {
		fmt.Println("============ start consumer ===========")
		logger.Info("Received message",
			"message_id", msg.MessageID,
			"topic", msg.Topic,
			"body", string(msg.Body))
		// TODO delete 这里只是为了在调试的时候进行数据打印
		fmt.Println("Received message",
			"message_id", msg.MessageID,
			"topic", msg.Topic,
			"body", string(msg.Body))

		// 检查是否为延时消息
		if msg.MessageType == protocol.MessageTypeDelay {
			logger.Info("Delay message detail", "born_at", time.UnixMilli(msg.BornTimestamp))
		}

		// 检查是否有标签
		if len(msg.Tags) > 0 {
			logger.Info("Message tags", "tags", msg.Tags)
		}

		// 检查是否为广播消息
		if msg.Broadcast {
			logger.Info("This is a broadcast message")
		}

		// 检查是否为顺序消息
		if msg.ShardingKey != "" {
			logger.Info("This is an order message", "sharding_key", msg.ShardingKey)
		}

		// 模拟处理时间
		time.Sleep(50 * time.Millisecond)

		logger.Info("Message processed successfully")
		fmt.Println("Message processed successfully")

		return protocol.ConsumeStatusSuccess
	}

	// 订阅主题
	logger.Info("Subscribing to topics...")
	if err := consumer.Subscribe("test_topic", messageHandler); err != nil {
		log.Fatalf("Failed to subscribe to test_topic: %v", err)
	}

	// 订阅广播主题
	if err := consumer.SubscribeBroadcast("broadcast_topic", messageHandler); err != nil {
		log.Fatalf("Failed to subscribe to broadcast_topic: %v", err)
	}

	// 订阅顺序消息主题
	if err := consumer.Subscribe("order_topic", messageHandler); err != nil {
		log.Fatalf("Failed to subscribe to order_topic: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}
	defer consumer.Shutdown()

	logger.Info("Consumer started successfully")
	logger.Info("Waiting for messages... (Press Ctrl+C to stop)")

	// 运行30秒
	time.Sleep(30 * time.Second)

	// 获取统计信息
	stats := consumer.GetStats()
	logger.Info("Consumer statistics",
		"group_name", stats["group_name"],
		"topics", stats["topics"],
		"running", stats["running"],
		"offsets", stats["offsets"])

	logger.Info("=== Consumer Example Completed ===")
}
