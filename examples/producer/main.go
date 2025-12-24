package main

import (
	"fmt"
	"log"
	"time"

	"litemq/pkg/client"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
)

func main() {
	logger.Info("=== LiteMQ Producer Example ===")

	// 创建客户端配置
	cfg := config.DefaultClientConfig()

	// 创建生产者
	producer := client.NewProducer(cfg)

	// 启动生产者
	if err := producer.Start(); err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Shutdown()

	logger.Info("Producer started successfully")

	// 发送普通消息
	logger.Info("Sending Normal Messages")
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Hello LiteMQ! Message #%d", i)
		result, err := producer.SendMessage("test_topic", []byte(message))
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue
		}

		logger.Info("Message sent successfully",
			"index", i,
			"message_id", result.MessageID,
			"offset", result.Offset)
		fmt.Println("Message sent successfully"+"index", i,
			"message_id", result.MessageID,
			"offset", result.Offset)
		time.Sleep(100 * time.Millisecond) // 短暂延迟
	}

	// 发送带标签的消息
	logger.Info("Sending Tagged Messages")
	tags := []string{"important", "notification"}
	result, err := producer.SendMessageWithTags("test_topic", []byte("Important notification"), tags)
	if err != nil {
		log.Printf("Failed to send tagged message: %v", err)
	} else {
		logger.Info("Tagged message sent",
			"message_id", result.MessageID,
			"offset", result.Offset)
	}

	// 发送延时消息
	logger.Info("Sending Delay Message")
	delayTime := time.Now().Add(10 * time.Second).UnixMilli()
	result, err = producer.SendDelayMessage("test_topic", []byte("This is a delay message"), delayTime)
	if err != nil {
		log.Printf("Failed to send delay message: %v", err)
	} else {
		logger.Info("Delay message sent",
			"message_id", result.MessageID,
			"offset", result.Offset,
			"deliver_at", time.UnixMilli(delayTime))
	}

	// 发送广播消息
	logger.Info("Sending Broadcast Message")
	result, err = producer.SendBroadcastMessage("broadcast_topic", []byte("Broadcast message to all consumers"))
	if err != nil {
		log.Printf("Failed to send broadcast message: %v", err)
	} else {
		logger.Info("Broadcast message sent",
			"message_id", result.MessageID,
			"offset", result.Offset)
	}

	// 发送顺序消息
	logger.Info("Sending Order Message")
	result, err = producer.SendOrderMessage("order_topic", []byte("Order message with sharding"), "order_123")
	if err != nil {
		log.Printf("Failed to send order message: %v", err)
	} else {
		logger.Info("Order message sent",
			"message_id", result.MessageID,
			"offset", result.Offset)
	}

	logger.Info("=== All messages sent successfully! ===")

	// 等待一段时间让消息处理完成
	time.Sleep(2 * time.Second)
}
