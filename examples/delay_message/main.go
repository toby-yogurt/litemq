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
	fmt.Println("=== LiteMQ 延时消息示例 ===\n")

	// 创建客户端配置
	cfg := config.DefaultClientConfig()

	// ========== 生产者部分 ==========
	fmt.Println("【1. 创建并启动生产者】")
	producer := client.NewProducer(cfg)
	if err := producer.Start(); err != nil {
		log.Fatalf("启动生产者失败: %v", err)
	}
	defer producer.Shutdown()

	fmt.Println("✅ 生产者已启动\n")

	// ========== 发送延时消息 ==========
	fmt.Println("【2. 发送延时消息】")
	now := time.Now()
	topic := "delay_topic"

	// 示例1: 5秒后执行
	delayTime1 := now.Add(5 * time.Second).UnixMilli()
	result1, err := producer.SendDelayMessage(topic, []byte("这是5秒后执行的消息"), delayTime1)
	if err != nil {
		log.Printf("❌ 发送延时消息失败: %v", err)
	} else {
		fmt.Printf("✅ 延时消息1已发送:\n")
		fmt.Printf("   - 消息ID: %s\n", result1.MessageID)
		fmt.Printf("   - 发送时间: %s\n", now.Format("15:04:05"))
		fmt.Printf("   - 计划执行时间: %s\n", time.UnixMilli(delayTime1).Format("15:04:05"))
		fmt.Printf("   - 延时: 5秒\n\n")
	}

	// 示例2: 10秒后执行
	delayTime2 := now.Add(10 * time.Second).UnixMilli()
	result2, err := producer.SendDelayMessage(topic, []byte("这是10秒后执行的消息"), delayTime2)
	if err != nil {
		log.Printf("❌ 发送延时消息失败: %v", err)
	} else {
		fmt.Printf("✅ 延时消息2已发送:\n")
		fmt.Printf("   - 消息ID: %s\n", result2.MessageID)
		fmt.Printf("   - 发送时间: %s\n", now.Format("15:04:05"))
		fmt.Printf("   - 计划执行时间: %s\n", time.UnixMilli(delayTime2).Format("15:04:05"))
		fmt.Printf("   - 延时: 10秒\n\n")
	}

	// 示例3: 30秒后执行（跨分钟边界）
	delayTime3 := now.Add(30 * time.Second).UnixMilli()
	result3, err := producer.SendDelayMessage(topic, []byte("这是30秒后执行的消息"), delayTime3)
	if err != nil {
		log.Printf("❌ 发送延时消息失败: %v", err)
	} else {
		fmt.Printf("✅ 延时消息3已发送:\n")
		fmt.Printf("   - 消息ID: %s\n", result3.MessageID)
		fmt.Printf("   - 发送时间: %s\n", now.Format("15:04:05"))
		fmt.Printf("   - 计划执行时间: %s\n", time.UnixMilli(delayTime3).Format("15:04:05"))
		fmt.Printf("   - 延时: 30秒\n\n")
	}

	// 示例4: 2分钟后执行（使用分钟级时间轮）
	delayTime4 := now.Add(2 * time.Minute).UnixMilli()
	result4, err := producer.SendDelayMessage(topic, []byte("这是2分钟后执行的消息"), delayTime4)
	if err != nil {
		log.Printf("❌ 发送延时消息失败: %v", err)
	} else {
		fmt.Printf("✅ 延时消息4已发送:\n")
		fmt.Printf("   - 消息ID: %s\n", result4.MessageID)
		fmt.Printf("   - 发送时间: %s\n", now.Format("15:04:05"))
		fmt.Printf("   - 计划执行时间: %s\n", time.UnixMilli(delayTime4).Format("15:04:05"))
		fmt.Printf("   - 延时: 2分钟\n\n")
	}

	// ========== 消费者部分 ==========
	fmt.Println("【3. 创建并启动消费者】")
	consumer := client.NewConsumer(cfg, "delay_consumer_group")

	// 定义消息处理函数
	messageHandler := func(msg *protocol.Message) protocol.ConsumeStatus {
		now := time.Now()
		bornTime := time.UnixMilli(msg.BornTimestamp)
		actualDelay := now.Sub(bornTime)

		fmt.Printf("📨 【收到延时消息】\n")
		fmt.Printf("   - 消息ID: %s\n", msg.MessageID)
		fmt.Printf("   - 消息内容: %s\n", string(msg.Body))
		fmt.Printf("   - 发送时间: %s\n", bornTime.Format("15:04:05"))
		fmt.Printf("   - 消费时间: %s\n", now.Format("15:04:05"))
		fmt.Printf("   - 实际延时: %v\n", actualDelay.Round(time.Second))
		fmt.Printf("   - 消息类型: 延时消息\n\n")

		return protocol.ConsumeStatusSuccess
	}

	// 订阅主题
	if err := consumer.Subscribe(topic, messageHandler); err != nil {
		log.Fatalf("订阅主题失败: %v", err)
	}

	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Shutdown()

	fmt.Println("✅ 消费者已启动，等待接收延时消息...\n")

	// ========== 工作原理说明 ==========
	fmt.Println("【延时消息工作原理】")
	fmt.Println(`
延时消息使用多层时间轮实现：

1. 消息存储：
   - 延时消息先存储到 CommitLog
   - 但不立即构建 ConsumeQueue 索引（消费者不可见）

2. 时间轮调度：
   - 根据延时时间选择合适的层级：
     * 0-60秒   → 第0层（秒级时间轮）
     * 1-60分钟 → 第1层（分钟级时间轮）
     * 1-24小时 → 第2层（小时级时间轮）
     * 1-365天  → 第3层（天级时间轮）

3. 时间推进：
   - 只启动秒级时间轮，每秒推进一个槽位
   - 当秒级时间轮转完一圈（60秒），推进分钟级时间轮
   - 消息从粗粒度层级自动降级到细粒度层级

4. 消息投递：
   - 当消息到期时，触发回调函数
   - 构建 ConsumeQueue 索引，使消息对消费者可见
   - 消费者可以拉取并消费消息

优势：
- O(1) 时间复杂度添加消息
- O(1) 时间复杂度检查到期消息
- 支持从秒级到天级的超长延时（最长365天）
- 秒级精度的延时投递
`)

	// 等待消息执行
	fmt.Println("等待延时消息执行...\n")
	time.Sleep(35 * time.Second)

	fmt.Println("\n=== 延时消息示例完成 ===")
}
