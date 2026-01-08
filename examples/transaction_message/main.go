package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"litemq/pkg/client"
	"litemq/pkg/common/config"
	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

func main() {
	fmt.Println("=== LiteMQ 事务消息示例 ===\n")
	fmt.Println("本示例演示分布式事务消息的两阶段提交机制\n")

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

	// ========== 消费者部分 ==========
	fmt.Println("【2. 创建并启动消费者】")
	consumer := client.NewConsumer(cfg, "transaction_consumer_group")

	// 定义消息处理函数
	messageHandler := func(msg *protocol.Message) protocol.ConsumeStatus {
		fmt.Printf("📨 【收到事务消息】\n")
		fmt.Printf("   - 消息ID: %s\n", msg.MessageID)
		fmt.Printf("   - 事务ID: %s\n", msg.TransactionID)
		fmt.Printf("   - 消息内容: %s\n", string(msg.Body))
		fmt.Printf("   - 消息状态: %s\n", getMessageStatus(msg.MessageStatus))
		fmt.Printf("   - 消息类型: 事务消息\n\n")

		return protocol.ConsumeStatusSuccess
	}

	// 订阅主题
	topic := "transaction_topic"
	if err := consumer.Subscribe(topic, messageHandler); err != nil {
		log.Fatalf("订阅主题失败: %v", err)
	}

	if err := consumer.Start(); err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	defer consumer.Shutdown()

	fmt.Println("✅ 消费者已启动\n")

	// ========== 场景1: 事务成功提交 ==========
	fmt.Println("【场景1: 事务成功提交】")
	fmt.Println("模拟订单创建流程：")
	fmt.Println("  1. 发送事务消息（Half消息）")
	fmt.Println("  2. 执行本地事务（创建订单）")
	fmt.Println("  3. 提交事务\n")

	// 步骤1: 发送事务消息（Half消息）
	orderID := fmt.Sprintf("ORDER-%d", time.Now().Unix())
	orderData := fmt.Sprintf(`{"order_id": "%s", "user_id": "user123", "amount": 100.00}`, orderID)

	fmt.Printf("📤 步骤1: 发送事务消息（Half消息）\n")
	result, err := producer.SendTransactionMessage(topic, []byte(orderData), "")
	if err != nil {
		log.Printf("❌ 发送事务消息失败: %v", err)
	} else {
		transactionID := result.MessageID // 使用消息ID作为事务ID
		fmt.Printf("   ✅ Half消息已发送\n")
		fmt.Printf("   - 消息ID: %s\n", result.MessageID)
		fmt.Printf("   - 事务ID: %s\n", transactionID)
		fmt.Printf("   - 订单ID: %s\n", orderID)
		fmt.Printf("   - 当前状态: PREPARED（预提交）\n")
		fmt.Printf("   - 消费者状态: 不可见（消息在Half消息存储中）\n\n")

		// 步骤2: 执行本地事务
		fmt.Printf("📝 步骤2: 执行本地事务（创建订单）\n")
		time.Sleep(1 * time.Second) // 模拟业务处理时间

		// 模拟业务逻辑成功
		businessSuccess := true
		if businessSuccess {
			fmt.Printf("   ✅ 本地事务执行成功（订单已创建）\n\n")

			// 步骤3: 提交事务
			fmt.Printf("✅ 步骤3: 提交事务\n")
			if err := producer.CommitTransaction(transactionID); err != nil {
				log.Printf("❌ 提交事务失败: %v", err)
			} else {
				fmt.Printf("   ✅ 事务已提交\n")
				fmt.Printf("   - 事务ID: %s\n", transactionID)
				fmt.Printf("   - 当前状态: COMMIT（已提交）\n")
				fmt.Printf("   - 消费者状态: 可见（消息已移动到正常Topic）\n\n")
			}
		}
	}

	time.Sleep(2 * time.Second)

	// ========== 场景2: 事务回滚 ==========
	fmt.Println("\n【场景2: 事务回滚】")
	fmt.Println("模拟库存不足导致订单创建失败：")
	fmt.Println("  1. 发送事务消息（Half消息）")
	fmt.Println("  2. 执行本地事务（检查库存）")
	fmt.Println("  3. 回滚事务\n")

	// 步骤1: 发送事务消息
	orderID2 := fmt.Sprintf("ORDER-%d", time.Now().Unix()+1)
	orderData2 := fmt.Sprintf(`{"order_id": "%s", "user_id": "user456", "amount": 200.00}`, orderID2)

	fmt.Printf("📤 步骤1: 发送事务消息（Half消息）\n")
	result2, err := producer.SendTransactionMessage(topic, []byte(orderData2), "")
	if err != nil {
		log.Printf("❌ 发送事务消息失败: %v", err)
	} else {
		transactionID2 := result2.MessageID
		fmt.Printf("   ✅ Half消息已发送\n")
		fmt.Printf("   - 消息ID: %s\n", result2.MessageID)
		fmt.Printf("   - 事务ID: %s\n", transactionID2)
		fmt.Printf("   - 订单ID: %s\n", orderID2)
		fmt.Printf("   - 当前状态: PREPARED（预提交）\n\n")

		// 步骤2: 执行本地事务
		fmt.Printf("📝 步骤2: 执行本地事务（检查库存）\n")
		time.Sleep(1 * time.Second)

		// 模拟业务逻辑失败（库存不足）
		hasStock := false
		if !hasStock {
			fmt.Printf("   ❌ 本地事务执行失败（库存不足）\n\n")

			// 步骤3: 回滚事务
			fmt.Printf("❌ 步骤3: 回滚事务\n")
			if err := producer.RollbackTransaction(transactionID2); err != nil {
				log.Printf("❌ 回滚事务失败: %v", err)
			} else {
				fmt.Printf("   ✅ 事务已回滚\n")
				fmt.Printf("   - 事务ID: %s\n", transactionID2)
				fmt.Printf("   - 当前状态: ROLLBACK（已回滚）\n")
				fmt.Printf("   - 消费者状态: 不可见（消息已删除）\n\n")
			}
		}
	}

	time.Sleep(2 * time.Second)

	// ========== 场景3: 事务回查 ==========
	fmt.Println("\n【场景3: 事务回查机制】")
	fmt.Println("模拟网络故障导致事务状态未知：")
	fmt.Println("  1. 发送事务消息（Half消息）")
	fmt.Println("  2. 执行本地事务（但未收到确认）")
	fmt.Println("  3. Broker主动回查事务状态\n")

	// 步骤1: 发送事务消息
	orderID3 := fmt.Sprintf("ORDER-%d", time.Now().Unix()+2)
	orderData3 := fmt.Sprintf(`{"order_id": "%s", "user_id": "user789", "amount": 300.00}`, orderID3)

	fmt.Printf("📤 步骤1: 发送事务消息（Half消息）\n")
	result3, err := producer.SendTransactionMessage(topic, []byte(orderData3), "")
	if err != nil {
		log.Printf("❌ 发送事务消息失败: %v", err)
	} else {
		transactionID3 := result3.MessageID
		fmt.Printf("   ✅ Half消息已发送\n")
		fmt.Printf("   - 消息ID: %s\n", result3.MessageID)
		fmt.Printf("   - 事务ID: %s\n", transactionID3)
		fmt.Printf("   - 订单ID: %s\n", orderID3)
		fmt.Printf("   - 当前状态: PREPARED（预提交）\n\n")

		// 步骤2: 执行本地事务（但模拟网络故障，未收到确认）
		fmt.Printf("📝 步骤2: 执行本地事务（但网络故障，未收到确认）\n")
		time.Sleep(1 * time.Second)
		fmt.Printf("   ⚠️  本地事务已执行，但提交/回滚命令丢失\n\n")

		// 步骤3: Broker回查（由Broker的回查服务自动执行）
		fmt.Printf("🔄 步骤3: Broker主动回查事务状态\n")
		fmt.Printf("   - Broker会定期检查长时间未决的事务消息\n")
		fmt.Printf("   - 主动向生产者查询事务状态\n")
		fmt.Printf("   - 根据查询结果决定提交或回滚\n")
		fmt.Printf("   - 确保事务的最终一致性\n\n")

		// 模拟回查结果：事务已成功
		fmt.Printf("   ✅ 回查结果: 事务已成功，自动提交\n")
		fmt.Printf("   - 事务ID: %s\n", transactionID3)
		fmt.Printf("   - 最终状态: COMMIT（已提交）\n\n")
	}

	time.Sleep(2 * time.Second)

	// ========== 工作原理说明 ==========
	fmt.Println("\n【事务消息工作原理】")
	fmt.Println(`
事务消息使用两阶段提交（2PC）机制：

阶段1: 预提交（Half消息）
  - 生产者发送事务消息到Broker
  - Broker存储为Half消息（PREPARED状态）
  - 消息存储在系统内部Topic（RMQ_SYS_TRANS_HALF_TOPIC）
  - 消费者不可见（未构建ConsumeQueue索引）

阶段2: 提交/回滚
  - 生产者执行本地事务
  - 根据结果发送提交或回滚命令
  - Broker处理命令：
    * 提交：将消息从Half消息存储移动到正常Topic，构建索引
    * 回滚：删除Half消息

回查机制：
  - Broker定期检查长时间未决的事务消息
  - 主动向生产者查询事务状态
  - 根据查询结果决定提交或回滚
  - 确保事务的最终一致性

优势：
- 保证分布式事务的最终一致性
- 支持事务回查，处理网络故障
- 消息不丢失，即使生产者崩溃也能恢复
`)

	// 等待消息处理
	fmt.Println("\n等待消息处理...")
	time.Sleep(5 * time.Second)

	fmt.Println("\n=== 事务消息示例完成 ===")
}

// getMessageStatus 获取消息状态描述
func getMessageStatus(status protocol.MessageStatus) string {
	switch status {
	case protocol.MessageStatusNormal:
		return "NORMAL（正常）"
	case protocol.MessageStatusPrepared:
		return "PREPARED（预提交）"
	case protocol.MessageStatusCommit:
		return "COMMIT（已提交）"
	case protocol.MessageStatusRollback:
		return "ROLLBACK（已回滚）"
	case protocol.MessageStatusDead:
		return "DEAD（死信）"
	default:
		return "UNKNOWN（未知）"
	}
}
