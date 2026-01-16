package main

import (
	"fmt"
	"litemq/pkg/broker"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== 时间轮测试 - 2分钟任务 ===")

	// 创建时间轮
	tw := broker.NewTimeWheel()

	// 任务映射
	tasks := make(map[int64]string)
	taskMutex := sync.RWMutex{}
	taskID := int64(0)

	// 设置回调
	tw.SetExpireCallback(func(offset int64) {
		taskMutex.RLock()
		taskName, exists := tasks[offset]
		taskMutex.RUnlock()

		if exists {
			fmt.Printf("✅ [执行] %s - 时间: %s\n", taskName, time.Now().Format("15:04:05"))
		} else {
			fmt.Printf("❌ [错误] 任务不存在: %d\n", offset)
		}
	})

	// 启动时间轮
	if err := tw.Start(); err != nil {
		fmt.Printf("启动失败: %v\n", err)
		return
	}
	defer tw.Stop()

	fmt.Printf("⏰ 当前时间: %s\n", time.Now().Format("15:04:05"))

	// 添加5秒任务（秒级）
	taskID++
	taskMutex.Lock()
	tasks[taskID] = "5秒任务"
	taskMutex.Unlock()
	tw.AddMessage(time.Now().Add(5*time.Second).UnixMilli(), taskID)
	fmt.Printf("📌 [添加] 5秒任务 - 将在 %s 执行\n", time.Now().Add(5*time.Second).Format("15:04:05"))

	// 添加65秒任务（应该放在分钟级，然后降级）
	taskID++
	taskMutex.Lock()
	tasks[taskID] = "65秒任务"
	taskMutex.Unlock()
	tw.AddMessage(time.Now().Add(65*time.Second).UnixMilli(), taskID)
	fmt.Printf("📌 [添加] 65秒任务 - 将在 %s 执行\n", time.Now().Add(65*time.Second).Format("15:04:05"))

	// 添加120秒任务（2分钟，分钟级）
	taskID++
	taskMutex.Lock()
	tasks[taskID] = "120秒任务(2分钟)"
	taskMutex.Unlock()
	tw.AddMessage(time.Now().Add(120*time.Second).UnixMilli(), taskID)
	fmt.Printf("📌 [添加] 120秒任务 - 将在 %s 执行\n", time.Now().Add(120*time.Second).Format("15:04:05"))

	// 打印时间轮状态
	fmt.Printf("\n=== 时间轮初始状态 ===\n")
	stats := tw.GetStats()
	fmt.Printf("运行状态: %v\n", stats["running"])
	fmt.Printf("总消息数: %v\n", stats["totalMessages"])

	// 等待并观察
	fmt.Printf("\n=== 等待任务执行 ===\n")

	// 等待70秒，观察5秒和65秒任务
	time.Sleep(70 * time.Second)

	// 打印状态
	stats = tw.GetStats()
	fmt.Printf("\n70秒后状态 - 总消息数: %v\n", stats["totalMessages"])

	// 再等待60秒，观察120秒任务
	time.Sleep(60 * time.Second)

	fmt.Printf("\n=== 测试完成 ===\n")
	stats = tw.GetStats()
	fmt.Printf("最终状态 - 总消息数: %v\n", stats["totalMessages"])
}
