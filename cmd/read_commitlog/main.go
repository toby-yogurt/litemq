package main

import (
	"encoding/binary"
	"fmt"
	"litemq/pkg/protocol"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: read_commitlog <commitlog_file_path>")
		fmt.Println("Example: read_commitlog data/broker/commitlog/00000000000000000000")
		os.Exit(1)
	}

	filePath := os.Args[1]

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Failed to get file info: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== CommitLog File Reader ===\n")
	fmt.Printf("File: %s\n", filePath)
	fmt.Printf("Size: %d bytes\n\n", fileInfo.Size())

	// 读取消息
	offset := int64(0)
	messageIndex := 0

	for offset < fileInfo.Size() {
		// 读取消息长度（4字节）
		sizeBytes := make([]byte, 4)
		n, err := file.ReadAt(sizeBytes, offset)
		if err != nil || n != 4 {
			if err != nil {
				fmt.Printf("\nError reading message size at offset %d: %v\n", offset, err)
			}
			break
		}

		messageSize := binary.BigEndian.Uint32(sizeBytes)
		if messageSize == 0 {
			fmt.Printf("\nReached end of valid messages at offset %d\n", offset)
			break
		}

		// 读取消息数据
		messageData := make([]byte, messageSize)
		n, err = file.ReadAt(messageData, offset+4)
		if err != nil || n != int(messageSize) {
			fmt.Printf("\nError reading message data at offset %d: %v\n", offset+4, err)
			break
		}

		// 解码消息
		msg, err := protocol.Decode(messageData)
		if err != nil {
			fmt.Printf("\n=== Message #%d (offset: %d) ===\n", messageIndex+1, offset)
			fmt.Printf("Error decoding message: %v\n", err)
			fmt.Printf("Raw data (first 100 bytes): %x\n", messageData[:min(100, len(messageData))])
			offset += 4 + int64(messageSize)
			messageIndex++
			continue
		}

		// 显示消息信息
		fmt.Printf("\n=== Message #%d (offset: %d, size: %d bytes) ===\n",
			messageIndex+1, offset, 4+messageSize)
		fmt.Printf("MessageID:     %s\n", msg.MessageID)
		fmt.Printf("Topic:         %s\n", msg.Topic)
		fmt.Printf("QueueID:       %d\n", msg.QueueID)
		fmt.Printf("QueueOffset:   %d\n", msg.QueueOffset)
		fmt.Printf("CommitLogOffset: %d\n", msg.CommitLogOffset)
		fmt.Printf("StoreSize:     %d\n", msg.StoreSize)

		if len(msg.Tags) > 0 {
			fmt.Printf("Tags:          %v\n", msg.Tags)
		}
		if len(msg.Keys) > 0 {
			fmt.Printf("Keys:          %v\n", msg.Keys)
		}
		if len(msg.Properties) > 0 {
			fmt.Printf("Properties:    %v\n", msg.Properties)
		}

		fmt.Printf("MessageType:   %d\n", msg.MessageType)
		fmt.Printf("Priority:      %d\n", msg.Priority)
		fmt.Printf("Reliability:   %d\n", msg.Reliability)

		if msg.DelayTime > 0 {
			fmt.Printf("DelayTime:     %d (deliver at: %s)\n",
				msg.DelayTime, time.UnixMilli(msg.DelayTime).Format(time.RFC3339))
		}
		if msg.TransactionID != "" {
			fmt.Printf("TransactionID: %s\n", msg.TransactionID)
		}
		if msg.ShardingKey != "" {
			fmt.Printf("ShardingKey:   %s\n", msg.ShardingKey)
		}
		if msg.Broadcast {
			fmt.Printf("Broadcast:     true\n")
		}

		fmt.Printf("BornTimestamp: %d (%s)\n",
			msg.BornTimestamp, time.UnixMilli(msg.BornTimestamp).Format(time.RFC3339))
		if msg.BornHost != "" {
			fmt.Printf("BornHost:      %s\n", msg.BornHost)
		}
		if msg.StoreHost != "" {
			fmt.Printf("StoreHost:     %s\n", msg.StoreHost)
		}
		if msg.StoreTimestamp > 0 {
			fmt.Printf("StoreTimestamp: %d (%s)\n",
				msg.StoreTimestamp, time.UnixMilli(msg.StoreTimestamp).Format(time.RFC3339))
		}

		// 显示消息体
		if len(msg.Body) > 0 {
			bodyStr := string(msg.Body)
			if len(bodyStr) > 200 {
				fmt.Printf("Body:          %s... (%d bytes)\n", bodyStr[:200], len(msg.Body))
			} else {
				fmt.Printf("Body:          %s (%d bytes)\n", bodyStr, len(msg.Body))
			}
		} else {
			fmt.Printf("Body:          (empty)\n")
		}

		// 更新偏移量
		offset += 4 + int64(messageSize)
		messageIndex++
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total messages: %d\n", messageIndex)
	fmt.Printf("Total size: %d bytes\n", offset)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
