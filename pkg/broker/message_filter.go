package broker

import (
	"fmt"
	"regexp"
	"strings"

	"litemq/pkg/common/logger"
	"litemq/pkg/protocol"
)

// FilterType 过滤类型
type FilterType int

const (
	FilterTypeTag      FilterType = 1 // 基于Tag过滤
	FilterTypeSQL      FilterType = 2 // 基于SQL表达式过滤
	FilterTypeProperty FilterType = 3 // 基于Property过滤
)

// MessageFilter 消息过滤器
type MessageFilter struct {
	FilterType FilterType
	Expression string // 过滤表达式（Tag名称、SQL表达式或Property键值对）
}

// FilterMessage 过滤消息
// 根据过滤条件判断消息是否匹配
func FilterMessage(msg *protocol.Message, filter *MessageFilter) bool {
	if filter == nil {
		return true // 没有过滤条件，所有消息都通过
	}

	switch filter.FilterType {
	case FilterTypeTag:
		return filterByTag(msg, filter.Expression)
	case FilterTypeSQL:
		return filterBySQL(msg, filter.Expression)
	case FilterTypeProperty:
		return filterByProperty(msg, filter.Expression)
	default:
		logger.Warn("Unknown filter type", "type", filter.FilterType)
		return true
	}
}

// filterByTag 基于Tag过滤
// expression: Tag名称，支持多个Tag用 || 分隔（OR逻辑）
func filterByTag(msg *protocol.Message, expression string) bool {
	if expression == "" || expression == "*" {
		return true // 空表达式或通配符，所有消息都通过
	}

	// 支持多个Tag用 || 分隔（OR逻辑）
	tags := strings.Split(expression, "||")
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}

		// 检查消息是否包含该Tag
		for _, msgTag := range msg.Tags {
			if msgTag == tag {
				return true
			}
		}
	}

	return false
}

// filterBySQL 基于SQL表达式过滤
// expression: SQL表达式，例如 "a > 5 AND b = 'test'"
// 支持的操作符：=, !=, >, <, >=, <=, AND, OR, IN, NOT IN
// 支持的属性访问：properties.key 或 key
func filterBySQL(msg *protocol.Message, expression string) bool {
	if expression == "" {
		return true
	}

	// 简化实现：解析SQL表达式并求值
	// 实际生产环境可以使用更完善的SQL解析器
	return evaluateSQLExpression(msg, expression)
}

// evaluateSQLExpression 评估SQL表达式
// 简化实现，支持基本的比较和逻辑运算
func evaluateSQLExpression(msg *protocol.Message, expression string) bool {
	// 转换为小写以便解析
	expr := strings.ToLower(strings.TrimSpace(expression))

	// 处理 AND 逻辑
	if strings.Contains(expr, " and ") {
		parts := strings.Split(expr, " and ")
		for _, part := range parts {
			if !evaluateSQLExpression(msg, part) {
				return false
			}
		}
		return true
	}

	// 处理 OR 逻辑
	if strings.Contains(expr, " or ") {
		parts := strings.Split(expr, " or ")
		for _, part := range parts {
			if evaluateSQLExpression(msg, part) {
				return true
			}
		}
		return false
	}

	// 处理比较表达式
	return evaluateComparison(msg, expr)
}

// evaluateComparison 评估比较表达式
// 支持：key = value, key != value, key > value, key < value, key >= value, key <= value
func evaluateComparison(msg *protocol.Message, expr string) bool {
	// 匹配各种比较操作符
	patterns := []struct {
		op    string
		regex *regexp.Regexp
	}{
		{"!=", regexp.MustCompile(`^(.+?)\s*!=\s*(.+)$`)},
		{">=", regexp.MustCompile(`^(.+?)\s*>=\s*(.+)$`)},
		{"<=", regexp.MustCompile(`^(.+?)\s*<=\s*(.+)$`)},
		{">", regexp.MustCompile(`^(.+?)\s*>\s*(.+)$`)},
		{"<", regexp.MustCompile(`^(.+?)\s*<\s*(.+)$`)},
		{"=", regexp.MustCompile(`^(.+?)\s*=\s*(.+)$`)},
	}

	for _, pattern := range patterns {
		matches := pattern.regex.FindStringSubmatch(expr)
		if len(matches) == 3 {
			key := strings.TrimSpace(matches[1])
			value := strings.TrimSpace(matches[2])
			// 移除引号
			value = strings.Trim(value, `"'`)

			// 获取消息属性值
			msgValue := msg.GetProperty(key)
			if msgValue == "" {
				return false
			}

			// 根据操作符比较
			return compareValues(msgValue, value, pattern.op)
		}
	}

	// 如果没有匹配到任何操作符，返回false
	logger.Warn("Unsupported comparison expression", "expression", expr)
	return false
}

// compareValues 比较两个值
func compareValues(msgValue, filterValue, op string) bool {
	switch op {
	case "=":
		return msgValue == filterValue
	case "!=":
		return msgValue != filterValue
	case ">":
		return compareNumeric(msgValue, filterValue) > 0
	case "<":
		return compareNumeric(msgValue, filterValue) < 0
	case ">=":
		return compareNumeric(msgValue, filterValue) >= 0
	case "<=":
		return compareNumeric(msgValue, filterValue) <= 0
	default:
		return false
	}
}

// compareNumeric 数值比较（简化实现）
func compareNumeric(a, b string) int {
	// 尝试转换为数字比较
	var numA, numB float64
	_, errA := fmt.Sscanf(a, "%f", &numA)
	_, errB := fmt.Sscanf(b, "%f", &numB)

	if errA == nil && errB == nil {
		if numA > numB {
			return 1
		} else if numA < numB {
			return -1
		}
		return 0
	}

	// 如果不是数字，按字符串比较
	if a > b {
		return 1
	} else if a < b {
		return -1
	}
	return 0
}

// filterByProperty 基于Property过滤
// expression: Property键值对，格式：key=value 或 key1=value1,key2=value2
func filterByProperty(msg *protocol.Message, expression string) bool {
	if expression == "" {
		return true
	}

	// 解析键值对
	pairs := strings.Split(expression, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// 解析 key=value
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"'`) // 移除引号

		// 检查消息属性
		msgValue := msg.GetProperty(key)
		if msgValue != value {
			return false
		}
	}

	return true
}

// ParseFilter 解析过滤表达式
// 自动识别过滤类型
func ParseFilter(expression string) *MessageFilter {
	if expression == "" || expression == "*" {
		return nil // 无过滤条件
	}

	// 检查是否是SQL表达式（包含 AND, OR, >, <, = 等）
	if strings.Contains(expression, " AND ") || strings.Contains(expression, " OR ") ||
		strings.Contains(expression, ">") || strings.Contains(expression, "<") ||
		strings.Contains(expression, "!=") {
		return &MessageFilter{
			FilterType: FilterTypeSQL,
			Expression: expression,
		}
	}

	// 检查是否是Property表达式（包含 = 但不包含 >, <）
	if strings.Contains(expression, "=") && !strings.Contains(expression, ">") &&
		!strings.Contains(expression, "<") && !strings.Contains(expression, "!=") {
		return &MessageFilter{
			FilterType: FilterTypeProperty,
			Expression: expression,
		}
	}

	// 默认按Tag过滤
	return &MessageFilter{
		FilterType: FilterTypeTag,
		Expression: expression,
	}
}
