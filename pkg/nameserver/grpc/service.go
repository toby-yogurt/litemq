package grpc

import (
	"context"

	pb "litemq/api/proto"
)

// NameServerInterface 定义NameServer需要实现的方法
type NameServerInterface interface {
	RegisterBroker(brokerInfo interface{}) error
	UnregisterBroker(brokerId string) error
	GetTopicRouteInfo(topic string) (interface{}, error)
	GetAllBrokerInfos() []interface{}
	UpdateBrokerHeartbeat(brokerId string)
}

// NameServerService 实现gRPC NameServer服务
type NameServerService struct {
	pb.UnimplementedNameServerServiceServer
	nameserver NameServerInterface
}

// NewNameServerService 创建NameServer gRPC服务
func NewNameServerService(ns NameServerInterface) *NameServerService {
	return &NameServerService{
		nameserver: ns,
	}
}

// convertProtoTopicsToMap 将proto topics转换为map
func convertProtoTopicsToMap(protoTopics map[string]*pb.QueueIds) map[string]interface{} {
	result := make(map[string]interface{})
	for topic, queueIds := range protoTopics {
		queueIdsList := make([]int32, len(queueIds.QueueIds))
		copy(queueIdsList, queueIds.QueueIds)
		result[topic] = queueIdsList
	}
	return result
}

// RegisterBroker 注册Broker
func (s *NameServerService) RegisterBroker(ctx context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	// 创建broker信息映射
	brokerInfo := map[string]interface{}{
		"broker_id":    req.BrokerId,
		"broker_name":  req.BrokerName,
		"broker_addr":  req.BrokerAddr,
		"cluster_name": req.ClusterName,
		"role":         req.Role,
		"topics":       convertProtoTopicsToMap(req.Topics),
	}

	err := s.nameserver.RegisterBroker(brokerInfo)
	if err != nil {
		return &pb.RegisterBrokerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.RegisterBrokerResponse{
		Success: true,
	}, nil
}

// UnregisterBroker 注销Broker
func (s *NameServerService) UnregisterBroker(ctx context.Context, req *pb.UnregisterBrokerRequest) (*pb.UnregisterBrokerResponse, error) {
	err := s.nameserver.UnregisterBroker(req.BrokerId)
	if err != nil {
		return &pb.UnregisterBrokerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.UnregisterBrokerResponse{
		Success: true,
	}, nil
}

// convertRouteInfoToProto 将路由信息转换为proto格式
func convertRouteInfoToProto(routeInfo interface{}) *pb.TopicRouteInfo {
	if info, ok := routeInfo.(map[string]interface{}); ok {
		return &pb.TopicRouteInfo{
			TopicName:   info["topic_name"].(string),
			QueueCount:  info["queue_count"].(int32),
			BrokerAddrs: info["broker_addrs"].([]string),
		}
	}
	return nil
}

// convertBrokerInfoToProto 将broker信息转换为proto格式
func convertBrokerInfoToProto(brokerInfo interface{}) *pb.BrokerInfo {
	if info, ok := brokerInfo.(map[string]interface{}); ok {
		topics := make(map[string]*pb.QueueIds)
		if topicsMap, ok := info["topics"].(map[string]interface{}); ok {
			for topic, queueIds := range topicsMap {
				if ids, ok := queueIds.([]interface{}); ok {
					queueIdsList := make([]int32, len(ids))
					for i, id := range ids {
						queueIdsList[i] = id.(int32)
					}
					topics[topic] = &pb.QueueIds{QueueIds: queueIdsList}
				}
			}
		}

		return &pb.BrokerInfo{
			BrokerId:    info["broker_id"].(string),
			BrokerName:  info["broker_name"].(string),
			BrokerAddr:  info["broker_addr"].(string),
			ClusterName: info["cluster_name"].(string),
			Role:        info["role"].(string),
			LastUpdate:  info["last_update"].(int64),
			Topics:      topics,
		}
	}
	return nil
}

// GetRouteInfo 获取路由信息
func (s *NameServerService) GetRouteInfo(ctx context.Context, req *pb.GetRouteInfoRequest) (*pb.GetRouteInfoResponse, error) {
	routeInfo, err := s.nameserver.GetTopicRouteInfo(req.Topic)
	if err != nil {
		return &pb.GetRouteInfoResponse{
			Error: err.Error(),
		}, nil
	}

	// 转换到proto格式
	protoRouteInfo := convertRouteInfoToProto(routeInfo)
	if protoRouteInfo == nil {
		return &pb.GetRouteInfoResponse{
			Error: "invalid route info format",
		}, nil
	}

	return &pb.GetRouteInfoResponse{
		RouteInfo: protoRouteInfo,
	}, nil
}

// GetAllBrokers 获取所有Broker
func (s *NameServerService) GetAllBrokers(ctx context.Context, req *pb.GetAllBrokersRequest) (*pb.GetAllBrokersResponse, error) {
	brokers := s.nameserver.GetAllBrokerInfos()

	// 转换到proto格式
	protoBrokers := make([]*pb.BrokerInfo, len(brokers))
	for i, broker := range brokers {
		protoBrokers[i] = convertBrokerInfoToProto(broker)
		if protoBrokers[i] == nil {
			continue // 跳过无效的broker信息
		}
	}

	return &pb.GetAllBrokersResponse{
		Brokers: protoBrokers,
	}, nil
}

// Heartbeat 心跳 (Broker向NameServer)
func (s *NameServerService) Heartbeat(ctx context.Context, req *pb.BrokerHeartbeatRequest) (*pb.BrokerHeartbeatResponse, error) {
	// 更新心跳
	s.nameserver.UpdateBrokerHeartbeat(req.BrokerId)

	return &pb.BrokerHeartbeatResponse{
		Success: true,
	}, nil
}

// convertProtoTopics 将proto格式的topics转换为内部格式
func convertProtoTopics(protoTopics map[string]*pb.QueueIds) map[string][]int {
	topics := make(map[string][]int)
	for topic, queueIds := range protoTopics {
		ids := make([]int, len(queueIds.QueueIds))
		for i, id := range queueIds.QueueIds {
			ids[i] = int(id)
		}
		topics[topic] = ids
	}
	return topics
}

// convertTopicsToProto 将内部格式的topics转换为proto格式
func convertTopicsToProto(topics map[string][]int) map[string]*pb.QueueIds {
	protoTopics := make(map[string]*pb.QueueIds)
	for topic, queueIds := range topics {
		ids := make([]int32, len(queueIds))
		for i, id := range queueIds {
			ids[i] = int32(id)
		}
		protoTopics[topic] = &pb.QueueIds{
			QueueIds: ids,
		}
	}
	return protoTopics
}

// ConfigRole 角色转换 (临时定义，实际应该在config包中)
type ConfigRole string

const (
	RoleMaster ConfigRole = "master"
	RoleSlave  ConfigRole = "slave"
)
