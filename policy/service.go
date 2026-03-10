package policy

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/goto/raccoon/publisher"
	kafkalib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// MetricEvalDuration tracks the latency of the full policy evaluation loop per event (ms).
const MetricEvalDuration = "policy_evaluation_duration_milliseconds"

// Service is the policy enforcement entry point.
// Service handlers in grpc, rest, websocket, and mqtt packages each hold a *Service
// and call Apply to filter events before forwarding them to the buffer channel.
//
// A nil *Service is safe to use — Apply returns the original slice unchanged.
type Service struct {
	chain Chain
}

// NewService builds a fully wired Service from the given config rules.
// It partitions the rules by action type, creates an eval.Cache per action,
// and assembles the action chain in priority order: Drop → OverrideTimestamp.
func NewService(
	rules []config.PolicyRule,
	producer publisher.KafkaProducer,
	overrideTopic string,
	deliveryChannel chan kafkalib.Event,
) *Service {
	dropCache := cache.NewCache(rulesForAction(rules, config.PolicyActionDrop))
	overrideCache := cache.NewCache(rulesForAction(rules, config.PolicyActionOverrideTimestamp))

	return &Service{
		chain: Chain{
			action.NewDrop(dropCache, action.DefaultChain()),
			action.NewOverrideTimestamp(overrideCache, action.DefaultChain(), producer, overrideTopic, deliveryChannel),
		},
	}
}

// Apply filters the event batch through the action chain and returns only events
// whose outcome is OutcomePassthrough (i.e. no policy consumed the event).
// Safe to call on a nil *Service — returns the original slice unchanged.
func (s *Service) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	if s == nil {
		return events
	}
	filtered := make([]*pb.Event, 0, len(events))
	for _, event := range events {
		evalStart := time.Now()
		meta := extractMetadata(
			event,
			connGroup,
			config.PolicyCfg.PublisherMapping,
			config.EventDistribution.PublisherPattern,
		)
		outcome := s.chain.Process(event, meta)
		metrics.Timing(
			MetricEvalDuration,
			time.Since(evalStart).Milliseconds(),
			fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, meta.EventType),
		)
		if outcome == action.OutcomePassthrough {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// rulesForAction returns the subset of rules matching the given action type.
func rulesForAction(rules []config.PolicyRule, actionType config.PolicyActionType) []config.PolicyRule {
	var filtered []config.PolicyRule
	for _, r := range rules {
		if r.Action.Type == actionType {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// extractMetadata builds eval.EventMetadata from a protobuf Event, its connection group,
// a conn_group→publisher map, and the topic format string (e.g. "clickstream-%s-log").
func extractMetadata(event *pb.Event, connGroup string, publisherMap map[string]string, topicFormat string) eval.EventMetadata {
	var ts time.Time
	if event.GetEventTimestamp() != nil {
		ts = event.GetEventTimestamp().AsTime()
	}
	return eval.EventMetadata{
		EventType:      event.GetType(),
		EventName:      event.GetEventName(),
		Product:        event.GetProduct(),
		ConnGroup:      connGroup,
		Publisher:      resolvePublisher(connGroup, publisherMap),
		TopicName:      fmt.Sprintf(topicFormat, event.GetType()),
		EventTimestamp: ts,
	}
}

// resolvePublisher maps a conn_group to a publisher name using the provided map.
// Falls back to the conn_group itself when no mapping is found.
func resolvePublisher(connGroup string, publisherMap map[string]string) string {
	if pub, ok := publisherMap[connGroup]; ok {
		return pub
	}
	return connGroup
}
