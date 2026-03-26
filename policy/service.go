package policy

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

// MetricEvalDuration is the service-level alias for the shared latency metric.
// Use the action-level metric (action.MetricEvalLatency) for per-action breakdown.
const MetricEvalDuration = action.MetricEvalLatency

// Service is the policy enforcement entry point.
// Service handlers in grpc, rest, websocket, and mqtt packages each hold a *Service
// and call Apply to filter events before forwarding them to the buffer channel.
type Service struct {
	chain Chain
}

// NewService builds a fully wired Service from the given config rules.
// It partitions the rules by action type, creates an eval.Cache per action,
// and assembles the action chain in priority order: Deactivate → Drop → OverrideTimestamp.
func NewService(rules []config.PolicyRule, overrideEventType string) *Service {
	dropCache := cache.NewCache(rulesForAction(rules, config.PolicyActionDrop))
	overrideCache := cache.NewCache(rulesForAction(rules, config.PolicyActionOverrideTimestamp))
	deactivateCache := cache.NewCache(rulesForAction(rules, config.PolicyActionDeactivate))

	known := map[config.PolicyActionType]bool{
		config.PolicyActionDrop:              true,
		config.PolicyActionOverrideTimestamp: true,
		config.PolicyActionDeactivate:        true,
	}
	for _, r := range rules {
		if !known[r.Action.Type] {
			logger.Errorf("policy: rule skipped — unknown action type %q for resource %q, details %+v", r.Action.Type, r.Resource, r.Details)
		}
	}

	return &Service{
		chain: Chain{
			action.NewDeactivate(deactivateCache, action.DefaultChain()),
			action.NewDrop(dropCache, action.DefaultChain()),
			action.NewOverrideTimestamp(overrideCache, action.DefaultChain(), overrideEventType),
		},
	}
}

// Apply runs the event batch through the action pipeline and returns only events
// that no action consumed (passthrough)
func (s *Service) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	if s == nil {
		return events
	}
	start := time.Now()
	result := s.chain.Apply(events, connGroup)
	metrics.Timing(MetricEvalDuration, time.Since(start).Milliseconds(), fmt.Sprintf("action=total,conn_group=%s", connGroup))
	return result
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
