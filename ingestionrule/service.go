package ingestionrule

import (
	"context"
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	evalcache "github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/ingestionrule/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/schemaregistry/deserialization"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// MetricEvalDuration is the service-level alias for the shared latency metric.
// Use the action-level metric (action.MetricEvalLatency) for per-action breakdown.
const MetricEvalDuration = action.MetricEvalLatency

// Service is the policy enforcement entry point.
// Service handlers in grpc, rest, websocket, and mqtt packages each hold a *Service
// and call Apply to filter events before forwarding them to the buffer channel.
type Service struct {
	chain            Chain
	duplicateChecker action.DuplicateChecker
	deserializer     *deserialization.Deserializer
}

// NewService builds a fully wired Service from the given config rules.
// It partitions the rules by action type, creates an eval.Cache per action,
// and assembles the action chain in priority order: Deactivate → Drop → OverrideTimestamp → Dedup.
func NewService(ctx context.Context, rules []config.PolicyRule, overrideEventType string) (*Service, error) {
	var deserializer *deserialization.Deserializer

	if config.DeserializationCfg.Enabled || config.DedupCfg.Enabled {
		stencil, err := schemaregistry.NewStencilClient()
		if err != nil {
			return nil, err
		}

		var cache *deserialization.SchemaCache

		if config.DeserializationCfg.Enabled {
			cache = deserialization.NewSchemaCache(ctx)
			cache.Start()
		}

		deserializer = deserialization.NewDeserializer(stencil, cache)
	}

	var chain Chain
	var duplicateChecker action.DuplicateChecker

	if config.PolicyCfg.Enabled {
		dropCache := evalcache.NewCache(rulesForAction(rules, config.PolicyActionDrop))
		overrideCache := evalcache.NewCache(rulesForAction(rules, config.PolicyActionOverrideTimestamp))
		deactivateCache := evalcache.NewCache(rulesForAction(rules, config.PolicyActionDeactivate))

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

		chain = Chain{
			action.NewDeactivate(deactivateCache, action.DefaultChain()),
			action.NewDrop(dropCache, action.DefaultChain()),
			action.NewOverrideTimestamp(overrideCache, action.DefaultChain(), overrideEventType),
		}

		if config.DedupCfg.Enabled {
			cacheClient, err := cache.NewRedisCache(ctx, config.MetricStatsd.FlushPeriodMs)
			if err != nil {
				return nil, err
			}

			duplicateChecker, err = cache.NewStore(ctx, cacheClient)
			if err != nil {
				return nil, err
			}

			chain = append(chain, action.NewDedup(duplicateChecker))
		}
	}

	return &Service{
		chain:            chain,
		duplicateChecker: duplicateChecker,
		deserializer:     deserializer,
	}, nil
}

// Close closes any resources used by the service during shutdown.
func (s *Service) Close() {
	if s == nil {
		return
	}

	if s.deserializer != nil {
		s.deserializer.Close()
	}

	if s.duplicateChecker != nil {
		if err := s.duplicateChecker.Close(); err != nil {
			logger.Errorf("failed to close duplicate checker: %v", err)
		}
	}
}

// DedupHealthCheck checks the health of the duplicate checker client.
func (s *Service) DedupHealthCheck() error {
	if s == nil || s.duplicateChecker == nil {
		return nil
	}

	return s.duplicateChecker.HealthCheck()
}

// CompassHealthCheck checks the health of the compass schema registry client.
func (s *Service) CompassHealthCheck() error {
	if s == nil || s.deserializer == nil {
		return nil
	}

	return s.deserializer.HealthCheck()
}

// Apply runs the event batch through the action pipeline and returns only events
// that no action consumed (passthrough)
func (s *Service) Apply(ctx context.Context, events []*pb.Event, connGroup string) []*model.EventWithMetadata {
	start := time.Now()
	defer func() {
		metrics.Timing(MetricEvalDuration, time.Since(start).Milliseconds(), fmt.Sprintf("action=total,conn_group=%s", connGroup))
	}()

	var metadataEvents []*model.EventWithMetadata
	if s == nil {
		var d *deserialization.Deserializer

		return d.Deserialize(events, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
	} else {
		metadataEvents = s.deserializer.Deserialize(events, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
	}

	sanitizedEvents := s.chain.Apply(ctx, metadataEvents, connGroup)

	return sanitizedEvents
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
