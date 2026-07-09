package action

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/ingestionrule/action/eventregistry"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// EventChecker defines the interface to retrieve event status from the cache.
type EventChecker interface {
	// GetEvents retrieves the status of an event from the cache.
	GetEvents(key string) (eventregistry.EventStatus, bool)
	// HealthCheck checks the health of the event checker.
	HealthCheck() error
	// Close closes the event checker.
	Close()
	// Start starts the event checker
	Start()
	// HasSynced returns true if the cache has successfully synced at least once.
	HasSynced() bool
	// BuildCacheKey builds the cache key for the given event.
	BuildCacheKey(topic, product, eventName string) string
}

// Deactivate is a policy action that unconditionally drops events matching the
// configured rules or unregistered in MSL cache. Unlike Drop, it requires no condition_type in the rule config.
type Deactivate struct {
	cache        *cache.Cache
	evalChain    Chain
	eventChecker EventChecker
}

// NewDeactivate creates a new Deactivate action with the given cache, evaluator chain, and registration store.
func NewDeactivate(c *cache.Cache, evalChain Chain, eventChecker EventChecker) *Deactivate {
	return &Deactivate{
		cache:        c,
		evalChain:    evalChain,
		eventChecker: eventChecker,
	}
}

// Apply evaluates every event in the batch against the deactivate policy rules and event registration cache.
// Matching or unregistered events are dropped unconditionally.
func (d *Deactivate) Apply(ctx context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
	start := time.Now()
	filtered := make([]*model.EventWithMetadata, 0, len(events))

	for _, meta := range events {
		if ok, _ := d.evalChain.Run(*meta, d.cache); ok {
			logger.Debugf("[deactivate.Apply] deactivating event: publisher=%s, event_type=%s, product=%s, event_name=%s, app_version=%s, platform=%s",
				meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.AppVersion, meta.Platform)
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
			continue
		}

		if d.eventChecker != nil && config.PolicyCfg.EventVerificationEnabled {
			if !d.eventChecker.HasSynced() {
				filtered = append(filtered, meta)
				continue
			}

			eventKey := d.eventChecker.BuildCacheKey(meta.TopicName, meta.Product, meta.EventName)
			status, ok := d.eventChecker.GetEvents(eventKey)
			if !ok {
				fallbackKey := d.eventChecker.BuildCacheKey("", meta.Product, meta.EventName)
				_, ok = d.eventChecker.GetEvents(fallbackKey)
				if ok {
					filtered = append(filtered, meta)
					continue
				}

				logger.Debugf("[deactivate.Apply] deactivating event: publisher=%s, event_type=%s, product=%s, event_name=%s, app_version=%s, platform=%s",
					meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.AppVersion, meta.Platform)
				metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_REGISTRY_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, meta.Type))

				filtered = append(filtered, meta)
				continue
			}

			if status == eventregistry.EventStatusInactive || status == eventregistry.EventStatusDeprecated {
				logger.Debugf("[deactivate.Apply] deactivating event: publisher=%s, event_type=%s, product=%s, event_name=%s, app_version=%s, platform=%s",
					meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.AppVersion, meta.Platform)
				metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_REGISTRY_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, meta.Type))
			}
		}

		filtered = append(filtered, meta)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEACTIVATE,conn_group=%s", connGroup))

	return filtered
}
