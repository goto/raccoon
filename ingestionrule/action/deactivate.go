package action

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/ingestionrule/action/eventchecker"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// EventChecker defines the interface to retrieve event status from the cache.
type EventChecker interface {
	// GetEvents retrieves the status of an event from the cache.
	GetEvents(key string) (eventchecker.EventStatus, bool)
	// HealthCheck checks the health of the event checker.
	HealthCheck() error
	// Close closes the event checker.
	Close()
	// Start starts the event checker
	Start()
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
		if d.evalChain.Run(*meta, d.cache) {
			logger.Debugf("[deactivate.Apply] deactivating event: publisher=%s, event_type=%s, product=%s, event_name=%s, app_version=%s, platform=%s",
				meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.AppVersion, meta.Platform)
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
			continue
		}

		if d.eventChecker != nil && config.PolicyCfg.EventVerificationEnabled {
			eventKey := d.buildCacheKey(*meta)
			status, ok := d.eventChecker.GetEvents(eventKey)
			if !ok || status == eventchecker.EventStatusInactive || status == eventchecker.EventStatusDeprecated {
				logger.Debugf("[deactivate.Apply] deactivating event: publisher=%s, event_type=%s, product=%s, event_name=%s, app_version=%s, platform=%s",
					meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.AppVersion, meta.Platform)
				metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, meta.Type))
				continue
			}
		}

		filtered = append(filtered, meta)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEACTIVATE,conn_group=%s", connGroup))

	return filtered
}

// buildCacheKey builds the cache key for the given event. The generated string is always a contiguous 16-character lowercase hexadecimal string
// (e.g., "3b0fe0c74e55b798").
//
// Key Format:
// The cache key is constructed using a fixed sequence of event attributes:
//  1. Publisher identifier
//  2. Topic name
//  3. Product name
//  4. Event name
//
// Each attribute is separated by a colon (":") before hashing.
//
// This composite key ensures that events from different publishers, topics, products,
// or event types will have unique cache keys, enabling effective deduplication
// and policy enforcement.
func (d *Deactivate) buildCacheKey(event model.EventWithMetadata) string {
	h := xxhash.New()

	const keySeparator = ":"

	// key: <publisher>:<topic>:<product>:<event_name>
	_, _ = io.WriteString(h, event.Publisher)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, event.TopicName)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, event.Product)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, event.EventName)
	hash := h.Sum64()

	return fmt.Sprintf("%016x", hash)
}
