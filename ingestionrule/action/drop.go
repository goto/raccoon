package action

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// Drop is a policy action that drops events matching the configured rules.
type Drop struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewDrop creates a new Drop action with the given cache and evaluator chain.
func NewDrop(c *cache.Cache, evalChain Chain) *Drop {
	return &Drop{
		cache:     c,
		evalChain: evalChain,
	}
}

// Apply evaluates every event in the batch against the drop policy rules.
// Events whose condition is breached are dropped (removed from the returned slice).
func (d *Drop) Apply(_ context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
	start := time.Now()
	filtered := make([]*model.EventWithMetadata, 0, len(events))

	for _, meta := range events {
		if d.evalChain.Run(*meta, d.cache) {
			logger.Debugf("[drop.Apply] dropping event: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s, event_timestamp_diff=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp, time.Since(meta.EventTimestamp))
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DROP_POLICY,conn_group=%s,product=%s,event_name=%s", connGroup, meta.Product, meta.EventName))
			continue
		}

		filtered = append(filtered, meta)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DROP,conn_group=%s", connGroup))

	return filtered
}
