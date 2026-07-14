package action

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/raccoon/config"
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
		if ok, resource := d.evalChain.Run(*meta, d.cache); ok {
			logger.Debugf("[drop.Apply] dropping event: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s, event_timestamp_diff=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp, time.Since(meta.EventTimestamp))
			reason := "DROP_POLICY"
			if resource == config.PolicyResourceGlobal {
				reason = "GLOBAL_DROP_POLICY"
				logger.Infof("[globaldrop.Apply] dropping event: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp)
			}

			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=%s,event_name=%s,product=%s,conn_group=%s,event_type=%s", reason, meta.EventName, meta.Product, connGroup, meta.Type))
			continue
		}

		filtered = append(filtered, meta)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DROP,conn_group=%s", connGroup))

	return filtered
}
