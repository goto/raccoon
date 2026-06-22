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

// Deactivate is a policy action that unconditionally drops events matching the
// configured rules. Unlike Drop, it requires no condition_type in the rule config.
type Deactivate struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewDeactivate creates a new Deactivate action with the given cache and evaluator chain.
func NewDeactivate(c *cache.Cache, evalChain Chain) *Deactivate {
	return &Deactivate{
		cache:     c,
		evalChain: evalChain,
	}
}

// Apply evaluates every event in the batch against the deactivate policy rules.
// Matching events are dropped unconditionally (removed from the returned slice).
func (d *Deactivate) Apply(_ context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
	start := time.Now()
	filtered := make([]*model.EventWithMetadata, 0, len(events))

	for _, meta := range events {
		if d.evalChain.Run(*meta, d.cache) {
			logger.Debugf("[deactivate.Apply] deactivating event: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp)
<<<<<<< HEAD
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, meta.Type))
=======
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, meta.Event.GetType()))
>>>>>>> 8f1245b (chore: adjust the event type)
			continue
		}

		filtered = append(filtered, meta)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEACTIVATE,conn_group=%s", connGroup))

	return filtered
}
