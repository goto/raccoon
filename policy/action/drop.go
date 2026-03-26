package action

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

// Drop is a policy action that drops events matching the configured rules.
type Drop struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewDrop creates a new Drop action with the given cache and evaluator chain.
func NewDrop(c *cache.Cache, evalChain Chain) *Drop {
	return &Drop{cache: c, evalChain: evalChain}
}

// Apply evaluates every event in the batch against the drop policy rules.
// Events whose condition is breached are dropped (removed from the returned slice).
func (d *Drop) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	filtered := make([]*pb.Event, 0, len(events))
	for _, event := range events {
		meta := ExtractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
		logger.Debugf("[drop.Apply] meta: event_name=%s, product=%s, publisher=%s, topic=%s, conn_group=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.ConnGroup)
		if d.evalChain.Run(meta, d.cache) {
			logger.Infof("[drop.Apply] dropping event: event_name=%s, product=%s, publisher=%s, conn_group=%s, topic=%s, event_timestamp=%s, event_timestamp_diff=%s", meta.EventName, meta.Product, meta.Publisher, meta.ConnGroup, meta.TopicName, meta.EventTimestamp, time.Since(meta.EventTimestamp))
			metrics.Increment(metricEventLossCount, fmt.Sprintf("reason=DROP_POLICY,event_name=%s,product=%s,conn_group=%s", meta.EventName, meta.Product, meta.ConnGroup))
			continue
		} else {
			logger.Infof("[drop.Skip] keeping event: event_name=%s, product=%s, publisher=%s, conn_group=%s, topic=%s, event_timestamp=%s, event_timestamp_diff=%s", meta.EventName, meta.Product, meta.Publisher, meta.ConnGroup, meta.TopicName, meta.EventTimestamp, time.Since(meta.EventTimestamp))
		}
		filtered = append(filtered, event)
	}
	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DROP,conn_group=%s", connGroup))
	return filtered
}
