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

// Deactivate is a policy action that unconditionally drops events matching the
// configured rules. Unlike Drop, it requires no condition_type in the rule config.
type Deactivate struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewDeactivate creates a new Deactivate action with the given cache and evaluator chain.
func NewDeactivate(c *cache.Cache, evalChain Chain) *Deactivate {
	return &Deactivate{cache: c, evalChain: evalChain}
}

// Apply evaluates every event in the batch against the deactivate policy rules.
// Matching events are dropped unconditionally (removed from the returned slice).
func (d *Deactivate) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	filtered := make([]*pb.Event, 0, len(events))
	for _, event := range events {
		meta := ExtractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
		logger.Debugf("[deactivate.Apply] meta: event_name=%s, product=%s, publisher=%s, topic=%s, conn_group=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.ConnGroup)
		if d.evalChain.Run(meta, d.cache) {
			logger.Infof("[deactivate.Apply] deactivating event: event_name=%s, product=%s, publisher=%s, conn_group=%s, topic=%s, event_timestamp=%s", meta.EventName, meta.Product, meta.Publisher, meta.ConnGroup, meta.TopicName, meta.EventTimestamp)
			metrics.Increment(metricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,event_name=%s,product=%s,conn_group=%s", meta.EventName, meta.Product, meta.ConnGroup))
			continue
		}
		filtered = append(filtered, event)
	}
	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEACTIVATE,conn_group=%s", connGroup))
	return filtered
}
