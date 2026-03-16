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

// OverrideTimestamp evaluates OVERRIDE_TIMESTAMP policies. When a matching rule's
// condition is breached, the event's Type is replaced with the override topic so
// it is routed to the correction pipeline by the normal worker.
type OverrideTimestamp struct {
	cache             *cache.Cache
	evalChain         Chain
	overrideEventType string
}

// NewOverrideTimestamp creates an OverrideTimestamp action.
// The cache must be pre-populated with OVERRIDE_TIMESTAMP rules only.
func NewOverrideTimestamp(
	c *cache.Cache,
	evalChain Chain,
	overrideEventType string,
) *OverrideTimestamp {
	return &OverrideTimestamp{
		cache:             c,
		evalChain:         evalChain,
		overrideEventType: overrideEventType,
	}
}

// Apply evaluates every event in the batch. Events whose condition is breached
// are cloned with Type set to the override topic and kept in the returned slice
// so the worker routes them to the correction pipeline. Events that do not match
// pass through unchanged.
func (o *OverrideTimestamp) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	for _, event := range events {
		meta := ExtractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
		logger.Debugf("[override_timestamp.Apply] meta: event_name=%s, product=%s, publisher=%s, topic=%s, conn_group=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.ConnGroup)
		if o.evalChain.Run(meta, o.cache) {
			logger.Infof("[override_timestamp.Apply] overriding timestamp: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s, override_type=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp, o.overrideEventType)
			event.Type = o.overrideEventType
			metrics.Increment(metricEventOverrideCount, fmt.Sprintf("reason=OVERRIDE_TIMESTAMP,event_name=%s,product=%s,publisher=%s", meta.EventName, meta.Product, meta.Publisher))
		}
	}
	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=OVERRIDE_TIMESTAMP,conn_group=%s", connGroup))
	return events
}
