package action

import (
	"context"
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/schemaregistry"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

// Deactivate is a policy action that unconditionally drops events matching the
// configured rules. Unlike Drop, it requires no condition_type in the rule config.
type Deactivate struct {
	cache     *cache.Cache
	evalChain Chain
	stencil   schemaregistry.StencilClient
}

// NewDeactivate creates a new Deactivate action with the given cache and evaluator chain.
func NewDeactivate(c *cache.Cache, evalChain Chain, stencil schemaregistry.StencilClient) *Deactivate {
	return &Deactivate{
		cache:     c,
		evalChain: evalChain,
		stencil:   stencil,
	}
}

// Apply evaluates every event in the batch against the deactivate policy rules.
// Matching events are dropped unconditionally (removed from the returned slice).
func (d *Deactivate) Apply(_ context.Context, events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	filtered := make([]*pb.Event, 0, len(events))

	for _, event := range events {
		meta, err := extractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern, d.stencil)
		if err != nil {
			logger.Errorf("deactivate: failed to extract metadata: %v", err)
			metrics.Increment(metricNameEventDeserializationError, fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, getErrorReason(err), event.Type, event.Product, event.EventName))

			filtered = append(filtered, event)
			continue
		}

		logger.Debugf("[deactivate.Apply] meta: event_name=%s, product=%s, publisher=%s, topic=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName)

		if d.evalChain.Run(meta, d.cache) {
			logger.Infof("[deactivate.Apply] deactivating event: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp)
			metrics.Increment(metricEventLossCount, fmt.Sprintf("reason=DEACTIVATE_POLICY,event_name=%s,product=%s,publisher=%s,event_type=%s", meta.EventName, meta.Product, meta.Publisher, meta.EventType))
			continue
		}

		filtered = append(filtered, event)
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEACTIVATE,conn_group=%s", connGroup))

	return filtered
}
