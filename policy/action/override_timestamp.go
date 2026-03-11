package action

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/goto/raccoon/publisher"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// OverrideTimestamp evaluates OVERRIDE_TIMESTAMP policies. When a matching rule's
// condition is breached, the event is published to the configured override topic
// for downstream timestamp correction and removed from normal ingestion.
// Events are batched so that ProduceBulk is invoked at most once per Apply call.
type OverrideTimestamp struct {
	cache           *cache.Cache
	evalChain       Chain
	producer        publisher.KafkaProducer
	overrideTopic   string
	deliveryChannel chan kafka.Event
}

// NewOverrideTimestamp creates an OverrideTimestamp action.
// The cache must be pre-populated with OVERRIDE_TIMESTAMP rules only.
func NewOverrideTimestamp(
	c *cache.Cache,
	evalChain Chain,
	producer publisher.KafkaProducer,
	overrideTopic string,
	deliveryChannel chan kafka.Event,
) *OverrideTimestamp {
	return &OverrideTimestamp{
		cache:           c,
		evalChain:       evalChain,
		producer:        producer,
		overrideTopic:   overrideTopic,
		deliveryChannel: deliveryChannel,
	}
}

// Apply evaluates every event in the batch. Events whose override-timestamp
// condition is breached are cloned with the override topic and published in a
// single ProduceBulk call. Only events that are NOT redirected remain in the
// returned slice.
func (o *OverrideTimestamp) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	filtered := make([]*pb.Event, 0, len(events))
	var overrideEvents []*pb.Event
	var metricTags []string

	for _, event := range events {
		meta := ExtractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern)
		if o.evalChain.Run(meta, o.cache) {
			overrideEvents = append(overrideEvents, &pb.Event{
				EventBytes:     event.GetEventBytes(),
				Type:           o.overrideTopic,
				EventName:      event.GetEventName(),
				Product:        event.GetProduct(),
				EventTimestamp: event.GetEventTimestamp(),
				IsMirrored:     event.GetIsMirrored(),
			})
			metricTags = append(metricTags, fmt.Sprintf("reason=OVERRIDE_TIMESTAMP,event_name=%s,product=%s,publisher=%s", meta.EventName, meta.Product, meta.Publisher))
			continue
		}
		filtered = append(filtered, event)
	}

	if len(overrideEvents) > 0 {
		o.publish(overrideEvents, connGroup, metricTags)
	}
	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=override_timestamp,conn_group=%s", connGroup))
	return filtered
}

// publish sends overrideEvents to Kafka in a single ProduceBulk call and records
// per-event metrics. The metric name is chosen once based on whether the call
// succeeds or fails, then applied to every tag in the batch.
func (o *OverrideTimestamp) publish(overrideEvents []*pb.Event, connGroup string, metricTags []string) {
	result := "success"
	if err := o.producer.ProduceBulk(overrideEvents, connGroup, o.deliveryChannel); err != nil {
		logger.Errorf("[policy.OverrideTimestamp] failed to publish %d events to override topic %s: %v",
			len(overrideEvents), o.overrideTopic, err)
		result = "failure"
	}
	for _, tag := range metricTags {
		metrics.Increment(metricEventOverride, fmt.Sprintf("%s,result=%s", tag, result))
	}
}
