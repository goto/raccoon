package action

import (
	"fmt"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/goto/raccoon/publisher"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	metricRedirectedTotal     = "policy_events_redirected_total"
	metricRedirectFailedTotal = "policy_events_redirect_failed_total"
)

// OverrideTimestamp evaluates OVERRIDE_TIMESTAMP policies. When a matching rule's
// timestamp threshold is breached, the event is published to the configured override
// topic for downstream timestamp correction and removed from normal ingestion.
type OverrideTimestamp struct {
	cache           *cache.Cache
	evalChain       Chain
	producer        publisher.KafkaProducer
	overrideTopic   string
	deliveryChannel chan kafka.Event
}

// NewOverrideTimestamp creates an OverrideTimestamp action.
// The cache must be pre-populated with OVERRIDE_TIMESTAMP rules only.
func NewOverrideTimestamp(c *cache.Cache, evalChain Chain, producer publisher.KafkaProducer, overrideTopic string, deliveryChannel chan kafka.Event) *OverrideTimestamp {
	return &OverrideTimestamp{
		cache:           c,
		evalChain:       evalChain,
		producer:        producer,
		overrideTopic:   overrideTopic,
		deliveryChannel: deliveryChannel,
	}
}

// Process checks whether an OVERRIDE_TIMESTAMP policy applies to the event.
// Returns (true, eval.OutcomeRedirected) when the event is forwarded to the override topic.
// Returns (false, eval.OutcomePassthrough) when no override policy matches or threshold is not breached.
func (o *OverrideTimestamp) Process(event *pb.Event, meta eval.EventMetadata) (bool, Outcome) {
	if !o.evalChain.Run(meta, o.cache) {
		return false, OutcomePassthrough
	}

	overrideEvent := &pb.Event{
		EventBytes:     event.GetEventBytes(),
		Type:           o.overrideTopic,
		EventName:      event.GetEventName(),
		Product:        event.GetProduct(),
		EventTimestamp: event.GetEventTimestamp(),
		IsMirrored:     event.GetIsMirrored(),
	}

	if err := o.producer.ProduceBulk([]*pb.Event{overrideEvent}, meta.ConnGroup, o.deliveryChannel); err != nil {
		logger.Errorf("[policy.OverrideTimestamp] failed to publish to override topic %s: %v", o.overrideTopic, err)
		metrics.Increment(metricRedirectFailedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	} else {
		metrics.Increment(metricRedirectedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	}

	return true, OutcomeRedirected
}
