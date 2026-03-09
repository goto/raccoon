package policy

import (
	"fmt"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/publisher"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// OverrideTimestampHandler evaluates OVERRIDE_TIMESTAMP policies. When a matching
// policy is breached, the event is published to the configured override topic
// (e.g. clickstream-invalid-et-log) for downstream timestamp correction, and
// removed from the normal ingestion path.
type OverrideTimestampHandler struct {
	Producer        publisher.KafkaProducer
	OverrideTopic   string
	DeliveryChannel chan kafka.Event
}

// Handle checks if an OVERRIDE_TIMESTAMP policy applies to the event.
// Returns (true, OutcomeRedirected) when the event is forwarded to the override topic.
// Returns (false, _) when no override policy applies.
func (o *OverrideTimestampHandler) Handle(event *pb.Event, meta EventMetadata, cache *Cache, evalChain Chain) (bool, Outcome) {
	if !evalChain.Run(meta, cache, ActionOverrideTimestamp) {
		return false, OutcomePassthrough
	}

	// Publish a single-event batch to the override topic.
	// We reuse the same pb.Event schema but set its Type to the override routing type.
	overrideEvent := &pb.Event{
		EventBytes:     event.GetEventBytes(),
		Type:           o.OverrideTopic,
		EventName:      event.GetEventName(),
		Product:        event.GetProduct(),
		EventTimestamp: event.GetEventTimestamp(),
		IsMirrored:     event.GetIsMirrored(),
	}

	err := o.Producer.ProduceBulk([]*pb.Event{overrideEvent}, meta.ConnGroup, o.DeliveryChannel)
	if err != nil {
		logger.Errorf("[policy.OverrideTimestampHandler] failed to publish event to override topic %s: %v", o.OverrideTopic, err)
		// Metric for publish failure — event is still removed from normal path to avoid double ingestion.
		metrics.Increment(MetricRedirectFailedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	} else {
		metrics.Increment(MetricRedirectedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	}

	return true, OutcomeRedirected
}
