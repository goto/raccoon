package mqtt

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/gojek/courier-go"
	"google.golang.org/protobuf/proto"

	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/identification"
	policypkg "github.com/goto/raccoon/ingestionrule"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/serialization"
)

// Handler processes MQTT messages and passes them to the Collector.
type Handler struct {
	Collector collection.Collector
	policy    *policypkg.Service
}

// MQTTHandler handles incoming MQTT messages, decodes them, records metrics,
// and sends them to the Collector.
func (h *Handler) MQTTHandler(ctx context.Context, c courier.PubSub, message *courier.Message) {
	start := time.Now()
	connGroup, err := h.extractConnGroup(message)
	if err != nil {
		h.recordMetrics("request", fmt.Sprintf("status=failed,conn_group=unknown,reason=%v", err), nil)
		logger.Errorf("mqtt message topic format is invalid: %s", message.Topic)
	}

	var req pb.SendEventRequest
	if err := message.DecodePayload(&req); err != nil {
		h.recordMetrics("request", fmt.Sprintf("status=failed,conn_group=%s,reason=serde", connGroup), nil)
		logger.Errorf("mqtt message decoding failed: %v", err)
		return
	}

	if proto.Equal(&req, &pb.SendEventRequest{}) {
		h.recordMetrics("request", fmt.Sprintf("status=failed,conn_group=%s,reason=empty", connGroup), nil)
		logger.Errorf("mqtt request message according proto format is empty")
		return
	}

	// Serialize to compute request size
	reqBytes, err := serialization.SerializeProto(&req)
	if err != nil {
		logger.Errorf("mqtt message serialization failed: %v", err)
	}

	// Record all metrics via generic function
	h.recordMetrics("request", fmt.Sprintf("status=success,conn_group=%s", connGroup), reqBytes)
	h.recordMetrics("event", fmt.Sprintf("conn_group=%s", connGroup), req.Events)
	for _, e := range req.Events {
		logger.Debugf("[mqtt.MQTTHandler] event: event_name=%s, product=%s, type=%s, event_timestamp=%s, req_guid=%s, conn_group=%s", e.EventName, e.Product, e.Type, e.GetEventTimestamp().AsTime(), req.ReqGuid, connGroup)
	}

	eventsWithMetadata := h.policy.Apply(ctx, req.Events, connGroup)

	timing_event_received := start.Sub(req.GetSentTime().AsTime()).Milliseconds()
	metrics.Timing("event_received_duration_milliseconds", timing_event_received, fmt.Sprintf("conn_group=%s", connGroup))

	h.Collector.Collect(ctx, &collection.CollectRequest{
		ConnectionIdentifier: identification.Identifier{Group: connGroup},
		TimeConsumed:         start,
		SentTime:             req.SentTime,
		Events:               eventsWithMetadata,
		AckFunc:              nil,
	})
}

// recordMetrics is a generic entry function that routes metric recording
// based on metricName.
func (h *Handler) recordMetrics(metricName string, tags string, data any) {
	switch metricName {
	case "request":
		h.recordRequestMetrics(tags, data)
	case "event":
		h.recordEventMetrics(tags, data)
	default:
		logger.Errorf("unknown metricName=%s ignored", metricName)
	}
}

// recordRequestMetrics captures request-level metrics (success/failure, bytes).
func (h *Handler) recordRequestMetrics(tags string, data any) {
	metrics.Increment("batches_read_total", tags)

	if reqBytes, ok := data.([]byte); ok && len(reqBytes) > 0 {
		metrics.Count("request_bytes_total", len(reqBytes), tags)
	}
}

// recordEventMetrics captures per-event metrics like count and size.
func (h *Handler) recordEventMetrics(tags string, data any) {
	events, ok := data.([]*pb.Event)
	if !ok {
		return
	}

	for _, e := range events {
		if e == nil {
			continue
		}
		eventTags := fmt.Sprintf("%s,event_type=%s,protocol_type=mqtt", tags, e.Type)
		metrics.Count("events_rx_bytes_total", len(e.EventBytes), eventTags)

		newTags := fmt.Sprintf("%s,event_type=%s,app_version=%s,platform=%s,protocol_type=mqtt", tags, e.Type, e.AppVersion, e.Platform)
		metrics.Increment("events_rx_total", newTags)
	}
}

func (h *Handler) extractConnGroup(message *courier.Message) (string, error) {
	topicParts := strings.Split(message.Topic, "/")

	switch {
	case len(topicParts) == 4 && topicParts[1] == "v1":
		return topicParts[2], nil
	case len(topicParts) == 5 && topicParts[1] == "v2":
		sourceApp := topicParts[2]
		if connGroup, ok := config.ServerMQTT.ConsumerConfig.V2AppConnGroupMapping[sourceApp]; ok {
			return connGroup, nil
		}
		return "", fmt.Errorf("unexpected app name in v2 topic: %s", message.Topic)
	default:
		return "", fmt.Errorf("invalid topic format: %s", message.Topic)
	}
}
