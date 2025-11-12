package mqtt

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"context"
	"fmt"
	"time"

	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/clients/go/log"
	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/identification"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/serialization"
	"google.golang.org/protobuf/proto"
)

// Handler processes MQTT messages and passes them to the Collector.
type Handler struct {
	Collector collection.Collector
}

// MQTTHandler handles incoming MQTT messages, decodes them, records metrics,
// and sends them to the Collector.
func (h *Handler) MQTTHandler(ctx context.Context, c courier.PubSub, message *courier.Message) {
	start := time.Now()
	group := config.ServerMQTT.ConnGroup
	log.Infof("MQTT message received with content %v", message)

	var req pb.SendEventRequest
	if err := message.DecodePayload(&req); err != nil {
		h.recordMetrics("request", fmt.Sprintf("status=failed,conn_group=%s,reason=serde", group), nil)
		log.Errorf("mqtt message decoding failed: %v", err)
		return
	}

	if proto.Equal(&req, &pb.SendEventRequest{}) {
		h.recordMetrics("request", fmt.Sprintf("status=failed,conn_group=%s,reason=empty", group), nil)
		log.Errorf("mqtt request message according proto format is empty")
		return
	}

	// Debug — can be removed after E2E verification
	for _, event := range req.Events {
		log.Infof("MQTT message content post deserialization event: %v", event)
	}
	log.Infof("MQTT message request id: %v", req.ReqGuid)

	// Serialize to compute request size
	reqBytes, err := serialization.SerializeProto(&req)
	if err != nil {
		log.Errorf("mqtt message serialization failed: %v", err)
	}

	// Record all metrics via generic function
	h.recordMetrics("request", fmt.Sprintf("status=success,conn_group=%s", group), reqBytes)
	h.recordMetrics("event", fmt.Sprintf("conn_group=%s", group), req.Events)

	h.Collector.Collect(ctx, &collection.CollectRequest{
		ConnectionIdentifier: identification.Identifier{Group: group},
		TimeConsumed:         start,
		SendEventRequest:     &req,
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
		log.Errorf("unknown metricName=%s ignored", metricName)
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
		eventTags := fmt.Sprintf("%s,event_type=%s", tags, e.Type)
		metrics.Increment("events_rx_total", eventTags)
		metrics.Count("events_rx_bytes_total", len(e.EventBytes), eventTags)
	}
}
