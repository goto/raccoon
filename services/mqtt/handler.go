package mqtt

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"context"
	"fmt"
	"github.com/goto/raccoon/clients/go/log"
	"time"

	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/identification"
	"github.com/goto/raccoon/metrics"
)

// Handler processes MQTT messages and passes them to the Collector.
type Handler struct {
	Collector collection.Collector
}

// MQTTHandler handles incoming MQTT messages, decodes them, records metrics,
// and sends them to the Collector.
func (h *Handler) MQTTHandler(ctx context.Context, c courier.PubSub, message *courier.Message) {
	start := time.Now()

	identifier := identification.Identifier{
		Group: config.ServerMQTT.ConnGroup,
	}

	var req pb.SendEventRequest
	if err := message.DecodePayload(&req); err != nil {
		metrics.Increment(
			"batches_read_total",
			fmt.Sprintf("status=failed,conn_group=%s,reason=serde", identifier.Group),
		)
		log.Errorf("mqtt message decoding failed due to : %v", err)
		return
	}

	metrics.Increment(
		"batches_read_total",
		fmt.Sprintf("status=success,conn_group=%s", identifier.Group),
	)

	h.recordEventMetrics(req.Events, identifier.Group)

	h.Collector.Collect(ctx, &collection.CollectRequest{
		ConnectionIdentifier: identifier,
		TimeConsumed:         start,
		SendEventRequest:     &req,
		AckFunc:              nil,
	})
}

// recordEventMetrics updates per-event metrics like byte size and event count.
func (h *Handler) recordEventMetrics(events []*pb.Event, group string) {
	for _, e := range events {
		if e == nil {
			continue
		}
		tags := fmt.Sprintf("conn_group=%s,event_type=%s", group, e.Type)
		metrics.Count("events_rx_bytes_total", len(e.EventBytes), tags)
		metrics.Increment("events_rx_total", tags)
	}
}
