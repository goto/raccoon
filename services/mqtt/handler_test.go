package mqtt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gojekfarm/xtools/xproto"
	"github.com/goto/raccoon/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/collection"
)

func TestHandler_MQTTHandler(t *testing.T) {
	req := pb.SendEventRequest{
		ReqGuid: "test-1",
		Events:  []*pb.Event{makeEvent("click", "data123")},
	}
	reqContent, _ := serialization.SerializeProto(&req)

	tests := []struct {
		name              string
		topic             string
		decoder           courier.Decoder
		expectCollectCall bool
		expectedGroup     string
	}{
		{
			name:              "valid mobile topic extracts user-type",
			topic:             "clickstream/v1/mobile/123",
			decoder:           protoDecoder(context.Background(), bytes.NewReader(reqContent)),
			expectCollectCall: true,
			expectedGroup:     "mobile",
		},
		{
			name:              "invalid topic - missing v1 segment",
			topic:             "clickstream/v2/mobile/123",
			decoder:           protoDecoder(context.Background(), bytes.NewReader(reqContent)),
			expectCollectCall: true, // Collects with empty group
			expectedGroup:     "",
		},
		{
			name:              "invalid topic - insufficient length",
			topic:             "clickstream/v1",
			decoder:           protoDecoder(context.Background(), bytes.NewReader(reqContent)),
			expectCollectCall: true, // Collects with empty group
			expectedGroup:     "",
		},
		{
			name:              "decode fails - should not call collector",
			topic:             "clickstream/v1/mobile/123",
			decoder:           mockInvalidDecoder{},
			expectCollectCall: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCollector := new(collection.MockCollector)
			ctx := context.Background()

			h := &Handler{
				Collector: mockCollector,
			}

			if tt.expectCollectCall {
				mockCollector.
					On("Collect", mock.Anything, mock.MatchedBy(func(r *collection.CollectRequest) bool {
						if r == nil || r.SendEventRequest == nil {
							return false
						}
						return r.ConnectionIdentifier.Group == tt.expectedGroup
					})).
					Return(nil).
					Once()
			}

			msg := courier.NewMessageWithDecoder(tt.decoder)
			msg.Topic = tt.topic

			h.MQTTHandler(ctx, nil, msg)

			mockCollector.AssertExpectations(t)
		})
	}
}

func TestHandler_RecordMetrics(t *testing.T) {
	h := &Handler{}

	events := []*pb.Event{
		makeEvent("purchase", "xyz"),
		makeEvent("click", "data123"),
		nil, // ensure nil events are skipped
	}
	reqBytes := []byte("test-req")

	tests := []struct {
		name   string
		metric string
		data   any
	}{
		{
			name:   "record request metrics with valid data",
			metric: "request",
			data:   reqBytes,
		},
		{
			name:   "record event metrics with valid data",
			metric: "event",
			data:   events,
		},
		{
			name:   "record metrics with nil data",
			metric: "event",
			data:   nil,
		},
		{
			name:   "record metrics with wrong type",
			metric: "event",
			data:   "unexpected-type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				h.recordMetrics(tt.metric, "conn_group=test-group", tt.data)
			}, "recordMetrics should not panic even with nil or unexpected type")
		})
	}
}

// Helper for decoding
func protoDecoder(ctx context.Context, r io.Reader) courier.Decoder {
	return xproto.NewDecoder(r)
}

// Mock invalid decoder
type mockInvalidDecoder struct{}

func (m mockInvalidDecoder) Decode(v interface{}) error {
	return errors.New("invalid proto message")
}

// Helper event factory
func makeEvent(eventType string, data string) *pb.Event {
	return &pb.Event{
		Type:       eventType,
		EventBytes: []byte(data),
	}
}
