package mqtt

import (
	"bytes"
	"context"
	"errors"
	"github.com/gojekfarm/xtools/xproto"
	"github.com/goto/raccoon/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io"
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/collection"
)

// --- Tests ---

func TestHandler_MQTTHandler(t *testing.T) {
	req := pb.SendEventRequest{
		ReqGuid: "test-1",
		Events:  []*pb.Event{makeEvent("click", "data123")},
	}
	reqContent, _ := serialization.SerializeProto(&req)

	tests := []struct {
		name              string
		decoder           courier.Decoder
		expectCollectCall bool
	}{
		{
			name:              "successfully decodes and collects message",
			decoder:           protoDecoder(context.Background(), bytes.NewReader(reqContent)),
			expectCollectCall: true,
		},
		{
			name:              "decode fails - should not call collector",
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
						return r != nil && r.SendEventRequest != nil
					})).
					Return(nil).
					Once()
			}

			msg := courier.NewMessageWithDecoder(tt.decoder)

			h.MQTTHandler(ctx, nil, msg)

			mockCollector.AssertExpectations(t)
		})
	}
}

func TestHandler_RecordEventMetrics(t *testing.T) {
	h := &Handler{}

	events := []*pb.Event{
		makeEvent("purchase", "xyz"),
		makeEvent("click", "data123"),
		nil, // ensure nil events are skipped
	}

	// We don’t need to verify metrics, just ensure no panics occur
	assert.NotPanics(t, func() {
		h.recordEventMetrics(events, "test-group")
	}, "recordEventMetrics should not panic")

	// Basic structural test
	assert.Equal(t, "purchase", events[0].Type)
	assert.Equal(t, []byte("xyz"), events[0].EventBytes)

	assert.Equal(t, "click", events[1].Type)
	assert.Equal(t, []byte("data123"), events[1].EventBytes)
}

func protoDecoder(ctx context.Context, r io.Reader) courier.Decoder {
	return xproto.NewDecoder(r)
}

// --- Mock Definitions ---

type mockInvalidDecoder struct {
}

func (m mockInvalidDecoder) Decode(v interface{}) error {
	return errors.New("invalid proto message")
}

// --- Helper setup ---

func makeEvent(eventType string, data string) *pb.Event {
	return &pb.Event{
		Type:       eventType,
		EventBytes: []byte(data),
	}
}
