package schemaregistry

import (
	"errors"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	testpb "github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil/testpb"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

type mockStencilClient struct {
	parseFunc func(string, []byte) (protoreflect.ProtoMessage, error)
}

func (m *mockStencilClient) Parse(className string, data []byte) (protoreflect.ProtoMessage, error) {
	if m.parseFunc != nil {
		return m.parseFunc(className, data)
	}
	return nil, errors.New("parse not implemented")
}

func (m *mockStencilClient) Serialize(string, interface{}) ([]byte, error) { return nil, nil }
func (m *mockStencilClient) GetDescriptor(string) (protoreflect.MessageDescriptor, error) { return nil, nil }
func (m *mockStencilClient) Close()                                                        {}
func (m *mockStencilClient) Refresh()                                                      {}

func TestDeserializeEvents(t *testing.T) {
	metrics.SetVoid()

	// Save original config to restore later
	origProtoMapping := config.DedupCfg.ProtoClassNameMapping
	defer func() {
		config.DedupCfg.ProtoClassNameMapping = origProtoMapping
	}()

	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"click_event": "gojek.de.raccoon.Event",
	}

	publisherMap := map[string]string{
		"group-1": "publisher-1",
	}

	now := time.Now().Truncate(time.Microsecond)
	tsProto := timestamppb.New(now)

	tests := []struct {
		name         string
		events       []*pb.Event
		connGroup    string
		publisherMap map[string]string
		topicFormat  string
		parseFunc    func(string, []byte) (protoreflect.ProtoMessage, error)
		verify       func(t *testing.T, results []*model.EventWithMetadata)
	}{
		{
			name: "successful deserialization",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "gojek.de.raccoon.Event", class)
				assert.Equal(t, []byte("event-bytes-data"), data)
				return &testpb.Event{
					EventName:      "inner_event_name",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta: &testpb.Meta{
						EventGuid: "some-guid-123",
					},
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "inner_event_name", res.EventName)
				assert.Equal(t, "generic", res.Product)
				assert.True(t, res.EventTimestamp.Equal(now.UTC()))
				assert.Equal(t, "some-guid-123", res.EventGUID)
				assert.Equal(t, "publisher-1", res.Publisher)
				assert.Equal(t, "topic-click_event", res.TopicName)
			},
		},
		{
			name: "publisher fallback when mapping does not exist",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
				},
			},
			connGroup:    "unknown-group",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				return &testpb.Event{
					EventName:      "inner_event_name",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta: &testpb.Meta{
						EventGuid: "some-guid-123",
					},
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "unknown-group", res.Publisher)
			},
		},
		{
			name: "error - proto class not found in mapping",
			events: []*pb.Event{
				{
					Type:       "unknown_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc:    nil,
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				// The function still returns a basic metadata object but with base fields, since enrich failed
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
				assert.Equal(t, "myproduct", res.Product) // base metadata normalization
				assert.Equal(t, "test_event", res.EventName)
			},
		},
		{
			name: "error - stencil parse fails",
			events: []*pb.Event{
				{
					Type:       "click_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				return nil, errors.New("stencil parse error")
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
				assert.Equal(t, "myproduct", res.Product)
			},
		},
		{
			name: "error - event_name field invalid/missing",
			events: []*pb.Event{
				{
					Type:       "click_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				// Return a message structure that causes getStringField to fail
				// testpb.Event has EventName (string) - so if we don't set it, it's just empty string which is fine, but to trigger error,
				// wait, let's see if we can trigger getStringField error.
				// In deserializer.go: getStringField gets "event_name" from message.
				// We can return a message type that does not have "event_name" field, like testpb.Meta
				return &testpb.Meta{EventGuid: "some-guid"}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
			},
		},
		{
			name: "error - product field invalid/missing",
			events: []*pb.Event{
				{
					Type:       "click_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				// return a message where product is a string field, causing GetEnumStringValue to error out
				return &testpb.InvalidProductEvent{
					EventName: "some-name",
					Product:   "not-an-enum",
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
			},
		},
		{
			name: "error - timestamp field invalid type",
			events: []*pb.Event{
				{
					Type:       "click_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				// return a message where event_timestamp is a string field, causing GetTimestampFieldValue to error out
				return &testpb.InvalidTimestampEvent{
					EventName:      "some-name",
					Product:        testpb.Product_Generic,
					EventTimestamp: "not-a-timestamp-proto-message",
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
			},
		},
		{
			name: "error - event_name string cast fails",
			events: []*pb.Event{
				{
					Type:       "click_event",
					Product:    "my_product",
					EventName:  "test_event",
					EventBytes: []byte("event-bytes-data"),
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				// return a message where event_name is a repeated string, causing getStringField's cast.ToStringE to error out
				return &testpb.InvalidStringCastEvent{
					EventName: []string{"value1", "value2"},
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
			},
		},
		{
			name: "error - event_guid field invalid/missing",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
				},
			},
			connGroup:    "group-1",
			publisherMap: publisherMap,
			topicFormat:  "topic-%s",
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				// return an event where Meta is nil, causing getStringField for meta.event_guid to fail
				return &testpb.Event{
					EventName:      "inner_event_name",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta:           nil,
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "publisher-1", res.Publisher)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientMock := &mockStencilClient{parseFunc: tt.parseFunc}
			stencilClient := StencilClient{Client: clientMock}

			results := DeserializeEvents(tt.events, tt.connGroup, tt.publisherMap, tt.topicFormat, stencilClient)
			tt.verify(t, results)
		})
	}
}
