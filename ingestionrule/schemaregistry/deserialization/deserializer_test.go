package deserialization

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
	"github.com/goto/raccoon/ingestionrule/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/schemaregistry/deserialization/mocks"
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

func (m *mockStencilClient) Serialize(string, interface{}) ([]byte, error)             { return nil, nil }
func (m *mockStencilClient) GetDescriptor(string) (protoreflect.MessageDescriptor, error) { return nil, nil }
func (m *mockStencilClient) Close()                                                      {}
func (m *mockStencilClient) Refresh()                                                    {}

func TestDeserializeEvents(t *testing.T) {
	metrics.SetVoid()

	// Save original config to restore later
	origProtoMapping := config.DedupCfg.ProtoClassNameMapping
	origDeserializationEnabled := config.DeserializationCfg.Enabled
	origPlatformWhitelist := config.DeserializationCfg.PlatformPublisherWhitelist
	origAppVersionWhitelist := config.DeserializationCfg.AppVersionPublisherWhitelist
	origExcludeEventTypeList := config.DeserializationCfg.ExcludeEventTypeList
	defer func() {
		config.DedupCfg.ProtoClassNameMapping = origProtoMapping
		config.DeserializationCfg.Enabled = origDeserializationEnabled
		config.DeserializationCfg.PlatformPublisherWhitelist = origPlatformWhitelist
		config.DeserializationCfg.AppVersionPublisherWhitelist = origAppVersionWhitelist
		config.DeserializationCfg.ExcludeEventTypeList = origExcludeEventTypeList
	}()

	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"click_event": "gojek.de.raccoon.Event",
	}
	config.DeserializationCfg.Enabled = true

	publisherMap := map[string]string{
		"group-1": "publisher-1",
	}

	now := time.Now().Truncate(time.Microsecond)
	tsProto := timestamppb.New(now)

	tests := []struct {
		name                 string
		events               []*pb.Event
		connGroup            string
		publisherMap         map[string]string
		topicFormat          string
		platformWhitelist    []string
		appVersionWhitelist  []string
		excludeEventTypeList []string
		parseFunc            func(string, []byte) (protoreflect.ProtoMessage, error)
		verify               func(t *testing.T, results []*model.EventWithMetadata)
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
		{
			name: "conditional deserialization - neither platform nor app_version whitelisted",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
					Platform:       pb.Platform_PLATFORM_ANDROID,
					AppVersion:     "1.0.0-envelope",
				},
			},
			connGroup:           "group-1",
			publisherMap:        publisherMap,
			topicFormat:         "topic-%s",
			platformWhitelist:   []string{"other-publisher"},
			appVersionWhitelist: []string{"other-publisher"},
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
				assert.Equal(t, pb.Platform_PLATFORM_ANDROID.String(), res.Platform)
				assert.Equal(t, "1.0.0-envelope", res.AppVersion)
			},
		},
		{
			name: "conditional deserialization - platform whitelisted, app_version not whitelisted",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
					Platform:       pb.Platform_PLATFORM_ANDROID,
					AppVersion:     "1.0.0-envelope",
				},
			},
			connGroup:           "group-1",
			publisherMap:        publisherMap,
			topicFormat:         "topic-%s",
			platformWhitelist:   []string{"publisher-1"},
			appVersionWhitelist: []string{"other-publisher"},
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
				assert.Equal(t, "1.0.0-envelope", res.AppVersion)
			},
		},
		{
			name: "event type is in exclude list",
			events: []*pb.Event{
				{
					Type:           "click_event",
					Product:        "my_product",
					EventName:      "test_event",
					EventBytes:     []byte("event-bytes-data"),
					EventTimestamp: tsProto,
				},
			},
			connGroup:            "group-1",
			publisherMap:         publisherMap,
			topicFormat:          "topic-%s",
			excludeEventTypeList: []string{"click_event"},
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				t.Error("parse should not be called because event type is excluded")
				return nil, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				// It should return basic/base metadata because we skipped enrichment/deserialization.
				assert.Equal(t, "publisher-1", res.Publisher)
				assert.Equal(t, "myproduct", res.Product) // Normalized base product name
				assert.Equal(t, "test_event", res.EventName)
				assert.Empty(t, res.EventGUID)
			},
		},
		{
			name: "successful deserialization with empty event_guid",
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
				return &testpb.Event{
					EventName:      "inner_event_name",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta: &testpb.Meta{
						EventGuid: "", // empty field
					},
				}, nil
			},
			verify: func(t *testing.T, results []*model.EventWithMetadata) {
				require.Len(t, results, 1)
				res := results[0]
				assert.Equal(t, "inner_event_name", res.EventName)
				assert.Equal(t, "generic", res.Product)
				assert.True(t, res.EventTimestamp.Equal(now.UTC()))
				assert.Equal(t, "", res.EventGUID) // should be empty, no error
				assert.Equal(t, "publisher-1", res.Publisher)
				assert.Equal(t, "topic-click_event", res.TopicName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.DeserializationCfg.PlatformPublisherWhitelist = tt.platformWhitelist
			config.DeserializationCfg.AppVersionPublisherWhitelist = tt.appVersionWhitelist
			config.DeserializationCfg.ExcludeEventTypeList = tt.excludeEventTypeList
			clientMock := &mockStencilClient{parseFunc: tt.parseFunc}
			stencilClient := schemaregistry.StencilClient{Client: clientMock}

			d := NewDeserializer(stencilClient, nil)
			results := d.Deserialize(tt.events, tt.connGroup, tt.publisherMap, tt.topicFormat)
			tt.verify(t, results)
		})
	}
}

func TestDeserializer_FallbackOrder(t *testing.T) {
	metrics.SetVoid()

	// Save original config to restore later
	origProtoMapping := config.DedupCfg.ProtoClassNameMapping
	origDeserializationEnabled := config.DeserializationCfg.Enabled
	defer func() {
		config.DedupCfg.ProtoClassNameMapping = origProtoMapping
		config.DeserializationCfg.Enabled = origDeserializationEnabled
	}()

	config.DeserializationCfg.Enabled = true

	now := time.Now().Truncate(time.Microsecond)
	tsProto := timestamppb.New(now)

	event := &pb.Event{
		Type:           "click_event",
		Product:        "my_product",
		EventName:      "test_event",
		EventBytes:     []byte("event-bytes-data"),
		EventTimestamp: tsProto,
	}

	t.Run("1. found in cache", func(t *testing.T) {
		config.DedupCfg.ProtoClassNameMapping = map[string]string{} // empty static config

		mockCache := mocks.NewSchemaRegistryCache(t)
		mockCache.EXPECT().Get("topic-click_event").Return("class.from.Cache", true)

		clientMock := &mockStencilClient{
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "class.from.Cache", class)
				return &testpb.Event{
					EventName:      "inner_event",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta:           &testpb.Meta{EventGuid: "guid-1"},
				}, nil
			},
		}

		d := NewDeserializer(schemaregistry.StencilClient{Client: clientMock}, mockCache)
		results := d.Deserialize([]*pb.Event{event}, "group-1", map[string]string{}, "topic-%s")
		require.Len(t, results, 1)
		assert.Equal(t, "guid-1", results[0].EventGUID)
	})

	t.Run("2. fallback to static mapping when not in cache", func(t *testing.T) {
		config.DedupCfg.ProtoClassNameMapping = map[string]string{
			"click_event": "class.from.Static",
		}

		mockCache := mocks.NewSchemaRegistryCache(t)
		mockCache.EXPECT().Get("topic-click_event").Return("", false)

		clientMock := &mockStencilClient{
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "class.from.Static", class)
				return &testpb.Event{
					EventName:      "inner_event",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta:           &testpb.Meta{EventGuid: "guid-2"},
				}, nil
			},
		}

		d := NewDeserializer(schemaregistry.StencilClient{Client: clientMock}, mockCache)
		results := d.Deserialize([]*pb.Event{event}, "group-1", map[string]string{}, "topic-%s")
		require.Len(t, results, 1)
		assert.Equal(t, "guid-2", results[0].EventGUID)
	})

	t.Run("3. error when found in neither cache nor static mapping", func(t *testing.T) {
		config.DedupCfg.ProtoClassNameMapping = map[string]string{}

		mockCache := mocks.NewSchemaRegistryCache(t)
		mockCache.EXPECT().Get("topic-click_event").Return("", false)

		d := NewDeserializer(schemaregistry.StencilClient{Client: &mockStencilClient{}}, mockCache)
		results := d.Deserialize([]*pb.Event{event}, "group-1", map[string]string{}, "topic-%s")
		require.Len(t, results, 1)
		assert.Empty(t, results[0].EventGUID)
	})
}

func TestDeserializer_NilCache(t *testing.T) {
	metrics.SetVoid()

	// Save original config to restore later
	origProtoMapping := config.DedupCfg.ProtoClassNameMapping
	origDeserializationEnabled := config.DeserializationCfg.Enabled
	defer func() {
		config.DedupCfg.ProtoClassNameMapping = origProtoMapping
		config.DeserializationCfg.Enabled = origDeserializationEnabled
	}()

	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"click_event": "class.from.Static",
	}
	config.DeserializationCfg.Enabled = true

	now := time.Now().Truncate(time.Microsecond)
	tsProto := timestamppb.New(now)

	event := &pb.Event{
		Type:           "click_event",
		Product:        "my_product",
		EventName:      "test_event",
		EventBytes:     []byte("event-bytes-data"),
		EventTimestamp: tsProto,
	}

	t.Run("typed nil pointer cache", func(t *testing.T) {
		var cache *SchemaCache = nil
		clientMock := &mockStencilClient{
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "class.from.Static", class)
				return &testpb.Event{
					EventName:      "inner_event",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta:           &testpb.Meta{EventGuid: "guid-typed-nil"},
				}, nil
			},
		}

		// This passes `cache` which is (*SchemaCache)(nil) wrapped in SchemaRegistryCache.
		// It shouldn't panic because:
		// 1. SchemaCache.Get has a nil check.
		// 2. It fallbacks to the static mapping.
		d := NewDeserializer(schemaregistry.StencilClient{Client: clientMock}, cache)
		results := d.Deserialize([]*pb.Event{event}, "group-1", map[string]string{}, "topic-%s")
		require.Len(t, results, 1)
		assert.Equal(t, "guid-typed-nil", results[0].EventGUID)
	})

	t.Run("nil interface cache", func(t *testing.T) {
		var cache SchemaRegistryCache = nil
		clientMock := &mockStencilClient{
			parseFunc: func(class string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "class.from.Static", class)
				return &testpb.Event{
					EventName:      "inner_event",
					Product:        testpb.Product_Generic,
					EventTimestamp: tsProto,
					Meta:           &testpb.Meta{EventGuid: "guid-nil-interface"},
				}, nil
			},
		}

		d := NewDeserializer(schemaregistry.StencilClient{Client: clientMock}, cache)
		results := d.Deserialize([]*pb.Event{event}, "group-1", map[string]string{}, "topic-%s")
		require.Len(t, results, 1)
		assert.Equal(t, "guid-nil-interface", results[0].EventGUID)
	})
}

