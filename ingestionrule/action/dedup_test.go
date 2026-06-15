package action_test

import (
	"context"
	"errors"
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/action/mocks"
)

func TestDedup_Apply_NilChecker(t *testing.T) {
	// Case 1: d is nil
	var d *action.Dedup
	events := []*pb.Event{{Type: "click"}}
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))

	// Case 2: d is not nil, but checker is nil
	d = action.NewDedup(schemaregistry.StencilClient{}, nil)
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))
}

func TestDedup_Apply_BypassDeduplicationWhenNotWhitelisted(t *testing.T) {
	// Configure whitelist to exclude group-1.
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"group-whitelisted": {},
	}

	mc := mocks.NewDuplicateChecker(t)
	d := action.NewDedup(schemaregistry.StencilClient{}, mc)
	events := []*pb.Event{{Type: "click"}}
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))
}

func TestDedup_Apply_DeduplicationWorkflow(t *testing.T) {
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"customer": {},
	}
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}
	config.PolicyCfg.PublisherMapping = map[string]string{
		"customer": "customer-publisher",
	}

	// 1. Success case: event is not a duplicate.
	t.Run("EventNotDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
			},
		}).Return([]bool{false}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-1",
					},
				},
			},
		}

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "ClickEventProto", className)
				assert.Equal(t, []byte("event-payload"), data)
				return parsedMsg, nil
			},
		}

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{
				Type:       "component",
				EventBytes: []byte("event-payload"),
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1)
	})

	// 2. Duplicate case: event is already in cache.
	t.Run("EventDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
			},
		}).Return([]bool{true}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-1",
					},
				},
			},
		}

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{
				Type:       "component",
				EventBytes: []byte("event-payload"),
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Empty(t, res) // Dropped
	})

	// 3. Batch with multiple duplicates within a single event slice.
	t.Run("BatchWithMultipleDuplicates", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)

		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{Publisher: "customer-publisher", EventGUID: "guid-1"},
			{Publisher: "customer-publisher", EventGUID: "guid-2"},
			{Publisher: "customer-publisher", EventGUID: "guid-3"},
		}).Return([]bool{false, true, true}, nil) // 1 unique, 2 duplicates

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				idSuffix := string(data)
				return &mockMessage{
					fields: map[string]any{
						"meta": &mockMessage{
							fields: map[string]any{
								"event_guid": "guid-" + idSuffix,
							},
						},
					},
				}, nil
			},
		}

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{Type: "component", EventBytes: []byte("1")}, // Will be marked false (unique)
			{Type: "component", EventBytes: []byte("2")}, // Will be marked true (duplicate)
			{Type: "component", EventBytes: []byte("3")}, // Will be marked true (duplicate)
		}

		res := d.Apply(context.Background(), events, "customer")

		// Assert that the 2 duplicates were dropped, leaving exactly 1 event
		assert.Len(t, res, 1)
		// Assert that the surviving event is the correct one (the first one)
		assert.Equal(t, []byte("1"), res[0].EventBytes)
	})

	// 4. Redis error case: fails open.
	t.Run("RedisErrorFailsOpen", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
			},
		}).Return(nil, errors.New("redis error"))

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-1",
					},
				},
			},
		}

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{
				Type:       "component",
				EventBytes: []byte("event-payload"),
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1) // Bypassed and allowed through
	})

	// 5. Conversion case: identifier fields are not strings but can be converted.
	t.Run("EventWithNonStringIdentifiers", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "789",
			},
		}).Return([]bool{false}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": []byte("789"),
					},
				},
			},
		}

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{
				Type:       "component",
				EventBytes: []byte("event-payload"),
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1)
	})
}

func TestDedup_Apply_ErrorsAndBypasses(t *testing.T) {
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"customer": {},
	}
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}
	config.PolicyCfg.PublisherMapping = map[string]string{
		"customer": "customer-publisher",
	}

	// 1. Proto class not found
	t.Run("ProtoClassNotFound", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		d := action.NewDedup(
			schemaregistry.StencilClient{},
			mc,
		)
		events := []*pb.Event{
			{
				Type: "unknown-component",
			},
		}
		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Fails open
	})

	// 2. Stencil parse error
	t.Run("StencilParseError", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return nil, errors.New("parse error")
			},
		}
		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)
		events := []*pb.Event{
			{
				Type: "component",
			},
		}
		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Fails open
	})

	// 3. EventGUID field not found
	t.Run("EventGUIDNotFound", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{},
		}
		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}
		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)
		events := []*pb.Event{
			{
				Type: "component",
			},
		}
		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Fails open
	})

	// 4. EventGUID field not convertible
	t.Run("EventGUIDNotConvertible", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": dummyList{}, // dummyList is not convertible to string
					},
				},
			},
		}
		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}
		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)
		events := []*pb.Event{
			{
				Type: "component",
			},
		}
		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Fails open
	})

	// 5. Empty metadata fields (empty EventGUID)
	t.Run("EmptyEventGUID", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "",
					},
				},
			},
		}
		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}
		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)
		events := []*pb.Event{
			{
				Type: "component",
			},
		}
		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Should fail open and return the event
	})
}

type dummyList struct {
	protoreflect.List
}

type mockStencilClient struct {
	parseFunc func(className string, data []byte) (protoreflect.ProtoMessage, error)
}

func (m *mockStencilClient) Parse(className string, data []byte) (protoreflect.ProtoMessage, error) {
	if m.parseFunc != nil {
		return m.parseFunc(className, data)
	}
	return nil, nil
}

func (m *mockStencilClient) Serialize(className string, data interface{}) ([]byte, error) {
	return nil, nil
}

func (m *mockStencilClient) GetDescriptor(className string) (protoreflect.MessageDescriptor, error) {
	return nil, nil
}

func (m *mockStencilClient) Close() {}

func (m *mockStencilClient) Refresh() {}

type mockMessage struct {
	fields   map[string]any
	fullName protoreflect.FullName
}

func (m *mockMessage) ProtoReflect() protoreflect.Message {
	return m
}

func (m *mockMessage) Descriptor() protoreflect.MessageDescriptor {
	return &mockMessageDescriptor{msg: m, fullName: m.fullName}
}
func (m *mockMessage) Type() protoreflect.MessageType                                    { return nil }
func (m *mockMessage) New() protoreflect.Message                                         { return nil }
func (m *mockMessage) Interface() protoreflect.ProtoMessage                              { return m }
func (m *mockMessage) Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {}
func (m *mockMessage) Has(protoreflect.FieldDescriptor) bool                             { return true }
func (m *mockMessage) Clear(protoreflect.FieldDescriptor)                                {}
func (m *mockMessage) Get(fd protoreflect.FieldDescriptor) protoreflect.Value {
	name := string(fd.Name())
	val := m.fields[name]
	if subMsg, ok := val.(*mockMessage); ok {
		return protoreflect.ValueOfMessage(subMsg)
	}
	return protoreflect.ValueOf(val)
}
func (m *mockMessage) Set(protoreflect.FieldDescriptor, protoreflect.Value) {}
func (m *mockMessage) Mutable(protoreflect.FieldDescriptor) protoreflect.Value {
	return protoreflect.Value{}
}
func (m *mockMessage) NewField(protoreflect.FieldDescriptor) protoreflect.Value {
	return protoreflect.Value{}
}
func (m *mockMessage) WhichOneof(protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	return nil
}
func (m *mockMessage) GetUnknown() protoreflect.RawFields { return nil }
func (m *mockMessage) SetUnknown(protoreflect.RawFields)  {}
func (m *mockMessage) IsValid() bool                      { return true }
func (m *mockMessage) ProtoMethods() *protoiface.Methods  { return nil }

type mockMessageDescriptor struct {
	protoreflect.MessageDescriptor
	msg      *mockMessage
	fullName protoreflect.FullName
}

func (d *mockMessageDescriptor) FullName() protoreflect.FullName {
	if d.fullName != "" {
		return d.fullName
	}
	return "mock.Message"
}

func (d *mockMessageDescriptor) Fields() protoreflect.FieldDescriptors {
	return &mockFieldDescriptors{msg: d.msg}
}

type mockFieldDescriptors struct {
	protoreflect.FieldDescriptors
	msg *mockMessage
}

func (fds *mockFieldDescriptors) ByName(name protoreflect.Name) protoreflect.FieldDescriptor {
	val, ok := fds.msg.fields[string(name)]
	if !ok {
		return nil
	}
	var kind protoreflect.Kind
	if _, ok := val.(*mockMessage); ok {
		kind = protoreflect.MessageKind
	} else if _, ok := val.(int64); ok {
		kind = protoreflect.Int64Kind
	} else if _, ok := val.(int32); ok {
		kind = protoreflect.Int32Kind
	} else {
		kind = protoreflect.StringKind
	}
	return &mockFieldDescriptor{name: name, kind: kind}
}

type mockFieldDescriptor struct {
	protoreflect.FieldDescriptor
	name protoreflect.Name
	kind protoreflect.Kind
}

func (fd *mockFieldDescriptor) Name() protoreflect.Name {
	return fd.name
}

func (fd *mockFieldDescriptor) Kind() protoreflect.Kind {
	return fd.kind
}
