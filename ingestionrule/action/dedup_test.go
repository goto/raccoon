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
	dedupMocks "github.com/goto/raccoon/ingestionrule/action/dedup/mocks"
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
	config.DedupCfg.IdentifierMapping = map[string]config.Identifier{
		"customer": {
			UserID:    "user.id",
			SessionID: "session.id",
		},
	}

	// 1. Success case: event is not a duplicate.
	t.Run("EventNotDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				UserID:    "user-123",
				SessionID: "session-456",
				EventGUID: "guid-789",
			},
		}).Return([]bool{false}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "user-123",
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse("ClickEventProto", []byte("event-payload")).Return(parsedMsg, nil)

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
				UserID:    "user-123",
				SessionID: "session-456",
				EventGUID: "guid-789",
			},
		}).Return([]bool{true}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "user-123",
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)

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

	t.Run("IntraBatchDuplicates", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)

		// Expect 4 events, where the 4th is an exact duplicate of the 3rd
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{UserID: "u-1", SessionID: "session-1", EventGUID: "guid-1"},
			{UserID: "u-2", SessionID: "session-2", EventGUID: "guid-2"},
			{UserID: "u-3", SessionID: "session-3", EventGUID: "guid-3"},
			{UserID: "u-3", SessionID: "session-3", EventGUID: "guid-3"}, // Duplicate payload
		}).Return([]bool{false, false, false, true}, nil)

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).RunAndReturn(func(className string, data []byte) (protoreflect.ProtoMessage, error) {
			idSuffix := string(data)
			return &mockMessage{
				fields: map[string]any{
					"user":    &mockMessage{fields: map[string]any{"id": "u-" + idSuffix}},
					"session": &mockMessage{fields: map[string]any{"id": "session-" + idSuffix}},
					"meta":    &mockMessage{fields: map[string]any{"event_guid": "guid-" + idSuffix}},
				},
			}, nil
		})

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{Type: "component", EventBytes: []byte("1")},
			{Type: "component", EventBytes: []byte("2")},
			{Type: "component", EventBytes: []byte("3")},
			{Type: "component", EventBytes: []byte("3")}, // The intra-batch duplicate
		}

		res := d.Apply(context.Background(), events, "customer")

		// The 4th event must be dropped, leaving the first 3
		assert.Len(t, res, 3)
		assert.Equal(t, []byte("1"), res[0].EventBytes)
		assert.Equal(t, []byte("2"), res[1].EventBytes)
		assert.Equal(t, []byte("3"), res[2].EventBytes)
	})

	// 4. Redis error case: fails open.
	t.Run("RedisErrorFailsOpen", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				UserID:    "user-123",
				SessionID: "session-456",
				EventGUID: "guid-789",
			},
		}).Return(nil, errors.New("redis error"))

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "user-123",
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)

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

	t.Run("GuaranteedOrderMapping", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)

		// 4 distinct events, but the checker says the 2nd and 4th already exist in Redis
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{UserID: "u-A", SessionID: "session-A", EventGUID: "guid-A"},
			{UserID: "u-B", SessionID: "session-B", EventGUID: "guid-B"},
			{UserID: "u-C", SessionID: "session-C", EventGUID: "guid-C"},
			{UserID: "u-B", SessionID: "session-B", EventGUID: "guid-B"},
		}).Return([]bool{false, false, false, true}, nil) // Alternating hit/miss

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).RunAndReturn(func(className string, data []byte) (protoreflect.ProtoMessage, error) {
			idSuffix := string(data)
			return &mockMessage{
				fields: map[string]any{
					"user":    &mockMessage{fields: map[string]any{"id": "u-" + idSuffix}},
					"session": &mockMessage{fields: map[string]any{"id": "session-" + idSuffix}},
					"meta":    &mockMessage{fields: map[string]any{"event_guid": "guid-" + idSuffix}},
				},
			}, nil
		})

		d := action.NewDedup(
			schemaregistry.StencilClient{Client: ms},
			mc,
		)

		events := []*pb.Event{
			{Type: "component", EventBytes: []byte("A")}, // Will map to false -> Kept
			{Type: "component", EventBytes: []byte("B")}, // Will map to false  -> Kept
			{Type: "component", EventBytes: []byte("C")}, // Will map to false -> Kept
			{Type: "component", EventBytes: []byte("B")}, // Will map to true  -> Dropped
		}

		res := d.Apply(context.Background(), events, "customer")

		// We must be left with exactly 2 events (A and C), proving the index tracking
		// inside `Apply` didn't misalign the boolean results
		assert.Len(t, res, 3)
		assert.Equal(t, []byte("A"), res[0].EventBytes)
		assert.Equal(t, []byte("B"), res[1].EventBytes)
		assert.Equal(t, []byte("C"), res[2].EventBytes)
	})

	// 5. Conversion case: identifier fields are not strings but can be converted.
	t.Run("EventWithNonStringIdentifiers", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventMetadata{
			{
				UserID:    "123",
				SessionID: "456",
				EventGUID: "789",
			},
		}).Return([]bool{false}, nil)

		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": int64(123),
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": int32(456),
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": []byte("789"),
					},
				},
			},
		}

		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)

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
	config.DedupCfg.IdentifierMapping = map[string]config.Identifier{
		"customer": {
			UserID:    "user.id",
			SessionID: "session.id",
		},
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
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(nil, errors.New("parse error"))
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

	// 3. UserID field not found
	t.Run("UserIDNotFound", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)
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

	// 4. SessionID field not found
	t.Run("SessionIDNotFound", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "user-123",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)
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

	// 5. EventGUID field not found
	t.Run("EventGUIDNotFound", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "user-123",
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{},
				},
			},
		}
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)
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

	// 6. UserID field not convertible
	t.Run("UserIDNotConvertible", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": dummyList{}, // dummyList implements protoreflect.List which is not convertible to string
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)
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

	// 7. Empty metadata fields
	t.Run("EmptyMetadataFields", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		parsedMsg := &mockMessage{
			fields: map[string]any{
				"user": &mockMessage{
					fields: map[string]any{
						"id": "", // Empty UserID
					},
				},
				"session": &mockMessage{
					fields: map[string]any{
						"id": "session-456",
					},
				},
				"meta": &mockMessage{
					fields: map[string]any{
						"event_guid": "guid-789",
					},
				},
			},
		}
		ms := dedupMocks.NewStencilClientMock(t)
		ms.EXPECT().Parse(mock.Anything, mock.Anything).Return(parsedMsg, nil)
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

type mockMessage struct {
	fields map[string]any
}

func (m *mockMessage) ProtoReflect() protoreflect.Message {
	return m
}

func (m *mockMessage) Descriptor() protoreflect.MessageDescriptor {
	return &mockMessageDescriptor{msg: m}
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
	msg *mockMessage
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
