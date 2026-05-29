package dedup

import (
	"errors"
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/dedup/mocks"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
)

func TestService_Apply_NilService(t *testing.T) {
	var s *Service
	events := []*pb.Event{{Type: "click"}}
	assert.Equal(t, events, s.Apply(events, "group-1"))
}

func TestService_Apply_BypassDeduplicationWhenNotWhitelisted(t *testing.T) {
	// Configure whitelist to exclude group-1.
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"group-whitelisted": {},
	}

	s := &Service{}
	events := []*pb.Event{{Type: "click"}}
	assert.Equal(t, events, s.Apply(events, "group-1"))
}

func TestService_Apply_DeduplicationWorkflow(t *testing.T) {
	// Configure whitelist to include group-whitelisted.
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"group-whitelisted": {},
	}
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"click-event-type": "ClickEventProto",
	}
	config.DedupCfg.PublisherIdentifierMapping = map[string]config.Identifier{
		"click-publisher": {
			UserID:    "user.id",
			SessionID: "session.id",
		},
	}

	publisherMapping := map[string]string{
		"group-whitelisted": "click-publisher",
	}

	// 1. Success case: event is not a duplicate.
	t.Run("EventNotDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().IsDuplicate(mock.Anything, cache.EventMetadata{
			UserID:    "user-123",
			SessionID: "session-456",
			EventGUID: "guid-789",
		}).Return(false, nil)

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

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				assert.Equal(t, "ClickEventProto", className)
				assert.Equal(t, []byte("event-payload"), data)
				return parsedMsg, nil
			},
		}

		s := &Service{
			stencil:          schemaregistry.StencilClient{Client: ms},
			publisherMapping: publisherMapping,
			checker:          mc,
		}

		events := []*pb.Event{
			{
				Type:       "click-event-type",
				EventBytes: []byte("event-payload"),
			},
		}

		res := s.Apply(events, "group-whitelisted")
		assert.Len(t, res, 1)
	})

	// 2. Duplicate case: event is already in cache.
	t.Run("EventDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().IsDuplicate(mock.Anything, cache.EventMetadata{
			UserID:    "user-123",
			SessionID: "session-456",
			EventGUID: "guid-789",
		}).Return(true, nil)

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

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}

		s := &Service{
			stencil:          schemaregistry.StencilClient{Client: ms},
			publisherMapping: publisherMapping,
			checker:          mc,
		}

		events := []*pb.Event{
			{
				Type:       "click-event-type",
				EventBytes: []byte("event-payload"),
			},
		}

		res := s.Apply(events, "group-whitelisted")
		assert.Empty(t, res) // Dropped
	})

	// 3. Redis error case: fails open.
	t.Run("RedisErrorFailsOpen", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().IsDuplicate(mock.Anything, cache.EventMetadata{
			UserID:    "user-123",
			SessionID: "session-456",
			EventGUID: "guid-789",
		}).Return(false, errors.New("redis error"))

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

		ms := &mockStencilClient{
			parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
				return parsedMsg, nil
			},
		}

		s := &Service{
			stencil:          schemaregistry.StencilClient{Client: ms},
			publisherMapping: publisherMapping,
			checker:          mc,
		}

		events := []*pb.Event{
			{
				Type:       "click-event-type",
				EventBytes: []byte("event-payload"),
			},
		}

		res := s.Apply(events, "group-whitelisted")
		assert.Len(t, res, 1) // Bypassed and allowed through
	})
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
