package ingestionrule_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule"
	"github.com/goto/raccoon/ingestionrule/schemaregistry"
	stencil "github.com/goto/stencil/clients/go"
)

func timestampProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

const testOverrideEventType = "invalid-et"

func buildRules(pastDrop, pastOverride time.Duration, withDeactivate bool) []config.PolicyRule {
	var rules []config.PolicyRule
	if withDeactivate {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		})
	}
	if pastDrop > 0 {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastDrop}},
			},
		})
	}
	if pastOverride > 0 {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastOverride}},
			},
		})
	}
	return rules
}

func newTestService(ctx context.Context, rules []config.PolicyRule, overrideEventType string, events []*pb.Event) (*ingestionrule.Service, error) {
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"":          "EventProto",
		"component": "EventProto",
		"page":      "EventProto",
	}

	svc, err := ingestionrule.NewService(ctx, rules, overrideEventType)
	if err != nil {
		return nil, err
	}

	ms := &mockStencilClient{
		events: events,
	}

	injectMockStencil(svc, ms)
	return svc, nil
}

func injectMockStencil(svc *ingestionrule.Service, client stencil.Client) {
	field := reflect.ValueOf(svc).Elem().FieldByName("stencil")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(schemaregistry.StencilClient{Client: client}))
}

func TestService_Apply_NilIsPassthrough(t *testing.T) {
	var svc *ingestionrule.Service
	events := []*pb.Event{{EventName: "click"}}
	assert.Equal(t, events, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DropTakesPriorityOverOverride(t *testing.T) {
	rules := buildRules(time.Hour, time.Hour, false)
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	svc, err := newTestService(context.Background(), rules, testOverrideEventType, events)
	assert.NoError(t, err)

	result := svc.Apply(context.Background(), events, "grp")
	assert.Empty(t, result)
}

func TestService_Apply_OverrideWhenNoDrop(t *testing.T) {
	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: time.Hour}},
			},
		},
	}
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	svc, err := newTestService(context.Background(), rules, testOverrideEventType, events)
	assert.NoError(t, err)

	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, testOverrideEventType, result[0].GetType())
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())}}
	svc, err := newTestService(context.Background(), nil, testOverrideEventType, events)
	assert.NoError(t, err)

	result := svc.Apply(context.Background(), events, "grp")
	assert.Equal(t, events, result)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	rules := buildRules(time.Hour, 0, false)
	clean := &pb.Event{EventName: "other", Product: "app", EventTimestamp: timestampProto(time.Now())}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	events := []*pb.Event{stale, clean}

	svc, err := newTestService(context.Background(), rules, testOverrideEventType, events)
	assert.NoError(t, err)

	result := svc.Apply(context.Background(), events, "grp")
	assert.Equal(t, []*pb.Event{clean}, result)
}

func TestService_Apply_DeactivateDropsEvent(t *testing.T) {
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	svc, err := newTestService(context.Background(), buildRules(0, 0, true), testOverrideEventType, events)
	assert.NoError(t, err)

	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivateTakesPriorityOverDrop(t *testing.T) {
	rules := buildRules(time.Hour, 0, true)
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	svc, err := newTestService(context.Background(), rules, testOverrideEventType, events)
	assert.NoError(t, err)

	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivatePassthroughWhenNoMatch(t *testing.T) {
	events := []*pb.Event{
		{EventName: "scroll", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	svc, err := newTestService(context.Background(), buildRules(0, 0, true), testOverrideEventType, events)
	assert.NoError(t, err)

	assert.Equal(t, events, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_UnknownActionTypeSkipped(t *testing.T) {
	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action:   config.PolicyActionConfig{Type: "UNKNOWN_ACTION"},
		},
	}
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())}}
	svc, err := newTestService(context.Background(), rules, testOverrideEventType, events)
	assert.NoError(t, err)

	result := svc.Apply(context.Background(), events, "grp")
	assert.Equal(t, events, result)
}

type mockStencilClient struct {
	events []*pb.Event
	index  int
}

func (m *mockStencilClient) Parse(className string, data []byte) (protoreflect.ProtoMessage, error) {
	if m.index >= len(m.events) {
		return nil, errors.New("index out of bounds")
	}
	event := m.events[m.index]
	m.index++

	ts := time.Now()
	if event.EventTimestamp != nil {
		ts = event.EventTimestamp.AsTime()
	}
	tsMsg := &mockMessage{
		fullName: "google.protobuf.Timestamp",
		fields: map[string]any{
			"seconds": ts.Unix(),
			"nanos":   int32(ts.Nanosecond()),
		},
	}
	metaMsg := &mockMessage{
		fields: map[string]any{
			"event_guid": "some-guid",
		},
	}

	var enumVal protoreflect.EnumNumber = 1
	if event.Product != "app" {
		enumVal = 2
	}

	parsedMsg := &mockMessage{
		fields: map[string]any{
			"meta":            metaMsg,
			"event_name":      event.EventName,
			"product":         enumVal,
			"event_timestamp": tsMsg,
		},
	}
	return parsedMsg, nil
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
	if string(name) == "product" {
		kind = protoreflect.EnumKind
	} else if _, ok := val.(*mockMessage); ok {
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

func (fd *mockFieldDescriptor) Enum() protoreflect.EnumDescriptor {
	return &mockEnumDescriptor{
		values: map[protoreflect.EnumNumber]string{
			1: "app",
			2: "other",
		},
	}
}

type mockEnumDescriptor struct {
	protoreflect.EnumDescriptor
	values map[protoreflect.EnumNumber]string
}

func (d *mockEnumDescriptor) Values() protoreflect.EnumValueDescriptors {
	return &mockEnumValueDescriptors{values: d.values}
}

type mockEnumValueDescriptors struct {
	protoreflect.EnumValueDescriptors
	values map[protoreflect.EnumNumber]string
}

func (v *mockEnumValueDescriptors) ByNumber(n protoreflect.EnumNumber) protoreflect.EnumValueDescriptor {
	name, ok := v.values[n]
	if !ok {
		return nil
	}
	return &mockEnumValueDescriptor{name: name}
}

type mockEnumValueDescriptor struct {
	protoreflect.EnumValueDescriptor
	name string
}

func (v *mockEnumValueDescriptor) Name() protoreflect.Name {
	return protoreflect.Name(v.name)
}
