package policy_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
	kafkalib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func timestampProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

type mockProducer struct{ mock.Mock }

func (m *mockProducer) ProduceBulk(events []*pb.Event, connGroup string, ch chan kafkalib.Event) error {
	return m.Called(events, connGroup, ch).Error(0)
}
func (m *mockProducer) HealthCheck() error { return m.Called().Error(0) }

func buildRules(pastDrop, pastOverride time.Duration) []config.PolicyRule {
	return []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastDrop}},
			},
		},
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastOverride}},
			},
		},
	}
}

func TestService_Apply_NilIsPassthrough(t *testing.T) {
	var svc *policy.Service
	events := []*pb.Event{{EventName: "click"}}
	assert.Equal(t, events, svc.Apply(events, "grp"))
}

func TestService_Apply_DropTakesPriority(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)
	rules := buildRules(time.Hour, time.Hour)
	svc := policy.NewService(rules, prod, "clickstream-invalid-et-log", deliveryChan)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(events, "grp")
	assert.Empty(t, result)
	prod.AssertNotCalled(t, "ProduceBulk")
}

func TestService_Apply_OverrideWhenNoDrop(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deliveryChan := make(chan kafkalib.Event, 10)

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
	svc := policy.NewService(rules, prod, "clickstream-invalid-et-log", deliveryChan)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(events, "grp")
	assert.Empty(t, result)
	prod.AssertCalled(t, "ProduceBulk", mock.Anything, "grp", deliveryChan)
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)
	svc := policy.NewService(nil, prod, "clickstream-invalid-et-log", deliveryChan)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(events, "grp")
	assert.Equal(t, events, result)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)
	rules := buildRules(time.Hour, 0)
	svc := policy.NewService(rules, prod, "clickstream-invalid-et-log", deliveryChan)

	clean := &pb.Event{EventName: "other", Product: "app"}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	result := svc.Apply([]*pb.Event{stale, clean}, "grp")
	assert.Equal(t, []*pb.Event{clean}, result)
	prod.AssertNotCalled(t, "ProduceBulk")
}
