package policy_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	kafkalib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// mockProducer is a mock KafkaProducer for override tests.
type mockProducer struct {
	mock.Mock
}

func (m *mockProducer) ProduceBulk(events []*pb.Event, connGroup string, deliveryChan chan kafkalib.Event) error {
	args := m.Called(events, connGroup, deliveryChan)
	return args.Error(0)
}

func (m *mockProducer) HealthCheck() error {
	args := m.Called()
	return args.Error(0)
}

func buildOverrideCache(past time.Duration) *policy.Cache {
	return policy.NewCache([]policy.PolicyConfig{
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: "click", Product: "app", Publisher: "pub-a"},
			Action: policy.ActionConfig{
				Type:                    policy.ActionOverrideTimestamp,
				EventTimestampThreshold: policy.EventTimestampThreshold{Past: policy.Duration{Duration: past}},
			},
		},
	})
}

func TestOverrideHandler_RedirectsWhenPolicyBreached(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	deliveryChan := make(chan kafkalib.Event, 10)
	handler := &policy.OverrideTimestampHandler{
		Producer:        prod,
		OverrideTopic:   "clickstream-invalid-et-log",
		DeliveryChannel: deliveryChan,
	}
	cache := buildOverrideCache(time.Hour)
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	event := &pb.Event{EventName: "click", Product: "app"}
	handled, outcome := handler.Handle(event, meta, cache, policy.DefaultChain())
	assert.True(t, handled)
	assert.Equal(t, policy.OutcomeRedirected, outcome)
	prod.AssertCalled(t, "ProduceBulk", mock.Anything, meta.ConnGroup, deliveryChan)
}

func TestOverrideHandler_PassthroughWhenWithinThreshold(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)
	handler := &policy.OverrideTimestampHandler{
		Producer:        prod,
		OverrideTopic:   "clickstream-invalid-et-log",
		DeliveryChannel: deliveryChan,
	}
	cache := buildOverrideCache(time.Hour)
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	event := &pb.Event{EventName: "click", Product: "app"}
	handled, _ := handler.Handle(event, meta, cache, policy.DefaultChain())
	assert.False(t, handled)
	prod.AssertNotCalled(t, "ProduceBulk")
}
