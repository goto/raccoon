package action_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	kafkalib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type mockProducer struct {
	mock.Mock
}

func (m *mockProducer) ProduceBulk(events []*pb.Event, connGroup string, deliveryChan chan kafkalib.Event) error {
	args := m.Called(events, connGroup, deliveryChan)
	return args.Error(0)
}

func (m *mockProducer) HealthCheck() error {
	return m.Called().Error(0)
}

func buildOverrideCache(name, product, publisher string, past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

func TestOverrideTimestamp_RedirectsWhenPolicyBreached(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deliveryChan := make(chan kafkalib.Event, 10)

	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	act := action.NewOverrideTimestamp(c, action.DefaultChain(), prod, "clickstream-invalid-et-log", deliveryChan)

	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		ConnGroup:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handled, outcome := act.Process(&pb.Event{EventName: "click"}, meta)
	assert.True(t, handled)
	assert.Equal(t, action.OutcomeRedirected, outcome)
	prod.AssertCalled(t, "ProduceBulk", mock.Anything, "pub-a", deliveryChan)
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)

	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	act := action.NewOverrideTimestamp(c, action.DefaultChain(), prod, "clickstream-invalid-et-log", deliveryChan)

	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	handled, _ := act.Process(&pb.Event{EventName: "click"}, meta)
	assert.False(t, handled)
	prod.AssertNotCalled(t, "ProduceBulk")
}
