package action_test

import (
	"errors"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
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
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

const overrideTopic = "clickstream-invalid-et-log"

func newOverrideAct(t *testing.T, prod *mockProducer, deliveryChan chan kafkalib.Event) *action.OverrideTimestamp {
	t.Helper()
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	return action.NewOverrideTimestamp(c, action.DefaultChain(), prod, overrideTopic, deliveryChan)
}

func staleEvent(name string) *pb.Event {
	return &pb.Event{
		EventName:      name,
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}
}

func TestOverrideTimestamp_RedirectsBreachedEvents(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deliveryChan := make(chan kafkalib.Event, 10)

	result := newOverrideAct(t, prod, deliveryChan).Apply([]*pb.Event{staleEvent("click")}, "pub-a")

	assert.Empty(t, result)
	prod.AssertCalled(t, "ProduceBulk", mock.Anything, "pub-a", deliveryChan)
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)

	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())}}
	result := newOverrideAct(t, prod, deliveryChan).Apply(events, "pub-a")

	assert.Equal(t, events, result)
	prod.AssertNotCalled(t, "ProduceBulk")
}

func TestOverrideTimestamp_BatchesProduceBulk(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deliveryChan := make(chan kafkalib.Event, 10)

	events := []*pb.Event{staleEvent("click"), staleEvent("scroll"), staleEvent("click")}
	result := newOverrideAct(t, prod, deliveryChan).Apply(events, "pub-a")

	// scroll has no matching policy → passthrough; 2 clicks are redirected
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].GetEventName())
	// ProduceBulk called exactly once with the 2 override events
	prod.AssertNumberOfCalls(t, "ProduceBulk", 1)
	assert.Len(t, prod.Calls[0].Arguments[0].([]*pb.Event), 2)
}

func TestOverrideTimestamp_PublishError_EventStillRemovedFromBatch(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("kafka down"))
	deliveryChan := make(chan kafkalib.Event, 10)

	result := newOverrideAct(t, prod, deliveryChan).Apply([]*pb.Event{staleEvent("click")}, "pub-a")

	// Event is removed from the ingestion batch regardless of publish outcome.
	assert.Empty(t, result)
	prod.AssertCalled(t, "ProduceBulk", mock.Anything, mock.Anything, mock.Anything)
}
