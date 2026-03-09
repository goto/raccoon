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

func buildMixedCache(pastDrop, pastOverride time.Duration) *policy.Cache {
	return policy.NewCache([]policy.PolicyConfig{
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: "click"},
			Action: policy.ActionConfig{
				Type:                    policy.ActionDrop,
				EventTimestampThreshold: policy.EventTimestampThreshold{Past: policy.Duration{Duration: pastDrop}},
			},
		},
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: "click"},
			Action: policy.ActionConfig{
				Type:                    policy.ActionOverrideTimestamp,
				EventTimestampThreshold: policy.EventTimestampThreshold{Past: policy.Duration{Duration: pastOverride}},
			},
		},
	})
}

func TestHandlerChain_DropTakesPriority(t *testing.T) {
	prod := &mockProducer{}
	deliveryChan := make(chan kafkalib.Event, 10)
	chain := policy.HandlerChain{
		&policy.DropHandler{},
		&policy.OverrideTimestampHandler{
			Producer:        prod,
			OverrideTopic:   "clickstream-invalid-et-log",
			DeliveryChannel: deliveryChan,
		},
	}
	// Both DROP and OVERRIDE thresholds breached, DROP should win.
	cache := buildMixedCache(time.Hour, time.Hour)
	meta := policy.EventMetadata{
		EventName:      "click",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	event := &pb.Event{EventName: "click"}
	outcome := chain.Process(event, meta, cache, policy.DefaultChain())
	assert.Equal(t, policy.OutcomeDropped, outcome)
	prod.AssertNotCalled(t, "ProduceBulk")
}

func TestHandlerChain_OverrideWhenNoDrop(t *testing.T) {
	prod := &mockProducer{}
	prod.On("ProduceBulk", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	deliveryChan := make(chan kafkalib.Event, 10)
	chain := policy.HandlerChain{
		&policy.DropHandler{},
		&policy.OverrideTimestampHandler{
			Producer:        prod,
			OverrideTopic:   "clickstream-invalid-et-log",
			DeliveryChannel: deliveryChan,
		},
	}
	// Only OVERRIDE threshold breached.
	cache := policy.NewCache([]policy.PolicyConfig{
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: "click"},
			Action: policy.ActionConfig{
				Type:                    policy.ActionOverrideTimestamp,
				EventTimestampThreshold: policy.EventTimestampThreshold{Past: policy.Duration{Duration: time.Hour}},
			},
		},
	})
	meta := policy.EventMetadata{
		EventName:      "click",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	event := &pb.Event{EventName: "click"}
	outcome := chain.Process(event, meta, cache, policy.DefaultChain())
	assert.Equal(t, policy.OutcomeRedirected, outcome)
}

func TestHandlerChain_PassthroughWhenNoPolicy(t *testing.T) {
	chain := policy.HandlerChain{
		&policy.DropHandler{},
	}
	cache := policy.NewCache(nil)
	meta := policy.EventMetadata{
		EventName:      "click",
		EventTimestamp: time.Now(),
	}
	event := &pb.Event{EventName: "click"}
	outcome := chain.Process(event, meta, cache, policy.DefaultChain())
	assert.Equal(t, policy.OutcomePassthrough, outcome)
}
