package policy_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
)

func buildDropCache(name, product, publisher string, past time.Duration) *policy.Cache {
	return policy.NewCache([]policy.PolicyConfig{
		{
			Resource: policy.ResourceEvent,
			Details:  policy.Details{Name: name, Product: product, Publisher: publisher},
			Action: policy.ActionConfig{
				Type:                    policy.ActionDrop,
				EventTimestampThreshold: policy.EventTimestampThreshold{Past: policy.Duration{Duration: past}},
			},
		},
	})
}

func makeEvent(name, product string, ts time.Time) *pb.Event {
	return &pb.Event{
		EventName: name,
		Product:   product,
	}
}

func TestDropHandler_DropsWhenPolicyBreached(t *testing.T) {
	cache := buildDropCache("click", "app", "pub-a", time.Hour)
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handler := &policy.DropHandler{}
	handled, outcome := handler.Handle(makeEvent("click", "app", meta.EventTimestamp), meta, cache, policy.DefaultChain())
	assert.True(t, handled)
	assert.Equal(t, policy.OutcomeDropped, outcome)
}

func TestDropHandler_PassthroughWhenWithinThreshold(t *testing.T) {
	cache := buildDropCache("click", "app", "pub-a", time.Hour)
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	handler := &policy.DropHandler{}
	handled, _ := handler.Handle(makeEvent("click", "app", meta.EventTimestamp), meta, cache, policy.DefaultChain())
	assert.False(t, handled)
}

func TestDropHandler_PassthroughWhenNoPolicyMatch(t *testing.T) {
	cache := buildDropCache("click", "app", "pub-a", time.Hour)
	meta := policy.EventMetadata{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handler := &policy.DropHandler{}
	handled, _ := handler.Handle(makeEvent("scroll", "app", meta.EventTimestamp), meta, cache, policy.DefaultChain())
	assert.False(t, handled)
}
