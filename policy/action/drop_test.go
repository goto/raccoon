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
)

func buildDropCache(name, product, publisher string, past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

func TestDrop_DropsWhenPolicyBreached(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	drop := action.NewDrop(c, action.DefaultChain())
	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handled, outcome := drop.Process(&pb.Event{EventName: "click"}, meta)
	assert.True(t, handled)
	assert.Equal(t, action.OutcomeDropped, outcome)
}

func TestDrop_PassthroughWhenWithinThreshold(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	drop := action.NewDrop(c, action.DefaultChain())
	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	handled, _ := drop.Process(&pb.Event{EventName: "click"}, meta)
	assert.False(t, handled)
}

func TestDrop_PassthroughWhenNoPolicyMatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	drop := action.NewDrop(c, action.DefaultChain())
	meta := eval.EventMetadata{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handled, _ := drop.Process(&pb.Event{EventName: "scroll"}, meta)
	assert.False(t, handled)
}

func TestDrop_PassthroughWhenMetadataIncomplete(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	drop := action.NewDrop(c, action.DefaultChain())
	meta := eval.EventMetadata{
		// Product missing
		EventName:      "click",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	handled, _ := drop.Process(&pb.Event{EventName: "click"}, meta)
	assert.False(t, handled)
}
