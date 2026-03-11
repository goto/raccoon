package action_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func buildDropCache(name, product, publisher string, past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

// newDrop is a test helper. Cache publisher must match the connGroup passed to
// Apply because ResolvePublisher falls back to connGroup when no mapping is set.
func newDrop(c *cache.Cache) *action.Drop {
	return action.NewDrop(c, action.DefaultChain())
}

func TestDrop_DropsBreachedEvents(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Empty(t, newDrop(c).Apply(events, "pub-a"))
}

func TestDrop_PassthroughWhenWithinThreshold(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Equal(t, events, newDrop(c).Apply(events, "pub-a"))
}

func TestDrop_PassthroughWhenNoPolicyMatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "scroll",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Equal(t, events, newDrop(c).Apply(events, "pub-a"))
}

func TestDrop_PassthroughWhenMetadataIncomplete(t *testing.T) {
	// Product is empty → EventEvaluator returns EvalSkip → passthrough.
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Equal(t, events, newDrop(c).Apply(events, "pub-a"))
}

func TestDrop_FiltersMixedBatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	staleTs := timestamppb.New(time.Now().Add(-2 * time.Hour))
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: staleTs},
		{EventName: "scroll", Product: "app", EventTimestamp: staleTs},
		{EventName: "click", Product: "app", EventTimestamp: staleTs},
	}
	result := newDrop(c).Apply(events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].GetEventName())
}
