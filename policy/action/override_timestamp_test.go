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

const overrideEventType = "invalid-et"

func newOverrideAct(t *testing.T) *action.OverrideTimestamp {
	t.Helper()
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	return action.NewOverrideTimestamp(c, action.DefaultChain(), overrideEventType)
}

func staleEvent(name string) *pb.Event {
	return &pb.Event{
		EventName:      name,
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}
}

func TestOverrideTimestamp_OverridesTypeOnBreachedEvent(t *testing.T) {
	result := newOverrideAct(t).Apply([]*pb.Event{staleEvent("click")}, "pub-a")

	assert.Len(t, result, 1)
	assert.Equal(t, overrideEventType, result[0].GetType())
	assert.Equal(t, "click", result[0].GetEventName())
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())}}
	result := newOverrideAct(t).Apply(events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].GetType()) // Type not overridden
}

func TestOverrideTimestamp_PassthroughWhenNoPolicyMatch(t *testing.T) {
	events := []*pb.Event{staleEvent("scroll")}
	result := newOverrideAct(t).Apply(events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].GetType())
}

func TestOverrideTimestamp_MixedBatch(t *testing.T) {
	events := []*pb.Event{staleEvent("click"), staleEvent("scroll"), staleEvent("click")}
	result := newOverrideAct(t).Apply(events, "pub-a")

	assert.Len(t, result, 3)
	assert.Equal(t, overrideEventType, result[0].GetType())
	assert.Empty(t, result[1].GetType()) // scroll: no policy match
	assert.Equal(t, overrideEventType, result[2].GetType())
}
