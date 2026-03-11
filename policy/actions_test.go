package policy_test

import (
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
)

// stubAction is a test double for policy.Action.
// It removes events whose EventName is in the drop set.
type stubAction struct {
	dropNames map[string]bool
}

func (s *stubAction) Apply(events []*pb.Event, _ string) []*pb.Event {
	var out []*pb.Event
	for _, e := range events {
		if !s.dropNames[e.GetEventName()] {
			out = append(out, e)
		}
	}
	return out
}

func TestPolicyChain_Apply_FiltersAcrossActions(t *testing.T) {
	chain := policy.Chain{
		&stubAction{dropNames: map[string]bool{"click": true}},
		&stubAction{dropNames: map[string]bool{"scroll": true}},
	}
	events := []*pb.Event{
		{EventName: "click"},
		{EventName: "scroll"},
		{EventName: "view"},
	}
	result := chain.Apply(events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "view", result[0].GetEventName())
}

func TestPolicyChain_Apply_PassthroughWhenNoActionFilters(t *testing.T) {
	chain := policy.Chain{
		&stubAction{dropNames: map[string]bool{}},
		&stubAction{dropNames: map[string]bool{}},
	}
	events := []*pb.Event{{EventName: "click"}, {EventName: "scroll"}}
	result := chain.Apply(events, "grp")
	assert.Equal(t, events, result)
}

func TestPolicyChain_Apply_PassthroughWhenEmpty(t *testing.T) {
	events := []*pb.Event{{EventName: "click"}}
	assert.Equal(t, events, policy.Chain{}.Apply(events, "grp"))
}

func TestPolicyChain_Apply_FirstActionRemovesEvents(t *testing.T) {
	chain := policy.Chain{
		&stubAction{dropNames: map[string]bool{"click": true}},
		&stubAction{dropNames: map[string]bool{}},
	}
	events := []*pb.Event{{EventName: "click"}, {EventName: "view"}}
	result := chain.Apply(events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "view", result[0].GetEventName())
}
