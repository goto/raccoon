package ingestionrule_test

import (
	"context"
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"

	"github.com/goto/raccoon/ingestionrule"
	"github.com/goto/raccoon/model"
)

// stubAction is a test double for ingestionrule.Action.
// It removes events whose EventName is in the drop set.
type stubAction struct {
	dropNames map[string]bool
}

func (s *stubAction) Apply(_ context.Context, events []*model.EventWithMetadata, _ string) []*model.EventWithMetadata {
	var out []*model.EventWithMetadata
	for _, e := range events {
		if !s.dropNames[e.EventName] {
			out = append(out, e)
		}
	}
	return out
}

func TestPolicyChain_Apply_FiltersAcrossActions(t *testing.T) {
	chain := ingestionrule.Chain{
		&stubAction{dropNames: map[string]bool{"click": true}},
		&stubAction{dropNames: map[string]bool{"scroll": true}},
	}
	pbClick := &pb.Event{EventName: "click"}
	pbScroll := &pb.Event{EventName: "scroll"}
	pbView := &pb.Event{EventName: "view"}

	events := []*model.EventWithMetadata{
		{EventName: "click", Event: pbClick},
		{EventName: "scroll", Event: pbScroll},
		{EventName: "view", Event: pbView},
	}
	result := chain.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "view", result[0].EventName)
}

func TestPolicyChain_Apply_PassthroughWhenNoActionFilters(t *testing.T) {
	chain := ingestionrule.Chain{
		&stubAction{dropNames: map[string]bool{}},
		&stubAction{dropNames: map[string]bool{}},
	}
	pbClick := &pb.Event{EventName: "click"}
	pbScroll := &pb.Event{EventName: "scroll"}

	events := []*model.EventWithMetadata{
		{EventName: "click", Event: pbClick},
		{EventName: "scroll", Event: pbScroll},
	}
	result := chain.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 2)
	assert.Equal(t, pbClick, result[0].Event)
	assert.Equal(t, pbScroll, result[1].Event)
}

func TestPolicyChain_Apply_PassthroughWhenEmpty(t *testing.T) {
	pbClick := &pb.Event{EventName: "click"}
	events := []*model.EventWithMetadata{
		{EventName: "click", Event: pbClick},
	}
	res := ingestionrule.Chain{}.Apply(context.Background(), events, "grp")
	assert.Len(t, res, 1)
	assert.Equal(t, pbClick, res[0].Event)
}

func TestPolicyChain_Apply_FirstActionRemovesEvents(t *testing.T) {
	chain := ingestionrule.Chain{
		&stubAction{dropNames: map[string]bool{"click": true}},
		&stubAction{dropNames: map[string]bool{}},
	}
	pbClick := &pb.Event{EventName: "click"}
	pbView := &pb.Event{EventName: "view"}

	events := []*model.EventWithMetadata{
		{EventName: "click", Event: pbClick},
		{EventName: "view", Event: pbView},
	}
	result := chain.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "view", result[0].EventName)
}
