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
		{EventName: "click", Type: pbClick.GetType(), Platform: pbClick.GetPlatform(), AppVersion: pbClick.GetAppVersion(), IsExclusive: pbClick.GetIsExclusive(), EventBytes: pbClick.GetEventBytes()},
		{EventName: "scroll", Type: pbScroll.GetType(), Platform: pbScroll.GetPlatform(), AppVersion: pbScroll.GetAppVersion(), IsExclusive: pbScroll.GetIsExclusive(), EventBytes: pbScroll.GetEventBytes()},
		{EventName: "view", Type: pbView.GetType(), Platform: pbView.GetPlatform(), AppVersion: pbView.GetAppVersion(), IsExclusive: pbView.GetIsExclusive(), EventBytes: pbView.GetEventBytes()},
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
		{EventName: "click", Type: pbClick.GetType(), Platform: pbClick.GetPlatform(), AppVersion: pbClick.GetAppVersion(), IsExclusive: pbClick.GetIsExclusive(), EventBytes: pbClick.GetEventBytes()},
		{EventName: "scroll", Type: pbScroll.GetType(), Platform: pbScroll.GetPlatform(), AppVersion: pbScroll.GetAppVersion(), IsExclusive: pbScroll.GetIsExclusive(), EventBytes: pbScroll.GetEventBytes()},
	}
	result := chain.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 2)
	assert.Equal(t, "click", result[0].EventName)
	assert.Equal(t, "scroll", result[1].EventName)
}

func TestPolicyChain_Apply_PassthroughWhenEmpty(t *testing.T) {
	pbClick := &pb.Event{EventName: "click"}
	events := []*model.EventWithMetadata{
		{EventName: "click", Type: pbClick.GetType(), Platform: pbClick.GetPlatform(), AppVersion: pbClick.GetAppVersion(), IsExclusive: pbClick.GetIsExclusive(), EventBytes: pbClick.GetEventBytes()},
	}
	res := ingestionrule.Chain{}.Apply(context.Background(), events, "grp")
	assert.Len(t, res, 1)
	assert.Equal(t, "click", res[0].EventName)
}

func TestPolicyChain_Apply_FirstActionRemovesEvents(t *testing.T) {
	chain := ingestionrule.Chain{
		&stubAction{dropNames: map[string]bool{"click": true}},
		&stubAction{dropNames: map[string]bool{}},
	}
	pbClick := &pb.Event{EventName: "click"}
	pbView := &pb.Event{EventName: "view"}

	events := []*model.EventWithMetadata{
		{EventName: "click", Type: pbClick.GetType(), Platform: pbClick.GetPlatform(), AppVersion: pbClick.GetAppVersion(), IsExclusive: pbClick.GetIsExclusive(), EventBytes: pbClick.GetEventBytes()},
		{EventName: "view", Type: pbView.GetType(), Platform: pbView.GetPlatform(), AppVersion: pbView.GetAppVersion(), IsExclusive: pbView.GetIsExclusive(), EventBytes: pbView.GetEventBytes()},
	}
	result := chain.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "view", result[0].EventName)
}
