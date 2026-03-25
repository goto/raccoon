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

func buildDeactivateEventCache(name, product, publisher string) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
}

func newDeactivate(c *cache.Cache) *action.Deactivate {
	return action.NewDeactivate(c, action.DefaultChain())
}

func TestDeactivate_DropsMatchingEvent(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Empty(t, newDeactivate(c).Apply(events, "pub-a"))
}

func TestDeactivate_PassthroughWhenNoPolicyMatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{{
		EventName:      "scroll",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Equal(t, events, newDeactivate(c).Apply(events, "pub-a"))
}

func TestDeactivate_DropsAlwaysRegardlessOfTimestamp(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	// Both a current and a very old event must be dropped — DEACTIVE is unconditional.
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now().Add(-365 * 24 * time.Hour))},
	}
	assert.Empty(t, newDeactivate(c).Apply(events, "pub-a"))
}

func TestDeactivate_FiltersMixedBatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "scroll", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
	}
	result := newDeactivate(c).Apply(events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].GetEventName())
}

func TestDeactivate_PassthroughWhenEmptyRules(t *testing.T) {
	c := cache.NewCache(nil)
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())}}
	assert.Equal(t, events, newDeactivate(c).Apply(events, "pub-a"))
}

func TestDeactivate_DropsMatchingTopicRule(t *testing.T) {
	c := cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceTopic,
			Details:  config.PolicyDetails{Name: "clickstream-page-log"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
	// event.Type drives the topic name via the publisher pattern format string;
	// set Type = "page-log" so fmt.Sprintf("clickstream-%s-log", "page-log") = "clickstream-page-log".
	events := []*pb.Event{{
		Type:           "page-log",
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Empty(t, newDeactivate(c).Apply(events, "pub-a"))
}
