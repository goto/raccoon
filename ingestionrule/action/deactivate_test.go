package action_test

import (
	"context"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/model"
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
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
		Event:          &pb.Event{},
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
		Event:          &pb.Event{},
	}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsAlwaysRegardlessOfTimestamp(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now(), Event: &pb.Event{}},
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now().Add(-365 * 24 * time.Hour), Event: &pb.Event{}},
	}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_FiltersMixedBatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now(), Event: &pb.Event{}},
		{EventName: "scroll", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now(), Event: &pb.Event{}},
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now(), Event: &pb.Event{}},
	}
	result := newDeactivate(c).Apply(context.Background(), events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].EventName)
}

func TestDeactivate_PassthroughWhenEmptyRules(t *testing.T) {
	c := cache.NewCache(nil)
	events := []*model.EventWithMetadata{{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now(), Event: &pb.Event{}}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsMatchingTopicRule(t *testing.T) {
	c := cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceTopic,
			Details:  config.PolicyDetails{Name: "clickstream-page-log"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
	events := []*model.EventWithMetadata{{
		TopicName:      "clickstream-page-log",
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
		Event:          &pb.Event{},
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}
