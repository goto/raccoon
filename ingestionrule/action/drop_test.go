package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/model"
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

func newDrop(c *cache.Cache) *action.Drop {
	return action.NewDrop(c, action.DefaultChain())
}

func TestDrop_DropsBreachedEvents(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}}
	assert.Empty(t, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenWithinThreshold(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*model.EventWithMetadata{{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenMetadataIncomplete(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_FiltersMixedBatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	staleTs := time.Now().Add(-2 * time.Hour)
	events := []*model.EventWithMetadata{
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: staleTs},
		{EventName: "scroll", Product: "app", Publisher: "pub-a", EventTimestamp: staleTs},
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: staleTs},
	}
	result := newDrop(c).Apply(context.Background(), events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].EventName)
}

func buildGlobalDropCache(past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceGlobal,
			Details:  config.PolicyDetails{},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

func TestDrop_DropsBreachedEventsGlobal(t *testing.T) {
	c := buildGlobalDropCache(time.Hour)
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}}
	assert.Empty(t, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

