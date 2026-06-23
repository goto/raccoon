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

func staleEvent(name string) *model.EventWithMetadata {
	return &model.EventWithMetadata{
		EventName:      name,
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
}

func TestOverrideTimestamp_OverridesTypeOnBreachedEvent(t *testing.T) {
	result := newOverrideAct(t).Apply(context.Background(), []*model.EventWithMetadata{staleEvent("click")}, "pub-a")

	assert.Len(t, result, 1)
	assert.Equal(t, overrideEventType, result[0].Type)
	assert.Equal(t, "click", result[0].EventName)
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].Type) // Type not overridden
}

func TestOverrideTimestamp_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	events := []*model.EventWithMetadata{staleEvent("scroll")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].Type)
}

func TestOverrideTimestamp_MixedBatch(t *testing.T) {
	events := []*model.EventWithMetadata{staleEvent("click"), staleEvent("scroll"), staleEvent("click")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Len(t, result, 3)
	assert.Equal(t, overrideEventType, result[0].Type)
	assert.Empty(t, result[1].Type) // scroll: no policy match
	assert.Equal(t, overrideEventType, result[2].Type)
}
