package eval_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/stretchr/testify/assert"
)

func makeEventRules(name, product, publisher string, past, future time.Duration) map[string]eval.Condition {
	key := name + product + publisher
	return map[string]eval.Condition{
		key: eval.NewTimestampCondition(config.PolicyTimestampThreshold{
			Past:   config.PolicyDuration{Duration: past},
			Future: config.PolicyDuration{Duration: future},
		}),
	}
}

func TestEventEvaluator_ApplyWhenPastThresholdBreached(t *testing.T) {
	ev := &eval.EventEvaluator{}
	rules := makeEventRules("click", "app", "pub-a", time.Hour, 0)
	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}

func TestEventEvaluator_SkipWhenWithinThreshold(t *testing.T) {
	ev := &eval.EventEvaluator{}
	rules := makeEventRules("click", "app", "pub-a", time.Hour, time.Hour)
	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.True(t, found) // rule was found, condition just not breached
}

func TestEventEvaluator_NoMatchOnDifferentName(t *testing.T) {
	ev := &eval.EventEvaluator{}
	rules := makeEventRules("click", "app", "pub-a", time.Hour, 0)
	meta := eval.EventMetadata{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.False(t, found) // no rule for this key
}

func TestEventEvaluator_SkipWhenMetadataIncomplete(t *testing.T) {
	ev := &eval.EventEvaluator{}
	rules := makeEventRules("click", "app", "pub-a", time.Hour, 0)
	meta := eval.EventMetadata{
		// EventName missing
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.False(t, found)
}

func TestEventEvaluator_ApplyWhenFutureThresholdBreached(t *testing.T) {
	ev := &eval.EventEvaluator{}
	rules := makeEventRules("click", "app", "pub-a", 0, time.Minute)
	meta := eval.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(10 * time.Minute),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}

func TestEventEvaluator_Resource(t *testing.T) {
	ev := &eval.EventEvaluator{}
	assert.Equal(t, config.PolicyResourceEvent, ev.Resource())
}

func TestEventEvaluator_AlwaysMatchesWithNoCondition(t *testing.T) {
	ev := &eval.EventEvaluator{}
	key := "click" + "app" + "pub-a"
	rules := map[string]eval.Condition{key: eval.NewNoCondition()}
	meta := eval.EventMetadata{
		EventName: "click",
		Product:   "app",
		Publisher: "pub-a",
		// no timestamp — NoCondition must still return true
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}
