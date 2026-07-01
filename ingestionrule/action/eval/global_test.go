package eval_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eval"
	"github.com/goto/raccoon/model"
	"github.com/stretchr/testify/assert"
)

func makeGlobalRules(past, future time.Duration) map[string]eval.Condition {
	return map[string]eval.Condition{
		"": eval.NewTimestampCondition(config.PolicyTimestampThreshold{
			Past:   config.PolicyDuration{Duration: past},
			Future: config.PolicyDuration{Duration: future},
		}),
	}
}

func TestGlobalEvaluator_ApplyWhenPastThresholdBreached(t *testing.T) {
	ev := &eval.GlobalEvaluator{}
	rules := makeGlobalRules(time.Hour, 0)
	meta := model.EventWithMetadata{
		TopicName:      "clickstream-foo-log",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}

func TestGlobalEvaluator_SkipWhenWithinThreshold(t *testing.T) {
	ev := &eval.GlobalEvaluator{}
	rules := makeGlobalRules(time.Hour, time.Hour)
	meta := model.EventWithMetadata{
		TopicName:      "clickstream-foo-log",
		EventTimestamp: time.Now(),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.True(t, found) // rule was found, condition just not breached
}

func TestGlobalEvaluator_NoMatchWhenNoGlobalRule(t *testing.T) {
	ev := &eval.GlobalEvaluator{}
	rules := map[string]eval.Condition{}
	meta := model.EventWithMetadata{
		TopicName:      "clickstream-bar-log",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.False(t, found) // no global rule
}

func TestGlobalEvaluator_Resource(t *testing.T) {
	ev := &eval.GlobalEvaluator{}
	assert.Equal(t, config.PolicyResourceGlobal, ev.Resource())
}

func TestGlobalEvaluator_AlwaysMatchesWithNoCondition(t *testing.T) {
	ev := &eval.GlobalEvaluator{}
	rules := map[string]eval.Condition{"": eval.NewNoCondition()}
	meta := model.EventWithMetadata{
		TopicName: "clickstream-page-log",
		// no timestamp — NoCondition must still return true
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}
