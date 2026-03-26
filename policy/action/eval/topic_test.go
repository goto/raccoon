package eval_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/stretchr/testify/assert"
)

func makeTopicRules(topicName string, past, future time.Duration) map[string]eval.Condition {
	key := topicName
	return map[string]eval.Condition{
		key: eval.NewTimestampCondition(config.PolicyTimestampThreshold{
			Past:   config.PolicyDuration{Duration: past},
			Future: config.PolicyDuration{Duration: future},
		}),
	}
}

func TestTopicEvaluator_ApplyWhenPastThresholdBreached(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	rules := makeTopicRules("clickstream-foo-log", time.Hour, 0)
	meta := eval.EventMetadata{
		TopicName:      "clickstream-foo-log",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}

func TestTopicEvaluator_SkipWhenWithinThreshold(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	rules := makeTopicRules("clickstream-foo-log", time.Hour, time.Hour)
	meta := eval.EventMetadata{
		TopicName:      "clickstream-foo-log",
		EventTimestamp: time.Now(),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.True(t, found) // rule was found, condition just not breached
}

func TestTopicEvaluator_NoMatchOnDifferentTopic(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	rules := makeTopicRules("clickstream-foo-log", time.Hour, 0)
	meta := eval.EventMetadata{
		TopicName:      "clickstream-bar-log",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.False(t, found) // no rule for this topic key
}

func TestTopicEvaluator_SkipWhenMetadataIncomplete(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	rules := makeTopicRules("clickstream-foo-log", time.Hour, 0)
	meta := eval.EventMetadata{
		// TopicName missing
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	result, found := ev.Evaluate(meta, rules)
	assert.False(t, result)
	assert.False(t, found)
}

func TestTopicEvaluator_Resource(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	assert.Equal(t, config.PolicyResourceTopic, ev.Resource())
}

func TestTopicEvaluator_AlwaysMatchesWithNoCondition(t *testing.T) {
	ev := &eval.TopicEvaluator{}
	rules := map[string]eval.Condition{"clickstream-page-log": eval.NewNoCondition()}
	meta := eval.EventMetadata{
		TopicName: "clickstream-page-log",
		// no timestamp — NoCondition must still return true
	}
	result, found := ev.Evaluate(meta, rules)
	assert.True(t, result)
	assert.True(t, found)
}
