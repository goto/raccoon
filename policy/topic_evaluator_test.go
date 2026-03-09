package policy_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
)

func makeDropTopicPolicy(topicName, publisher string, pastDur, futureDur time.Duration) policy.PolicyConfig {
	return policy.PolicyConfig{
		Resource: policy.ResourceTopic,
		Details:  policy.Details{Name: topicName, Publisher: publisher},
		Action: policy.ActionConfig{
			Type: policy.ActionDrop,
			EventTimestampThreshold: policy.EventTimestampThreshold{
				Past:   policy.Duration{Duration: pastDur},
				Future: policy.Duration{Duration: futureDur},
			},
		},
	}
}

func TestTopicEvaluator_ApplyWhenPastThresholdBreached(t *testing.T) {
	ev := &policy.TopicEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropTopicPolicy("clickstream-foo-log", "pub-a", time.Hour, 0),
	}
	meta := policy.EventMetadata{
		TopicName:      "clickstream-foo-log",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	assert.Equal(t, policy.EvalApply, ev.Evaluate(meta, policies))
}

func TestTopicEvaluator_SkipWhenWithinThreshold(t *testing.T) {
	ev := &policy.TopicEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropTopicPolicy("clickstream-foo-log", "pub-a", time.Hour, time.Hour),
	}
	meta := policy.EventMetadata{
		TopicName:      "clickstream-foo-log",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	assert.Equal(t, policy.EvalSkip, ev.Evaluate(meta, policies))
}

func TestTopicEvaluator_NoMatchOnDifferentTopic(t *testing.T) {
	ev := &policy.TopicEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropTopicPolicy("clickstream-foo-log", "pub-a", time.Hour, 0),
	}
	meta := policy.EventMetadata{
		TopicName:      "clickstream-bar-log",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	assert.Equal(t, policy.EvalNoMatch, ev.Evaluate(meta, policies))
}

func TestTopicEvaluator_WildcardTopic(t *testing.T) {
	ev := &policy.TopicEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropTopicPolicy("", "pub-a", time.Hour, 0), // wildcard topic
	}
	meta := policy.EventMetadata{
		TopicName:      "clickstream-anything-log",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	assert.Equal(t, policy.EvalApply, ev.Evaluate(meta, policies))
}
