package policy_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
)

func makeDropEventPolicy(name, product, publisher string, pastDur, futureDur time.Duration) policy.PolicyConfig {
	return policy.PolicyConfig{
		Resource: policy.ResourceEvent,
		Details:  policy.Details{Name: name, Product: product, Publisher: publisher},
		Action: policy.ActionConfig{
			Type: policy.ActionDrop,
			EventTimestampThreshold: policy.EventTimestampThreshold{
				Past:   policy.Duration{Duration: pastDur},
				Future: policy.Duration{Duration: futureDur},
			},
		},
	}
}

func TestEventEvaluator_ApplyWhenPastThresholdBreached(t *testing.T) {
	ev := &policy.EventEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropEventPolicy("click", "app", "pub-a", time.Hour, 0),
	}
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour), // too old
	}
	assert.Equal(t, policy.EvalApply, ev.Evaluate(meta, policies))
}

func TestEventEvaluator_SkipWhenWithinThreshold(t *testing.T) {
	ev := &policy.EventEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropEventPolicy("click", "app", "pub-a", time.Hour, time.Hour),
	}
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}
	assert.Equal(t, policy.EvalSkip, ev.Evaluate(meta, policies))
}

func TestEventEvaluator_NoMatchOnDifferentName(t *testing.T) {
	ev := &policy.EventEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropEventPolicy("click", "app", "pub-a", time.Hour, 0),
	}
	meta := policy.EventMetadata{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	assert.Equal(t, policy.EvalNoMatch, ev.Evaluate(meta, policies))
}

func TestEventEvaluator_WildcardName(t *testing.T) {
	ev := &policy.EventEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropEventPolicy("", "", "pub-a", time.Hour, 0), // wildcard name + product
	}
	meta := policy.EventMetadata{
		EventName:      "any-event",
		Product:        "any-product",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
	assert.Equal(t, policy.EvalApply, ev.Evaluate(meta, policies))
}

func TestEventEvaluator_ApplyWhenFutureThresholdBreached(t *testing.T) {
	ev := &policy.EventEvaluator{}
	policies := []policy.PolicyConfig{
		makeDropEventPolicy("click", "app", "pub-a", 0, time.Minute),
	}
	meta := policy.EventMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(10 * time.Minute), // future
	}
	assert.Equal(t, policy.EvalApply, ev.Evaluate(meta, policies))
}
