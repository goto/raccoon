package eval_test

import (
	"testing"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/stretchr/testify/assert"
)

func threshold(past, future time.Duration) config.PolicyTimestampThreshold {
	return config.PolicyTimestampThreshold{
		Past:   config.PolicyDuration{Duration: past},
		Future: config.PolicyDuration{Duration: future},
	}
}

// --- WithinThreshold ---

func TestWithinThreshold_TrueWhenNoBoundsSet(t *testing.T) {
	// zero durations mean unbounded — any timestamp is within range
	assert.True(t, eval.WithinThreshold(threshold(0, 0), time.Now().Add(-100*time.Hour)))
	assert.True(t, eval.WithinThreshold(threshold(0, 0), time.Now().Add(100*time.Hour)))
}

func TestWithinThreshold_FalseWhenPastExceeded(t *testing.T) {
	assert.False(t, eval.WithinThreshold(threshold(time.Hour, 0), time.Now().Add(-2*time.Hour)))
}

func TestWithinThreshold_TrueWhenWithinPastBound(t *testing.T) {
	assert.True(t, eval.WithinThreshold(threshold(time.Hour, 0), time.Now().Add(-30*time.Minute)))
}

func TestWithinThreshold_FalseWhenFutureExceeded(t *testing.T) {
	assert.False(t, eval.WithinThreshold(threshold(0, time.Minute), time.Now().Add(10*time.Minute)))
}

func TestWithinThreshold_TrueWhenWithinFutureBound(t *testing.T) {
	assert.True(t, eval.WithinThreshold(threshold(0, time.Hour), time.Now().Add(30*time.Minute)))
}

// --- TimestampCondition ---

func TestTimestampCondition_BreachedWhenPastExceeded(t *testing.T) {
	cond := eval.NewTimestampCondition(threshold(time.Hour, 0))
	meta := eval.EventMetadata{EventTimestamp: time.Now().Add(-2 * time.Hour)}
	assert.True(t, cond.Breached(meta))
}

func TestTimestampCondition_NotBreachedWhenWithinPastBound(t *testing.T) {
	cond := eval.NewTimestampCondition(threshold(time.Hour, 0))
	meta := eval.EventMetadata{EventTimestamp: time.Now().Add(-30 * time.Minute)}
	assert.False(t, cond.Breached(meta))
}

func TestTimestampCondition_BreachedWhenFutureExceeded(t *testing.T) {
	cond := eval.NewTimestampCondition(threshold(0, time.Minute))
	meta := eval.EventMetadata{EventTimestamp: time.Now().Add(10 * time.Minute)}
	assert.True(t, cond.Breached(meta))
}

func TestTimestampCondition_NotBreachedWhenNoBoundsSet(t *testing.T) {
	cond := eval.NewTimestampCondition(threshold(0, 0))
	meta := eval.EventMetadata{EventTimestamp: time.Now().Add(-100 * time.Hour)}
	assert.False(t, cond.Breached(meta))
}
