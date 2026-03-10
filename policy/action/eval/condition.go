package eval

import (
	"time"

	"github.com/goto/raccoon/config"
)

// Condition is the policy predicate that decides whether an action should be applied.
// Implementations can inspect any aspect of the event (e.g. timestamp, payload size).
type Condition interface {
	Breached(meta EventMetadata) bool
}

// NewTimestampCondition returns a Condition that checks the event timestamp against the
// configured past/future threshold. Returns true (breached) when the timestamp falls
// outside the allowed window.
func NewTimestampCondition(threshold config.PolicyTimestampThreshold) Condition {
	return timestampCondition{threshold: threshold}
}

type timestampCondition struct {
	threshold config.PolicyTimestampThreshold
}

func (c timestampCondition) Breached(meta EventMetadata) bool {
	return !WithinThreshold(c.threshold, meta.EventTimestamp)
}

// WithinThreshold returns true when the event timestamp is within the allowed window
// (no action should be taken). It returns false (threshold breached) when:
//   - past  > 0 AND eventTs < now - past
//   - future > 0 AND eventTs > now + future
func WithinThreshold(threshold config.PolicyTimestampThreshold, eventTs time.Time) bool {
	now := time.Now()
	if threshold.Past.Duration > 0 && eventTs.Before(now.Add(-threshold.Past.Duration)) {
		return false
	}
	if threshold.Future.Duration > 0 && eventTs.After(now.Add(threshold.Future.Duration)) {
		return false
	}
	return true
}
