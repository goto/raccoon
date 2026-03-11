package policy

import "time"

// EvalResult is the outcome of a single evaluator.
type EvalResult int

const (
	// EvalNoMatch means the evaluator found no applicable policy; the chain continues.
	EvalNoMatch EvalResult = iota
	// EvalApply means a matching policy was found and its threshold was breached; the action should be applied.
	EvalApply
	// EvalSkip means a matching policy was found but the threshold was NOT breached; the action should NOT be applied.
	EvalSkip
)

// Evaluator is implemented by every step in the policy evaluation chain.
// Evaluate receives the event metadata and the policies for the current action+resource bucket.
// It returns EvalApply, EvalSkip, or EvalNoMatch.
type Evaluator interface {
	Evaluate(meta EventMetadata, policies []PolicyConfig) EvalResult
}

// Chain is an ordered list of evaluators. It runs evaluators in order and
// stops at the first non-NoMatch result. Returns true if the action should be
// applied (EvalApply), false otherwise.
type Chain []Evaluator

// Run executes the evaluator chain against the given metadata and action.
// Policies for each resource bucket (event, topic) are fetched from the cache.
// Returns true → caller should apply the action; false → pass the event through.
func (c Chain) Run(meta EventMetadata, cache *Cache, action ActionType) bool {
	for _, ev := range c {
		var policies []PolicyConfig
		switch ev.(type) {
		case *EventEvaluator:
			policies = cache.Get(action, ResourceEvent)
		case *TopicEvaluator:
			policies = cache.Get(action, ResourceTopic)
		default:
			policies = nil
		}
		result := ev.Evaluate(meta, policies)
		switch result {
		case EvalApply:
			return true
		case EvalSkip:
			return false
			// EvalNoMatch: continue to next evaluator
		}
	}
	return false
}

// WithinThreshold returns true when the event timestamp is within the allowed window,
// i.e. no action should be taken. It returns false (threshold breached) when:
//   - past  > 0 AND eventTs < now - past
//   - future > 0 AND eventTs > now + future
func WithinThreshold(threshold EventTimestampThreshold, eventTs time.Time) bool {
	now := time.Now()
	if threshold.Past.Duration > 0 && eventTs.Before(now.Add(-threshold.Past.Duration)) {
		return false
	}
	if threshold.Future.Duration > 0 && eventTs.After(now.Add(threshold.Future.Duration)) {
		return false
	}
	return true
}

// DefaultChain returns the standard evaluation chain: EventEvaluator → TopicEvaluator.
func DefaultChain() Chain {
	return Chain{
		&EventEvaluator{},
		&TopicEvaluator{},
	}
}
