package eval

import "github.com/goto/raccoon/config"

// EventEvaluator matches event-level policies (resource = "event").
// Matching criteria: details.name, details.product, details.publisher — all fields must be present.
type EventEvaluator struct{}

// Resource declares that this evaluator handles event-scoped rules.
func (e *EventEvaluator) Resource() config.PolicyResourceType {
	return config.PolicyResourceEvent
}

// Evaluate returns EvalSkip when the event metadata is incomplete (name, product, or publisher absent).
// Otherwise it performs a single lookup using nameproductpublisher as the key and delegates
// to the Condition to decide whether the action should be applied.
func (e *EventEvaluator) Evaluate(meta EventMetadata, rules map[string]Condition) EvalResult {
	if meta.EventName == "" || meta.Product == "" || meta.Publisher == "" {
		return EvalSkip
	}
	condition, ok := rules[meta.EventName+meta.Product+meta.Publisher]
	if !ok {
		return EvalNoMatch
	}
	if condition.Breached(meta) {
		return EvalApply
	}
	return EvalSkip
}
