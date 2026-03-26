package eval

import "github.com/goto/raccoon/config"

// EventEvaluator matches event-level policies (resource = "event").
// Matching criteria: details.name, details.product, details.publisher — all fields must be present.
type EventEvaluator struct{}

// Resource declares that this evaluator handles event-scoped rules.
func (e *EventEvaluator) Resource() config.PolicyResourceType {
	return config.PolicyResourceEvent
}

// Evaluate returns (false, false) when metadata is incomplete or no rule exists
// for this event key. Returns (condition.Breached(), true) when a rule is found —
// the chain stops here regardless of whether the condition is breached.
func (e *EventEvaluator) Evaluate(meta EventMetadata, rules map[string]Condition) (bool, bool) {
	if meta.EventName == "" || meta.Product == "" || meta.Publisher == "" {
		return false, false
	}
	condition, ok := rules[meta.EventName+meta.Product+meta.Publisher]
	if !ok {
		return false, false
	}
	return condition.Breached(meta), true
}
