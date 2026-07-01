package eval

import (
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/model"
)

// GlobalEvaluator matches global-level policies (resource = "global").
// Matching criteria: no condition is required — this rule matches every event.
type GlobalEvaluator struct{}

// Resource declares that this evaluator handles global-level rules.
func (t *GlobalEvaluator) Resource() config.PolicyResourceType {
	return config.PolicyResourceGlobal
}

// Evaluate returns (false, false) when no rule exists in the rules map.
// Returns (condition.Breached(meta), true) when a rule is found — the chain stops
// here regardless of whether the condition is breached.
func (t *GlobalEvaluator) Evaluate(meta model.EventWithMetadata, rules map[string]Condition) (bool, bool) {
	condition, ok := rules[""]
	if !ok {
		return false, false
	}

	return condition.Breached(meta), true
}
