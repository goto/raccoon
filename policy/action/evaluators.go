package action

import (
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

// Evaluator is implemented by every step in the evaluation chain.
// Resource declares the policy resource type this evaluator handles so the
// chain can look up the correct conditions from the cache.
type Evaluator interface {
	Resource() config.PolicyResourceType
	Evaluate(meta eval.EventMetadata, rules map[string]eval.Condition) eval.EvalResult
}

// Chain is an ordered list of evaluators. Run stops at the first non-NoMatch result.
// Returns true → the action should be applied; false → pass the event through.
type Chain []Evaluator

// Run fetches the rules for each evaluator's resource type from the cache,
// then evaluates in order until a conclusive match is found.
// Returns true only when an evaluator returns EvalApply; false if no evaluator matched.
func (c Chain) Run(meta eval.EventMetadata, ruleCache *cache.Cache) bool {
	for _, ev := range c {
		rules := ruleCache.Get(ev.Resource())
		if ev.Evaluate(meta, rules) == eval.EvalApply {
			return true
		}
	}
	return false
}

// DefaultChain returns the standard evaluation chain: EventEvaluator → TopicEvaluator.
func DefaultChain() Chain {
	return Chain{
		&eval.EventEvaluator{},
		&eval.TopicEvaluator{},
	}
}
