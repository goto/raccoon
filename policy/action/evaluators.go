package action

import (
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

// Evaluator is implemented by every step in the evaluation chain.
// Resource declares the policy resource type this evaluator handles so the
// chain can look up the correct conditions from the cache.
// Evaluate returns (result, found): found=true means a rule was found for this
// specific event key and the chain must stop; found=false means no rule matched
// and the chain should continue to the next evaluator.
type Evaluator interface {
	Resource() config.PolicyResourceType
	Evaluate(meta eval.EventMetadata, rules map[string]eval.Condition) (result bool, found bool)
}

// Chain is an ordered list of evaluators. Run stops at the first evaluator that
// finds a matching rule key (found=true) and returns its condition result.
// If no evaluator finds a rule for the event, returns false.
type Chain []Evaluator

// Run fetches the rules for each evaluator's resource type from the cache,
// then evaluates in order. Stops as soon as an evaluator finds a matching rule
// key (found=true) and returns the condition result. If an event-level rule is
// found but the condition is not breached, the chain stops and returns false —
// topic-level rules are never consulted.
func (c Chain) Run(meta eval.EventMetadata, ruleCache *cache.Cache) bool {
	for _, ev := range c {
		rules := ruleCache.Get(ev.Resource())
		result, found := ev.Evaluate(meta, rules)
		if found {
			return result
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
