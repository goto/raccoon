package action

import (
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

// Outcome represents the final result of processing an event through an action.
type Outcome int

const (
	// OutcomePassthrough means the event should proceed to normal ingestion.
	OutcomePassthrough Outcome = iota
	// OutcomeDropped means the event was discarded by a DROP policy.
	OutcomeDropped
	// OutcomeRedirected means the event was forwarded to the override topic.
	OutcomeRedirected
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
// then evaluates in order until a conclusive result is reached.
func (c Chain) Run(meta eval.EventMetadata, ruleCache *cache.Cache) bool {
	for _, ev := range c {
		rules := ruleCache.Get(ev.Resource())
		switch ev.Evaluate(meta, rules) {
		case eval.EvalApply:
			return true
		case eval.EvalSkip:
			return false
			// EvalNoMatch: continue to next evaluator
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
