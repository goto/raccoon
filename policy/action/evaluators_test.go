package action_test

import (
	"testing"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
)

// stubEvaluator is a test double for action.Evaluator.
// result is the condition result; found controls whether the evaluator signals
// a rule was found (stopping the chain).
type stubEvaluator struct {
	resource config.PolicyResourceType
	result   bool
	found    bool
}

func (s *stubEvaluator) Resource() config.PolicyResourceType { return s.resource }
func (s *stubEvaluator) Evaluate(_ eval.EventMetadata, _ map[string]eval.Condition) (bool, bool) {
	return s.result, s.found
}

func emptyCache() *cache.Cache { return cache.NewCache(nil) }

func TestChain_Run_ApplyWhenFirstEvaluatorMatches(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: true, found: true},
	}
	assert.True(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenOnlySkip(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false, found: false},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_StopsAtEventRuleFoundEvenWhenNotBreached(t *testing.T) {
	// Event evaluator finds a rule (found=true) but condition not breached (result=false).
	// Topic evaluator would return true — but chain must NOT reach it.
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false, found: true},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: true, found: true},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FallsThruToTopicWhenEventNotFound(t *testing.T) {
	// Event evaluator finds no rule (found=false) — chain continues to topic evaluator.
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false, found: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: true, found: true},
	}
	assert.True(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenAllNoMatch(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false, found: true},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: false, found: true},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenNoRulesFound(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false, found: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: false, found: false},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_EmptyChainReturnsFalse(t *testing.T) {
	assert.False(t, action.Chain{}.Run(eval.EventMetadata{}, emptyCache()))
}
