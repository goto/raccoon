package action_test

import (
	"testing"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
)

// stubEvaluator is a test double for action.Evaluator that returns a fixed result.
type stubEvaluator struct {
	resource config.PolicyResourceType
	result   bool
}

func (s *stubEvaluator) Resource() config.PolicyResourceType { return s.resource }
func (s *stubEvaluator) Evaluate(_ eval.EventMetadata, _ map[string]eval.Condition) bool {
	return s.result
}

func emptyCache() *cache.Cache { return cache.NewCache(nil) }

func TestChain_Run_ApplyWhenFirstEvaluatorMatches(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: true},
	}
	assert.True(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenOnlySkip(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_SkipDoesNotShortCircuit(t *testing.T) {
	// Skip does not stop evaluation; a later Apply should still win.
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: true},
	}
	assert.True(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_ContinuesToNextOnNoMatch(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: true},
	}
	assert.True(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenAllNoMatch(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: false},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_FalseWhenAllSkipAndNoMatch(t *testing.T) {
	chain := action.Chain{
		&stubEvaluator{resource: config.PolicyResourceEvent, result: false},
		&stubEvaluator{resource: config.PolicyResourceTopic, result: false},
	}
	assert.False(t, chain.Run(eval.EventMetadata{}, emptyCache()))
}

func TestChain_Run_EmptyChainReturnsFalse(t *testing.T) {
	assert.False(t, action.Chain{}.Run(eval.EventMetadata{}, emptyCache()))
}
