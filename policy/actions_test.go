package policy_test

import (
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/stretchr/testify/assert"
)

// stubAction is a test double for policy.Action.
// If handled is true it returns the given outcome and stops the chain.
type stubAction struct {
	handled bool
	outcome action.Outcome
}

func (s *stubAction) Process(_ *pb.Event, _ eval.EventMetadata) (bool, action.Outcome) {
	return s.handled, s.outcome
}

var dummyEvent = &pb.Event{EventName: "click"}
var dummyMeta = eval.EventMetadata{}

func TestPolicyChain_Process_ReturnsFirstHandledOutcome(t *testing.T) {
	chain := policy.Chain{
		&stubAction{handled: true, outcome: action.OutcomeDropped},
		&stubAction{handled: true, outcome: action.OutcomeRedirected},
	}
	assert.Equal(t, action.OutcomeDropped, chain.Process(dummyEvent, dummyMeta))
}

func TestPolicyChain_Process_SkipsNonHandlingAction(t *testing.T) {
	chain := policy.Chain{
		&stubAction{handled: false, outcome: action.OutcomePassthrough},
		&stubAction{handled: true, outcome: action.OutcomeRedirected},
	}
	assert.Equal(t, action.OutcomeRedirected, chain.Process(dummyEvent, dummyMeta))
}

func TestPolicyChain_Process_PassthroughWhenNoActionHandles(t *testing.T) {
	chain := policy.Chain{
		&stubAction{handled: false},
		&stubAction{handled: false},
	}
	assert.Equal(t, action.OutcomePassthrough, chain.Process(dummyEvent, dummyMeta))
}

func TestPolicyChain_Process_PassthroughWhenEmpty(t *testing.T) {
	assert.Equal(t, action.OutcomePassthrough, policy.Chain{}.Process(dummyEvent, dummyMeta))
}
