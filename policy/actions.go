package policy

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy/action"
	"github.com/goto/raccoon/policy/action/eval"
)

// Action is implemented by each ingestion policy action (Drop, OverrideTimestamp).
// Process returns (true, outcome) when the action consumed the event, stopping the chain.
// Process returns (false, OutcomePassthrough) when the action does not apply, letting the
// next action in the chain evaluate the event.
type Action interface {
	Process(event *pb.Event, meta eval.EventMetadata) (handled bool, outcome action.Outcome)
}

// Chain is an ordered list of Actions applied to a single event.
// It runs each action in sequence and stops at the first one that handles the event.
type Chain []Action

// Process runs the event through every action in the chain.
// Returns the outcome of the first action that handles the event,
// or OutcomePassthrough if no action consumed it.
func (c Chain) Process(event *pb.Event, meta eval.EventMetadata) action.Outcome {
	for _, a := range c {
		if handled, outcome := a.Process(event, meta); handled {
			return outcome
		}
	}
	return action.OutcomePassthrough
}
