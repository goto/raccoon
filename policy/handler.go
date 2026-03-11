package policy

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
)

// Outcome represents the result of processing an event through the HandlerChain.
type Outcome int

const (
	// OutcomePassthrough means the event should proceed to normal ingestion.
	OutcomePassthrough Outcome = iota
	// OutcomeDropped means the event was dropped by a policy action.
	OutcomeDropped
	// OutcomeRedirected means the event was forwarded to the override topic.
	OutcomeRedirected
)

// Handler is implemented by each action handler in the chain.
// Handle returns (true, outcome) if it consumed the event, or (false, _) to pass to the next handler.
type Handler interface {
	Handle(event *pb.Event, meta EventMetadata, cache *Cache, evalChain Chain) (handled bool, outcome Outcome)
}

// HandlerChain is the ordered list of action handlers applied to every event.
// The standard order is: DropHandler → OverrideTimestampHandler → (passthrough).
type HandlerChain []Handler

// Process runs the event through every handler in sequence and returns the final outcome.
// The first handler that returns handled=true stops the chain.
func (hc HandlerChain) Process(event *pb.Event, meta EventMetadata, cache *Cache, evalChain Chain) Outcome {
	for _, h := range hc {
		if handled, outcome := h.Handle(event, meta, cache, evalChain); handled {
			return outcome
		}
	}
	return OutcomePassthrough
}
