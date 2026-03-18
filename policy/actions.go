package policy

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
)

// Action is implemented by each ingestion policy action (Drop, OverrideTimestamp).
// Apply receives the full event batch and connection group, returns only the events
// that should continue to normal ingestion. Each action owns its iteration and
// I/O, allowing batch-level optimisations such as a single ProduceBulk call.
type Action interface {
	Apply(events []*pb.Event, connGroup string) []*pb.Event
}

// Chain is an ordered pipeline of Actions. Each action receives the output of the
// previous one, so Drop filters first, then OverrideTimestamp processes the remainder.
type Chain []Action

// Apply runs the event batch through every action in sequence.
// Each action removes the events it consumed; the final slice contains only
// events that no action handled (passthrough).
func (c Chain) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	for _, a := range c {
		events = a.Apply(events, connGroup)
	}
	return events
}
