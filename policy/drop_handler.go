package policy

import (
	"fmt"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/metrics"
)

// DropHandler evaluates DROP policies. When a matching policy is breached,
// the event is discarded and no further handlers are invoked.
type DropHandler struct{}

// Handle checks if a DROP policy applies to the event.
// Returns (true, OutcomeDropped) when the event is dropped.
// Returns (false, _) when no DROP policy applies, allowing the chain to continue.
func (d *DropHandler) Handle(event *pb.Event, meta EventMetadata, cache *Cache, evalChain Chain) (bool, Outcome) {
	if !evalChain.Run(meta, cache, ActionDrop) {
		return false, OutcomePassthrough
	}
	metrics.Increment(MetricDroppedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	return true, OutcomeDropped
}
