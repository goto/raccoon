package action

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"

	"github.com/goto/raccoon/metrics"
)

// DedupService defines the contract for event deduplication.
type DedupService interface {
	Apply(events []*pb.Event, connGroup string) []*pb.Event
}

// Dedup is a policy action that deduplicates events using the dedup service.
type Dedup struct {
	dedupSvc DedupService
}

// NewDedup creates a new Dedup action with the given dedup service.
func NewDedup(dedupSvc DedupService) *Dedup {
	return &Dedup{dedupSvc: dedupSvc}
}

// Apply delegates event deduplication to the underlying dedup service.
func (d *Dedup) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()

	events = d.dedupSvc.Apply(events, connGroup)

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEDUP,conn_group=%s", connGroup))

	return events
}
