package action

import (
	"fmt"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy/action/eval"
	"github.com/goto/raccoon/policy/action/eval/cache"
)

const metricDroppedTotal = "policy_events_dropped_total"

// Drop is a policy action that drops events matching the configured rules.
type Drop struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewDrop creates a new Drop action with the given cache and evaluator chain.
func NewDrop(c *cache.Cache, evalChain Chain) *Drop {
	return &Drop{cache: c, evalChain: evalChain}
}

// Process evaluates the event against the policy rules. If the rules match and
// the threshold is breached the event is dropped (handled=true, OutcomeDropped).
// Otherwise it is passed through.
func (d *Drop) Process(event *pb.Event, meta eval.EventMetadata) (bool, Outcome) {
	if !d.evalChain.Run(meta, d.cache) {
		return false, OutcomePassthrough
	}
	metrics.Increment(metricDroppedTotal, fmt.Sprintf("conn_group=%s,event_type=%s", meta.ConnGroup, meta.EventType))
	return true, OutcomeDropped
}
