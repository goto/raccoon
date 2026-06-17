package action

import (
	"context"
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

// DuplicateChecker defines the capability to verify event uniqueness.
type DuplicateChecker interface {
	AreDuplicates(ctx context.Context, events []cache.EventMetadata) ([]bool, error)
	HealthCheck() error
	Close() error
}

// processState holds the state of each event being processed.
type processState struct {
	// isValid indicates whether the event has valid metadata and should be checked for duplication.
	isValid bool
}

// Dedup is a policy action that deduplicates events using duplicate checker and schema registry.
type Dedup struct {
	stencil schemaregistry.StencilClient
	checker DuplicateChecker
}

// NewDedup creates a new Dedup action with the given dependencies.
func NewDedup(stencil schemaregistry.StencilClient, checker DuplicateChecker) *Dedup {
	return &Dedup{
		stencil: stencil,
		checker: checker,
	}
}

// Apply performs event deduplication.
func (d *Dedup) Apply(ctx context.Context, events []*pb.Event, connGroup string) []*pb.Event {
	start := time.Now()
	defer func() {
		metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=DEDUP,conn_group=%s", connGroup))
	}()

	if d == nil || d.checker == nil {
		return events
	}

	if _, isWhitelisted := config.DedupCfg.WhitelistConnGroup[connGroup]; !isWhitelisted {
		return events
	}

	states := make([]processState, len(events))
	metadataBatch := make([]cache.EventMetadata, 0, len(events))

	for i, event := range events {
		startDeserialize := time.Now()
		meta, err := ExtractMetadata(event, connGroup, config.PolicyCfg.PublisherMapping, config.EventDistribution.PublisherPattern, d.stencil)
		metrics.Timing(metricNameEventDeserializationLatency, time.Since(startDeserialize).Milliseconds(), fmt.Sprintf("conn_group=%s", connGroup))

		if err != nil {
			logger.Errorf("dedup: failed to extract metadata: %v", err)
			metrics.Increment(metricNameEventDeserializationError, fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, getErrorReason(err), event.Type, event.Product, event.EventName))
			states[i] = processState{isValid: false}
			continue
		}

		if meta.EventGUID == "" || meta.Publisher == "" {
			logger.Errorf("dedup: missing metadata fields: %+v for conn_group=%s,product=%s,event_name=%s", meta, connGroup, event.Product, event.EventName)
			states[i] = processState{isValid: false}
			continue
		}

		states[i] = processState{isValid: true}
		metadataBatch = append(metadataBatch, cache.EventMetadata{
			Publisher: meta.Publisher,
			EventGUID: meta.EventGUID,
			EventName: meta.EventName,
			Product:   meta.Product,
		})
	}

	var isDuplicateResults []bool
	var cacheErr error

	if len(metadataBatch) > 0 {
		startCheck := time.Now()
		isDuplicateResults, cacheErr = d.checker.AreDuplicates(ctx, metadataBatch)
		metrics.Timing(metricNameEventDuplicateCheckerLatency, time.Since(startCheck).Milliseconds(), fmt.Sprintf("conn_group=%s", connGroup))
	}

	uniqueEvents := make([]*pb.Event, 0, len(events))
	resultIdx := 0 // Tracks our position in the isDuplicateResults slice

	for i, state := range states {
		event := events[i]
		if !state.isValid {
			uniqueEvents = append(uniqueEvents, event)
			continue
		}

		if cacheErr != nil {
			if resultIdx == 0 { // Only log once per batch
				logger.Errorf("dedup: cache batch verification failed, bypassing filter: %v", cacheErr)
			}

			uniqueEvents = append(uniqueEvents, event)
			resultIdx++
			continue
		}

		isDuplicate := isDuplicateResults[resultIdx]
		meta := metadataBatch[resultIdx]
		resultIdx++

		if isDuplicate {
			metrics.Increment(metricEventLossCount, fmt.Sprintf("reason=DEDUP_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", meta.EventName, meta.Product, connGroup, event.Type))
			continue
		}

		uniqueEvents = append(uniqueEvents, event)
	}

	return uniqueEvents
}
