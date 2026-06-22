package action

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// DuplicateChecker defines the capability to verify event uniqueness.
type DuplicateChecker interface {
	AreDuplicates(ctx context.Context, events []cache.EventWithMetadata) ([]bool, error)
	HealthCheck() error
	Close() error
}

// processState holds the state of each event being processed.
type processState struct {
	// isValid indicates whether the event has valid metadata and should be checked for duplication.
	isValid bool
}

// Dedup is a policy action that deduplicates events using duplicate checker.
type Dedup struct {
	checker DuplicateChecker
}

// NewDedup creates a new Dedup action with the given dependencies.
func NewDedup(checker DuplicateChecker) *Dedup {
	return &Dedup{
		checker: checker,
	}
}

// Apply performs event deduplication.
func (d *Dedup) Apply(ctx context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
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
	metadataBatch := make([]cache.EventWithMetadata, 0, len(events))

	for i, meta := range events {
		if meta.EventGUID == "" || meta.Publisher == "" {
			logger.Errorf("dedup: missing metadata fields: %+v for conn_group=%s,product=%s,event_name=%s", meta, connGroup, meta.Event.Product, meta.Event.EventName)
			states[i] = processState{isValid: false}
			continue
		}

		states[i] = processState{isValid: true}
		metadataBatch = append(metadataBatch, cache.EventWithMetadata{
			Publisher: meta.Publisher,
			EventGUID: meta.EventGUID,
			EventName: meta.EventName,
			Product:   meta.Product,
		})
	}

	var isDuplicateResults []bool
	var cacheErr error

	if len(metadataBatch) > 0 {
		isDuplicateResults, cacheErr = d.checker.AreDuplicates(ctx, metadataBatch)
	}

	uniqueEvents := make([]*model.EventWithMetadata, 0, len(events))
	resultIdx := 0 // Tracks our position in the isDuplicateResults slice

	for i, state := range states {
		meta := events[i]
		if !state.isValid {
			uniqueEvents = append(uniqueEvents, meta)
			continue
		}

		if cacheErr != nil {
			if resultIdx == 0 { // Only log once per batch
				logger.Errorf("dedup: cache batch verification failed, bypassing filter: %v", cacheErr)
			}

			uniqueEvents = append(uniqueEvents, meta)
			resultIdx++
			continue
		}

		isDuplicate := isDuplicateResults[resultIdx]
		resMeta := metadataBatch[resultIdx]
		resultIdx++

		if isDuplicate {
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DEDUP_POLICY,publisher=%s,product=%s,event_name=%s", resMeta.Publisher, resMeta.Product, resMeta.EventName))
			continue
		}

		uniqueEvents = append(uniqueEvents, meta)
	}

	return uniqueEvents
}
