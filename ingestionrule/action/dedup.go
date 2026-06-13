package action

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/dedup/protoutil"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	metricNameEventDeserializationError    = "event_deserialization_error"
	metricNameEventDeserializationLatency  = "event_deserialization_latency"
	metricNameEventDuplicateCheckerLatency = "event_duplicate_checker_latency"
)

const (
	reasonProtoClassNotFound = "proto class not found"
	reasonStencilParseError  = "stencil parse error"
	reasonPublisherNotFound  = "publisher not found"

	reasonUserIDNotFound    = "userID not found"
	reasonUserIDTypeInvalid = "userID type invalid"

	reasonSessionIDNotFound    = "sessionID not found"
	reasonSessionIDTypeInvalid = "sessionID type invalid"

	reasonEventNameNotFound    = "event_name not found"
	reasonEventNameTypeInvalid = "event_name type invalid"

	reasonEventTimestampNotFound    = "event_timestamp not found"
	reasonEventTimestampTypeInvalid = "event_timestamp type invalid"
)

const (
	protoFieldEventName      = "event_name"
	protoFieldProduct        = "product"
	protoFieldEventTimestamp = "event_timestamp"
	protoFieldEventGUID      = "event_guid"
)

// DuplicateChecker defines the capability to verify event uniqueness.
type DuplicateChecker interface {
	AreDuplicates(ctx context.Context, events []cache.EventMetadata) ([]bool, error)
	HealthCheck() error
	Close() error
}

// processState holds the state of each event being processed.
type processState struct {
	// event is the original event being processed.
	event *pb.Event
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
		meta, err := d.extractMetadata(event, connGroup)
		metrics.Timing(metricNameEventDeserializationLatency, time.Since(startDeserialize).Milliseconds(), fmt.Sprintf("conn_group=%s", connGroup))

		if err != nil {
			logger.Errorf("dedup: failed to extract metadata: %v", err)
			states[i] = processState{event: event, isValid: false}
			continue
		}

		if meta.EventGUID == "" || meta.Publisher == "" {
			logger.Errorf("dedup: missing metadata fields: %+v for conn_group=%s,product=%s,event_name=%s", meta, connGroup, event.Product, event.EventName)
			states[i] = processState{event: event, isValid: false}
			continue
		}

		states[i] = processState{event: event, isValid: true}
		metadataBatch = append(metadataBatch, meta)
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

	for _, state := range states {
		if !state.isValid {
			uniqueEvents = append(uniqueEvents, state.event)
			continue
		}

		if cacheErr != nil {
			if resultIdx == 0 { // Only log once per batch
				logger.Errorf("dedup: cache batch verification failed, bypassing filter: %v", cacheErr)
			}

			uniqueEvents = append(uniqueEvents, state.event)
			resultIdx++
			continue
		}

		isDuplicate := isDuplicateResults[resultIdx]
		resultIdx++

		if isDuplicate {
			metrics.Increment(metricEventLossCount, fmt.Sprintf("reason=DEDUP_POLICY,event_name=%s,product=%s,conn_group=%s,event_type=%s", state.event.EventName, state.event.Product, connGroup, state.event.Type))
			continue
		}

		uniqueEvents = append(uniqueEvents, state.event)
	}

	return uniqueEvents
}

// extractMetadata deserializes dynamic protobuf payloads using Stencil and handles identity field extractions.
func (d *Dedup) extractMetadata(event *pb.Event, connGroup string) (cache.EventMetadata, error) {
	protoClass, ok := config.DedupCfg.ProtoClassNameMapping[event.Type]
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonProtoClassNotFound, event.Type, event.Product, event.EventName))
		return cache.EventMetadata{}, fmt.Errorf("failed to find proto class for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	publisher, ok := config.PolicyCfg.PublisherMapping[connGroup]
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonPublisherNotFound, event.Type, event.Product, event.EventName))
		return cache.EventMetadata{}, fmt.Errorf("failed to publisher for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	parsedMsg, err := d.stencil.Client.Parse(protoClass, event.EventBytes)
	if err != nil {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonStencilParseError, event.Type, event.Product, event.EventName))
		return cache.EventMetadata{}, fmt.Errorf("failed to parse proto class for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	ref := parsedMsg.ProtoReflect()

	eventGUID, err := d.getStringField(ref, protoFieldEventGUID, connGroup, event, protoFieldEventGUID, reasonEventNameNotFound, reasonEventNameTypeInvalid)
	if err != nil {
		return cache.EventMetadata{}, err
	}

	return cache.EventMetadata{
		EventGUID: eventGUID,
		Publisher: publisher,
	}, nil
}

// getStringField is a helper function to safely extract, convert to string, and handle error telemetry for identifier fields.
func (d *Dedup) getStringField(
	ref protoreflect.Message,
	path string,
	connGroup string,
	event *pb.Event,
	fieldName string,
	reasonNotFound string,
	reasonTypeInvalid string,
) (string, error) {
	rawVal, ok := protoutil.GetFieldValue(ref, strings.Split(path, "."))
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonNotFound, event.Type, event.Product, event.EventName))
		return "", fmt.Errorf("failed to find %s for conn_group=%s,event_type=%s,product=%s,event_name=%s", fieldName, connGroup, event.Type, event.Product, event.EventName)
	}

	val, err := cast.ToStringE(rawVal)
	if err != nil {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonTypeInvalid, event.Type, event.Product, event.EventName))
		return "", fmt.Errorf("%s field type is not convertible to string for conn_group=%s,event_type=%s,product=%s,event_name=%s: %w", fieldName, connGroup, event.Type, event.Product, event.EventName, err)
	}

	return val, nil
}
