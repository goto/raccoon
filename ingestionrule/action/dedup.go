package action

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/dedup/protoutil"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	metricNameEventDeserializationError = "event_deserialization_error"
)

const (
	reasonProtoClassNotFound = "proto class not found"
	reasonStencilParseError  = "stencil parse error"
	reasonPublisherNotFound  = "publisher not found"

	reasonUserIDNotFound    = "userID not found"
	reasonUserIDTypeInvalid = "userID type invalid"

	reasonSessionIDNotFound    = "sessionID not found"
	reasonSessionIDTypeInvalid = "sessionID type invalid"

	reasonEventGUIDNotFound    = "eventGUID not found"
	reasonEventGUIDTypeInvalid = "eventGUID type invalid"
)

// DuplicateChecker defines the capability to verify event uniqueness.
type DuplicateChecker interface {
	IsDuplicate(ctx context.Context, event cache.EventMetadata) (bool, error)
	HealthCheck() error
	Close() error
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
func (d *Dedup) Apply(events []*pb.Event, connGroup string) []*pb.Event {
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

	uniqueEvents := make([]*pb.Event, 0, len(events))

	for _, event := range events {
		meta, err := d.extractMetadata(event, connGroup)
		if err != nil {
			logger.Errorf("dedup: failed to extract metadata: %v", err)
			uniqueEvents = append(uniqueEvents, event)
			continue
		}

		isDuplicate, cacheErr := d.checker.IsDuplicate(context.Background(), meta)
		if cacheErr != nil {
			logger.Errorf("dedup: cache verification failed, bypassing filter: %v", cacheErr)
			uniqueEvents = append(uniqueEvents, event)
			continue
		}

		if isDuplicate {
			continue
		}

		uniqueEvents = append(uniqueEvents, event)
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

	parsedMsg, err := d.stencil.Client.Parse(protoClass, event.EventBytes)
	if err != nil {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonStencilParseError, event.Type, event.Product, event.EventName))
		return cache.EventMetadata{}, fmt.Errorf("failed to parse proto class for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	userIdentifier := config.DedupCfg.IdentifierMapping[connGroup]
	ref := parsedMsg.ProtoReflect()

	userID, err := d.getStringField(ref, userIdentifier.UserID, connGroup, event, "userID", reasonUserIDNotFound, reasonUserIDTypeInvalid)
	if err != nil {
		return cache.EventMetadata{}, err
	}

	sessionID, err := d.getStringField(ref, userIdentifier.SessionID, connGroup, event, "sessionID", reasonSessionIDNotFound, reasonSessionIDTypeInvalid)
	if err != nil {
		return cache.EventMetadata{}, err
	}

	const eventGUIDProtoField = "meta.event_guid"
	eventGUID, err := d.getStringField(ref, eventGUIDProtoField, connGroup, event, "eventGUID", reasonEventGUIDNotFound, reasonEventGUIDTypeInvalid)
	if err != nil {
		return cache.EventMetadata{}, err
	}

	return cache.EventMetadata{
		EventGUID: eventGUID,
		SessionID: sessionID,
		UserID:    userID,
	}, nil
}

// getStringField is a helper function to safely extract, type-assert, and handle error telemetry for string fields.
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

	val, ok := rawVal.(string)
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,product=%s,event_name=%s", connGroup, reasonTypeInvalid, event.Type, event.Product, event.EventName))
		return "", fmt.Errorf("%s field is not a string for conn_group=%s,event_type=%s,product=%s,event_name=%s", fieldName, connGroup, event.Type, event.Product, event.EventName)
	}

	return val, nil
}
