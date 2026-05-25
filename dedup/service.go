package dedup

import (
	"context"
	"fmt"
	"strings"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"

	"github.com/goto/raccoon/cache"
	"github.com/goto/raccoon/clients"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/lib/protoutil"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	metricNameEventDeserializationError = "event_deserialization_error"
)

const (
	reasonStencilParseError = "stencil parse error"
	reasonPublisherNotFound = "publisher not found"
	reasonUserIDNotFound    = "userID not found"
	reasonSessionIDNotFound = "sessionID not found"
	reasonEventGUIDNotFound = "eventGUID not found"
)

// DuplicateChecker defines the capability to verify event uniqueness.
//
//go:generate  mockery --name=DuplicateChecker --with-expecter --output=./mocks
type DuplicateChecker interface {
	IsDuplicate(ctx context.Context, event cache.EventMetadata) (bool, error)
	Close() error
}

type Service struct {
	stencil          clients.StencilClient
	publisherMapping map[string]string
	checker          DuplicateChecker
}

func NewService(ctx context.Context) (*Service, error) {
	if !config.DedupCfg.Enabled {
		return nil, nil
	}

	stencil, err := clients.NewStencilClient()
	if err != nil {
		return nil, err
	}

	return &Service{
		stencil:          stencil,
		publisherMapping: config.PolicyCfg.PublisherMapping,
		checker:          cache.NewStore(ctx),
	}, nil
}

func (s *Service) Apply(events []*pb.Event, connGroup string) []*pb.Event {
	if s == nil {
		return events
	}

	if _, isWhitelisted := config.DedupCfg.WhitelistConnGroup[connGroup]; !isWhitelisted {
		return events
	}

	uniqueEvents := make([]*pb.Event, 0, len(events))

	for _, event := range events {
		meta, err := s.extractMetadata(event, connGroup)
		if err != nil {
			logger.Errorf("failed to deserialize event: %s", err)
			continue
		}

		// Perform the atomic cache lookup & set transaction
		isDuplicate, cacheErr := s.checker.IsDuplicate(context.Background(), meta)
		if cacheErr != nil {
			// Fail-open strategy: If Redis fails, log the error but let the event pass
			// to guarantee ingestion availability under cache infrastructure pressure.
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
func (s *Service) extractMetadata(event *pb.Event, connGroup string) (cache.EventMetadata, error) {
	protoClass := config.DedupCfg.ProtoClassNameMapping[event.Type]
	parsedMsg, err := s.stencil.Client.Parse(protoClass, event.EventBytes)
	if err != nil {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,event_name=%s,product=%s", connGroup, reasonStencilParseError, event.Type, event.EventName, event.Product))
		return cache.EventMetadata{}, err
	}

	publisher, ok := s.publisherMapping[connGroup]
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,event_name=%s,product=%s", connGroup, reasonPublisherNotFound, event.Type, event.EventName, event.Product))
		return cache.EventMetadata{}, fmt.Errorf("failed to find publisher for %q conn_group=", connGroup)
	}

	userIdentifier := config.DedupCfg.PublisherIdentifierMapping[publisher]
	ref := parsedMsg.ProtoReflect()

	rawUserID, ok := protoutil.GetFieldValue(ref, strings.Split(userIdentifier.UserID, "."))
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,event_name=%s,product=%s", connGroup, reasonUserIDNotFound, event.Type, event.EventName, event.Product))
		return cache.EventMetadata{}, fmt.Errorf("failed to find userID for %q conn_group", connGroup)
	}

	rawSessionID, ok := protoutil.GetFieldValue(ref, strings.Split(userIdentifier.SessionID, "."))
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,event_name=%s,product=%s", connGroup, reasonSessionIDNotFound, event.Type, event.EventName, event.Product))
		return cache.EventMetadata{}, fmt.Errorf("failed to find sessionID for %q conn_group", connGroup)
	}

	const eventGUIDProtoField = "meta.event_guid"

	rawEventGUID, ok := protoutil.GetFieldValue(ref, strings.Split(eventGUIDProtoField, "."))
	if !ok {
		metrics.Increment(metricNameEventDeserializationError,
			fmt.Sprintf("conn_group=%s,reason=%s,event_type=%s,event_name=%s,product=%s", connGroup, reasonEventGUIDNotFound, event.Type, event.EventName, event.Product))
		return cache.EventMetadata{}, fmt.Errorf("failed to find eventGUID for %q conn_group", connGroup)
	}

	userID, _ := rawUserID.(string)
	sessionID, _ := rawSessionID.(string)
	eventGuid, _ := rawEventGUID.(string)

	return cache.EventMetadata{
		EventGUID: eventGuid,
		SessionID: sessionID,
		UserID:    userID,
	}, nil
}

func (s *Service) Close() {
	if s.checker != nil {
		if err := s.checker.Close(); err != nil {
			logger.Errorf("failed to close duplicate checker: %v", err)
		}
	}
}
