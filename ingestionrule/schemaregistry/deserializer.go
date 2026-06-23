package schemaregistry

import (
	"fmt"
	"strings"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// MetricEventLossCount is the service-level alias for the shared event loss count metric.
const MetricEventLossCount = action.MetricEventLossCount

const (
	metricNameEventDeserializationLatency = "event_deserialization_latency"
)

const (
	protoFieldEventGUID      = "meta.event_guid"
	protoFieldEventName      = "event_name"
	protoFieldEventProduct   = "product"
	protoFieldEventTimestamp = "event_timestamp"
)

// DeserializeEvents extracts metadata for the entire batch of events and tracks error and latency metrics.
func DeserializeEvents(
	events []*pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string,
	stencil StencilClient,
) []*model.EventWithMetadata {
	metadataBatch := make([]*model.EventWithMetadata, 0, len(events))

	for _, event := range events {
		startDeserialize := time.Now()
		meta, err := enrichEventMetadata(event, connGroup, publisherMap, topicFormat, stencil)
		metrics.Timing(metricNameEventDeserializationLatency, time.Since(startDeserialize).Milliseconds(), fmt.Sprintf("conn_group=%s", connGroup))

		if err != nil {
			logger.Errorf("deserialization error: %v", err)
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DESERIALIZATION_ERROR,conn_group=%s,product=%s,event_name=%s,event_type=%s", connGroup, meta.Product, meta.EventName, meta.Type))
		}

		metadataBatch = append(metadataBatch, &meta)
	}

	return metadataBatch
}

// enrichEventMetadata builds a complete EventWithMetadata for an event.
// It initializes base metadata from the event envelope and enriches it by
// parsing the inner payload (event_name, product, timestamp, and GUID) using Stencil.
//
// Returns:
//   - model.EventWithMetadata: An EventWithMetadata struct containing the extracted metadata.
//   - error: An error if any occurs during metadata extraction (e.g., proto class not found, payload parsing failure).
func enrichEventMetadata(
	event *pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string,
	stencil StencilClient,
) (model.EventWithMetadata, error) {
	meta := extractBaseMetadata(
		event,
		connGroup,
		publisherMap,
		topicFormat,
	)

	if stencil.Client == nil {
		return meta, nil
	}

	protoClass, ok := config.DedupCfg.ProtoClassNameMapping[event.Type]
	if !ok {
		return meta, fmt.Errorf("failed to find proto class for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	parsedMsg, err := stencil.Client.Parse(protoClass, event.EventBytes)
	if err != nil {
		return meta, fmt.Errorf("failed to publisher for conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, event.Type, event.Product, event.EventName)
	}

	ref := parsedMsg.ProtoReflect()

	eventName, err := getStringField(ref, protoFieldEventName, protoFieldEventName, meta)
	if err != nil {
		return meta, err
	}

	meta.EventName = eventName

	product, err := protoutil.GetEnumStringValue(ref, protoFieldEventProduct)
	if err != nil {
		return meta, fmt.Errorf("failed to extract %q value for publisher=%s,product=%s,event_name=%s: %w", protoFieldEventProduct, meta.Publisher, meta.Product, meta.EventName, err)
	}

	// normalize across iOS/Android variants (e.g. "My_App" → "myapp")
	meta.Product = strings.ReplaceAll(strings.ToLower(product), "_", "")

	ts, err := protoutil.GetTimestampFieldValue(ref, protoFieldEventTimestamp)
	if err != nil {
		return meta, err
	}

	meta.EventTimestamp = ts

	eventGUID, err := getStringField(ref, protoFieldEventGUID, protoFieldEventGUID, meta)
	if err != nil {
		return meta, err
	}

	meta.EventGUID = eventGUID

	return meta, nil
}

// extractBaseMetadata extracts base metadata from an event.
func extractBaseMetadata(
	event *pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string) model.EventWithMetadata {
	var ts time.Time
	if event.GetEventTimestamp() != nil {
		ts = event.GetEventTimestamp().AsTime()
	}

	return model.EventWithMetadata{
		TopicName:      fmt.Sprintf(topicFormat, event.GetType()),
		Publisher:      resolvePublisher(connGroup, publisherMap),
		Product:        strings.ReplaceAll(strings.ToLower(event.GetProduct()), "_", ""), // normalize across iOS/Android variants (e.g. "My_App" → "myapp")
		EventName:      event.GetEventName(),
		EventTimestamp: ts,
		Type:           event.GetType(),
		Platform:       event.GetPlatform(),
		AppVersion:     event.GetAppVersion(),
		IsExclusive:    event.GetIsExclusive(),
		EventBytes:     event.GetEventBytes(),
	}
}

// getStringField is a helper function to safely extract and convert to string.
func getStringField(
	ref protoreflect.Message,
	path string,
	fieldName string,
	meta model.EventWithMetadata,
) (string, error) {
	rawVal, err := protoutil.GetFieldValue(ref, strings.Split(path, "."))
	if err != nil {
		return "", fmt.Errorf("failed to extract %q value for publisher=%s,product=%s,event_name=%s: %w", fieldName, meta.Publisher, meta.Product, meta.EventName, err)
	}

	val, err := cast.ToStringE(rawVal)
	if err != nil {
		return "", fmt.Errorf("failed to convert %q value to string for publisher=%s,product=%s,event_name=%s: %w", fieldName, meta.Publisher, meta.Product, meta.EventName, err)
	}

	return val, nil
}

// resolvePublisher maps a conn_group to a publisher name using the provided map.
// Falls back to the conn_group itself when no mapping is found.
func resolvePublisher(connGroup string, publisherMap map[string]string) string {
	if pub, ok := publisherMap[connGroup]; ok {
		return pub
	}

	logger.Errorf("policy: no publisher mapping found for conn_group %q, falling back to conn_group", connGroup)
	return connGroup
}
