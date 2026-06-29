package deserialization

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// MetricEventLossCount is the service-level alias for the shared event loss count metric.
const MetricEventLossCount = action.MetricEventLossCount

const (
	metricNameEventDeserializationLatency    = "event_deserialization_latency"
	metricNameEventDeserializationEmptyField = "event_deserialization_empty_field_total"
)

const (
	protoFieldEventGUID      = "meta.event_guid"
	protoFieldEventName      = "event_name"
	protoFieldEventProduct   = "product"
	protoFieldEventTimestamp = "event_timestamp"
	protoFieldPlatform       = "meta.device.operating_system"
	protoFieldAppVersion     = "meta.app.version"
)

// SchemaRegistryCache is an interface for retrieving proto class names for topics.
type SchemaRegistryCache interface {
	// Get retrieve proto class name for a given topic
	Get(topic string) (string, bool)
	// Close closes the schema cache
	Close()
	// HealthCheck checks the health of the schema registry
	HealthCheck() error
}

type Deserializer struct {
	stencil              schemaregistry.StencilClient
	cache                SchemaRegistryCache
	excludeEventTypeList []string
}

func NewDeserializer(stencil schemaregistry.StencilClient, cache SchemaRegistryCache) *Deserializer {
	return &Deserializer{
		stencil:              stencil,
		cache:                cache,
		excludeEventTypeList: config.DeserializationCfg.ExcludeEventTypeList,
	}
}

// Deserialize extracts metadata for the entire batch of events and tracks error and latency metrics.
func (d *Deserializer) Deserialize(
	events []*pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string,
) []*model.EventWithMetadata {
	metadataBatch := make([]*model.EventWithMetadata, 0, len(events))

	for _, event := range events {
		startDeserialize := time.Now()
		meta, err := d.enrichEventMetadata(event, connGroup, publisherMap, topicFormat)
		metrics.Timing(metricNameEventDeserializationLatency, time.Since(startDeserialize).Milliseconds(), fmt.Sprintf("conn_group=%s", connGroup))

		if err != nil {
			logger.Errorf("deserialization error for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s: %v",
				meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.Platform, meta.AppVersion, err)
			metrics.Increment(MetricEventLossCount, fmt.Sprintf("reason=DESERIALIZATION_ERROR,conn_group=%s,product=%s,event_name=%s,event_type=%s", connGroup, meta.Product, meta.EventName, meta.Type))

			continue
		}

		metadataBatch = append(metadataBatch, &meta)
	}

	return metadataBatch
}

// Close closes the schema cache used by the deserializer during shutdown.
func (d *Deserializer) Close() {
	if d != nil && d.cache != nil {
		d.cache.Close()
	}
}

// HealthCheck checks the health of the underlying schema registry cache/server.
func (d *Deserializer) HealthCheck() error {
	if d == nil || d.cache == nil {
		return nil
	}
	return d.cache.HealthCheck()
}

// enrichEventMetadata builds a complete EventWithMetadata for an event.
// It initializes base metadata from the event envelope and enriches it by
// parsing the inner payload (event_name, product, timestamp, and GUID) using Stencil.
//
// Returns:
//   - model.EventWithMetadata: An EventWithMetadata struct containing the extracted metadata.
//   - error: An error if any occurs during metadata extraction (e.g., proto class not found, payload parsing failure).
func (d *Deserializer) enrichEventMetadata(
	event *pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string,
) (model.EventWithMetadata, error) {
	meta := extractBaseMetadata(
		event,
		connGroup,
		publisherMap,
		topicFormat,
	)

	if d == nil || d.stencil.Client == nil {
		return meta, nil
	}

	// Skip deserialization if the event type is in the exclude list.
	if slices.Contains(d.excludeEventTypeList, meta.Type) {
		return meta, nil
	}

	var protoClass string
	var found bool
	if d.cache != nil {
		protoClass, found = d.cache.Get(meta.TopicName)
	}

	if !found {
		logger.Infof("proto class not found in cache, using proto class name from config for event_type=%s", meta.Type)

		protoClass, found = config.DedupCfg.ProtoClassNameMapping[meta.Type]
	}

	if !found {
		return meta, fmt.Errorf(
			"failed to find proto class for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s",
			meta.Publisher,
			meta.Type,
			meta.Product,
			meta.EventName,
			meta.Platform,
			meta.AppVersion,
		)
	}

	parsedMsg, err := d.stencil.Client.Parse(protoClass, meta.EventBytes)
	if err != nil {
		return meta, fmt.Errorf(
			"failed to parse event bytes for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s: %w",
			meta.Publisher,
			meta.Type,
			meta.Product,
			meta.EventName,
			meta.Platform,
			meta.AppVersion,
			err,
		)
	}

	var errs []error
	ref := parsedMsg.ProtoReflect()

	eventGUID, err := getStringField(ref, protoFieldEventGUID, connGroup, meta, true)
	if err != nil {
		errs = append(errs, err)
	} else {
		meta.EventGUID = eventGUID
	}

	if !config.DeserializationCfg.Enabled {
		return meta, errors.Join(errs...)
	}

	eventName, err := getStringField(ref, protoFieldEventName, connGroup, meta, true)
	if err != nil {
		errs = append(errs, err)
	} else {
		meta.EventName = eventName
	}

	product, err := protoutil.GetEnumStringValue(ref, protoFieldEventProduct)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to extract %q value: %w", protoFieldEventProduct, err))
	} else {
		meta.Product = product
	}

	ts, err := protoutil.GetTimestampFieldValue(ref, protoFieldEventTimestamp)
	if err != nil {
		errs = append(errs, err)
	} else {
		meta.EventTimestamp = ts
	}

	if isPublisherWhitelisted(config.DeserializationCfg.PlatformPublisherWhitelist, meta.Publisher) {
		platform, err := getStringField(ref, protoFieldPlatform, connGroup, meta, false)
		if err != nil {
			errs = append(errs, err)
		} else {
			meta.Platform = platform
		}
	}

	if isPublisherWhitelisted(config.DeserializationCfg.AppVersionPublisherWhitelist, meta.Publisher) {
		appVersion, err := getStringField(ref, protoFieldAppVersion, connGroup, meta, false)
		if err != nil {
			errs = append(errs, err)
		} else {
			meta.AppVersion = appVersion
		}
	}

	return meta, errors.Join(errs...)
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
		Product:        event.GetProduct(),
		EventName:      event.GetEventName(),
		EventTimestamp: ts,
		Type:           event.GetType(),
		Platform:       event.GetPlatform().String(),
		AppVersion:     event.GetAppVersion(),
		IsExclusive:    event.GetIsExclusive(),
		EventBytes:     event.GetEventBytes(),
	}
}

// isPublisherWhitelisted checks if the publisher is whitelisted for the given config.
func isPublisherWhitelisted(whitelist []string, publisher string) bool {
	if len(whitelist) == 0 {
		return true
	}

	return slices.Contains(whitelist, publisher)
}

// getStringField is a helper function to safely extract and convert to string.
func getStringField(
	ref protoreflect.Message,
	fieldName, connGroup string,
	meta model.EventWithMetadata,
	isMandatory bool,
) (string, error) {
	rawVal, err := protoutil.GetFieldValue(ref, strings.Split(fieldName, "."), isMandatory)
	if err != nil {
		return "", fmt.Errorf("failed to extract %q value: %w", fieldName, err)
	}

	val, err := cast.ToStringE(rawVal)
	if err != nil {
		return "", fmt.Errorf("failed to convert %q value to string: %w", fieldName, err)
	}

	if val == "" {
		metrics.Increment(
			metricNameEventDeserializationEmptyField,
			fmt.Sprintf("field_name=%s,conn_group=%s,event_type=%s,product=%s,event_name=%s", fieldName, connGroup, meta.Type, meta.Product, meta.EventName),
		)
		logger.Infof("field %q is empty", fieldName)
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
