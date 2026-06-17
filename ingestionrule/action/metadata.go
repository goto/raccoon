package action

import (
	"fmt"
	"strings"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/protoutil"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/action/eval"
	"github.com/goto/raccoon/logger"
	"github.com/spf13/cast"
	"google.golang.org/protobuf/reflect/protoreflect"
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

	reasonEventGUIDNotFound    = "event_guid not found"
	reasonEventGUIDTypeInvalid = "event_guid type invalid"

	reasonEventNameNotFound    = "event_name not found"
	reasonEventNameTypeInvalid = "event_name type invalid"

	reasonProductNotFound    = "product not found"
	reasonProductTypeInvalid = "product type invalid"

	reasonEventTimestampNotFound    = "event_timestamp not found"
	reasonEventTimestampTypeInvalid = "event_timestamp type invalid"
)

const (
	protoFieldEventGUID    = "meta.event_guid"
	protoFieldEventName    = "event_name"
	protoFieldEventProduct = "product"
)

// ExtractMetadataWithPayload builds an EventMetadata using the deserialized payload (parsed using Stencil)
// for event_name, product, and event_timestamp, returning an error if parsing fails.
func ExtractMetadata(
	event *pb.Event,
	connGroup string,
	publisherMap map[string]string,
	topicFormat string,
	stencil schemaregistry.StencilClient,
) (eval.EventMetadata, error) {
	meta := eval.EventMetadata{
		EventType: event.GetType(),
		ConnGroup: connGroup,
		Publisher: resolvePublisher(connGroup, publisherMap),
		TopicName: fmt.Sprintf(topicFormat, event.GetType()),
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

	if eventGUID, err := getStringField(ref, protoFieldEventGUID, protoFieldEventGUID, reasonEventGUIDNotFound, reasonEventGUIDTypeInvalid); err == nil {
		meta.EventGUID = eventGUID
	}

	if eventName, err := getStringField(ref, protoFieldEventName, protoFieldEventName, reasonEventNameNotFound, reasonEventNameTypeInvalid); err == nil {
		meta.EventName = eventName
	}

	if product := protoutil.GetEnumStringValue(ref, protoFieldEventProduct); product != "" {
		meta.Product = product
	}

	if ts, err := protoutil.GetTimestampFieldValue(ref, "event_timestamp"); err == nil {
		meta.EventTimestamp = ts
	}

	return meta, nil
}

// getStringField is a helper function to safely extract and convert to string.
func getStringField(
	ref protoreflect.Message,
	path string,
	fieldName string,
	reasonNotFound string,
	reasonTypeInvalid string,
) (string, error) {
	rawVal, ok := protoutil.GetFieldValue(ref, strings.Split(path, "."))
	if !ok {
		return "", fmt.Errorf("%s", reasonNotFound)
	}

	val, err := cast.ToStringE(rawVal)
	if err != nil {
		return "", fmt.Errorf("%s: %w", reasonTypeInvalid, err)
	}

	return val, nil
}

func getErrorReason(err error) string {
	if err == nil {
		return ""
	}
	errStr := err.Error()
	reasons := []string{
		reasonProtoClassNotFound,
		reasonStencilParseError,
		reasonPublisherNotFound,
		reasonEventGUIDNotFound,
		reasonEventGUIDTypeInvalid,
		reasonEventNameNotFound,
		reasonEventNameTypeInvalid,
		reasonProductNotFound,
		reasonProductTypeInvalid,
		reasonEventTimestampNotFound,
		reasonEventTimestampTypeInvalid,
	}
	for _, r := range reasons {
		if strings.Contains(errStr, r) {
			return r
		}
	}
	return "unknown error"
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
