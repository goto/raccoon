package deserialization

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cast"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// getStringField is a helper function to safely extract and convert to string.
func getStringField(
	ref protoreflect.Message,
	fieldName, connGroup string,
	meta model.EventWithMetadata,
	isMandatory bool,
) (string, error) {
	rawVal, err := protoutil.GetFieldValue(ref, strings.Split(fieldName, "."), isMandatory)
	if err != nil {
		return "", err
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
		logger.Debugf(
			"field %q is empty for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s",
			fieldName, meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.Platform, meta.AppVersion,
		)
	}

	return val, nil
}

// getEnumField is a helper function to safely extract the string name of an enum field.
func getEnumField(ref protoreflect.Message, fieldName string) (string, error) {
	rawVal, err := protoutil.GetEnumStringValue(ref, fieldName)
	if err != nil {
		return "", err
	}

	return rawVal, nil
}

// getTimestampField is a helper function to safely extract a time.Time from a google.protobuf.Timestamp field.
func getTimestampField(ref protoreflect.Message, fieldName string) (time.Time, error) {
	ts, err := protoutil.GetTimestampFieldValue(ref, fieldName)
	if err != nil {
		return time.Time{}, err
	}

	return ts, nil
}

// isPublisherWhitelisted checks if the publisher is whitelisted for the given config.
func isPublisherWhitelisted(whitelist []string, publisher string) bool {
	if len(whitelist) == 0 {
		return true
	}

	return slices.Contains(whitelist, publisher)
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
