package protoutil

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// GetFieldValue retrieves the value of a field from a protobuf message by its path.
// It returns the field value and a boolean indicating whether the field was found.
func GetFieldValue(msg protoreflect.Message, path []string) (any, bool) {
	currentMsg := msg

	for i, fieldName := range path {
		fieldDesc := currentMsg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fieldDesc == nil {
			return nil, false // Path doesn't exist
		}

		val := currentMsg.Get(fieldDesc)

		if i == len(path)-1 {
			if fieldDesc.Kind() == protoreflect.MessageKind {
				return nil, false
			}

			return val.Interface(), true
		}

		if fieldDesc.Kind() != protoreflect.MessageKind || !val.Message().IsValid() {
			return nil, false
		}

		currentMsg = val.Message()
	}

	return nil, false
}

// GetEnumStringValue safely extracts the string name of an enum field from a dynamic message.
// It returns an empty string if the field doesn't exist, is not an enum, or is not set.
// If the enum number is invalid, it falls back to returning the number as a string.
func GetEnumStringValue(msg protoreflect.Message, fieldName string) string {
	// Get the descriptor for the field by its name.
	fieldDesc := msg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		// Field does not exist in the proto definition.
		return ""
	}

	// Verify that the field is actually an enum.
	if fieldDesc.Kind() != protoreflect.EnumKind {
		// The field is not an enum type.
		return ""
	}

	// Get the value and look up its string name.
	value := msg.Get(fieldDesc)
	enumNumber := value.Enum()

	valueDescriptor := fieldDesc.Enum().Values().ByNumber(enumNumber)
	if valueDescriptor == nil {
		// The number is not valid for this enum. Fall back to the number.
		return fmt.Sprintf("%d", enumNumber)
	}

	return string(valueDescriptor.Name())
}

// GetTimestampFieldValue safely extracts a time.Time from a dynamic message field representing a google.protobuf.Timestamp.
func GetTimestampFieldValue(msg protoreflect.Message, fieldName string) (time.Time, error) {
	fieldDesc := msg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		return time.Time{}, fmt.Errorf("field %q not found", fieldName)
	}

	val := msg.Get(fieldDesc)
	// Ensure the value is a valid message and is of the expected type.
	if !val.IsValid() || fieldDesc.Kind() != protoreflect.MessageKind || val.Message().Descriptor().FullName() != "google.protobuf.Timestamp" {
		return time.Time{}, fmt.Errorf("field %q is not a valid google.protobuf.Timestamp", fieldName)
	}

	tsMsg := val.Message()

	return protoTimestampToTime(tsMsg)
}

// protoTimestampToTime takes a dynamic message representing a
// google.protobuf.Timestamp and converts it to a time.Time.
// It returns an error if the message does not have the expected 'seconds' and
// 'nanos' fields, or if the fields are not valid.
func protoTimestampToTime(msg protoreflect.Message) (time.Time, error) {
	secondsDesc := msg.Descriptor().Fields().ByName("seconds")
	nanosDesc := msg.Descriptor().Fields().ByName("nanos")

	if secondsDesc == nil || nanosDesc == nil {
		return time.Time{}, errors.New("message does not have the expected 'seconds' and 'nanos' fields")
	}

	seconds := msg.Get(secondsDesc).Int()
	nanos := msg.Get(nanosDesc).Int()

	return time.Unix(seconds, nanos), nil
}
