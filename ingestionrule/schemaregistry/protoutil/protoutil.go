package protoutil

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// GetFieldValue retrieves the value of a field from a protobuf message by its path.
// It returns the field value and an error if any occurs during traversal.
// If isMandatory is set to true, it returns an error if the final leaf field value is empty.
func GetFieldValue(msg protoreflect.Message, path []string, isMandatory bool) (any, error) {
	if len(path) == 0 {
		return nil, errors.New("path cannot be empty")
	}

	currentMsg := msg

	for i, fieldName := range path {
		fieldDesc := currentMsg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
		if fieldDesc == nil {
			if !isMandatory {
				return nil, nil
			}

			return nil, fmt.Errorf("field %q not found in path", fieldName)
		}

		val := currentMsg.Get(fieldDesc)

		if i == len(path)-1 {
			if fieldDesc.Kind() == protoreflect.MessageKind {
				return nil, fmt.Errorf("final field %q is a message, expected a scalar value", fieldName)
			}

			if isMandatory && isEmptyValue(val, fieldDesc) {
				return nil, fmt.Errorf("mandatory field %q is empty", fieldName)
			}

			return val.Interface(), nil
		}

		if fieldDesc.Kind() != protoreflect.MessageKind {
			return nil, fmt.Errorf("intermediate field %q is not a message", fieldName)
		}

		currentMsg = val.Message()
	}

	return nil, errors.New("unexpected error resolving path")
}

// GetEnumStringValue safely extracts the string name of a root-level enum field from a dynamic message.
// It returns the string name of the enum value, or an error if the field doesn't exist, isn't an enum, or resolves to an empty string.
// If the enum number is not defined in the schema, it falls back to returning the number as a string with no error.
//
// Note: This function only works for top-level fields of the provided message and does not support nested enum paths.
func GetEnumStringValue(msg protoreflect.Message, fieldName string) (string, error) {
	fieldDesc := msg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		return "", fmt.Errorf("field %q does not exist", fieldName)
	}

	if fieldDesc.Kind() != protoreflect.EnumKind {
		return "", fmt.Errorf("field %q is not an enum type (got %s)", fieldName, fieldDesc.Kind().String())
	}

	value := msg.Get(fieldDesc)
	enumNumber := value.Enum()

	valueDescriptor := fieldDesc.Enum().Values().ByNumber(enumNumber)
	if valueDescriptor == nil {
		return fmt.Sprintf("%d", enumNumber), nil
	}

	return string(valueDescriptor.Name()), nil
}

// GetTimestampFieldValue safely extracts a time.Time from a dynamic message field representing a google.protobuf.Timestamp.
func GetTimestampFieldValue(msg protoreflect.Message, fieldName string) (time.Time, error) {
	fieldDesc := msg.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		return time.Time{}, fmt.Errorf("field %q not found", fieldName)
	}

	val := msg.Get(fieldDesc)
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

// isEmptyValue checks if a protobuf field value is considered empty.
func isEmptyValue(val protoreflect.Value, fd protoreflect.FieldDescriptor) bool {
	if !val.IsValid() {
		return true
	}

	if fd.IsList() {
		return val.List().Len() == 0
	}

	if fd.IsMap() {
		return val.Map().Len() == 0
	}

	switch fd.Kind() {
	case protoreflect.StringKind:
		return val.String() == ""
	case protoreflect.BytesKind:
		return len(val.Bytes()) == 0
	}

	return false
}
