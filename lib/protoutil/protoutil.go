package protoutil

import (
	"fmt"

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
