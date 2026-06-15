package protoutil

import (
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
