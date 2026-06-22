package model

import (
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
)

// EventWithMetadata holds the attributes extracted from an incoming event and its
// connection context. It is the primary input to the policy evaluation chain.
type EventWithMetadata struct {
	// EventName is the resolved event name, derived from the deserialized payload.
	EventName string
	// Product is the resolved product, derived from the deserialized payload.
	Product string
	// Publisher is the resolved publisher, derived from connection group mapping.
	Publisher string
	// TopicName is the Kafka topic this event would be produced to by default.
	TopicName string
	// EventGUID is the unique identifier of the event.
	EventGUID string

	// Extracted fields
	// Type denotes the event type used for the Kafka topic distribution.
	Type string
	// EventTimestamp is the event's own timestamp.
	EventTimestamp time.Time
	// EventBytes holds the raw bytes of the serialized event payload.
	EventBytes []byte
	// Platform denotes the client platform generating the event (e.g., Android, iOS, Flutter).
	Platform pb.Platform
	// AppVersion denotes the version of the client application generating the event.
	AppVersion string
	// IsExclusive indicates whether the event is mirrored to both MQTT and Websocket.
	IsExclusive bool
}
