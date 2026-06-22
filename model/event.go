package model

import (
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
)

// EventWithMetadata holds the attributes extracted from an incoming event and its
// connection context. It is the primary input to the policy evaluation chain.
type EventWithMetadata struct {
	// Event is the original protobuf Event
	Event *pb.Event
	// EventName is the protobuf Event.EventName field.
	EventName string
	// Product is the protobuf Event.Product field.
	Product string
	// Publisher is the resolved publisher, derived from ConnGroup via the mapping.
	Publisher string
	// TopicName is the Kafka topic this event would be produced to by default.
	TopicName string
	// EventTimestamp is the event's own timestamp.
	EventTimestamp time.Time
	// EventGUID is the unique identifier of the event.
	EventGUID string
}
