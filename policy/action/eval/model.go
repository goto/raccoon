package eval

import "time"

// EventMetadata holds the attributes extracted from an incoming event and its
// connection context. It is the primary input to the policy evaluation chain.
type EventMetadata struct {
	// EventType is the protobuf Event.Type field (used to derive topic name).
	EventType string
	// EventName is the protobuf Event.EventName field.
	EventName string
	// Product is the protobuf Event.Product field.
	Product string
	// ConnGroup is the connection group from the request header.
	ConnGroup string
	// Publisher is the resolved publisher, derived from ConnGroup via the mapping.
	Publisher string
	// TopicName is the Kafka topic this event would be produced to by default.
	TopicName string
	// EventTimestamp is the event's own timestamp.
	EventTimestamp time.Time
}
