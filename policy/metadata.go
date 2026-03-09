package policy

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
)

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

// ExtractMetadata builds EventMetadata from a protobuf Event, its connection group,
// a conn_group→publisher map, and the topic format string (e.g. "clickstream-%s-log").
func ExtractMetadata(event *pb.Event, connGroup string, publisherMap map[string]string, topicFormat string) EventMetadata {
	var ts time.Time
	if event.GetEventTimestamp() != nil {
		ts = event.GetEventTimestamp().AsTime()
	}

	return EventMetadata{
		EventType:      event.GetType(),
		EventName:      event.GetEventName(),
		Product:        event.GetProduct(),
		ConnGroup:      connGroup,
		Publisher:      ResolvePublisher(connGroup, publisherMap),
		TopicName:      fmt.Sprintf(topicFormat, event.GetType()),
		EventTimestamp: ts,
	}
}
