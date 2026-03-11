package action

import (
	"fmt"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/policy/action/eval"
)

// ExtractMetadata builds an EventMetadata from a protobuf Event, its connection
// group, a conn_group→publisher map, and the topic format string
// (e.g. "clickstream-%s-log").
func ExtractMetadata(event *pb.Event, connGroup string, publisherMap map[string]string, topicFormat string) eval.EventMetadata {
	var ts time.Time
	if event.GetEventTimestamp() != nil {
		ts = event.GetEventTimestamp().AsTime()
	}
	return eval.EventMetadata{
		EventType:      event.GetType(),
		EventName:      event.GetEventName(),
		Product:        event.GetProduct(),
		ConnGroup:      connGroup,
		Publisher:      ResolvePublisher(connGroup, publisherMap),
		TopicName:      fmt.Sprintf(topicFormat, event.GetType()),
		EventTimestamp: ts,
	}
}

// ResolvePublisher maps a conn_group to a publisher name using the provided map.
// Falls back to the conn_group itself when no mapping is found.
func ResolvePublisher(connGroup string, publisherMap map[string]string) string {
	if pub, ok := publisherMap[connGroup]; ok {
		return pub
	}
	return connGroup
}
