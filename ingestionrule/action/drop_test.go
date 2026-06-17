package action_test

import (
	"context"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/schemaregistry"
)

func buildDropCache(name, product, publisher string, past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

// newDrop is a test helper. Cache publisher must match the connGroup passed to
// Apply because ResolvePublisher falls back to connGroup when no mapping is set.
func newDrop(c *cache.Cache) *action.Drop {
	return action.NewDrop(c, action.DefaultChain(), schemaregistry.StencilClient{})
}

func TestDrop_DropsBreachedEvents(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Empty(t, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenWithinThreshold(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "scroll",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_PassthroughWhenMetadataIncomplete(t *testing.T) {
	// Product is empty → EventEvaluator returns false → passthrough.
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	events := []*pb.Event{{
		EventName:      "click",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}}
	assert.Equal(t, events, newDrop(c).Apply(context.Background(), events, "pub-a"))
}

func TestDrop_FiltersMixedBatch(t *testing.T) {
	c := buildDropCache("click", "app", "pub-a", time.Hour)
	staleTs := timestamppb.New(time.Now().Add(-2 * time.Hour))
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: staleTs},
		{EventName: "scroll", Product: "app", EventTimestamp: staleTs},
		{EventName: "click", Product: "app", EventTimestamp: staleTs},
	}
	result := newDrop(c).Apply(context.Background(), events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].GetEventName())
}

func TestDrop_UsesDeserializedPayload(t *testing.T) {
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}

	staleTime := time.Now().Add(-2 * time.Hour)
	tsMsg := &mockMessage{
		fullName: "google.protobuf.Timestamp",
		fields: map[string]any{
			"seconds": staleTime.Unix(),
			"nanos":   int32(staleTime.Nanosecond()),
		},
	}

	metaMsg := &mockMessage{
		fields: map[string]any{
			"event_guid": "test-guid-123",
		},
	}

	parsedMsg := &mockMessage{
		fields: map[string]any{
			"meta":            metaMsg,
			"event_name":      "deserialized-click",
			"product":         protoreflect.EnumNumber(1),
			"event_timestamp": tsMsg,
		},
	}

	ms := &mockStencilClient{
		parseFunc: func(className string, data []byte) (protoreflect.ProtoMessage, error) {
			return parsedMsg, nil
		},
	}

	c := buildDropCache("deserialized-click", "clickstream", "pub-a", time.Hour)
	d := action.NewDrop(c, action.DefaultChain(), schemaregistry.StencilClient{Client: ms})

	events := []*pb.Event{{
		Type:           "component",
		EventName:      "wrapper-click",
		Product:        "wrapper-app",
		EventBytes:     []byte("some-bytes"),
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)), // breached
	}}

	assert.Empty(t, d.Apply(context.Background(), events, "pub-a"))
}
