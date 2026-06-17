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

func buildOverrideCache(name, product, publisher string, past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

const overrideEventType = "invalid-et"

func newOverrideAct(t *testing.T) *action.OverrideTimestamp {
	t.Helper()
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	return action.NewOverrideTimestamp(c, action.DefaultChain(), overrideEventType, schemaregistry.StencilClient{})
}

func staleEvent(name string) *pb.Event {
	return &pb.Event{
		EventName:      name,
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}
}

func TestOverrideTimestamp_OverridesTypeOnBreachedEvent(t *testing.T) {
	result := newOverrideAct(t).Apply(context.Background(), []*pb.Event{staleEvent("click")}, "pub-a")

	assert.Len(t, result, 1)
	assert.Equal(t, overrideEventType, result[0].GetType())
	assert.Equal(t, "click", result[0].GetEventName())
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())}}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].GetType()) // Type not overridden
}

func TestOverrideTimestamp_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	events := []*pb.Event{staleEvent("scroll")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].GetType())
}

func TestOverrideTimestamp_MixedBatch(t *testing.T) {
	events := []*pb.Event{staleEvent("click"), staleEvent("scroll"), staleEvent("click")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Len(t, result, 3)
	assert.Equal(t, overrideEventType, result[0].GetType())
	assert.Empty(t, result[1].GetType()) // scroll: no policy match
	assert.Equal(t, overrideEventType, result[2].GetType())
}

func TestOverrideTimestamp_UsesDeserializedPayload(t *testing.T) {
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

	c := buildOverrideCache("deserialized-click", "clickstream", "pub-a", time.Hour)
	o := action.NewOverrideTimestamp(c, action.DefaultChain(), overrideEventType, schemaregistry.StencilClient{Client: ms})

	events := []*pb.Event{{
		Type:           "component",
		EventName:      "wrapper-click",
		Product:        "wrapper-app",
		EventBytes:     []byte("some-bytes"),
		EventTimestamp: timestamppb.New(time.Now()), // wrapper timestamp is fresh (won't trigger override)
	}}

	result := o.Apply(context.Background(), events, "pub-a")

	assert.Len(t, result, 1)
	assert.Equal(t, overrideEventType, result[0].GetType()) // should trigger override because deserialized timestamp is stale
}
