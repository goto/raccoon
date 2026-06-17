package action_test

import (
	"context"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/dedup/schemaregistry"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func buildDeactivateEventCache(name, product, publisher string) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
}

func newDeactivate(c *cache.Cache) *action.Deactivate {
	return action.NewDeactivate(c, action.DefaultChain(), schemaregistry.StencilClient{})
}

func TestDeactivate_DropsMatchingEvent(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{{
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{{
		EventName:      "scroll",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsAlwaysRegardlessOfTimestamp(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	// Both a current and a very old event must be dropped — DEACTIVE is unconditional.
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now().Add(-365 * 24 * time.Hour))},
	}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_FiltersMixedBatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "scroll", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
		{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())},
	}
	result := newDeactivate(c).Apply(context.Background(), events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].GetEventName())
}

func TestDeactivate_PassthroughWhenEmptyRules(t *testing.T) {
	c := cache.NewCache(nil)
	events := []*pb.Event{{EventName: "click", Product: "app", EventTimestamp: timestamppb.New(time.Now())}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsMatchingTopicRule(t *testing.T) {
	config.EventDistribution.PublisherPattern = "clickstream-%s-log"
	c := cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceTopic,
			Details:  config.PolicyDetails{Name: "clickstream-page-log"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
	events := []*pb.Event{{
		Type:           "page",
		EventName:      "click",
		Product:        "app",
		EventTimestamp: timestamppb.New(time.Now()),
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_UsesDeserializedPayload(t *testing.T) {
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}

	metaMsg := &mockMessage{
		fields: map[string]any{
			"event_guid": "test-guid-123",
		},
	}

	staleTime := time.Now()
	tsMsg := &mockMessage{
		fullName: "google.protobuf.Timestamp",
		fields: map[string]any{
			"seconds": staleTime.Unix(),
			"nanos":   int32(staleTime.Nanosecond()),
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

	c := buildDeactivateEventCache("deserialized-click", "clickstream", "pub-a")
	d := action.NewDeactivate(c, action.DefaultChain(), schemaregistry.StencilClient{Client: ms})

	events := []*pb.Event{{
		Type:           "component",
		EventName:      "wrapper-click",
		Product:        "wrapper-app",
		EventBytes:     []byte("some-bytes"),
		EventTimestamp: timestamppb.New(time.Now()),
	}}

	assert.Empty(t, d.Apply(context.Background(), events, "pub-a"))
}
