package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	testpb "github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil/testpb"
	"github.com/goto/raccoon/model"
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

func newOverrideAct(t *testing.T) *action.OverrideTimestamp {
	t.Helper()
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	return action.NewOverrideTimestamp(c, action.DefaultChain())
}

func staleEvent(name string) *model.EventWithMetadata {
	return &model.EventWithMetadata{
		EventName:      name,
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now().Add(-2 * time.Hour),
	}
}

func TestOverrideTimestamp_OverridesTypeOnBreachedEvent(t *testing.T) {
	result := newOverrideAct(t).Apply(context.Background(), []*model.EventWithMetadata{staleEvent("click")}, "pub-a")

	assert.Len(t, result, 1)
	assert.Empty(t, result[0].Type)
	assert.Equal(t, "click", result[0].EventName)
}

func TestOverrideTimestamp_PassthroughWhenWithinThreshold(t *testing.T) {
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].Type) // Type not overridden
}

func TestOverrideTimestamp_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	events := []*model.EventWithMetadata{staleEvent("scroll")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Equal(t, events, result)
	assert.Empty(t, result[0].Type)
}

func TestOverrideTimestamp_MixedBatch(t *testing.T) {
	events := []*model.EventWithMetadata{staleEvent("click"), staleEvent("scroll"), staleEvent("click")}
	result := newOverrideAct(t).Apply(context.Background(), events, "pub-a")

	assert.Len(t, result, 3)
	assert.Empty(t, result[0].Type)
	assert.Empty(t, result[1].Type) // scroll: no policy match
	assert.Empty(t, result[2].Type)
}

func TestOverrideTimestamp_Success(t *testing.T) {
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	act := action.NewOverrideTimestamp(c, action.DefaultChain())

	staleTime := time.Now().Add(-2 * time.Hour)
	eventProto := &testpb.Event{
		EventName:      "click",
		Product:        testpb.Product_Generic,
		EventTimestamp: timestamppb.New(staleTime),
	}
	eventBytes, err := proto.Marshal(eventProto)
	require.NoError(t, err)

	meta := &model.EventWithMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		TopicName:      "click-topic",
		Type:           "click",
		EventTimestamp: staleTime,
		EventBytes:     eventBytes,
		ProtoMsg:       eventProto,
	}

	result := act.Apply(context.Background(), []*model.EventWithMetadata{meta}, "pub-a")

	assert.Len(t, result, 1)
	assert.WithinDuration(t, time.Now(), result[0].EventTimestamp, 2*time.Second)

	updatedEventProto := &testpb.Event{}
	err = proto.Unmarshal(result[0].EventBytes, updatedEventProto)
	require.NoError(t, err)

	assert.Equal(t, "click", updatedEventProto.EventName)
	assert.WithinDuration(t, time.Now(), updatedEventProto.EventTimestamp.AsTime(), 2*time.Second)
}

func TestOverrideTimestamp_NilProtoMsgSkipped(t *testing.T) {
	c := buildOverrideCache("click", "app", "pub-a", time.Hour)
	act := action.NewOverrideTimestamp(c, action.DefaultChain())

	staleTime := time.Now().Add(-2 * time.Hour)
	meta := &model.EventWithMetadata{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		TopicName:      "click-topic",
		Type:           "click",
		EventTimestamp: staleTime,
		EventBytes:     []byte("some-bytes"),
		ProtoMsg:       nil,
	}

	result := act.Apply(context.Background(), []*model.EventWithMetadata{meta}, "pub-a")

	assert.Len(t, result, 1)
	assert.Equal(t, staleTime, result[0].EventTimestamp) // remains unchanged
}

func buildOverrideGlobalCache(past time.Duration) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceGlobal,
			Details:  config.PolicyDetails{},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: past}},
			},
		},
	})
}

func TestOverrideTimestamp_GlobalSuccess(t *testing.T) {
	c := buildOverrideGlobalCache(time.Hour)
	act := action.NewOverrideTimestamp(c, action.DefaultChain())

	staleTime := time.Now().UTC().Add(-2 * time.Hour)
	eventProto := &testpb.Event{
		EventName:      "some-event",
		Product:        testpb.Product_Generic,
		EventTimestamp: timestamppb.New(staleTime),
	}
	eventBytes, err := proto.Marshal(eventProto)
	require.NoError(t, err)

	meta := &model.EventWithMetadata{
		EventName:      "some-event",
		Product:        "app",
		Publisher:      "pub-a",
		TopicName:      "any-topic",
		Type:           "some-event",
		EventTimestamp: staleTime,
		EventBytes:     eventBytes,
		ProtoMsg:       eventProto,
	}

	result := act.Apply(context.Background(), []*model.EventWithMetadata{meta}, "pub-a")

	assert.Len(t, result, 1)
	assert.WithinDuration(t, time.Now().UTC(), result[0].EventTimestamp, 2*time.Second)

	updatedEventProto := &testpb.Event{}
	err = proto.Unmarshal(result[0].EventBytes, updatedEventProto)
	require.NoError(t, err)

	assert.Equal(t, "some-event", updatedEventProto.EventName)
	assert.WithinDuration(t, time.Now().UTC(), updatedEventProto.EventTimestamp.AsTime(), 2*time.Second)
}
