package action

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// OverrideTimestamp evaluates OVERRIDE_TIMESTAMP policies. When a matching rule's
// condition is breached, the event's timestamp is updated both in the metadata and
// inside its serialized event bytes using the pre-parsed ProtoMsg.
type OverrideTimestamp struct {
	cache     *cache.Cache
	evalChain Chain
}

// NewOverrideTimestamp creates an OverrideTimestamp action.
// The cache must be pre-populated with OVERRIDE_TIMESTAMP rules only.
func NewOverrideTimestamp(
	c *cache.Cache,
	evalChain Chain,
) *OverrideTimestamp {
	return &OverrideTimestamp{
		cache:     c,
		evalChain: evalChain,
	}
}

const protoFieldEventTimestamp = "event_timestamp"

// Apply evaluates every event in the batch. Events whose condition is breached
// have their event timestamp updated to the current time, both in the metadata struct
// and in the serialized event bytes.
func (o *OverrideTimestamp) Apply(_ context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
	start := time.Now()

	for _, meta := range events {
		if ok, _ := o.evalChain.Run(*meta, o.cache); ok {
			parsedMsg := meta.ProtoMsg
			if parsedMsg == nil {
				continue
			}

			ref := parsedMsg.ProtoReflect()
			fieldDesc := ref.Descriptor().Fields().ByName(protoFieldEventTimestamp)
			if fieldDesc == nil {
				logger.Errorf("[override_timestamp.Apply] event_timestamp field not found in schema for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s", meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.Platform, meta.AppVersion)
				metrics.Increment(metricEventOverrideCount, fmt.Sprintf("type=timestamp_field_not_found,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
				continue
			}

			serverProcessTime := time.Now().UTC()
			tsProto := timestamppb.New(serverProcessTime)

			ref.Set(fieldDesc, protoreflect.ValueOfMessage(tsProto.ProtoReflect()))

			newBytes, err := proto.Marshal(parsedMsg)
			if err != nil {
				logger.Errorf("[override_timestamp.Apply] failed to serialize overridden event for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s: %v", meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.Platform, meta.AppVersion, err)
				metrics.Increment(metricEventOverrideCount, fmt.Sprintf("type=serialize_error,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
				continue
			}

			logger.Debugf(
				"[override_timestamp.Apply] overriding timestamp: event_name=%s, product=%s, publisher=%s, topic=%s, app_version=%s, platform=%s, original_event_timestamp=%s, new_event_timestamp=%s",
				meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.AppVersion, meta.Platform, meta.EventTimestamp, serverProcessTime,
			)

			meta.EventBytes = newBytes
			meta.EventTimestamp = serverProcessTime

			metrics.Increment(metricEventOverrideCount, fmt.Sprintf("type=success,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
		}
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=OVERRIDE_TIMESTAMP,conn_group=%s", connGroup))

	return events
}
