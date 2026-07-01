package action

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/model"
)

// SchemaCache defines a read-only interface for schema lookups.
type SchemaCache interface {
	Get(topic string) (string, bool)
}

// StencilClient defines the interface for parsing event bytes using stencil.
type StencilClient interface {
	Parse(className string, data []byte) (protoreflect.ProtoMessage, error)
}

// OverrideTimestamp evaluates OVERRIDE_TIMESTAMP policies. When a matching rule's
// condition is breached, the event's timestamp is updated both in the metadata and
// inside its serialized event bytes using Stencil.
type OverrideTimestamp struct {
	cache         *cache.Cache
	evalChain     Chain
	stencilClient StencilClient
	schemaCache   SchemaCache
}

// NewOverrideTimestamp creates an OverrideTimestamp action.
// The cache must be pre-populated with OVERRIDE_TIMESTAMP rules only.
func NewOverrideTimestamp(
	c *cache.Cache,
	evalChain Chain,
	stencilClient StencilClient,
	schemaCache SchemaCache,
) *OverrideTimestamp {
	return &OverrideTimestamp{
		cache:         c,
		evalChain:     evalChain,
		stencilClient: stencilClient,
		schemaCache:   schemaCache,
	}
}

const metricNameEventOverrideErrorCount = "event_override_error_count"

const protoFieldEventTimestamp = "event_timestamp"

// Apply evaluates every event in the batch. Events whose condition is breached
// have their event timestamp updated to the current time, both in the metadata struct
// and in the serialized event bytes.
func (o *OverrideTimestamp) Apply(_ context.Context, events []*model.EventWithMetadata, connGroup string) []*model.EventWithMetadata {
	start := time.Now()

	for _, meta := range events {
		if o.evalChain.Run(*meta, o.cache) {
			logger.Debugf("[override_timestamp.Apply] overriding timestamp: event_name=%s, product=%s, publisher=%s, topic=%s, event_timestamp=%s", meta.EventName, meta.Product, meta.Publisher, meta.TopicName, meta.EventTimestamp)

			protoClass, found := o.schemaCache.Get(meta.TopicName)
			if !found {
				protoClass, found = config.DedupCfg.ProtoClassNameMapping[meta.Type]
			}

			if !found {
				continue
			}

			parsedMsg, err := o.stencilClient.Parse(protoClass, meta.EventBytes)
			if err != nil {
				continue
			}

			ref := parsedMsg.ProtoReflect()
			fieldDesc := ref.Descriptor().Fields().ByName(protoFieldEventTimestamp)
			if fieldDesc == nil {
				logger.Errorf("[override_timestamp.Apply] event_timestamp field not found in schema for %s", protoClass)
				metrics.Increment(metricNameEventOverrideErrorCount, fmt.Sprintf("reason=timestamp_field_not_found,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
				continue
			}

			serverProcessTime := time.Now().UTC()
			tsProto := timestamppb.New(serverProcessTime)

			ref.Set(fieldDesc, protoreflect.ValueOfMessage(tsProto.ProtoReflect()))

			newBytes, err := proto.Marshal(parsedMsg)
			if err != nil {
				logger.Errorf("[override_timestamp.Apply] failed to serialize overridden event for publisher=%s,event_type=%s,product=%s,event_name=%s,platform=%s,app_version=%s: %v", meta.Publisher, meta.Type, meta.Product, meta.EventName, meta.Platform, meta.AppVersion, err)
				metrics.Increment(metricNameEventOverrideErrorCount, fmt.Sprintf("reason=serialize_error,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
				continue
			}

			meta.EventBytes = newBytes
			meta.EventTimestamp = serverProcessTime

			metrics.Increment(metricEventOverrideCount, fmt.Sprintf("reason=OVERRIDE_TIMESTAMP,conn_group=%s,event_type=%s,product=%s,event_name=%s", connGroup, meta.Type, meta.Product, meta.EventName))
		}
	}

	metrics.Timing(MetricEvalLatency, time.Since(start).Milliseconds(), fmt.Sprintf("action=OVERRIDE_TIMESTAMP,conn_group=%s", connGroup))

	return events
}
