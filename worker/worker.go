package worker

import (
	"fmt"
	"sync"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/policy"
	"github.com/goto/raccoon/publisher"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Pool spawn goroutine as much as Size that will listen to EventsChannel. On Close, wait for all data in EventsChannel to be processed.
type Pool struct {
	Size                int
	deliveryChannelSize int
	EventsChannel       <-chan collection.CollectRequest
	kafkaProducer       publisher.KafkaProducer
	wg                  sync.WaitGroup
	// policyCache holds parsed ingestion policies; nil means policy enforcement is disabled.
	policyCache *policy.Cache
	// policyChain is the ordered list of action handlers applied to every event.
	policyChain policy.HandlerChain
	// policyEvalChain is the ordered list of evaluators used inside each handler.
	policyEvalChain policy.Chain
}

// CreateWorkerPool create new Pool struct given size and EventsChannel worker.
func CreateWorkerPool(size int, eventsChannel <-chan collection.CollectRequest, deliveryChannelSize int, kafkaProducer publisher.KafkaProducer) *Pool {
	return &Pool{
		Size:                size,
		deliveryChannelSize: deliveryChannelSize,
		EventsChannel:       eventsChannel,
		kafkaProducer:       kafkaProducer,
		wg:                  sync.WaitGroup{},
	}
}

// StartWorkers initialize worker pool as much as Pool.Size
func (w *Pool) StartWorkers() {
	w.wg.Add(w.Size)
	for i := 0; i < w.Size; i++ {
		go func(workerName string) {
			logger.Info("Running worker: " + workerName)
			deliveryChan := make(chan kafka.Event, w.deliveryChannelSize)
			for request := range w.EventsChannel {
				startTimeWorker := time.Now()
				tags := fmt.Sprintf("conn_group=%s", request.ConnectionIdentifier.Group)

				metrics.Timing("event_idle_in_channel_duration_milliseconds", (startTimeWorker.Sub(request.TimePushed)).Milliseconds(), tags)

				//@TODO - Should add integration tests to prove that the worker receives the same message that it produced, on the delivery channel it created
				err := w.kafkaProducer.ProduceBulk(
					request.GetEvents(), request.ConnectionIdentifier.Group, deliveryChan,
					request.GetSentTime().AsTime(), request.TimeConsumed, startTimeWorker,
				)

				if request.AckFunc != nil {
					request.AckFunc(err)
				}

				totalErr := 0
				if err != nil {
					for _, err := range err.(publisher.BulkError).Errors {
						if err != nil {
							logger.Errorf("[worker] Fail to publish message to kafka %v", err)
							totalErr++
						}
					}
				}
				lenBatch := int64(len(request.GetEvents()))
				logger.Debug(fmt.Sprintf("Success sending messages, %v", lenBatch-int64(totalErr)))
			}
			w.wg.Done()
		}(fmt.Sprintf("worker-%d", i))
	}
}

// WithPolicy attaches a policy cache, handler chain, and evaluator chain to the pool.
// When set, the policy chain is applied to every event before it is sent to Kafka.
// Call this before StartWorkers.
func (w *Pool) WithPolicy(cache *policy.Cache, handlerChain policy.HandlerChain, evalChain policy.Chain) *Pool {
	w.policyCache = cache
	w.policyChain = handlerChain
	w.policyEvalChain = evalChain
	return w
}

// applyPolicy runs each event in the batch through the policy handler chain.
// Events with an OutcomePassthrough are kept; dropped or redirected events are excluded
// from the returned slice. Returns the original slice unchanged when policy is disabled.
func (w *Pool) applyPolicy(events []*pb.Event, connGroup string) []*pb.Event {
	if w.policyCache == nil || len(w.policyChain) == 0 {
		return events
	}
	filtered := make([]*pb.Event, 0, len(events))
	for _, event := range events {
		evalStart := time.Now()
		meta := policy.ExtractMetadata(
			event,
			connGroup,
			config.PolicyCfg.PublisherMapping,
			config.EventDistribution.PublisherPattern,
		)
		outcome := w.policyChain.Process(event, meta, w.policyCache, w.policyEvalChain)
		metrics.Timing(
			policy.MetricEvalDuration,
			time.Since(evalStart).Milliseconds(),
			fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, meta.EventType),
		)
		if outcome == policy.OutcomePassthrough {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// FlushWithTimeOut waits for the workers to complete the pending the messages
// to be flushed to the publisher within a timeout.
// Returns true if waiting timed out, meaning not all the events could be processed before this timeout.
func (w *Pool) FlushWithTimeOut(timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		w.wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
