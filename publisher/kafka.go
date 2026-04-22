package publisher

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	// Importing librd to make it work on vendor mode
	_ "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka/librdkafka"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	errUnknownTopic     = "Local: Unknown topic"           //error msg while producing a message to a topic which is not present in the kafka cluster
	errLargeMessageSize = "Broker: Message size too large" //error msg while producing a message which is larger than message.max.bytes config
)

// KafkaProducer Produce data to kafka synchronously
type KafkaProducer interface {
	// ProduceBulk message to kafka. Block until all messages are sent. Return array of error. Order is not guaranteed.
	ProduceBulk(
		events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event,
		startTimeClient, startTimeServer, startTimeWorker time.Time,
	) error

	HealthCheck() error
}

func NewKafka() (*Kafka, error) {
	kp, err := newKafkaClient(config.PublisherKafka.ToKafkaConfigMap())
	if err != nil {
		return &Kafka{}, err
	}

	topicFormat := map[bool]string{
		false: config.EventDistribution.PublisherPattern,
		true:  config.EventDistribution.PublisherPattern,
	}

	if config.ServerMQTT.Enable {
		topicFormat[false] = config.EventDistribution.NotExclusivePublisherPattern
	}

	k := &Kafka{
		kp:                     kp,
		flushInterval:          config.PublisherKafka.FlushInterval,
		topicFormat:            topicFormat,
		dlqTopicName:           config.PublisherKafka.DLQTopicName,
		eventTypePrefixMapping: config.PublisherKafka.EventTypePrefixMapping,
	}

	return k, nil
}

func NewKafkaFromClient(client Client, flushInterval int, topicFormat map[bool]string, dlqTopicName string, eventTypePrefixMapping map[string]string) *Kafka {
	return &Kafka{
		kp:                     client,
		flushInterval:          flushInterval,
		topicFormat:            topicFormat,
		dlqTopicName:           dlqTopicName,
		eventTypePrefixMapping: eventTypePrefixMapping,
	}
}

type Kafka struct {
	kp                     Client
	flushInterval          int
	topicFormat            map[bool]string
	dlqTopicName           string
	eventTypePrefixMapping map[string]string
}

type deliveryMetadata struct {
	order int
	isDLQ bool
}

func (pr *Kafka) overrideEventType(eventType string) string {
	if len(pr.eventTypePrefixMapping) == 0 || eventType == "" {
		return eventType
	}

	// Event types are expected in <prefix>-<rest> form for direct map lookup.
	prefix, rest, found := strings.Cut(eventType, "-")
	if !found {
		return eventType
	}

	targetPrefix, ok := pr.eventTypePrefixMapping[prefix]
	if !ok {
		return eventType
	}

	return targetPrefix + "-" + rest
}

func (pr *Kafka) queueToDLQ(event *pb.Event, order int, deliveryChannel chan kafka.Event) error {
	if pr.dlqTopicName == "" {
		return fmt.Errorf("dlq topic is not configured")
	}

	if event.GetEventTimestamp() == nil || event.GetEventTimestamp().AsTime().IsZero() {
		event.EventTimestamp = timestamppb.Now()
	}

	payload, err := serialization.SerializeProto(event)
	if err != nil {
		return fmt.Errorf("serialize dlq event: %w", err)
	}

	dlqTopic := pr.dlqTopicName
	message := &kafka.Message{
		Value:          payload,
		TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
		Opaque:         deliveryMetadata{order: order, isDLQ: true},
	}

	if err := pr.kp.Produce(message, deliveryChannel); err != nil {
		return fmt.Errorf("%v %s", err, dlqTopic)
	}

	return nil
}

// ProduceBulk messages to kafka. Block until all messages are sent. Return array of error. Order of Errors is guaranteed.
// DeliveryChannel needs to be exclusive. DeliveryChannel is exposed for recyclability purpose.
//
// startTimeClient: represents the event send time specified by client
// startTimeServer: represents the time when the event is received by raccoon server
// startTimeWorker: represents the time when the event is picked up by worker to be sent to kafka
func (pr *Kafka) ProduceBulk(
	events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event,
	startTimeClient, startTimeServer, startTimeWorker time.Time,
) error {
	startTimeEvents := make([]time.Time, len(events))

	errors := make([]error, len(events))
	totalProcessed := 0
	for order, event := range events {
		eventType := pr.overrideEventType(event.GetType())
		//override event type if prefix mapping exist and event type is in expected format, otherwise use the original event type
		event.Type = eventType
		topic := fmt.Sprintf(pr.topicFormat[event.GetIsExclusive()], event.Type)
		message := &kafka.Message{
			Value:          event.EventBytes,
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Opaque:         deliveryMetadata{order: order},
		}

		logger.Debugf("Clickstream-event-monitoring: event_name=%s, product=%s, type=%s, conn_group=%s, event_timestamp=%s, is_exclusive=%s",
			event.GetEventName(),
			event.GetProduct(),
			event.GetType(),
			connGroup,
			event.GetEventTimestamp().AsTime().String(),
			fmt.Sprintf("%t", event.GetIsExclusive()),
		)

		tags := fmt.Sprintf(
			"conn_group=%s,event_type=%s,topic=%s,is_exclusive=%t,app_version=%s,platform=%s",
			connGroup, event.Type, topic, event.GetIsExclusive(), event.AppVersion, event.Platform,
		)
		metrics.Increment("clickstream_event_routed_total", tags)

		startTimeEvents[order] = time.Now()

		err := pr.kp.Produce(message, deliveryChannel)
		if err != nil {
			metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s", connGroup, event.Type))
			var errorTag string
			switch err.Error() {
			case errUnknownTopic:
				errors[order] = fmt.Errorf("%v %s", err, topic)
				errorTag = "TOPIC_NOT_FOUND"
				dlqErr := pr.queueToDLQ(event, order, deliveryChannel)
				if dlqErr != nil {
					logger.Errorf("dlq publish failed on topic=%s: %v", pr.dlqTopicName, dlqErr)
					metrics.Increment("dlq_publish_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s,reason=%s", connGroup, event.Type, "produce_failed"))
				} else {
					totalProcessed++
				}
			case errLargeMessageSize:
				errors[order] = fmt.Errorf("%v %s", err, topic)
				errorTag = "MESSAGE_TOO_LARGE"
			default:
				errors[order] = err
				logger.Errorf("produce to kafka failed due to: %v on topic : %s", err, topic)
				errorTag = "unknown"
			}

			metrics.Increment("kafka_error", fmt.Sprintf("type=%s,event_type=%s,conn_group=%s",
				errorTag, event.Type, connGroup))

			if errorTag == "unknown" {
				errorTag = "KAFKA_ERROR"
			}

			tags := fmt.Sprintf("reason=%s,event_name=%s,product=%s,conn_group=%s,app_version=%s,platform=%s",
				errorTag, event.EventName, strings.ReplaceAll(strings.ToLower(event.Product), "_", ""), connGroup, event.AppVersion, event.Platform,
			)

			metrics.Increment("clickstream_data_loss", tags)
			continue
		}

		metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, event.Type))
		totalProcessed++
	}

	// Wait for deliveryChannel as many as processed
	for i := 0; i < totalProcessed; i++ {
		d := <-deliveryChannel
		m := d.(*kafka.Message)

		meta, ok := m.Opaque.(deliveryMetadata)
		if !ok {
			logger.Errorf("failed to cast kafka event opaque to delivery metadata for conn_group=%s, skipping processing the delivery report for this message", connGroup)
			continue
		}

		event := events[meta.order]
		if meta.isDLQ {
			if m.TopicPartition.Error != nil {
				logger.Errorf("dlq delivery report failed on topic=%s: %v", *m.TopicPartition.Topic, m.TopicPartition.Error)
				metrics.Increment("dlq_publish_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s,reason=%s", connGroup, event.Type, "delivery_failed"))
			} else {
				metrics.Increment("dlq_publish_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, event.Type))
			}
			continue
		}

		if m.TopicPartition.Error != nil {
			eventType := event.Type
			metrics.Decrement("kafka_messages_delivered_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_error", fmt.Sprintf("type=%s,event_type=%s,conn_group=%s", "delivery_failed", eventType, connGroup))

			tags := fmt.Sprintf("reason=%s,event_name=%s,product=%s,conn_group=%s,app_version=%s,platform=%s",
				"KAFKA_ERROR", event.EventName, strings.ReplaceAll(strings.ToLower(event.Product), "_", ""), connGroup, event.AppVersion, event.Platform,
			)
			metrics.Increment("clickstream_data_loss", tags)
			errors[meta.order] = m.TopicPartition.Error
		} else {
			startTimeEvent := startTimeEvents[meta.order]

			// granular metric, el: event level
			metrics.Timing("el_kafka_processing_duration_milliseconds", time.Since(startTimeEvent).Milliseconds(), fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, event.Type))

			// grouped metric, el: event level
			metrics.Timing("el_worker_processing_duration_milliseconds", time.Since(startTimeWorker).Milliseconds(), fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, event.Type))
			metrics.Timing("el_server_processing_duration_milliseconds", time.Since(startTimeServer).Milliseconds(), fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, event.Type))
			metrics.Timing("el_event_processing_duration_milliseconds", time.Since(startTimeClient).Milliseconds(), fmt.Sprintf("conn_group=%s,event_type=%s", connGroup, event.Type))
		}
	}

	if allNil(errors) {
		return nil
	}
	return BulkError{Errors: errors}
}

func (pr *Kafka) ReportStats() {
	for v := range pr.kp.Events() {
		switch e := v.(type) {
		case *kafka.Stats:
			var stats map[string]interface{}
			if err := json.Unmarshal([]byte(e.String()), &stats); err != nil {
				logger.Errorf("failed to unmarshal kafka stats: %v", err)
				continue
			}
			pr.reportBatchMetrics(stats)
			metrics.Gauge("kafka_local_queue_messages", stats["msg_cnt"], "")
			metrics.Gauge("kafka_local_queue_bytes", stats["msg_size"], "")
			brokersRawJson, ok := stats["brokers"]
			if !ok || brokersRawJson == nil {
				logger.Errorf("kafka broker stats missing or null brokers field")
				continue
			}
			brokers := brokersRawJson.(map[string]interface{})
			metrics.Gauge("kafka_tx_messages_total", stats["txmsgs"], "")
			metrics.Gauge("kafka_tx_messages_bytes_total", stats["txmsg_bytes"], "")
			for _, broker := range brokers {
				brokerStats := broker.(map[string]interface{})
				rttValue := brokerStats["rtt"].(map[string]interface{})
				nodeName := strings.Split(brokerStats["nodename"].(string), ":")[0]

				metrics.Gauge("kafka_brokers_tx_total", brokerStats["tx"], fmt.Sprintf("broker=%s", nodeName))
				metrics.Gauge("kafka_brokers_tx_bytes_total", brokerStats["txbytes"], fmt.Sprintf("broker=%s", nodeName))
				metrics.Gauge("kafka_brokers_rtt_average_milliseconds", rttValue["avg"], fmt.Sprintf("broker=%s", nodeName))
			}

		default:
			logger.Infof("Ignored %v \n", e)
		}
	}
}

func (pr *Kafka) reportBatchMetrics(stats map[string]interface{}) {
	topicsRaw, ok := stats["topics"].(map[string]interface{})
	if !ok || len(topicsRaw) == 0 {
		logger.Debug("No topics produced yet — skipping batch metrics")
		return
	}

	for topicName, topicData := range topicsRaw {
		topicStats, ok := topicData.(map[string]interface{})
		if !ok {
			continue
		}
		batchSizeAvg := 0.0
		if bs, ok := topicStats["batchsize"].(map[string]interface{}); ok {
			batchSizeAvg = getFloat(bs, "avg")
		}
		// Emit metrics
		metrics.Gauge("kafka_producer_batch_size_avg_bytes", batchSizeAvg, fmt.Sprintf("topic=%s", topicName))
	}
}

func getFloat(m map[string]interface{}, key string) float64 {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		}
	}
	return 0
}

// Close wait for outstanding messages to be delivered within given flush interval timeout.
func (pr *Kafka) Close() int {
	remaining := pr.kp.Flush(pr.flushInterval)
	logger.Info(fmt.Sprintf("Outstanding events still un-flushed : %d", remaining))
	pr.kp.Close()
	return remaining
}

func allNil(errors []error) bool {
	for _, err := range errors {
		if err != nil {
			return false
		}
	}
	return true
}

type ProducerStats struct {
	EventCounts map[string]int
	ErrorCounts map[string]int
}

type BulkError struct {
	Errors []error
}

func (b BulkError) Error() string {
	err := "error when sending messages: "
	for i, mErr := range b.Errors {
		if i != 0 {
			err += fmt.Sprintf(", %v", mErr)
			continue
		}
		err += mErr.Error()
	}
	return err
}

func (pr *Kafka) HealthCheck() error {
	logger.Debug("Health Check: probing kafka-broker")
	topic := config.PublisherKafka.HealthCheckConfig.TopicName
	_, err := pr.kp.GetMetadata(&topic, false, config.PublisherKafka.HealthCheckConfig.TimeOut)
	return err
}
