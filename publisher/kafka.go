package publisher

import (
	"encoding/json"
	"fmt"
	"github.com/goto/raccoon/proto"
	"github.com/goto/raccoon/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	// Importing librd to make it work on vendor mode
	_ "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka/librdkafka"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	errUnknownTopic     = "Local: Unknown topic"           //error msg while producing a message to a topic which is not present in the kafka cluster
	errLargeMessageSize = "Broker: Message size too large" //error msg while producing a message which is larger than message.max.bytes config
)

var DeliveryEventCount int64

// KafkaProducer Produce data to kafka synchronously
type KafkaProducer interface {
	// ProduceBulk message to kafka. Block until all messages are sent. Return array of error. Order is not guaranteed.
	ProduceBulk(events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event) error
}

func NewKafka() (*Kafka, error) {
	kp, err := newKafkaClient(config.PublisherKafka.ToKafkaConfigMap())
	if err != nil {
		return &Kafka{}, err
	}
	return &Kafka{
		kp:                     kp,
		flushInterval:          config.PublisherKafka.FlushInterval,
		topicFormat:            config.EventDistribution.PublisherPattern,
		deliveryReportInterval: config.PublisherKafka.DeliveryReportInterval,
		deliveryReportTopic:    config.PublisherKafka.DeliveryReportTopic,
	}, nil
}

func NewKafkaFromClient(client Client, flushInterval int, topicFormat string, deliveryReportInterval time.Duration,
	topicName string) *Kafka {
	return &Kafka{
		kp:                     client,
		flushInterval:          flushInterval,
		topicFormat:            topicFormat,
		deliveryReportInterval: deliveryReportInterval,
		deliveryReportTopic:    topicName,
	}
}

type Kafka struct {
	kp                     Client
	flushInterval          int
	topicFormat            string
	deliveryReportInterval time.Duration
	deliveryReportTopic    string
}

// ProduceBulk messages to kafka. Block until all messages are sent. Return array of error. Order of Errors is guaranteed.
// DeliveryChannel needs to be exclusive. DeliveryChannel is exposed for recyclability purpose.
func (pr *Kafka) ProduceBulk(events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event) error {
	errors := make([]error, len(events))
	totalProcessed := 0
	for order, event := range events {
		topic := fmt.Sprintf(pr.topicFormat, event.Type)
		message := &kafka.Message{
			Value:          event.EventBytes,
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Opaque:         order,
		}

		err := pr.kp.Produce(message, deliveryChannel)
		if err != nil {
			metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s", connGroup, event.Type))
			var errorTag string
			switch err.Error() {
			case errUnknownTopic:
				errors[order] = fmt.Errorf("%v %s", err, topic)
				errorTag = "unknown_topic"
			case errLargeMessageSize:
				errors[order] = fmt.Errorf("%v %s", err, topic)
				errorTag = "message_too_large"
			default:
				errors[order] = err
				logger.Errorf("produce to kafka failed due to: %v on topic : %s", err, topic)
				errorTag = "unknown"
			}
			metrics.Increment("kafka_error", fmt.Sprintf("type=%s,event_type=%s,conn_group=%s",
				errorTag, event.Type, connGroup))
			continue
		}
		metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, event.Type))
		totalProcessed++
	}
	// Wait for deliveryChannel as many as processed
	for i := 0; i < totalProcessed; i++ {
		d := <-deliveryChannel
		m := d.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			eventType := events[i].Type
			metrics.Decrement("kafka_messages_delivered_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_error", fmt.Sprintf("type=%s,event_type=%s,conn_group=%s", "delivery_failed", eventType, connGroup))
			order := m.Opaque.(int)
			errors[order] = m.TopicPartition.Error
		} else {
			atomic.AddInt64(&DeliveryEventCount, 1) // if no error is received, increment the count
		}
	}

	if allNil(errors) {
		return nil
	}
	return BulkError{Errors: errors}
}

func (pr *Kafka) produceTotalEventMessage(topicName string, event *proto.TotalEventCountMessage) error {
	value, err := serialization.SerializeProto(event)
	if err != nil {
		return fmt.Errorf("failed to serialize proto: %w", err)
	}
	return pr.kp.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
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
			fmt.Printf("Ignored %v \n", e)
		}
	}
}

func (pr *Kafka) ReportDeliveryEventCount() {
	ticker := time.NewTicker(pr.deliveryReportInterval)
	defer ticker.Stop()

	for range ticker.C {
		// read the value
		eventCount := atomic.LoadInt64(&DeliveryEventCount)
		//build kafka message
		msg := &proto.TotalEventCountMessage{
			EventTimestamp: timestamppb.Now(),
			EventCount:     int32(eventCount),
		}
		//produce to kafka
		pr.produceTotalEventMessage(pr.deliveryReportTopic, msg)
		//reset the counter
		atomic.StoreInt64(&DeliveryEventCount, 0)
	}
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
