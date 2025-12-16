package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	// Importing librd to make it work on vendor mode
	_ "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka/librdkafka"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	clients "github.com/goto/raccoon/clients/http"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	errUnknownTopic     = "Local: Unknown topic"           //error msg while producing a message to a topic which is not present in the kafka cluster
	errLargeMessageSize = "Broker: Message size too large" //error msg while producing a message which is larger than message.max.bytes config
)

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
	k := &Kafka{
		kp:            kp,
		flushInterval: config.PublisherKafka.FlushInterval,
		topicFormat:   config.EventDistribution.PublisherPattern,
		clients:       clients.NewHTTPClient(1 * time.Second),

		// Initialize the nested map structure
		fallbackBuffer:   make(map[string]map[fallbackKey]int),
		fallbackInterval: 30 * time.Second,
		fallbackStop:     make(chan struct{}),
	}

	k.startFallbackWorker()

	return k, nil
}

func NewKafkaFromClient(client Client, flushInterval int, topicFormat string) *Kafka {
	return &Kafka{
		kp:            client,
		flushInterval: flushInterval,
		topicFormat:   topicFormat,
	}
}

// Key for aggregation (Publisher is removed from key if it's always same as connGroup,
// but keeping it here for clarity or if logic changes is fine.
// Optimization: We can remove Publisher from key since it's redundant with ConnGroup key in the map).
type fallbackKey struct {
	EventName string
	Product   string
}

type Kafka struct {
	kp            Client
	flushInterval int
	topicFormat   string
	clients       *clients.HTTPClient

	fallbackBuffer   map[string]map[fallbackKey]int
	fallbackMu       sync.Mutex
	fallbackInterval time.Duration
	fallbackStop     chan struct{}
	fallbackWg       sync.WaitGroup
}

// ProduceBulk messages to kafka. Block until all messages are sent. Return array of error. Order of Errors is guaranteed.
// DeliveryChannel needs to be exclusive. DeliveryChannel is exposed for recyclability purpose.
func (pr *Kafka) ProduceBulk(events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event) error {
	errors := make([]error, len(events))
	totalProcessed := 0

	var failedEvents []*pb.Event

	for order, event := range events {
		topic := fmt.Sprintf(pr.topicFormat, event.Type)
		message := &kafka.Message{
			Value:          event.EventBytes,
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Opaque:         order,
		}

		logger.Infof("Clickstream-event-monitoring: event_name=%s, product=%s, type=%s, conn_group=%s, event_timestamp=%s",
			event.EventName,
			event.Product,
			event.Type,
			connGroup,
			event.EventTimestamp.AsTime().String(),
		)

		err := pr.kp.Produce(message, deliveryChannel)
		if err != nil {
			failedEvents = append(failedEvents, event)

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

			metrics.Increment("clickstream_data_loss", fmt.Sprintf("reason=%s,event_name=%s,product=%s,conn_group=%s",
				errorTag, event.EventName, event.Product, connGroup,
			))

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
			order := m.Opaque.(int)
			event := events[order]

			failedEvents = append(failedEvents, event)

			eventType := events[i].Type
			metrics.Decrement("kafka_messages_delivered_total", fmt.Sprintf("success=true,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_messages_delivered_total", fmt.Sprintf("success=false,conn_group=%s,event_type=%s", connGroup, eventType))
			metrics.Increment("kafka_error", fmt.Sprintf("type=%s,event_type=%s,conn_group=%s", "delivery_failed", eventType, connGroup))
			metrics.Increment("clickstream_data_loss", fmt.Sprintf("reason=%s,event_name=%s,product=%s,conn_group=%s",
				"delivery_failed", events[i].EventName, events[i].Product, connGroup,
			))

			errors[order] = m.TopicPartition.Error
		}
	}

	if len(failedEvents) > 0 {
		pr.queueFallback(failedEvents, connGroup)
	}

	if allNil(errors) {
		return nil
	}

	return BulkError{Errors: errors}
}

func (pr *Kafka) startFallbackWorker() {
	pr.fallbackWg.Add(1)
	go func() {
		defer pr.fallbackWg.Done()
		ticker := time.NewTicker(pr.fallbackInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pr.flushFallbackBuffer()
			case <-pr.fallbackStop:
				return
			}
		}
	}()
}

func (pr *Kafka) queueFallback(events []*pb.Event, connGroup string) {
	pr.fallbackMu.Lock()
	defer pr.fallbackMu.Unlock()

	if _, ok := pr.fallbackBuffer[connGroup]; !ok {
		pr.fallbackBuffer[connGroup] = make(map[fallbackKey]int)
	}

	for _, e := range events {
		// 1. Extract ONLY the lightweight strings we need
		key := fallbackKey{
			EventName: e.EventName,
			Product:   e.Product,
		}

		// 2. Increment count
		pr.fallbackBuffer[connGroup][key]++

		// 3. CRITICAL: We do NOT store 'e' (the heavy protobuf pointer).
		// Once this function finishes, the heavy 'events' slice
		// is eligible for Garbage Collection.
	}

	metrics.Gauge("fallback_buffer_unique_keys", len(pr.fallbackBuffer[connGroup]), fmt.Sprintf("conn_group=%s", connGroup))
}

func (pr *Kafka) flushFallbackBuffer() {
	pr.fallbackMu.Lock()
	if len(pr.fallbackBuffer) == 0 {
		pr.fallbackMu.Unlock()
		return
	}

	// Swap buffer
	batchToSend := pr.fallbackBuffer
	pr.fallbackBuffer = make(map[string]map[fallbackKey]int)
	pr.fallbackMu.Unlock()

	for connGroup, countsMap := range batchToSend {
		if len(countsMap) == 0 {
			continue
		}

		payloadEvents := make([]IngestEvent, 0, len(countsMap))

		for key, count := range countsMap {
			payloadEvents = append(payloadEvents, IngestEvent{
				// REQUIREMENT: Publisher is set to connGroup
				Publisher:  connGroup,
				EventName:  key.EventName,
				Product:    key.Product,
				EventCount: count,
			})
		}

		pr.sendFallback(payloadEvents, connGroup)
	}
}

func (pr *Kafka) sendFallback(events []IngestEvent, connGroup string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Updated JSON Contract
	reqBody := IngestPayload{
		ReqQuid: "",
		Events:  events,
	}

	req := clients.Request{
		Method:      http.MethodPost,
		BaseURL:     "http://kafka-producer.i.s-id-gtdp-01.gopay.sh",
		Path:        "/api/v1/ingest",
		ContentType: "application/json",
		Body:        reqBody,
	}

	_, err := pr.clients.DoRequest(ctx, req)
	if err != nil {
		logger.Errorf("Failed to send fallback events for connGroup %s: %v", connGroup, err)
		metrics.Increment("fallback_ingest_error", fmt.Sprintf("conn_group=%s", connGroup))
		return
	}

	logger.Infof("Successfully sent fallback batch for connGroup %s", connGroup)
	metrics.Increment("fallback_ingest_success", fmt.Sprintf("conn_group=%s", connGroup))
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
	close(pr.fallbackStop)
	pr.fallbackWg.Wait()
	pr.flushFallbackBuffer() // Flush counts before exit

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
