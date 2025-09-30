package stats_test

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/stats"
)

func TestFlushTotalEventStat(t *testing.T) {
	// Arrange
	eventCh := make(chan int32, 10)
	mockKP := &mockKafkaProducer{}
	flushInterval := 1 * time.Millisecond
	topicName := "test-topic"

	ts := stats.CreateTotalEventStat(mockKP, flushInterval, topicName, eventCh)
	go ts.FlushTotalEventStat()

	// Send some counts
	eventCh <- 2
	eventCh <- 3

	// Wait longer than flush interval to ensure ticker fires
	time.Sleep(2 * flushInterval)

	// Assert
	if len(mockKP.produced) == 0 {
		t.Error("expected ProduceEventStat to be called, got 0")
	}

	result := mockKP.produced[0].EventCount
	expected := int32(5)
	if result != expected {
		t.Errorf("expected EventCount=%d, got %d", expected, result)
	}

	// Also check timestamp is set
	if mockKP.produced[0].EventTimestamp.AsTime().IsZero() {
		t.Error("expected non-zero EventTimestamp")
	}
}

// Optional: add a timeout test to ensure aggregation resets
func TestFlushResetsAfterPublish(t *testing.T) {
	eventCh := make(chan int32, 10)
	mockKP := &mockKafkaProducer{}
	flushInterval := 1 * time.Millisecond
	ts := stats.CreateTotalEventStat(mockKP, flushInterval, "topic", eventCh)

	go ts.FlushTotalEventStat()

	// First batch
	eventCh <- 1
	time.Sleep(2 * flushInterval)

	// Second batch
	eventCh <- 4
	time.Sleep(2 * flushInterval)

	if len(mockKP.produced) < 2 {
		t.Fatalf("expected at least 2 ProduceEventStat calls, got %d", len(mockKP.produced))
	}

	if mockKP.produced[0].EventCount != 1 {
		t.Errorf("expected first flush=1, got %d", mockKP.produced[0].EventCount)
	}
	if mockKP.produced[1].EventCount != 4 {
		t.Errorf("expected second flush=4, got %d", mockKP.produced[1].EventCount)
	}
}

// mockKafkaProducer implements publisher.KafkaProducer
type mockKafkaProducer struct {
	produced []*pb.TotalEventCountMessage
}

func (m *mockKafkaProducer) ProduceBulk(_ []*pb.Event, _ string, _ chan kafka.Event) error {
	return nil
}

func (m *mockKafkaProducer) ProduceEventStat(_ string, event *pb.TotalEventCountMessage) error {
	m.produced = append(m.produced, event)
	return nil
}
