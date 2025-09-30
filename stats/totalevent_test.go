package stats_test

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/stats"
)

func TestFlushTotalEventStat(t *testing.T) {
	// setup
	eventCh := make(chan int32, 10)
	producedCh := make(chan *pb.TotalEventCountMessage, 10) // <- initialize channel
	mockKP := &mockKafkaProducer{producedCh: producedCh}
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
	select {
	case msg := <-producedCh:
		if msg.EventCount != 5 {
			t.Errorf("expected EventCount=5, got %d", msg.EventCount)
		}
		if msg.EventTimestamp.AsTime().IsZero() {
			t.Error("expected non-zero EventTimestamp")
		}
	default:
		t.Error("expected ProduceEventStat to be called, got 0")
	}
}

// add a timeout test to ensure aggregation resets
func TestFlushResetsAfterPublish(t *testing.T) {
	eventCh := make(chan int32, 10)
	producedCh := make(chan *pb.TotalEventCountMessage, 10)
	mockKP := &mockKafkaProducer{producedCh: producedCh}
	flushInterval := 1 * time.Millisecond
	ts := stats.CreateTotalEventStat(mockKP, flushInterval, "topic", eventCh)

	go ts.FlushTotalEventStat()

	// First batch
	eventCh <- 1
	time.Sleep(2 * flushInterval)

	// Second batch
	eventCh <- 4
	time.Sleep(2 * flushInterval)

	// Assert first batch
	select {
	case msg := <-producedCh:
		if msg.EventCount != 1 {
			t.Errorf("expected first flush=1, got %d", msg.EventCount)
		}
	default:
		t.Fatal("expected first ProduceEventStat call")
	}

	// Assert second batch
	select {
	case msg := <-producedCh:
		if msg.EventCount != 4 {
			t.Errorf("expected second flush=4, got %d", msg.EventCount)
		}
	default:
		t.Fatal("expected second ProduceEventStat call")
	}
}

// mockKafkaProducer implements publisher.KafkaProducer
type mockKafkaProducer struct {
	producedCh chan *pb.TotalEventCountMessage
}

func (m *mockKafkaProducer) ProduceBulk(_ []*pb.Event, _ string, _ chan kafka.Event) error {
	return nil
}

func (m *mockKafkaProducer) ProduceEventStat(_ string, event *pb.TotalEventCountMessage) error {
	m.producedCh <- event
	return nil
}
