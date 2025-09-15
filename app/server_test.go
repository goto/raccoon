package app

import (
	"context"
	"github.com/goto/raccoon/publisher"
	"github.com/goto/raccoon/services"
	"github.com/goto/raccoon/worker"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/goto/raccoon/collection"
)

func TestShutDownServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockKafka := &mockKafkaClient{}

	kp := publisher.NewKafkaFromClient(mockKafka, 50, "test")

	shutdownCh := make(chan bool, 1)
	bufferCh := make(chan collection.CollectRequest, 1)

	services := services.Create(bufferCh, ctx)

	wp := worker.CreateWorkerPool(1, bufferCh, 1, kp)

	// run shutdown in background
	sigCh := make(chan os.Signal, 1)
	go shutDownServer(ctx, cancel, services, bufferCh, wp, kp, shutdownCh, sigCh)

	// send a termination signal after short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		sigCh <- syscall.SIGTERM
	}()

	select {
	case <-shutdownCh:
		t.Log("shutdown executed successfully")
	case <-time.After(500 * time.Millisecond):
		t.Error("shutdown execution failed")
	}

	if !isClosed(bufferCh) {
		t.Errorf("expected buffer channel to be closed")
	}
	if !mockKafka.FlushCalled {
		t.Errorf("expected Kafka.FlushCalled to be called")
	}
	if !mockKafka.CloseCalled {
		t.Errorf("expected Kafka.CloseCalled to be called")
	}
}

func isClosed(ch <-chan collection.CollectRequest) bool {
	select {
	case _, ok := <-ch:
		return !ok
	default:
		return false
	}
}

// ---- Mocks ----
// mockKafkaClient is a mock for the Client interface
type mockKafkaClient struct {
	// Tracking flags
	ProduceCalled bool
	CloseCalled   bool
	FlushCalled   bool
	EventsCalled  bool

	ReturnFlushLeft int
	EventChan       chan kafka.Event
}

func (m *mockKafkaClient) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	m.ProduceCalled = true
	return nil
}

func (m *mockKafkaClient) Close() {
	m.CloseCalled = true
}

func (m *mockKafkaClient) Flush(timeout int) int {
	m.FlushCalled = true
	return m.ReturnFlushLeft
}

func (m *mockKafkaClient) Events() chan kafka.Event {
	m.EventsCalled = true
	if m.EventChan == nil {
		m.EventChan = make(chan kafka.Event, 1)
	}
	return m.EventChan
}
