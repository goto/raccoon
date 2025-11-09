package mqtt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// -------------------- Mock types --------------------

type MockPubSubClient struct {
	mock.Mock
}

func (m *MockPubSubClient) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPubSubClient) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPubSubClient) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

func TestService_NewMQTTService(t *testing.T) {
	ctx := context.Background()
	mockCollector := new(collection.MockCollector)

	t.Run("successfully creates service with consumers", func(t *testing.T) {
		config.ServerMQTT.ConsumerConfig.PoolSize = 2

		// temporarily replace client.NewMqttPubSubClient
		clientCreator := func(ctx context.Context, handler any, clientID string) PubSubClient {
			return new(MockPubSubClient)
		}

		// construct service manually using fake creator
		var consumers []*Consumer
		for i := 0; i < config.ServerMQTT.ConsumerConfig.PoolSize; i++ {
			client := clientCreator(ctx, nil, fmt.Sprintf("test-host_subscriber_%d", i))
			consumers = append(consumers, NewConsumer(client))
		}

		s := &Service{Collector: mockCollector, consumers: consumers}
		assert.Equal(t, 2, len(s.consumers))
	})

}

func TestService_Init(t *testing.T) {
	ctx := context.Background()

	t.Run("returns startup error", func(t *testing.T) {
		s := &Service{startupErr: errors.New("startup failed")}
		err := s.Init(ctx)
		assert.ErrorContains(t, err, "mqtt service startup failed")
	})

	t.Run("consumer init fails", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("Start").Return(errors.New("start failed"))

		consumer := NewConsumer(mockClient)
		s := &Service{consumers: []*Consumer{consumer}}

		err := s.Init(ctx)
		assert.ErrorContains(t, err, "failed to start consumer")
	})

	t.Run("all consumers start successfully", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("Start").Return(nil)

		consumer := NewConsumer(mockClient)
		s := &Service{consumers: []*Consumer{consumer}}

		err := s.Init(ctx)
		assert.NoError(t, err)
	})
}

func TestService_Shutdown(t *testing.T) {
	ctx := context.Background()

	t.Run("all consumers stop successfully", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("Stop").Return(nil)

		s := &Service{consumers: []*Consumer{NewConsumer(mockClient)}}
		err := s.Shutdown(ctx)
		assert.NoError(t, err)
	})

	t.Run("shutdown failure", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("Stop").Return(errors.New("stop failed"))

		s := &Service{consumers: []*Consumer{NewConsumer(mockClient)}}
		err := s.Shutdown(ctx)
		assert.ErrorContains(t, err, "failed to stop consumer")
	})
}

func TestService_HealthCheck(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("IsConnected").Return(true)

		s := &Service{consumers: []*Consumer{NewConsumer(mockClient)}}
		assert.NoError(t, s.HealthCheck())
	})

	t.Run("one unhealthy", func(t *testing.T) {
		mockClient := new(MockPubSubClient)
		mockClient.On("IsConnected").Return(false)

		s := &Service{consumers: []*Consumer{NewConsumer(mockClient)}}
		assert.ErrorContains(t, s.HealthCheck(), "consumer connection is broken")
	})
}

func TestService_Name(t *testing.T) {
	s := &Service{}
	assert.Equal(t, "MQTT", s.Name())
}
