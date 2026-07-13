package client

import (
	"context"
	"errors"
	"github.com/gojek/courier-go"
	"github.com/goto/raccoon/config"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMqttPubSubClient_Start(t *testing.T) {

	t.Run("Start success", func(t *testing.T) {

		wg := &sync.WaitGroup{}
		wg.Add(1)
		mockResolver := &mockResolver{wg: wg}
		mockResolver.On("Start").Return().Once()

		mockClient := new(mockCourierClient)
		mockClient.On("Start").Return(nil).Once()

		m := &MqttPubSubClient{client: mockClient, resolver: mockResolver}
		err := m.Start()
		assert.NoError(t, err)

		wg.Wait() // wait for goroutine completion
		mockResolver.AssertExpectations(t)
		mockClient.AssertExpectations(t)

	})

	t.Run("Start failure", func(t *testing.T) {

		wg := &sync.WaitGroup{}
		wg.Add(1)
		mockResolver := &mockResolver{wg: wg}
		mockResolver.On("Start").Return().Once()

		mockClient := new(mockCourierClient)
		mockClient.On("Start").Return(errors.New("connection failed")).Once()

		m := &MqttPubSubClient{client: mockClient, resolver: mockResolver}
		err := m.Start()
		assert.ErrorContains(t, err, "failed to start MQTT client")

		wg.Wait() // wait for goroutine completion
		mockResolver.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

}

func TestMqttPubSubClient_Stop(t *testing.T) {

	t.Run("Stop always succeeds", func(t *testing.T) {
		mockClient := new(mockCourierClient)
		mockResolver := new(mockResolver)

		mockClient.On("Stop").Return().Once()

		m := &MqttPubSubClient{
			client:   mockClient,
			resolver: mockResolver,
		}

		err := m.Stop()
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestMqttPubSubClient_IsConnected(t *testing.T) {

	t.Run("IsConnected reflects client state", func(t *testing.T) {
		mockClient := new(mockCourierClient)
		mockResolver := new(mockResolver)

		mockClient.On("IsConnected").Return(true).Once()

		m := &MqttPubSubClient{
			client:   mockClient,
			resolver: mockResolver,
		}

		assert.True(t, m.IsConnected())
		mockClient.AssertExpectations(t)
	})
}

func TestRegisterHandler(t *testing.T) {
	handler := func(ctx context.Context, ps courier.PubSub, msg *courier.Message) {}

	t.Run("subscribes to both v1 and v2 topics when v2 is enabled and configured", func(t *testing.T) {
		config.ServerMQTT.ConsumerConfig.TopicFormat = "ex/v1/+/+"
		config.ServerMQTT.ConsumerConfig.TopicFormatV2 = "ex/v2/+/+/+"
		config.ServerMQTT.ConsumerConfig.EnableV2Topic = true
		t.Cleanup(func() {
			config.ServerMQTT.ConsumerConfig.TopicFormat = ""
			config.ServerMQTT.ConsumerConfig.TopicFormatV2 = ""
			config.ServerMQTT.ConsumerConfig.EnableV2Topic = false
		})

		ps := new(subscribeRecorder)
		ps.On("Subscribe", "ex/v1/+/+").Return(nil).Once()
		ps.On("Subscribe", "ex/v2/+/+/+").Return(nil).Once()

		registerHandler(context.Background(), handler)(ps)

		ps.AssertExpectations(t)
	})

	t.Run("does not subscribe to v2 topic when EnableV2Topic is false, even if configured", func(t *testing.T) {
		config.ServerMQTT.ConsumerConfig.TopicFormat = "ex/v1/+/+"
		config.ServerMQTT.ConsumerConfig.TopicFormatV2 = "ex/v2/+/+/+"
		config.ServerMQTT.ConsumerConfig.EnableV2Topic = false
		t.Cleanup(func() {
			config.ServerMQTT.ConsumerConfig.TopicFormat = ""
			config.ServerMQTT.ConsumerConfig.TopicFormatV2 = ""
		})

		ps := new(subscribeRecorder)
		ps.On("Subscribe", "ex/v1/+/+").Return(nil).Once()

		registerHandler(context.Background(), handler)(ps)

		ps.AssertExpectations(t)
		ps.AssertNotCalled(t, "Subscribe", "ex/v2/+/+/+")
	})

	t.Run("attempts to subscribe to v2 topic as configured, even if empty, when EnableV2Topic is true", func(t *testing.T) {
		config.ServerMQTT.ConsumerConfig.TopicFormat = "ex/v1/+/+"
		config.ServerMQTT.ConsumerConfig.TopicFormatV2 = ""
		config.ServerMQTT.ConsumerConfig.EnableV2Topic = true
		t.Cleanup(func() {
			config.ServerMQTT.ConsumerConfig.TopicFormat = ""
			config.ServerMQTT.ConsumerConfig.EnableV2Topic = false
		})

		ps := new(subscribeRecorder)
		ps.On("Subscribe", "ex/v1/+/+").Return(nil).Once()
		ps.On("Subscribe", "").Return(errors.New("invalid topic")).Once()

		registerHandler(context.Background(), handler)(ps)

		ps.AssertExpectations(t)
	})

	t.Run("subscribes to v2 topic literally, without stripping stray quote characters", func(t *testing.T) {
		config.ServerMQTT.ConsumerConfig.TopicFormat = "ex/v1/+/+"
		config.ServerMQTT.ConsumerConfig.TopicFormatV2 = `""`
		config.ServerMQTT.ConsumerConfig.EnableV2Topic = true
		t.Cleanup(func() {
			config.ServerMQTT.ConsumerConfig.TopicFormat = ""
			config.ServerMQTT.ConsumerConfig.TopicFormatV2 = ""
			config.ServerMQTT.ConsumerConfig.EnableV2Topic = false
		})

		ps := new(subscribeRecorder)
		ps.On("Subscribe", "ex/v1/+/+").Return(nil).Once()
		ps.On("Subscribe", `""`).Return(errors.New("invalid topic")).Once()

		registerHandler(context.Background(), handler)(ps)

		ps.AssertExpectations(t)
	})
}

func TestNewMqttPubSubClient(t *testing.T) {

	t.Run("new pubsub client should be created", func(t *testing.T) {
		mockPubSub := new(MockPubSub)
		config.ServerMQTT.ConsulConfig.KVKey = "/test/path"
		config.ServerMQTT.ConsulConfig.Address = "localhost:8085"
		config.ServerMQTT.ConsulConfig.HealthOnly = true
		config.ServerMQTT.ConsulConfig.WaitTime = 1 * time.Second
		config.ServerMQTT.AuthConfig.Username = "test"
		config.ServerMQTT.AuthConfig.Password = "pass"
		c, err := NewMqttPubSubClient(context.Background(), mockPubSub.Subscribe, "test-client-1")
		assert.Nil(t, err)
		assert.NotNil(t, c.client)
		assert.NotNil(t, c.resolver)
	})

}

// ---- Mock Definitions ----

type mockCourierClient struct {
	mock.Mock
}

func (m *mockCourierClient) Start() error {
	args := m.Called()
	return args.Error(0)
}
func (m *mockCourierClient) Stop() {
	m.Called()
}
func (m *mockCourierClient) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

type mockResolver struct {
	mock.Mock
	wg *sync.WaitGroup
}

func (m *mockResolver) Start() {
	defer m.wg.Done()
	m.Called()
}

type MockPubSub struct {
	mock.Mock
}

func (m *MockPubSub) Subscribe(ctx context.Context, c courier.PubSub, message *courier.Message) {
	m.Called(ctx, c, message)
	return
}

// subscribeRecorder implements courier.PubSub so registerHandler's Subscribe
// calls can be asserted against directly.
type subscribeRecorder struct {
	mock.Mock
}

func (m *subscribeRecorder) Publish(ctx context.Context, topic string, message interface{}, opts ...courier.Option) error {
	return nil
}

func (m *subscribeRecorder) Subscribe(ctx context.Context, topic string, callback courier.MessageHandler, opts ...courier.Option) error {
	args := m.Called(topic)
	return args.Error(0)
}

func (m *subscribeRecorder) SubscribeMultiple(ctx context.Context, topicsWithQos map[string]courier.QOSLevel, callback courier.MessageHandler) error {
	return nil
}

func (m *subscribeRecorder) Unsubscribe(ctx context.Context, topics ...string) error {
	return nil
}

func (m *subscribeRecorder) IsConnected() bool {
	return true
}
