package client

import (
	"context"
	"fmt"
	"github.com/gojek/courier-go"
	"github.com/gojek/courier-go/consul"
	"github.com/gojekfarm/xtools/xproto"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"io"
)

// MqttPubSubClient wraps a courier MQTT client with start/stop lifecycle management.
type MqttPubSubClient struct {
	client   client
	resolver resolver
}

// client defines the minimal interface for a courier MQTT client instance.
// It abstracts basic lifecycle operations and connection state management.
type client interface {
	// Start begins the client’s operation and establishes connections
	// to the MQTT broker or messaging backend.
	Start() error

	// Stop terminates the client’s operation gracefully, releasing
	// any resources and closing network connections.
	Stop()

	// IsConnected reports whether the client is currently connected
	// to the MQTT broker.
	IsConnected() bool
}

// resolver defines the interface for a service discovery resolver
// responsible for discovering and updating MQTT broker endpoints dynamically.
//
// Implementations like ConsulResolver use this interface to periodically
// fetch and update broker addresses for high availability and fault tolerance.
type resolver interface {
	// Start initiates the resolver’s background process to watch for
	// endpoint changes.
	Start()
}

// NewMqttPubSubClient initializes a new MQTT client with Consul-based service discovery and credentials.
func NewMqttPubSubClient(ctx context.Context, handler courier.MessageHandler, clientID string) (*MqttPubSubClient, error) {
	consulCfg := config.ServerMQTT.ConsulConfig

	rs, err := consul.NewResolver(&consul.Config{
		ConsulAddress: consulCfg.Address,
		HealthyOnly:   consulCfg.HealthOnly,
		KVKey:         consulCfg.KVKey,
		WaitTime:      consulCfg.WaitTime,
	})
	if err != nil {
		logger.Infof("error while creating the consul new resolver %v", err)
		return nil, fmt.Errorf("failed to create consul resolver: %w", err)
	}

	credFetcher, err := newCredentialFetcher()
	if err != nil {
		logger.Infof("error while creating credential %v", err)
		return nil, fmt.Errorf("failed to create credential fetcher: %w", err)
	}

	clientOpts := []courier.ClientOption{
		courier.WithResolver(rs),
		courier.UseMultiConnectionMode,
		courier.ConnectRetryInterval(config.ServerMQTT.ConsumerConfig.RetryIntervalInSec),
		courier.WithCredentialFetcher(credFetcher),
		courier.WithCleanSession(true),
		courier.WithClientID(clientID),
		courier.WithMaintainOrder(false),
		courier.WithPahoLogLevel(courier.ParseLogLevel(config.ServerMQTT.ConsumerConfig.LogLevel)),
		courier.WithWriteTimeout(config.ServerMQTT.ConsumerConfig.WriteTimeoutInSec),
		courier.KeepAlive(config.ServerMQTT.ConsumerConfig.KeepAlive),
		courier.WithOnConnect(registerHandler(ctx, handler)),
		courier.WithLogger(NewLogger()),
		courier.WithCustomDecoder(protoDecoder),
	}

	c, err := courier.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MQTT client: %w", err)
	}

	logger.Infof("MQTT client initialized successfully for clientID=%s", clientID)
	return &MqttPubSubClient{client: c, resolver: rs}, nil
}

// registerHandler registers the subscription handler when the client connects.
func registerHandler(ctx context.Context, handler courier.MessageHandler) func(courier.PubSub) {
	return func(ps courier.PubSub) {
		topic := config.ServerMQTT.ConsumerConfig.TopicFormat
		if err := ps.Subscribe(ctx, topic, handler, courier.QOSZero); err != nil {
			metrics.Increment(
				"mqtt_error",
				fmt.Sprintf("reason=subscribe_failed"),
			)
			logger.Errorf("failed to register MQTT handler for topic %q: %v", topic, err)
		} else {
			logger.Infof("successfully registered MQTT handler for topic %q", topic)
		}
	}
}

// Start begins the MQTT client operation.
func (m *MqttPubSubClient) Start() error {
	go m.resolver.Start()
	if err := m.client.Start(); err != nil {
		metrics.Increment(
			"mqtt_error",
			fmt.Sprintf("reason=start_failed"),
		)
		logger.Infof("MQTT client start failed due to %v", err)
		return fmt.Errorf("failed to start MQTT client: %w", err)
	}
	logger.Infof("MQTT client started successfully")
	return nil
}

// Stop gracefully stops the MQTT client.
func (m *MqttPubSubClient) Stop() error {
	m.client.Stop()
	logger.Infof("MQTT client stopped successfully")
	return nil
}

// IsConnected checks the connection status.
func (m *MqttPubSubClient) IsConnected() bool {
	return m.client.IsConnected()
}

// protoDecoder decodes the proto messages.
func protoDecoder(ctx context.Context, r io.Reader) courier.Decoder {
	return xproto.NewDecoder(r)
}
