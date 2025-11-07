package client

import (
	"context"
	"fmt"
	"github.com/gojek/courier-go"
	"github.com/gojek/courier-go/consul"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
)

// MqttPubSubClient wraps a courier MQTT client with start/stop lifecycle management.
type MqttPubSubClient struct {
	client         *courier.Client
	consulResolver *consul.Resolver
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
		return nil, fmt.Errorf("failed to create consul resolver: %w", err)
	}

	credFetcher, err := newCredentialFetcher()
	if err != nil {
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
		courier.WithOnConnect(registerHandler(ctx, handler)),
		courier.WithLogger(logger.GetLogger()),
	}

	client, err := courier.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MQTT client: %w", err)
	}

	logger.Infof("MQTT client initialized successfully for clientID=%s and client %v", clientID, client)
	return &MqttPubSubClient{client: client, consulResolver: rs}, nil
}

// registerHandler registers the subscription handler when the client connects.
func registerHandler(ctx context.Context, handler courier.MessageHandler) func(courier.PubSub) {
	return func(ps courier.PubSub) {
		topic := config.ServerMQTT.ConsumerConfig.TopicFormat
		if err := ps.Subscribe(ctx, topic, handler, courier.QOSZero); err != nil {
			logger.Errorf("failed to register MQTT handler for topic %q: %v", topic, err)
		} else {
			logger.Infof("successfully registered MQTT handler for topic %q", topic)
		}
	}
}

// Start begins the MQTT client operation.
func (m *MqttPubSubClient) Start() error {
	go m.consulResolver.Start()
	if err := m.client.Start(); err != nil {
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
