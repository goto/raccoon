package mqtt

import (
	"fmt"
	"github.com/goto/raccoon/logger"
)

// Consumer wraps a PubSubClient and provides lifecycle control.
type Consumer struct {
	client PubSubClient
}

// PubSubClient defines the basic contract for any MQTT client implementation.
type PubSubClient interface {
	Start() error
	Stop() error
	IsConnected() bool
}

// NewConsumer creates a new Consumer instance with the provided PubSubClient.
func NewConsumer(client PubSubClient) *Consumer {
	return &Consumer{client: client}
}

// Init initializes and starts the underlying PubSubClient.
func (c *Consumer) Init() error {
	if err := c.client.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	} else {
		logger.Info("[consumer:] client started sucessfully !!")
	}
	return nil
}

// Shutdown gracefully shuts down the underlying PubSubClient.
func (c *Consumer) Shutdown() error {
	if err := c.client.Stop(); err != nil {
		return fmt.Errorf("failed to stop consumer: %w", err)
	} else {
		logger.Info("[consumer:] client stopped sucessfully !!")
	}
	return nil
}

// IsHealthy checks the connection health of underlying PubSubClient.
func (c *Consumer) IsHealthy() bool {
	return c.client.IsConnected()
}
