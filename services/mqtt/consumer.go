package mqtt

import (
	"fmt"
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

// Start initializes and starts the underlying PubSubClient.
func (c *Consumer) Start() error {
	if err := c.client.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the underlying PubSubClient.
func (c *Consumer) Stop() error {
	if err := c.client.Stop(); err != nil {
		return fmt.Errorf("failed to stop consumer: %w", err)
	}
	return nil
}

// IsConnected checks the connection health of underlying PubSubClient.
func (c *Consumer) IsConnected() bool {
	return c.client.IsConnected()
}
