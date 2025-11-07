package mqtt

import (
	"context"
	"errors"
	"fmt"
	"github.com/goto/raccoon/health"
	"github.com/goto/raccoon/logger"
	"os"

	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/services/mqtt/client"
)

// Service manages a pool of MQTT consumers.
type Service struct {
	Collector  collection.Collector
	consumers  []*Consumer
	startupErr error
}

// NewMQTTService initializes the MQTT service and its consumer pool.
func NewMQTTService(collector collection.Collector, ctx context.Context) *Service {
	hostName, err := os.Hostname()
	if err != nil {
		return &Service{Collector: collector, startupErr: fmt.Errorf("failed to get hostname: %w", err)}
	}

	poolSize := config.ServerMQTT.ConsumerConfig.PoolSize
	consumers := make([]*Consumer, 0, poolSize)

	for i := 0; i < poolSize; i++ {
		clientID := fmt.Sprintf("%s_subscriber_%d", hostName, i)
		mqttClient, err := client.NewMqttPubSubClient(ctx, (&Handler{Collector: collector}).MQTTHandler, clientID)
		if err != nil {
			return &Service{
				Collector:  collector,
				startupErr: fmt.Errorf("failed to create MQTT client for %s: %w", clientID, err),
			}
		}
		logger.Infof("[service:] MQTT client created with ID %v", clientID)
		consumers = append(consumers, NewConsumer(mqttClient))
	}
	s := &Service{Collector: collector, consumers: consumers}
	//register the health check of connection to broker
	health.Register("mqtt-broker", s.HealthCheck)

	return s
}

// Init starts all consumers.
func (s *Service) Init(ctx context.Context) error {
	logger.Infof("[service]: MQTT service init called")
	if s.startupErr != nil {
		return fmt.Errorf("mqtt service startup failed: %w", s.startupErr)
	}

	for _, con := range s.consumers {
		if err := con.Init(); err != nil {
			return fmt.Errorf("failed to start consumer: %w", err)
		} else {
			logger.Info("[service:] MQTT consumer started")
		}
	}

	return nil
}

// Name returns the name of the service.
func (*Service) Name() string {
	return "MQTT"
}

// Shutdown stops all consumers gracefully.
func (s *Service) Shutdown(ctx context.Context) error {
	for _, con := range s.consumers {
		if err := con.Shutdown(); err != nil {
			return fmt.Errorf("failed to stop consumer: %w", err)
		} else {
			logger.Info("[service:] MQTT consumer stopped")
		}
	}
	return nil
}

// HealthCheck checks the health all consumers.
func (s *Service) HealthCheck() error {
	for _, con := range s.consumers {
		if status := con.IsHealthy(); !status {
			return errors.New("consumer connection is broken")
		} else {
			logger.Info("[service:] Health check pass")
		}
	}
	return nil
}
