package config

import (
	"bytes"
	"os"
	"strings"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var PublisherKafka publisherKafka
var dynamicKafkaClientConfigPrefix = "PUBLISHER_KAFKA_CLIENT_"

// publisherKafka defines configuration parameters for a Kafka-based publisher.
// It includes flushing behavior and health check settings.
type publisherKafka struct {
	FlushInterval     int         // Interval (in seconds) to flush the events during shutdown
	HealthCheckConfig healthcheck // Configuration for Kafka nroker health check
}

// healthcheck holds settings used to monitor the health of Kafka broker
type healthcheck struct {
	TopicName string // Kafka topic name used for health check
	TimeOut   int    // Timeout duration (in seconds) for health check operations
}

func (k publisherKafka) ToKafkaConfigMap() *confluent.ConfigMap {
	configMap := &confluent.ConfigMap{}
	for key, value := range viper.AllSettings() {
		if strings.HasPrefix(strings.ToUpper(key), dynamicKafkaClientConfigPrefix) {
			clientConfig := key[len(dynamicKafkaClientConfigPrefix):]
			configMap.SetKey(strings.Join(strings.Split(clientConfig, "_"), "."), value)
		}
	}
	return configMap
}

func dynamicKafkaClientConfigLoad() []byte {
	var kafkaConfigs []string
	for _, v := range os.Environ() {
		if strings.HasPrefix(strings.ToUpper(v), dynamicKafkaClientConfigPrefix) {
			kafkaConfigs = append(kafkaConfigs, v)
		}
	}
	yamlFormatted := []byte(
		strings.Replace(strings.Join(kafkaConfigs, "\n"), "=", ": ", -1))
	return yamlFormatted
}

func publisherKafkaConfigLoader() {
	viper.SetDefault("PUBLISHER_KAFKA_CLIENT_QUEUE_BUFFERING_MAX_MESSAGES", "100000")
	viper.SetDefault("PUBLISHER_KAFKA_FLUSH_INTERVAL_MS", "1000")
	viper.SetDefault("PUBLISHER_KAFKA_HEALTHCHECK_TOPIC_NAME", "clickstream-test-log")
	viper.SetDefault("PUBLISHER_KAFKA_HEALTHCHECK_TIMEOUT_MS", "5000")
	viper.MergeConfig(bytes.NewBuffer(dynamicKafkaClientConfigLoad()))

	PublisherKafka = publisherKafka{
		FlushInterval: util.MustGetInt("PUBLISHER_KAFKA_FLUSH_INTERVAL_MS"),
		HealthCheckConfig: healthcheck{
			TopicName: util.MustGetString("PUBLISHER_KAFKA_HEALTHCHECK_TOPIC_NAME"),
			TimeOut:   util.MustGetInt("PUBLISHER_KAFKA_HEALTHCHECK_TIMEOUT_MS"),
		},
	}
}
