package config

import (
	"bytes"
	"os"
	"strings"
	"time"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
	confluent "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var PublisherKafka publisherKafka
var dynamicKafkaClientConfigPrefix = "PUBLISHER_KAFKA_CLIENT_"

type publisherKafka struct {
	FlushInterval             int
	DeliveryReportInterval    time.Duration
	ClickstreamStatsTopicName string
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
	viper.SetDefault("PUBLISHER_KAFKA_CLICKSTREAM_STATS_TOPIC_NAME", "clickstream-total-event-log")
	viper.MergeConfig(bytes.NewBuffer(dynamicKafkaClientConfigLoad()))
	PublisherKafka = publisherKafka{
		FlushInterval:             util.MustGetInt("PUBLISHER_KAFKA_FLUSH_INTERVAL_MS"),
		ClickstreamStatsTopicName: util.MustGetString("PUBLISHER_KAFKA_CLICKSTREAM_STATS_TOPIC_NAME"),
	}
}
