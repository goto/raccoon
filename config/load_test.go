package config

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	viper.Reset()
	viper.AutomaticEnv()
	os.Exit(m.Run())
}

func TestLogLevel(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	logConfigLoader()
	assert.Equal(t, "debug", Log.Level)
}

func TestServerConfig(t *testing.T) {
	// default value test
	serverConfigLoader()
	assert.Equal(t, false, Server.DedupEnabled)

	// override value test
	os.Setenv("SERVER_BATCH_DEDUP_IN_CONNECTION_ENABLED", "true")
	serverConfigLoader()
	assert.Equal(t, true, Server.DedupEnabled)
}

func TestServerWsConfig(t *testing.T) {
	os.Setenv("SERVER_WEBSOCKET_PORT", "8080")
	os.Setenv("SERVER_WEBSOCKET_PING_INTERVAL_MS", "1")
	os.Setenv("SERVER_WEBSOCKET_PONG_WAIT_INTERVAL_MS", "1")
	os.Setenv("SERVER_WEBSOCKET_SERVER_SHUTDOWN_GRACE_PERIOD_MS", "3")
	os.Setenv("SERVER_WEBSOCKET_CONN_ID_HEADER", "X-User-ID")
	serverWsConfigLoader()
	assert.Equal(t, "8080", ServerWs.AppPort)
	assert.Equal(t, time.Duration(1)*time.Millisecond, ServerWs.PingInterval)
	assert.Equal(t, time.Duration(1)*time.Millisecond, ServerWs.PongWaitInterval)

}

func TestGRPCServerConfig(t *testing.T) {
	os.Setenv("SERVER_GRPC_PORT", "8081")
	serverGRPCConfigLoader()
	assert.Equal(t, "8081", ServerGRPC.Port)
}

func TestServerMQTTConfig(t *testing.T) {
	os.Setenv("SERVER_MQTT_CONSUL_ADDRESS", "consul:8081")
	os.Setenv("SERVER_MQTT_CONSUL_KV_KEY", "kv/path")
	os.Setenv("SERVER_MQTT_CONSUL_HEALTH_ONLY", "true")
	os.Setenv("SERVER_MQTT_CONSUL_WAIT_TIME", "300")
	os.Setenv("SERVER_MQTT_AUTH_USERNAME", "test")
	os.Setenv("SERVER_MQTT_AUTH_PASSWORD", "pass")
	os.Setenv("SERVER_MQTT_CONSUMER_RETRY_INTERVAL_IN_SEC", "1")
	os.Setenv("SERVER_MQTT_CONSUMER_WRITE_TIMEOUT_IN_SEC", "1")
	os.Setenv("SERVER_MQTT_CONSUMER_LOG_LEVEL", "warn")
	os.Setenv("SERVER_MQTT_CONSUMER_POOL_SIZE", "1")
	os.Setenv("SERVER_MQTT_CONSUMER_TOPIC_FORMAT", "default-topic")
	os.Setenv("SERVER_MQTT_CONNECTION_GROUP", "consumer")
	serverMQTTConfigLoader()
	assert.Equal(t, "consul:8081", ServerMQTT.ConsulConfig.Address)
	assert.Equal(t, "kv/path", ServerMQTT.ConsulConfig.KVKey)
	assert.Equal(t, true, ServerMQTT.ConsulConfig.HealthOnly)
	assert.Equal(t, 300*time.Second, ServerMQTT.ConsulConfig.WaitTime)
	assert.Equal(t, "test", ServerMQTT.AuthConfig.Username)
	assert.Equal(t, "pass", ServerMQTT.AuthConfig.Password)
	assert.Equal(t, 1*time.Second, ServerMQTT.ConsumerConfig.RetryIntervalInSec)
	assert.Equal(t, 1*time.Second, ServerMQTT.ConsumerConfig.WriteTimeoutInSec)
	assert.Equal(t, "warn", ServerMQTT.ConsumerConfig.LogLevel)
	assert.Equal(t, 1, ServerMQTT.ConsumerConfig.PoolSize)
	assert.Equal(t, "default-topic", ServerMQTT.ConsumerConfig.TopicFormat)

}

func TestDynamicConfigLoad(t *testing.T) {
	os.Setenv("PUBLISHER_KAFKA_CLIENT_RANDOM", "anything")
	os.Setenv("PUBLISHER_KAFKA_CLIENT_BOOTSTRAP_SERVERS", "localhost:9092")
	viper.SetConfigType("yaml")
	viper.ReadConfig(bytes.NewBuffer(dynamicKafkaClientConfigLoad()))
	assert.Equal(t, "anything", viper.GetString("PUBLISHER_KAFKA_CLIENT_RANDOM"))
	assert.Equal(t, "localhost:9092", viper.GetString("PUBLISHER_KAFKA_CLIENT_BOOTSTRAP_SERVERS"))
}

func TestKafkaConfig_ToKafkaConfigMap(t *testing.T) {
	os.Setenv("PUBLISHER_KAFKA_FLUSH_INTERVAL_MS", "1000")
	os.Setenv("PUBLISHER_KAFKA_CLIENT_BOOTSTRAP_SERVERS", "kafka:9092")
	os.Setenv("PUBLISHER_KAFKA_CLIENT_ACKS", "1")
	os.Setenv("PUBLISHER_KAFKA_CLIENT_QUEUE_BUFFERING_MAX_MESSAGES", "10000")
	os.Setenv("SOMETHING_PUBLISHER_KAFKA_CLIENT_SOMETHING", "anything")
	os.Setenv("PUBLISHER_KAFKA_HEALTHCHECK_TOPIC_NAME", "test-log")
	os.Setenv("PUBLISHER_KAFKA_HEALTHCHECK_TIMEOUT_MS", "5000")
	publisherKafkaConfigLoader()
	kafkaConfig := PublisherKafka.ToKafkaConfigMap()
	bootstrapServer, _ := kafkaConfig.Get("bootstrap.servers", "")
	topic, _ := kafkaConfig.Get("topic", "")
	something, _ := kafkaConfig.Get("client.something", "")
	assert.Equal(t, "kafka:9092", bootstrapServer)
	assert.Equal(t, "", topic)
	assert.NotEqual(t, something, "anything")
	assert.Equal(t, 4, len(*kafkaConfig))
	assert.Equal(t, "test-log", PublisherKafka.HealthCheckConfig.TopicName)
	assert.Equal(t, 5000, PublisherKafka.HealthCheckConfig.TimeOut)
}

func TestWorkerConfig(t *testing.T) {
	os.Setenv("WORKER_POOL_SIZE", "2")
	os.Setenv("WORKER_BUFFER_CHANNEL_SIZE", "5")
	os.Setenv("WORKER_KAFKA_DELIVERY_CHANNEL_SIZE", "10")
	os.Setenv("WORKER_BUFFER_FLUSH_TIMEOUT_MS", "100000")
	workerConfigLoader()
	assert.Equal(t, time.Duration(100)*time.Second, Worker.WorkerFlushTimeout)
	assert.Equal(t, 10, Worker.DeliveryChannelSize)
	assert.Equal(t, 5, Worker.ChannelSize)
	assert.Equal(t, 2, Worker.WorkersPoolSize)
}

func TestPolicyConfig_Defaults(t *testing.T) {
	viper.Reset()
	viper.AutomaticEnv()
	policyConfigLoader()
	assert.False(t, PolicyCfg.Enabled)
	assert.Empty(t, PolicyCfg.Rules)
	assert.Equal(t, "invalid-et", PolicyCfg.OverrideEventType)
	assert.Empty(t, PolicyCfg.PublisherMapping)
}

func TestPolicyConfig_Enabled(t *testing.T) {
	os.Setenv("POLICY_ENABLED", "true")
	policyConfigLoader()
	assert.True(t, PolicyCfg.Enabled)
	os.Unsetenv("POLICY_ENABLED")
}

func TestPolicyConfig_OverrideEventType(t *testing.T) {
	os.Setenv("POLICY_OVERRIDE_TOPIC", "my-override-type")
	policyConfigLoader()
	assert.Equal(t, "my-override-type", PolicyCfg.OverrideEventType)
	os.Unsetenv("POLICY_OVERRIDE_TOPIC")
}

func TestPolicyConfig_Rules(t *testing.T) {
	os.Setenv("POLICY_CONFIG", `[{"resource":"event","details":{"name":"click","product":"app","publisher":"gojek"},"action":{"type":"DROP","condition_type":"timestamp_threshold","event_timestamp_threshold":{"past":"24h","future":"1h"}}}]`)
	policyConfigLoader()
	assert.Len(t, PolicyCfg.Rules, 1)
	r := PolicyCfg.Rules[0]
	assert.Equal(t, PolicyResourceEvent, r.Resource)
	assert.Equal(t, "click", r.Details.Name)
	assert.Equal(t, "app", r.Details.Product)
	assert.Equal(t, "gojek", r.Details.Publisher)
	assert.Equal(t, PolicyActionDrop, r.Action.Type)
	assert.Equal(t, PolicyConditionTimestampThreshold, r.Action.ConditionType)
	assert.Equal(t, 24*time.Hour, r.Action.EventTimestampThreshold.Past.Duration)
	assert.Equal(t, 1*time.Hour, r.Action.EventTimestampThreshold.Future.Duration)
	os.Unsetenv("POLICY_CONFIG")
}

func TestPolicyConfig_PublisherMapping(t *testing.T) {
	os.Setenv("POLICY_PUBLISHER_MAPPING", `{"customer":"gojek","driver":"gopartner"}`)
	policyConfigLoader()
	assert.Equal(t, map[string]string{"customer": "gojek", "driver": "gopartner"}, PolicyCfg.PublisherMapping)
	os.Unsetenv("POLICY_PUBLISHER_MAPPING")
}

func TestPolicyDuration_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{name: "valid duration", input: `"2h"`, expected: 2 * time.Hour},
		{name: "empty string", input: `""`, expected: 0},
		{name: "invalid duration", input: `"notaduration"`, wantErr: true},
		{name: "not a string", input: `123`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d PolicyDuration
			err := d.UnmarshalJSON([]byte(tt.input))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, d.Duration)
			}
		})
	}
}

func TestPolicyDuration_MarshalJSON(t *testing.T) {
	d := PolicyDuration{Duration: 30 * time.Minute}
	b, err := d.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, `"30m0s"`, string(b))
}
