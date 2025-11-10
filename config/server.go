package config

import (
	"time"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

var Server server
var ServerWs serverWs
var ServerGRPC serverGRPC
var ServerMQTT serverMQTT

type server struct {
	DedupEnabled bool
}

type serverWs struct {
	AppPort           string
	ServerMaxConn     int
	ReadBufferSize    int
	WriteBufferSize   int
	CheckOrigin       bool
	PingInterval      time.Duration
	PongWaitInterval  time.Duration
	WriteWaitInterval time.Duration
	PingerSize        int
	ConnIDHeader      string
	ConnGroupHeader   string
	ConnGroupDefault  string
}

type serverGRPC struct {
	Port         string
	TLSEnabled   bool
	TLSCertPath  string
	TLSPublicKey string
}

// serverMQTT represents the complete configuration for an MQTT server setup.
// It includes authentication, Consul configuration, and consumer-specific settings.
type serverMQTT struct {
	ConsulConfig   consul   // Configuration related to Consul service discovery
	AuthConfig     auth     // MQTT authentication credentials
	ConsumerConfig consumer // Consumer behavior and connection settings
	ConnGroup      string   // Connection group name used for identifying MQTT client group
}

// auth defines MQTT authentication credentials.
type auth struct {
	Username string // Username for authenticating with the MQTT broker
	Password string // Password for authenticating with the MQTT broker
}

// consul holds configuration details for connecting and interacting with a Consul agent.
type consul struct {
	Address    string        // Address of the Consul agent (e.g., localhost:8500)
	HealthOnly bool          // When true, only healthy service instances are used
	KVKey      string        // Key in Consul KV store for retrieving MQTT Broker Address
	WaitTime   time.Duration // Maximum wait time for Consul blocking queries
}

// consumer contains configuration parameters controlling MQTT message consumption.
type consumer struct {
	RetryIntervalInSec time.Duration // Time interval (in seconds) before retrying connection
	LogLevel           string        // Log verbosity level (e.g., "info", "debug", "error")
	WriteTimeoutInSec  time.Duration // Timeout duration (in seconds) for write operations
	PoolSize           int           // Number of concurrent consumers to consume from MQTT topic
	TopicFormat        string        // Format or pattern for subscribing to MQTT topics (e.g., "share/raccoon/{service}")
	KeepAlive          time.Duration // Amount of time that the client should wait before sending a PING request to the broker
}

func serverConfigLoader() {
	viper.SetDefault("SERVER_BATCH_DEDUP_IN_CONNECTION_ENABLED", "false")
	Server = server{
		DedupEnabled: util.MustGetBool("SERVER_BATCH_DEDUP_IN_CONNECTION_ENABLED"),
	}
}

func serverWsConfigLoader() {
	viper.SetDefault("SERVER_WEBSOCKET_PORT", "8080")
	viper.SetDefault("SERVER_WEBSOCKET_MAX_CONN", 30000)
	viper.SetDefault("SERVER_WEBSOCKET_READ_BUFFER_SIZE", 10240)
	viper.SetDefault("SERVER_WEBSOCKET_WRITE_BUFFER_SIZE", 10240)
	viper.SetDefault("SERVER_WEBSOCKET_CHECK_ORIGIN", true)
	viper.SetDefault("SERVER_WEBSOCKET_PING_INTERVAL_MS", "30000")
	viper.SetDefault("SERVER_WEBSOCKET_PONG_WAIT_INTERVAL_MS", "60000") //should be more than the ping period
	viper.SetDefault("SERVER_WEBSOCKET_WRITE_WAIT_INTERVAL_MS", "5000")
	viper.SetDefault("SERVER_WEBSOCKET_PINGER_SIZE", 1)
	viper.SetDefault("SERVER_WEBSOCKET_CONN_GROUP_HEADER", "")
	viper.SetDefault("SERVER_WEBSOCKET_CONN_GROUP_DEFAULT", "--default--")

	ServerWs = serverWs{
		AppPort:           util.MustGetString("SERVER_WEBSOCKET_PORT"),
		ServerMaxConn:     util.MustGetInt("SERVER_WEBSOCKET_MAX_CONN"),
		ReadBufferSize:    util.MustGetInt("SERVER_WEBSOCKET_READ_BUFFER_SIZE"),
		WriteBufferSize:   util.MustGetInt("SERVER_WEBSOCKET_WRITE_BUFFER_SIZE"),
		CheckOrigin:       util.MustGetBool("SERVER_WEBSOCKET_CHECK_ORIGIN"),
		PingInterval:      util.MustGetDuration("SERVER_WEBSOCKET_PING_INTERVAL_MS", time.Millisecond),
		PongWaitInterval:  util.MustGetDuration("SERVER_WEBSOCKET_PONG_WAIT_INTERVAL_MS", time.Millisecond),
		WriteWaitInterval: util.MustGetDuration("SERVER_WEBSOCKET_WRITE_WAIT_INTERVAL_MS", time.Millisecond),
		PingerSize:        util.MustGetInt("SERVER_WEBSOCKET_PINGER_SIZE"),
		ConnIDHeader:      util.MustGetString("SERVER_WEBSOCKET_CONN_ID_HEADER"),
		ConnGroupHeader:   util.MustGetString("SERVER_WEBSOCKET_CONN_GROUP_HEADER"),
		ConnGroupDefault:  util.MustGetString("SERVER_WEBSOCKET_CONN_GROUP_DEFAULT"),
	}
}

func serverGRPCConfigLoader() {
	viper.SetDefault("SERVER_GRPC_PORT", "8081")
	viper.SetDefault("SERVER_GRPC_TLS_ENABLED", false)
	viper.SetDefault("SERVER_GRPC_TLS_CERT_PATH", "cert/server.crt")
	viper.SetDefault("SERVER_GRPC_TLS_PUBLIC_KEY", "cert/server.key")
	ServerGRPC = serverGRPC{
		Port:         util.MustGetString("SERVER_GRPC_PORT"),
		TLSEnabled:   util.MustGetBool("SERVER_GRPC_TLS_ENABLED"),
		TLSCertPath:  util.MustGetString("SERVER_GRPC_TLS_CERT_PATH"),
		TLSPublicKey: util.MustGetString("SERVER_GRPC_TLS_PUBLIC_KEY"),
	}
}

func serverMQTTConfigLoader() {
	viper.SetDefault("SERVER_MQTT_CONSUL_ADDRESS", "consul:8081")
	viper.SetDefault("SERVER_MQTT_CONSUL_KV_KEY", "kv/path")
	viper.SetDefault("SERVER_MQTT_CONSUL_HEALTH_ONLY", true)
	viper.SetDefault("SERVER_MQTT_CONSUL_WAIT_TIME", 300)
	viper.SetDefault("SERVER_MQTT_AUTH_USERNAME", "test")
	viper.SetDefault("SERVER_MQTT_AUTH_PASSWORD", "pass")
	viper.SetDefault("SERVER_MQTT_CONSUMER_RETRY_INTERVAL_IN_SEC", 1)
	viper.SetDefault("SERVER_MQTT_CONSUMER_WRITE_TIMEOUT_IN_SEC", 1)
	viper.SetDefault("SERVER_MQTT_CONSUMER_KEEP_ALIVE_IN_SEC", 45)
	viper.SetDefault("SERVER_MQTT_CONSUMER_LOG_LEVEL", "warn")
	viper.SetDefault("SERVER_MQTT_CONSUMER_POOL_SIZE", 1)
	viper.SetDefault("SERVER_MQTT_CONSUMER_TOPIC_FORMAT", "default-topic")
	viper.SetDefault("SERVER_MQTT_CONNECTION_GROUP", "default")

	ServerMQTT = serverMQTT{
		ConsulConfig: consul{
			Address:    util.MustGetString("SERVER_MQTT_CONSUL_ADDRESS"),
			HealthOnly: util.MustGetBool("SERVER_MQTT_CONSUL_HEALTH_ONLY"),
			KVKey:      util.MustGetString("SERVER_MQTT_CONSUL_KV_KEY"),
			WaitTime:   util.MustGetDuration("SERVER_MQTT_CONSUL_WAIT_TIME", time.Second),
		},
		AuthConfig: auth{
			Username: util.MustGetString("SERVER_MQTT_AUTH_USERNAME"),
			Password: util.MustGetString("SERVER_MQTT_AUTH_PASSWORD"),
		},
		ConsumerConfig: consumer{
			RetryIntervalInSec: util.MustGetDuration("SERVER_MQTT_CONSUMER_RETRY_INTERVAL_IN_SEC", time.Second),
			LogLevel:           util.MustGetString("SERVER_MQTT_CONSUMER_LOG_LEVEL"),
			WriteTimeoutInSec:  util.MustGetDuration("SERVER_MQTT_CONSUMER_WRITE_TIMEOUT_IN_SEC", time.Second),
			PoolSize:           util.MustGetInt("SERVER_MQTT_CONSUMER_POOL_SIZE"),
			TopicFormat:        util.MustGetString("SERVER_MQTT_CONSUMER_TOPIC_FORMAT"),
			KeepAlive:          util.MustGetDuration("SERVER_MQTT_CONSUMER_KEEP_ALIVE_IN_SEC", time.Second),
		},
		ConnGroup: util.MustGetString("SERVER_MQTT_CONNECTION_GROUP"),
	}
}
