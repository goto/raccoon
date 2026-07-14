package config

import (
	"github.com/goto/raccoon/logger"
	"github.com/spf13/viper"
)

var loaded bool

// Load configs from env or yaml and set it to respective keys
func Load() {
	if loaded {
		return
	}
	loaded = true
	viper.AutomaticEnv()
	viper.SetConfigName(".env")
	viper.AddConfigPath("./")
	viper.AddConfigPath("../")
	viper.AddConfigPath("../../")
	viper.SetConfigType("env")
	viper.ReadInConfig()

	logConfigLoader()
	publisherKafkaConfigLoader()
	serverConfigLoader()
	serverWsConfigLoader()
	serverGRPCConfigLoader()
	workerConfigLoader()
	metricStatsdConfigLoader()
	eventDistributionConfigLoader()
	eventConfigLoader()
	serverMQTTConfigLoader()
	policyConfigLoader()
	redisConfigLoader()
	dedupConfigLoader()
	stencilConfigLoader()
	deserializationConfigLoader()
	compassConfigLoader()
	metadataLayerConfigLoader()
}

func LogConfig() {
	logger.Info("Config: Log level: " + Log.Level)
	logger.Infof("Config: Server: %+v", Server)
	logger.Infof("Config: ServerWs: %+v", ServerWs)
	logger.Infof("Config: ServerGRPC: %+v", ServerGRPC)

	mqttSafe := ServerMQTT
	mqttSafe.AuthConfig.Password = "[REDACTED]"
	logger.Infof("Config: ServerMQTT: %+v", mqttSafe)

	logger.Infof("Config: Worker: %+v", Worker)
	logger.Infof("Config: Event: %+v", Event)
	logger.Infof("Config: PublisherKafka: %+v", PublisherKafka)
	logger.Infof("Config: MetricStatsd: %+v", MetricStatsd)
	logger.Infof("Config: EventDistribution: %+v", EventDistribution)
	logger.Infof("Config: PolicyCfg: %+v", PolicyCfg)

	redisSafe := RedisCfg
	redisSafe.Password = "[REDACTED]"
	logger.Infof("Config: RedisCfg: %+v", redisSafe)

	logger.Infof("Config: DedupCfg: %+v", DedupCfg)
	logger.Infof("Config: StencilCfg: %+v", StencilCfg)
	logger.Infof("Config: DeserializationCfg: %+v", DeserializationCfg)
	logger.Infof("Config: CompassCfg: %+v", CompassCfg)
	logger.Infof("Config: MetadataLayerCfg: %+v", MetadataLayerCfg)
}
