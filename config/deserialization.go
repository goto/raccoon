package config

import (
	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// DeserializationCfg holds runtime configuration for the deserialization feature.
var DeserializationCfg deserializationConfig

type deserializationConfig struct {
	// Enabled controls whether deserialization is active.
	// Set DESERIALIZATION_ENABLED=true to enable.
	Enabled bool
}

func deserializationConfigLoader() {
	viper.SetDefault("DESERIALIZATION_ENABLED", "false")

	DeserializationCfg = deserializationConfig{
		Enabled: util.MustGetBool("DESERIALIZATION_ENABLED"),
	}
}
