package config

import (
	"encoding/json"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// DeserializationCfg holds runtime configuration for the deserialization feature.
var DeserializationCfg deserializationConfig

type deserializationConfig struct {
	// Enabled controls whether deserialization is active.
	// Set DESERIALIZATION_ENABLED=true to enable.
	Enabled                        bool
	// AppVersionPublisherWhitelist controls the publisher list that is allowed to 
	// pass event that has app_version in metadata.
	// If the list is empty, it means all publishers are allowed.
	AppVersionPublisherWhitelist   []string
	// PlatformPublisherWhitelist controls the publisher list that is allowed to 
	// pass event that has platform in metadata.
	// If the list is empty, it means all publishers are allowed.
	PlatformPublisherWhitelist     []string
}

func deserializationConfigLoader() {
	viper.SetDefault("DESERIALIZATION_ENABLED", "false")
	viper.SetDefault("DESERIALIZATION_APP_VERSION_PUBLISHER_WHITELIST", "[]")
	viper.SetDefault("DESERIALIZATION_PLATFORM_PUBLISHER_WHITELIST", "[]")

	var appVersionWhitelist []string
	rawAppVersionWhitelist := util.MustGetString("DESERIALIZATION_APP_VERSION_PUBLISHER_WHITELIST")
	if rawAppVersionWhitelist != "" && rawAppVersionWhitelist != "[]" {
		if err := json.Unmarshal([]byte(rawAppVersionWhitelist), &appVersionWhitelist); err != nil {
			panic("deserialization: invalid DESERIALIZATION_APP_VERSION_PUBLISHER_WHITELIST: " + err.Error())
		}
	}

	var platformWhitelist []string
	rawPlatformWhitelist := util.MustGetString("DESERIALIZATION_PLATFORM_PUBLISHER_WHITELIST")
	if rawPlatformWhitelist != "" && rawPlatformWhitelist != "[]" {
		if err := json.Unmarshal([]byte(rawPlatformWhitelist), &platformWhitelist); err != nil {
			panic("deserialization: invalid DESERIALIZATION_PLATFORM_PUBLISHER_WHITELIST: " + err.Error())
		}
	}

	DeserializationCfg = deserializationConfig{
		Enabled:                      util.MustGetBool("DESERIALIZATION_ENABLED"),
		AppVersionPublisherWhitelist: appVersionWhitelist,
		PlatformPublisherWhitelist:   platformWhitelist,
	}
}
