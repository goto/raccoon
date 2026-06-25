package config

import (
	"encoding/json"

	"github.com/spf13/viper"

	"github.com/goto/raccoon/config/util"
)

// DeserializationCfg holds runtime configuration for the deserialization feature.
var DeserializationCfg deserializationConfig

type deserializationConfig struct {
	// Enabled controls whether deserialization is active.
	// Set DESERIALIZATION_ENABLED=true to enable.
	Enabled bool
	// AppVersionPublisherWhitelist controls the publisher list that is allowed to
	// pass event that has app_version in metadata.
	// If the list is empty, it means all publishers are allowed.
	AppVersionPublisherWhitelist []string
	// PlatformPublisherWhitelist controls the publisher list that is allowed to
	// pass event that has platform in metadata.
	// If the list is empty, it means all publishers are allowed.
	PlatformPublisherWhitelist []string
	// ExcludeEventTypeList controls the event type list that is not allowed to
	// be deserialized.
	// If the list is empty, it means no event types are excluded.
	ExcludeEventTypeList []string
}

func deserializationConfigLoader() {
	viper.SetDefault("DESERIALIZATION_ENABLED", "false")
	viper.SetDefault("DESERIALIZATION_APP_VERSION_PUBLISHER_WHITELIST", "[]")
	viper.SetDefault("DESERIALIZATION_PLATFORM_PUBLISHER_WHITELIST", "[]")
	viper.SetDefault("DESERIALIZATION_EXCLUDE_EVENT_TYPE_LIST", "[]")

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

	var excludeEventTypeList []string
	rawExcludeEventTypeList := util.MustGetString("DESERIALIZATION_EXCLUDE_EVENT_TYPE_LIST")
	if rawExcludeEventTypeList != "" && rawExcludeEventTypeList != "[]" {
		if err := json.Unmarshal([]byte(rawExcludeEventTypeList), &excludeEventTypeList); err != nil {
			panic("deserialization: invalid DESERIALIZATION_EXCLUDE_EVENT_TYPE_LIST: " + err.Error())
		}
	}

	DeserializationCfg = deserializationConfig{
		Enabled:                      util.MustGetBool("DESERIALIZATION_ENABLED"),
		AppVersionPublisherWhitelist: appVersionWhitelist,
		PlatformPublisherWhitelist:   platformWhitelist,
		ExcludeEventTypeList:         excludeEventTypeList,
	}
}
