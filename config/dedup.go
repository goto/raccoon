package config

import (
	"encoding/json"
	"time"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// DedupCfg holds runtime configuration for the deduplication feature.
var DedupCfg dedupConfig

// Identifier holds the user and session ID field mappings for a connection group.
type Identifier struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
}

type dedupConfig struct {
	// Enabled controls whether deduplication is active.
	// Set DEDUP_ENABLED=true to enable.
	Enabled bool
	// ProtoClassNameMapping maps event_type to proto class name.
	ProtoClassNameMapping map[string]string
	// WhitelistConnGroup is a list of connection groups that are processed with dedup.
	WhitelistConnGroup map[string]struct{}
	// ConnGroupCacheDuration is a map of connection groups to their cache durations.
	ConnGroupCacheDuration map[string]time.Duration
}

func dedupConfigLoader() {
	viper.SetDefault("DEDUP_ENABLED", "false")

	var rawWhitelist []string

	rawWhitelistStr := util.MustGetString("DEDUP_WHITELIST_CONN_GROUP")
	if err := json.Unmarshal([]byte(rawWhitelistStr), &rawWhitelist); err != nil {
		panic("config: invalid DEDUP_WHITELIST_CONN_GROUP: " + err.Error())
	}

	whitelistMap := make(map[string]struct{}, len(rawWhitelist))
	for _, cg := range rawWhitelist {
		whitelistMap[cg] = struct{}{}
	}

	protoClassNameMap := make(map[string]string)
	rawProtoMapping := util.MustGetString("DEDUP_PROTO_CLASS_NAME_MAPPING")
	if rawProtoMapping != "" {
		if err := json.Unmarshal([]byte(rawProtoMapping), &protoClassNameMap); err != nil {
			panic("config: invalid DEDUP_PROTO_CLASS_NAME_MAPPING: " + err.Error())
		}
	}

	rawDurationMap := make(map[string]string)
	rawConnGroupCacheDuration := util.MustGetString("DEDUP_CONN_GROUP_CACHE_DURATION")

	if rawConnGroupCacheDuration != "" {
		if err := json.Unmarshal([]byte(rawConnGroupCacheDuration), &rawDurationMap); err != nil {
			panic("config: invalid DEDUP_CONN_GROUP_CACHE_DURATION JSON format: " + err.Error())
		}
	}

	connGroupCacheDuration := make(map[string]time.Duration)

	for key, stringDuration := range rawDurationMap {
		duration, err := time.ParseDuration(stringDuration)
		if err != nil {
			panic("config: invalid duration string '" + stringDuration + "' for key " + key + ": " + err.Error())
		}
		
		connGroupCacheDuration[key] = duration
	}

	DedupCfg = dedupConfig{
		Enabled:                util.MustGetBool("DEDUP_ENABLED"),
		WhitelistConnGroup:     whitelistMap,
		ProtoClassNameMapping:  protoClassNameMap,
		ConnGroupCacheDuration: connGroupCacheDuration,
	}
}
