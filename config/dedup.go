package config

import (
	"encoding/json"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// DedupCfg holds runtime configuration for the deduplication feature.
var DedupCfg dedupConfig

type Identifier struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
}

type dedupConfig struct {
	// Enabled controls whether deduplication is active.
	// Set DEDUP_ENABLED=true to enable.
	Enabled bool
	// IdentifierMapping maps publisher to user/session IDs.
	PublisherIdentifierMapping map[string]Identifier
	// ProtoClassNameMapping maps event_type to proto class name.
	ProtoClassNameMapping map[string]string
	// WhitelistConnGroup is a list of connection groups that are exempt from deduplication.
	WhitelistConnGroup map[string]struct{}
}

func dedupConfigLoader() {
	viper.SetDefault("DEDUP_ENABLED", "false")

	pubIdentifierMap := make(map[string]Identifier)
	rawMapping := util.MustGetString("DEDUP_IDENTIFIER_MAPPING")
	if rawMapping != "" {
		if err := json.Unmarshal([]byte(rawMapping), &pubIdentifierMap); err != nil {
			panic("config: invalid DEDUP_IDENTIFIER_MAPPING: " + err.Error())
		}
	}

	rawWhitelist := util.MustGetStringSlice("DEDUP_WHITELIST_CONN_GROUP")
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

	DedupCfg = dedupConfig{
		Enabled:                    util.MustGetBool("DEDUP_ENABLED"),
		WhitelistConnGroup:         whitelistMap,
		PublisherIdentifierMapping: pubIdentifierMap,
		ProtoClassNameMapping:      protoClassNameMap,
	}
}
