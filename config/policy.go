package config

import (
	"encoding/json"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// PolicyConfig holds configuration for the event ingestion policy feature.
var PolicyCfg policyConfig

type policyConfig struct {
	// Enabled controls whether policy enforcement is active. Set POLICY_ENABLED=true.
	Enabled bool
	// ConfigFile is the path to the JSON policy rules file. Set POLICY_CONFIG_FILE=<path>.
	ConfigFile string
	// OverrideTopic is the Kafka topic used for OVERRIDE_TIMESTAMP redirection.
	// Defaults to "clickstream-invalid-et-log".
	OverrideTopic string
	// PublisherMapping maps conn_group names to publisher names.
	// Set via POLICY_PUBLISHER_MAPPING as a JSON object string, e.g.:
	//   POLICY_PUBLISHER_MAPPING='{"customer":"gojek","driver":"gopartner"}'
	PublisherMapping map[string]string
}

func policyConfigLoader() {
	viper.SetDefault("POLICY_ENABLED", "false")
	viper.SetDefault("POLICY_CONFIG_FILE", "")
	viper.SetDefault("POLICY_OVERRIDE_TOPIC", "clickstream-invalid-et-log")
	viper.SetDefault("POLICY_PUBLISHER_MAPPING", "")

	publisherMapping := make(map[string]string)
	rawMapping := util.MustGetString("POLICY_PUBLISHER_MAPPING")
	if rawMapping != "" {
		_ = json.Unmarshal([]byte(rawMapping), &publisherMapping)
	}

	PolicyCfg = policyConfig{
		Enabled:          util.MustGetBool("POLICY_ENABLED"),
		ConfigFile:       util.MustGetString("POLICY_CONFIG_FILE"),
		OverrideTopic:    util.MustGetString("POLICY_OVERRIDE_TOPIC"),
		PublisherMapping: publisherMapping,
	}
}
