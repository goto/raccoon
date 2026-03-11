package config

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/goto/raccoon/config/util"
	"github.com/spf13/viper"
)

// ---- Policy rule schema types ----

// PolicyResourceType identifies the kind of resource a policy applies to.
type PolicyResourceType string

// PolicyActionType identifies the action to be taken when a policy condition is met.
type PolicyActionType string

// PolicyConditionType identifies the kind of condition that triggers an action.
type PolicyConditionType string

const (
	PolicyResourceEvent PolicyResourceType = "event"
	PolicyResourceTopic PolicyResourceType = "topic"

	PolicyActionDrop              PolicyActionType = "DROP"
	PolicyActionOverrideTimestamp PolicyActionType = "OVERRIDE_TIMESTAMP"

	// PolicyConditionTimestampThreshold triggers the action when the event timestamp
	// falls outside the configured past/future window.
	PolicyConditionTimestampThreshold PolicyConditionType = "timestamp_threshold"
)

// PolicyDuration is a time.Duration that unmarshals from a Go duration string (e.g. "1h", "30m").
type PolicyDuration struct {
	time.Duration
}

func (d *PolicyDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("policy: duration must be a string, got: %w", err)
	}
	if s == "" {
		d.Duration = 0
		return nil
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("policy: invalid duration %q: %w", s, err)
	}
	d.Duration = dur
	return nil
}

func (d PolicyDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// PolicyTimestampThreshold defines the allowed time window for an event timestamp.
// A zero duration means no limit in that direction.
type PolicyTimestampThreshold struct {
	Past   PolicyDuration `json:"past"`
	Future PolicyDuration `json:"future"`
}

// PolicyActionConfig describes what action to take and the condition that triggers it.
type PolicyActionConfig struct {
	Type                    PolicyActionType         `json:"type"`
	ConditionType           PolicyConditionType      `json:"condition_type"`
	EventTimestampThreshold PolicyTimestampThreshold `json:"event_timestamp_threshold"`
}

// PolicyDetails holds the matching criteria for a policy rule.
// An empty string in any field acts as a wildcard (matches any value).
type PolicyDetails struct {
	Name      string `json:"name"`
	Product   string `json:"product,omitempty"`
	Publisher string `json:"publisher"`
}

// PolicyRule is the top-level structure representing one policy entry.
// Example JSON:
//
//	{"resource":"event","details":{"name":"click","product":"app","publisher":"gojek"},
//	 "action":{"type":"DROP","event_timestamp_threshold":{"past":"24h","future":"1h"}}}
type PolicyRule struct {
	Resource PolicyResourceType `json:"resource"`
	Details  PolicyDetails      `json:"details"`
	Action   PolicyActionConfig `json:"action"`
}

// ---- Policy system configuration ----

// PolicyCfg holds runtime configuration for the event ingestion policy feature.
var PolicyCfg policyConfig

type policyConfig struct {
	// Enabled controls whether policy enforcement is active.
	// Set POLICY_ENABLED=true to enable.
	Enabled bool
	// Rules holds the parsed policy rules.
	// Configure via POLICY_CONFIG as a JSON array string, e.g.:
	//   POLICY_CONFIG='[{"resource":"event","details":{...},"action":{...}}]'
	Rules []PolicyRule
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
	viper.SetDefault("POLICY_CONFIG", "[]")
	viper.SetDefault("POLICY_OVERRIDE_TOPIC", "clickstream-invalid-et-log")
	viper.SetDefault("POLICY_PUBLISHER_MAPPING", "")

	var rules []PolicyRule
	rawConfig := util.MustGetString("POLICY_CONFIG")
	if rawConfig != "" && rawConfig != "[]" {
		_ = json.Unmarshal([]byte(rawConfig), &rules)
	}

	publisherMapping := make(map[string]string)
	rawMapping := util.MustGetString("POLICY_PUBLISHER_MAPPING")
	if rawMapping != "" {
		_ = json.Unmarshal([]byte(rawMapping), &publisherMapping)
	}

	PolicyCfg = policyConfig{
		Enabled:          util.MustGetBool("POLICY_ENABLED"),
		Rules:            rules,
		OverrideTopic:    util.MustGetString("POLICY_OVERRIDE_TOPIC"),
		PublisherMapping: publisherMapping,
	}
}
