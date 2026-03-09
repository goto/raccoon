package policy

import (
	"encoding/json"
	"fmt"
	"time"
)

// ResourceType identifies the kind of resource a policy applies to.
type ResourceType string

// ActionType identifies the action to be taken when a policy condition is met.
type ActionType string

const (
	ResourceEvent ResourceType = "event"
	ResourceTopic ResourceType = "topic"

	ActionDrop              ActionType = "DROP"
	ActionOverrideTimestamp ActionType = "OVERRIDE_TIMESTAMP"
)

// Duration is a time.Duration that marshals/unmarshals as a Go duration string (e.g. "1h", "30m").
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
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

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// EventTimestampThreshold defines the allowed time window for an event timestamp.
// Past is how far in the past the timestamp may be; Future is how far ahead.
// A zero value means no limit in that direction.
type EventTimestampThreshold struct {
	Past   Duration `json:"past"`
	Future Duration `json:"future"`
}

// ActionConfig describes what action to take and the timestamp thresholds that trigger it.
type ActionConfig struct {
	Type                    ActionType              `json:"type"`
	EventTimestampThreshold EventTimestampThreshold `json:"event_timestamp_threshold"`
}

// Details holds the matching criteria for a policy.
// For ResourceEvent, Name is the event name, Product is optional, Publisher maps to conn_group.
// For ResourceTopic, Name is the topic name, Publisher maps to conn_group.
type Details struct {
	Name      string `json:"name"`
	Product   string `json:"product,omitempty"`
	Publisher string `json:"publisher"`
}

// PolicyConfig is the top-level structure representing one policy entry.
type PolicyConfig struct {
	Resource ResourceType `json:"resource"`
	Details  Details      `json:"details"`
	Action   ActionConfig `json:"action"`
}
