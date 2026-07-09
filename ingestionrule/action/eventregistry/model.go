package eventregistry

type eventsResponse struct {
	Success bool    `json:"success"`
	Data    []event `json:"data"`
}

type event struct {
	Name    string      `json:"name"`
	Product string      `json:"product"`
	Table   string      `json:"table"`
	Status  EventStatus `json:"status"`
}

const (
	// EventStatusUnspecified represents an unspecified status of event.
	// This is mainly used as a default value, and it is not considered a valid event status.
	EventStatusUnspecified EventStatus = ""
	// EventStatusDraft represents a draft status of event.
	EventStatusDraft EventStatus = "DRAFT"
	// EventStatusActive represents an active status of event.
	EventStatusActive EventStatus = "ACTIVE"
	// EventStatusInactive represents an inactive status of event.
	EventStatusInactive EventStatus = "INACTIVE"
	// EventStatusDeprecated represents a deprecated status of event.
	EventStatusDeprecated EventStatus = "DEPRECATED"
)

// EventStatus represents the status of an event.
type EventStatus string

// String returns the string representation.
func (es EventStatus) String() string {
	return string(es)
}
