package eventregistry

type eventsResponse struct {
	Success bool                      `json:"success"`
	Data    map[string]payloadReadAll `json:"data"`
}

type payloadReadAll struct {
	Publisher string        `json:"publisher"`
	Product   string        `json:"product"`
	Name      string        `json:"name"`
	Source    payloadSource `json:"source"`
}

type payloadSource struct {
	Project string `json:"project"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
}

type Event struct {
	Publisher string
	Product   string
	EventName string
	TableName string
	Status    EventStatus
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
