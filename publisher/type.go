package publisher

type IngestEvent struct {
	Publisher  string `json:"publisher"`
	EventName  string `json:"eventName"`
	Product    string `json:"product"`
	EventCount int    `json:"eventCount"`
}

type IngestPayload struct {
	// ConnGroup removed from top level based on your snippet
	ReqQuid string        `json:"req_quid"`
	Events  []IngestEvent `json:"events"`
}
