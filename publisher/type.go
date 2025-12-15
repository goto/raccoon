package publisher

type IngestPayload struct {
	ConnGroup string        `json:"connGroup"`
	ReqQuid   string        `json:"req_quid"`
	Events    []IngestEvent `json:"events"`
}

type IngestEvent struct {
	Type           string `json:"type"`
	EventName      string `json:"eventName"`
	Product        string `json:"product"`
	EventTimestamp string `json:"eventTimestamp"`
	Bytes          string `json:"bytes"`
}
