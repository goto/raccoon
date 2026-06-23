package deserialization

type compassResponse struct {
	Data []compassItem `json:"data"`
}

type compassItem struct {
	Name string      `json:"name"`
	Data compassData `json:"data"`
}

type compassData struct {
	Attributes compassAttributes `json:"attributes"`
}

type compassAttributes struct {
	Schemas []compassSchema `json:"schemas"`
}

type compassSchema struct {
	Name string `json:"name"`
}
