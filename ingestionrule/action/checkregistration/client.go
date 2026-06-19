package checkregistration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type Publisher struct {
	Name string
}

type Event struct {
	Product   string
	EventName string
}

type publishersResponse struct {
	Success bool     `json:"success"`
	Data    []string `json:"data"`
}

type eventsResponse struct {
	Success bool                    `json:"success"`
	Data    map[string]eventPayload `json:"data"`
}

type eventPayload struct {
	Publisher string `json:"publisher"`
	Product   string `json:"product"`
	Name      string `json:"name"`
}

type MSLClient struct {
	baseURL string
	client  *http.Client
}

func NewMSLClient(baseURL string) *MSLClient {
	return &MSLClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 2 * time.Minute,
		},
	}
}

func (c *MSLClient) GetPublishers(
	ctx context.Context,
) ([]Publisher, error) {

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s/v1/publishers", c.baseURL),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"get publishers failed: status=%d",
			resp.StatusCode,
		)
	}

	var result publishersResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	publishers := make([]Publisher, 0, len(result.Data))

	for _, p := range result.Data {
		publishers = append(publishers, Publisher{
			Name: p,
		})
	}

	return publishers, nil
}

func (c *MSLClient) GetEvents(
	ctx context.Context,
	publisher string,
) ([]Event, error) {

	u, err := url.Parse(
		fmt.Sprintf("%s/v1/events", c.baseURL),
	)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("publisher", publisher)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		u.String(),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"get events failed: publisher=%s status=%d",
			publisher,
			resp.StatusCode,
		)
	}

	var result eventsResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	events := make([]Event, 0, len(result.Data))

	for _, e := range result.Data {
		events = append(events, Event{
			Product:   e.Product,
			EventName: e.Name,
		})
	}

	return events, nil
}
