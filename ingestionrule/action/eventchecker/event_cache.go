package eventchecker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/goto/raccoon/config"
	httpwrapper "github.com/goto/raccoon/ingestionrule/http"
	"github.com/goto/raccoon/ingestionrule/synccache"
	"github.com/goto/raccoon/logger"
)

// HTTPClient is an interface for making HTTP requests.
type HTTPClient interface {
	// DoRequest sends an HTTP request.
	DoRequest(ctx context.Context, request httpwrapper.Request) (json.RawMessage, error)
}

type EventCache struct {
	cache      *synccache.Cache[map[string]EventStatus]
	httpClient HTTPClient
	httpHost   string
}

func NewEventCache(ctx context.Context) *EventCache {
	e := &EventCache{
		httpClient: httpwrapper.NewHTTPClient(
			config.MslCfg.HTTPRequestTimeout,
			config.MslCfg.HTTPMaxRetry,
			config.MslCfg.HTTPRetryBackoff,
		),
		httpHost: strings.TrimSuffix(config.MslCfg.HTTPHost, "/"),
	}

	e.cache = synccache.NewCache(
		ctx,
		"event cache",
		e.loadEventMap,
		config.MslCfg.SyncInterval,
		make(map[string]EventStatus),
	)

	return e
}

func (e *EventCache) Start() {
	if e == nil || e.cache == nil {
		return
	}

	e.cache.Start()
}

func (e *EventCache) Close() {
	if e == nil || e.cache == nil {
		return
	}

	e.cache.Close()
}

func (e *EventCache) GetEvents(key string) (EventStatus, bool) {
	if e == nil {
		return EventStatusUnspecified, false
	}

	m := e.cache.Get()
	status, ok := m[key]

	return status, ok
}

// HealthCheck checks the health of the MSL by sending a GET request to the /ping endpoint.
func (e *EventCache) HealthCheck() error {
	if e == nil {
		return errors.New("event cache is disabled")
	}

	if e.httpHost == "" {
		return errors.New("event cache: MSL HTTP host is empty")
	}

	_, err := e.httpClient.DoRequest(context.Background(), httpwrapper.Request{
		Method:  http.MethodGet,
		BaseURL: e.httpHost,
		Path:    "/ping",
	})
	if err != nil {
		return fmt.Errorf("event cache health check request failed: %w", err)
	}

	return nil
}

// loadEventMap fetches events for all configured publishers concurrently.
func (e *EventCache) loadEventMap(ctx context.Context) (map[string]EventStatus, error) {
	var publishers []string
	for _, pub := range config.PolicyCfg.PublisherMapping {
		publishers = append(publishers, pub)
	}

	type result struct {
		publisher string
		events    []Event
		err       error
	}

	results := make(chan result, len(publishers))
	for _, pub := range publishers {
		go func(publisher string) {
			events, err := e.fetchEvents(ctx, publisher)
			results <- result{
				publisher: publisher,
				events:    events,
				err:       err,
			}
		}(pub)
	}

	newEvents := make(map[string]EventStatus)
	var errs []error

	for i := 0; i < len(publishers); i++ {
		res := <-results
		if res.err != nil {
			errs = append(errs, fmt.Errorf("failed to fetch events for publisher %s: %v", res.publisher, res.err))
			continue
		}

		for _, evt := range res.events {
			key := e.buildCacheKey(evt)
			newEvents[key] = evt.Status
		}
	}

	if len(errs) > 0 {
		logger.Errorf("event cache: failed to fetch events for one or more publishers: %v", errs)
		return nil, fmt.Errorf("event cache: failed to fetch events for one or more publishers: %w", errors.Join(errs...))
	}

	logger.Infof("event cache successfully loaded %d registered events", len(newEvents))
	return newEvents, nil
}

// fetchEvents retrieves a list of events for a specific publisher from the MSL API.
func (e *EventCache) fetchEvents(ctx context.Context, publisher string) ([]Event, error) {
	if e.httpHost == "" {
		return nil, errors.New("MSL HTTP host is empty")
	}

	q := url.Values{}
	q.Set("publisher", publisher)
	q.Set("type", "clickstream")

	bodyData, err := e.httpClient.DoRequest(ctx, httpwrapper.Request{
		Method:      http.MethodGet,
		BaseURL:     e.httpHost,
		Path:        "/v1/events",
		QueryParams: q,
	})
	if err != nil {
		return nil, err
	}

	var resp eventsResponse
	if err := json.Unmarshal(bodyData, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	events := make([]Event, 0, len(resp.Data))
	for _, e := range resp.Data {
		events = append(events, Event{
			Product:   e.Product,
			EventName: e.Name,
			Publisher: e.Publisher,
			TableName: e.Source.Table,
			Status:    EventStatusActive,
		})
	}

	return events, nil
}

// buildCacheKey constructs a deterministic, fixed-length unique identifier
// for an event payload to be stored in in-memory cache.
//
// Algorithm & Performance:
// It utilizes the xxHash v2 (XXH64) non-cryptographic hashing algorithm.
// XXH64 uses highly optimized 64-bit arithmetic and assembly language, operating near
// RAM speed limits while maintaining an exceptionally low collision probability suitable
// for high-throughput deduplication keys.
//
// Key Format:
// The generated string is always a contiguous 16-character lowercase hexadecimal string
// representing the complete 64-bit signature ($8 \text{ bytes} \times 2 \text{ hex characters/byte}$)
// padded with leading zeros if necessary
func (e *EventCache) buildCacheKey(event Event) string {
	d := xxhash.New()

	const keySeparator = ":"

	// key: <publisher>:<topic>:<product>:<event_name>
	_, _ = io.WriteString(d, event.Publisher)
	_, _ = io.WriteString(d, keySeparator)
	_, _ = io.WriteString(d, strings.ReplaceAll(event.TableName, "_", "-"))
	_, _ = io.WriteString(d, keySeparator)
	_, _ = io.WriteString(d, event.Product)
	_, _ = io.WriteString(d, keySeparator)
	_, _ = io.WriteString(d, event.EventName)
	hash := d.Sum64()

	return fmt.Sprintf("%016x", hash)
}
