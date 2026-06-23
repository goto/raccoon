package deserialization

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

// HTTPClient is an interface for making HTTP requests.
type HTTPClient interface {
	// Do sends an HTTP request and returns an HTTP response.
	Do(req *http.Request) (*http.Response, error)
}

type SchemaCache struct {
	httpHost     string
	authEmail    string
	httpClient   HTTPClient
	schemaMap    atomic.Value
	ctx          context.Context
	cancel       context.CancelFunc
	syncInterval time.Duration
}

func NewSchemaCache(ctx context.Context, httpHost, authEmail string, syncInterval, httpTimeout time.Duration) *SchemaCache {
	cCtx, cancel := context.WithCancel(ctx)
	c := &SchemaCache{
		httpHost:     strings.TrimSuffix(httpHost, "/"),
		authEmail:    authEmail,
		httpClient:   &http.Client{Timeout: httpTimeout},
		ctx:          cCtx,
		cancel:       cancel,
		syncInterval: syncInterval,
	}

	c.schemaMap.Store(make(map[string]string))

	return c
}

// Start begins the schema cache's background worker to periodically fetch and update the schema map.
func (c *SchemaCache) Start() {
	if err := c.sync(); err != nil {
		logger.Errorf("schema cache initial sync failed: %v", err)
	}

	go c.worker()
}

// Close cancels the schema cache's background worker and frees resources.
func (c *SchemaCache) Close() {
	c.cancel()
}

// Get retrieves the proto class name for a given topic.
func (c *SchemaCache) Get(topic string) (string, bool) {
	val := c.schemaMap.Load()
	if val == nil {
		return "", false
	}

	m, ok := val.(map[string]string)
	if !ok {
		return "", false
	}

	protoClass, ok := m[topic]

	return protoClass, ok
}

// worker runs in a background goroutine and periodically fetches and updates the schema map.
func (c *SchemaCache) worker() {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	const metricNameSchemaSyncFailureCount = "schema_sync_failure_count"

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.sync(); err != nil {
				metrics.Increment(metricNameSchemaSyncFailureCount, "")
				logger.Errorf("schema cache sync failed: %v", err)
			}
		}
	}
}

// sync fetches the latest schema map from Compass and updates the local cache.
func (c *SchemaCache) sync() error {
	newMap, err := c.fetchSchemaMap()
	if err != nil {
		return err
	}

	c.schemaMap.Store(newMap)

	return nil
}

// fetchSchemaMap fetches the latest schema map from Compass and returns it.
func (c *SchemaCache) fetchSchemaMap() (map[string]string, error) {
	if c.httpHost == "" {
		return nil, errors.New("compass HTTP host is empty")
	}

	const (
		queryParamsText         = "text"
		queryParamsDisableFuzzy = "flags.disable_fuzzy"
		queryParamsProjectID    = "filter[data.attributes.project_id]"
		queryParamsStreamName   = "filter[data.attributes.stream_name]"
		queryParamsTypeFilter   = "filter[type]"
	)

	params := url.Values{}
	params.Add(queryParamsText, "clickstream--log")
	params.Add(queryParamsDisableFuzzy, "true")
	params.Add(queryParamsProjectID, config.CompassCfg.ProjectIDLocation)
	params.Add(queryParamsStreamName, "extstream")
	params.Add(queryParamsTypeFilter, "topic")

	const compassSearchEndpoint = "/v1beta1/search"

	reqURL := fmt.Sprintf("%s%s?%s", c.httpHost, compassSearchEndpoint, params.Encode())
	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	const requestHeaderXAuthEmail = "X-Auth-Email"

	if c.authEmail != "" {
		req.Header.Set(requestHeaderXAuthEmail, c.authEmail)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-200 status code: %d, body: %s", resp.StatusCode, readResponseBodySnippet(resp.Body))
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return nil, fmt.Errorf("received non-json response type: %q, body: %s", contentType, readResponseBodySnippet(resp.Body))
	}

	var parsedResp compassResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsedResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	newMap := make(map[string]string)
	for _, item := range parsedResp.Data {
		if len(item.Data.Attributes.Schemas) >= 1 && item.Data.Attributes.Schemas[0].Name != "" {
			newMap[item.Name] = item.Data.Attributes.Schemas[0].Name
		}
	}

	return newMap, nil
}

// readResponseBodySnippet reads the first 1024 bytes from the response body and returns it as a string.
func readResponseBodySnippet(body io.Reader) string {
	limitReader := io.LimitReader(body, 1024)
	bodyBytes, _ := io.ReadAll(limitReader)

	return string(bodyBytes)
}
