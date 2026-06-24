package deserialization

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goto/raccoon/config"
	httpwrapper "github.com/goto/raccoon/ingestionrule/schemaregistry/deserialization/http"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

// HTTPClient is an interface for making HTTP requests.
type HTTPClient interface {
	DoRequest(ctx context.Context, request httpwrapper.Request) (json.RawMessage, error)
}

type httpConfig struct {
	httpHost  string
	authEmail string
}

type startupConfig struct {
	startupMaxRetry     int
	startupRetryBackoff time.Duration
}

type SchemaCache struct {
	schemaMap     atomic.Value
	ctx           context.Context
	cancel        context.CancelFunc
	httpClient    HTTPClient
	httpConfig    httpConfig
	startupConfig startupConfig
	syncInterval  time.Duration
}

func NewSchemaCache(ctx context.Context) *SchemaCache {
	cCtx, cancel := context.WithCancel(ctx)
	c := &SchemaCache{
		httpClient: httpwrapper.NewHTTPClient(config.CompassCfg.HTTPRequestTimeout),
		ctx:        cCtx,
		cancel:     cancel,
		httpConfig: httpConfig{
			httpHost:  strings.TrimSuffix(config.CompassCfg.HTTPHost, "/"),
			authEmail: config.CompassCfg.AuthEmail,
		},
		startupConfig: startupConfig{
			startupMaxRetry:     config.CompassCfg.StartupMaxRetry,
			startupRetryBackoff: config.CompassCfg.StartupRetryBackoff,
		},
		syncInterval: config.CompassCfg.SyncInterval,
	}

	c.schemaMap.Store(make(map[string]string))

	return c
}

// Start performs the initial sync synchronously with exponential backoff retries.
// If it fails after the maximum configured attempts, it logs the failure and still launches the periodic background worker.
// If it succeeds, it launches the periodic background worker.
func (c *SchemaCache) Start() {
	var (
		maxRetry = c.startupConfig.startupMaxRetry
		backoff  = c.startupConfig.startupRetryBackoff
	)

	const backoffMultiplier = 2

	var err error

	for attempt := 0; attempt < maxRetry; attempt++ {
		err = c.sync()
		if err == nil {
			go c.worker()

			return
		}

		logger.Errorf("schema cache initial sync attempt %d failed: %v", attempt+1, err)

		if attempt < maxRetry-1 {
			select {
			case <-c.ctx.Done():
				logger.Errorf("context cancelled during schema cache startup: %v", c.ctx.Err())
				return
			case <-time.After(backoff):
			}

			backoff *= backoffMultiplier
		}
	}

	logger.Errorf("failed to fetch schema cache after %d attempts: %v. Cache will be empty initially and rely on fallback config.", maxRetry, err)

	go c.worker()
}

// Close cancels the schema cache's background worker and frees resources.
func (c *SchemaCache) Close() {
	c.cancel()
}

// HealthCheck checks the health of the compass API by sending a GET request to the /ping endpoint.
func (c *SchemaCache) HealthCheck() error {
	if c.httpConfig.httpHost == "" {
		return errors.New("compass HTTP host is empty")
	}

	_, err := c.httpClient.DoRequest(c.ctx, httpwrapper.Request{
		Method:  http.MethodGet,
		BaseURL: c.httpConfig.httpHost,
		Path:    "/ping",
	})
	if err != nil {
		return fmt.Errorf("compass health check request failed: %w", err)
	}

	return nil
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

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.sync(); err != nil {
				logger.Errorf("schema cache sync failed: %v", err)
			}
		}
	}
}

// sync fetches the latest schema map from Compass and updates the local cache.
func (c *SchemaCache) sync() error {
	newMap, err := c.fetchSchemaMap()
	if err != nil {
		const metricNameSchemaSyncFailureCount = "schema_sync_failure_count"

		metrics.Increment(metricNameSchemaSyncFailureCount, "")
		return err
	}

	c.schemaMap.Store(newMap)

	return nil
}

// fetchSchemaMap fetches the latest schema map from Compass and returns it.
func (c *SchemaCache) fetchSchemaMap() (map[string]string, error) {
	if c.httpConfig.httpHost == "" {
		return nil, errors.New("compass HTTP host is empty")
	}

	params := url.Values{}
	params.Add("text", "clickstream--log")
	params.Add("flags.disable_fuzzy", "true")
	params.Add("filter[data.attributes.project_id]", config.CompassCfg.ProjectIDLocation)
	params.Add("filter[data.attributes.stream_name]", "extstream")
	params.Add("filter[type]", "topic")

	headers := make(map[string]string)
	if c.httpConfig.authEmail != "" {
		headers["X-Auth-Email"] = c.httpConfig.authEmail
	}

	bodyData, err := c.httpClient.DoRequest(c.ctx, httpwrapper.Request{
		Method:      http.MethodGet,
		BaseURL:     c.httpConfig.httpHost,
		Path:        "/v1beta1/search",
		Headers:     headers,
		QueryParams: params,
	})
	if err != nil {
		return nil, err
	}

	var parsedResp compassResponse
	if err := json.Unmarshal(bodyData, &parsedResp); err != nil {
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
