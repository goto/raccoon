package deserialization

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/goto/raccoon/config"
	httpwrapper "github.com/goto/raccoon/ingestionrule/http"
	"github.com/goto/raccoon/ingestionrule/synccache"
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

type SchemaCache struct {
	ctx        context.Context
	cache      *synccache.Cache[map[string]string]
	httpClient HTTPClient
	httpConfig struct {
		httpHost  string
		authEmail string
	}
	metricName string
}

func NewSchemaCache(ctx context.Context, metricName string) *SchemaCache {
	sc := &SchemaCache{
		ctx: ctx,
		httpClient: httpwrapper.NewHTTPClient(
			config.CompassCfg.HTTPRequestTimeout,
			config.CompassCfg.HTTPMaxRetry,
			config.CompassCfg.HTTPRetryBackoff,
		),
		httpConfig: httpConfig{
			httpHost:  strings.TrimSuffix(config.CompassCfg.HTTPHost, "/"),
			authEmail: config.CompassCfg.AuthEmail,
		},
		metricName: metricName,
	}

	sc.cache = synccache.NewCache(
		ctx,
		"schema cache",
		sc.loadSchemaMap,
		config.CompassCfg.SyncInterval,
		make(map[string]string),
		false,
	)

	return sc
}

// Start performs the initial sync synchronously with exponential backoff retries.
func (c *SchemaCache) Start() {
	if c == nil {
		return
	}

	c.cache.Start()
}

// Close cancels the schema cache's background worker and frees resources.
func (c *SchemaCache) Close() {
	if c == nil {
		return
	}

	c.cache.Close()
}

// HealthCheck checks the health of the compass API by sending a GET request to the /ping endpoint.
func (c *SchemaCache) HealthCheck() error {
	if c == nil {
		return errors.New("schema cache is disabled")
	}

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
	if c == nil {
		return "", false
	}

	m := c.cache.Get()
	protoClass, ok := m[topic]

	return protoClass, ok
}

// loadSchemaMap fetches the latest assets from Compass and load into schema map.
func (c *SchemaCache) loadSchemaMap(ctx context.Context) (map[string]string, error) {
	schemas, err := c.fetchAssets(ctx)
	if err != nil {
		metrics.Increment(c.metricName, fmt.Sprintf("status=failed,type=fetch_compass_assets,reason=%s", err.Error()))
		return nil, err
	}

	newMap := make(map[string]string)
	for _, item := range schemas {
		if len(item.Data.Attributes.Schemas) >= 1 && item.Data.Attributes.Schemas[0].Name != "" {
			newMap[item.Name] = item.Data.Attributes.Schemas[0].Name
		}
	}

	metrics.Increment(c.metricName, "status=success,type=fetch_compass_assets")

	return newMap, nil
}
// fetchAssets retrieves list of assets from Compass API.
func (c *SchemaCache) fetchAssets(ctx context.Context) ([]compassItem, error) {
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

	bodyData, err := c.httpClient.DoRequest(ctx, httpwrapper.Request{
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

	return parsedResp.Data, nil
}
