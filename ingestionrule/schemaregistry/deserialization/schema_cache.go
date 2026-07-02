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
}

func NewSchemaCache(ctx context.Context) *SchemaCache {
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
	}

	sc.cache = synccache.NewCache(
		ctx,
		"schema cache",
		sc.fetchSchemaMap,
		config.CompassCfg.SyncInterval,
		make(map[string]string),
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

func (c *SchemaCache) sync() error {
	if c == nil {
		return nil
	}

	return c.cache.Sync()
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

// fetchSchemaMap fetches the latest schema map from Compass and returns it.
func (c *SchemaCache) fetchSchemaMap(ctx context.Context) (map[string]string, error) {
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

	newMap := make(map[string]string)
	for _, item := range parsedResp.Data {
		if len(item.Data.Attributes.Schemas) >= 1 && item.Data.Attributes.Schemas[0].Name != "" {
			newMap[item.Name] = item.Data.Attributes.Schemas[0].Name
		}
	}

	return newMap, nil
}
