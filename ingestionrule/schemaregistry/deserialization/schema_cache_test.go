package deserialization

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/schemaregistry/deserialization/mocks"
	"github.com/goto/raccoon/metrics"
)

func TestSchemaCache_FetchSchemaMap_Success(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	jsonResponse := `{
		"data": [
			{
				"name": "topic-a",
				"data": {
					"attributes": {
						"schemas": [
							{
								"name": "proto.ClassA"
							}
						]
					}
				}
			},
			{
				"name": "topic-b",
				"data": {
					"attributes": {
						"schemas": []
					}
				}
			}
		]
	}`

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(json.RawMessage(jsonResponse), nil)

	config.CompassCfg.HTTPHost = "http://compass.io"
	config.CompassCfg.AuthEmail = "auth-email"
	config.CompassCfg.SyncInterval = time.Minute
	config.CompassCfg.HTTPRequestTimeout = time.Second
	config.CompassCfg.HTTPMaxRetry = 3
	config.CompassCfg.HTTPRetryBackoff = 200 * time.Millisecond

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "test-metric")
	cache.httpClient = mockClient

	res, err := cache.loadSchemaMap(ctx)
	assert.NoError(t, err)

	assert.Equal(t, "proto.ClassA", res["topic-a"])
	_, ok := res["topic-b"]
	assert.False(t, ok)
}

func TestSchemaCache_FetchSchemaMap_Non200(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	errorBody := "internal server error detailed error msg"

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("received non-200 status code: 500, body: %s", errorBody))

	config.CompassCfg.HTTPHost = "http://compass.io"
	config.CompassCfg.AuthEmail = "auth-email"
	config.CompassCfg.SyncInterval = time.Minute
	config.CompassCfg.HTTPRequestTimeout = time.Second
	config.CompassCfg.HTTPMaxRetry = 3
	config.CompassCfg.HTTPRetryBackoff = 200 * time.Millisecond

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "test-metric")
	cache.httpClient = mockClient

	_, err := cache.loadSchemaMap(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received non-200 status code: 500")
	assert.Contains(t, err.Error(), errorBody)
}

func TestSchemaCache_FetchSchemaMap_NonJSON(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("received non-json response type: \"text/plain\", body: some plain text"))

	config.CompassCfg.HTTPHost = "http://compass.io"
	config.CompassCfg.AuthEmail = "auth-email"
	config.CompassCfg.SyncInterval = time.Minute
	config.CompassCfg.HTTPRequestTimeout = time.Second
	config.CompassCfg.HTTPMaxRetry = 3
	config.CompassCfg.HTTPRetryBackoff = 200 * time.Millisecond

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "test-metric")
	cache.httpClient = mockClient

	_, err := cache.loadSchemaMap(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received non-json response type: \"text/plain\"")
	assert.Contains(t, err.Error(), "some plain text")
}

func TestSchemaCache_HealthCheck_Success(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(json.RawMessage("OK"), nil)

	config.CompassCfg.HTTPHost = "http://compass.io"
	ctx := context.Background()
	cache := NewSchemaCache(ctx, "test-metric")
	cache.httpClient = mockClient

	err := cache.HealthCheck()
	assert.NoError(t, err)
}

func TestSchemaCache_HealthCheck_Error(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("compass health check returned status code: 500"))

	config.CompassCfg.HTTPHost = "http://compass.io"
	ctx := context.Background()
	cache := NewSchemaCache(ctx, "test-metric")
	cache.httpClient = mockClient

	err := cache.HealthCheck()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "compass health check returned status code: 500")
}
