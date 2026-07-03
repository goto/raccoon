package eventregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/eventregistry/mocks"
	"github.com/goto/raccoon/ingestionrule/synccache"
	"github.com/goto/raccoon/metrics"
)

func NewTestEventCache(registeredEvents map[string]EventStatus) *EventCache {
	s := &EventCache{}
	s.cache = synccache.NewCache(
		context.Background(),
		"test store",
		func(ctx context.Context) (map[string]EventStatus, error) { return registeredEvents, nil },
		0,
		registeredEvents,
		false,
	)
	return s
}

func TestEventCache_LoadEventMap_Success(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	jsonResponse := `{
		"success": true,
		"data": {
			"event1": {
				"publisher": "grp",
				"product": "app",
				"name": "click",
				"source": {
					"table": "clickstream_click_log"
				}
			}
		}
	}`

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(json.RawMessage(jsonResponse), nil)

	originalMapping := config.PolicyCfg.PublisherMapping
	config.PolicyCfg.PublisherMapping = map[string]string{
		"grp": "grp",
	}
	defer func() {
		config.PolicyCfg.PublisherMapping = originalMapping
	}()

	config.MetadataLayerCfg.HTTPHost = "http://msl.io"

	ctx := context.Background()
	cache := NewEventCache(ctx, "test-metric")
	cache.httpClient = mockClient

	newMap, err := cache.loadEventMap(ctx)
	assert.NoError(t, err)

	expectedHashedKey := cache.buildCacheKey(Event{
		Publisher: "grp",
		TableName: "clickstream_click_log",
		Product:   "app",
		EventName: "click",
	})

	status, ok := newMap[expectedHashedKey]
	assert.True(t, ok)
	assert.Equal(t, EventStatusActive, status)
}

func TestEventCache_LoadEventMap_Error(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("received non-200 status code: 500"))

	originalMapping := config.PolicyCfg.PublisherMapping
	config.PolicyCfg.PublisherMapping = map[string]string{
		"grp": "grp",
	}
	defer func() {
		config.PolicyCfg.PublisherMapping = originalMapping
	}()

	config.MetadataLayerCfg.HTTPHost = "http://msl.io"

	ctx := context.Background()
	cache := NewEventCache(ctx, "test-metric")
	cache.httpClient = mockClient

	_, err := cache.loadEventMap(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received non-200 status code: 500")
}

func TestEventCache_HealthCheck_Success(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(json.RawMessage("OK"), nil)

	config.MetadataLayerCfg.HTTPHost = "http://msl.io"
	ctx := context.Background()
	cache := NewEventCache(ctx, "test-metric")
	cache.httpClient = mockClient

	err := cache.HealthCheck()
	assert.NoError(t, err)
}

func TestEventCache_HealthCheck_Error(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	mockClient.On("DoRequest", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("msl health check returned status code: 500"))

	config.MetadataLayerCfg.HTTPHost = "http://msl.io"
	ctx := context.Background()
	cache := NewEventCache(ctx, "test-metric")
	cache.httpClient = mockClient

	err := cache.HealthCheck()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "msl health check returned status code: 500")
}

func TestEventCache_GetEvents(t *testing.T) {
	metrics.SetVoid()

	event := Event{
		Publisher: "grp",
		TableName: "clickstream_click_log",
		Product:   "app",
		EventName: "click",
	}

	cacheHelper := &EventCache{}
	hashedKey := cacheHelper.buildCacheKey(event)

	cache := NewTestEventCache(map[string]EventStatus{
		hashedKey: EventStatusActive,
	})

	status, ok := cache.GetEvents(hashedKey)
	assert.True(t, ok)
	assert.Equal(t, EventStatusActive, status)

	_, ok = cache.GetEvents("some-other-hash")
	assert.False(t, ok)

	var nilCache *EventCache
	_, ok = nilCache.GetEvents(hashedKey)
	assert.False(t, ok)
}
