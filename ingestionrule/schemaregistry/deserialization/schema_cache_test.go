package deserialization

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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

	respHeaders := http.Header{}
	respHeaders.Set("Content-Type", "application/json")

	mockClient.On("Do", mock.Anything).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(jsonResponse)),
		Header:     respHeaders,
	}, nil)

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "http://compass.io", "auth-email", time.Minute, time.Second)
	cache.httpClient = mockClient

	err := cache.sync()
	assert.NoError(t, err)

	val, ok := cache.Get("topic-a")
	assert.True(t, ok)
	assert.Equal(t, "proto.ClassA", val)

	_, ok = cache.Get("topic-b")
	assert.False(t, ok)
}

func TestSchemaCache_FetchSchemaMap_Non200(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	errorBody := "internal server error detailed error msg"

	mockClient.On("Do", mock.Anything).Return(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(bytes.NewBufferString(errorBody)),
		Header:     http.Header{},
	}, nil)

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "http://compass.io", "auth-email", time.Minute, time.Second)
	cache.httpClient = mockClient

	err := cache.sync()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received non-200 status code: 500")
	assert.Contains(t, err.Error(), errorBody)
}

func TestSchemaCache_FetchSchemaMap_NonJSON(t *testing.T) {
	metrics.SetVoid()
	mockClient := mocks.NewHTTPClient(t)

	respHeaders := http.Header{}
	respHeaders.Set("Content-Type", "text/plain")

	mockClient.On("Do", mock.Anything).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString("some plain text")),
		Header:     respHeaders,
	}, nil)

	ctx := context.Background()
	cache := NewSchemaCache(ctx, "http://compass.io", "auth-email", time.Minute, time.Second)
	cache.httpClient = mockClient

	err := cache.sync()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received non-json response type: \"text/plain\"")
	assert.Contains(t, err.Error(), "some plain text")
}
