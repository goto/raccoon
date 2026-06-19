package checkregistration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 1. Test Client Initialization & Timeout Requirement
func TestMSLClient_Timeout(t *testing.T) {
	client := NewMSLClient("http://localhost:8080")

	require.NotNil(t, client)
	require.NotNil(t, client.client)
	// Verifies that the client strictly has a 2-minute timeout configured
	assert.Equal(t, 2*time.Minute, client.client.Timeout)
}

// 2. Test GetPublishers API and Payload Parsing
func TestMSLClient_GetPublishers(t *testing.T) {
	tests := []struct {
		name             string
		mockStatus       int
		mockResponseBody interface{}
		wantPublishers   []Publisher
		wantErr          bool
	}{
		{
			name:       "Success - Parses publisher names only",
			mockStatus: http.StatusOK,
			mockResponseBody: publishersResponse{
				Success: true,
				Data:    []string{"publisher-alpha", "publisher-beta"},
			},
			wantPublishers: []Publisher{
				{Name: "publisher-alpha"},
				{Name: "publisher-beta"},
			},
			wantErr: false,
		},
		{
			name:             "Failure - Non 200 OK Response Status",
			mockStatus:       http.StatusInternalServerError,
			mockResponseBody: nil,
			wantPublishers:   nil,
			wantErr:          true,
		},
		{
			name:             "Failure - Malformed JSON Parsing",
			mockStatus:       http.StatusOK,
			mockResponseBody: "{ invalid json string }",
			wantPublishers:   nil,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test server acting as the MSL API
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v1/publishers", r.URL.Path)
				assert.Equal(t, http.MethodGet, r.Method)

				w.WriteHeader(tt.mockStatus)
				if tt.mockResponseBody != nil {
					if str, ok := tt.mockResponseBody.(string); ok {
						w.Write([]byte(str))
					} else {
						json.NewEncoder(w).Encode(tt.mockResponseBody)
					}
				}
			}))
			defer server.Close()

			client := NewMSLClient(server.URL)
			got, err := client.GetPublishers(context.Background())

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPublishers, got)
			}
		})
	}
}

// 3. Test GetEvents API with Publisher filtering and Payload Parsing
func TestMSLClient_GetEvents(t *testing.T) {
	tests := []struct {
		name             string
		publisherArg     string
		mockStatus       int
		mockResponseBody interface{}
		wantEvents       []Event
		wantErr          bool
	}{
		{
			name:         "Success - Parses event and product names correctly",
			publisherArg: "pub-1",
			mockStatus:   http.StatusOK,
			mockResponseBody: eventsResponse{
				Success: true,
				Data: map[string]eventPayload{
					"event-key-1": {
						Publisher: "pub-1",
						Product:   "bi-analytics",
						Name:      "click_event",
					},
					"event-key-2": {
						Publisher: "pub-1",
						Product:   "core-platform",
						Name:      "impression_event",
					},
				},
			},
			wantEvents: []Event{
				{Product: "bi-analytics", EventName: "click_event"},
				{Product: "core-platform", EventName: "impression_event"},
			},
			wantErr: false,
		},
		{
			name:             "Failure - Target Server Returns 404/500",
			publisherArg:     "pub-1",
			mockStatus:       http.StatusBadRequest,
			mockResponseBody: nil,
			wantEvents:       nil,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Assert endpoint routing and query extraction rules
				assert.Equal(t, "/v1/events", r.URL.Path)
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, tt.publisherArg, r.URL.Query().Get("publisher"))

				w.WriteHeader(tt.mockStatus)
				if tt.mockResponseBody != nil {
					json.NewEncoder(w).Encode(tt.mockResponseBody)
				}
			}))
			defer server.Close()

			client := NewMSLClient(server.URL)
			got, err := client.GetEvents(context.Background(), tt.publisherArg)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				// Elements order from maps isn't deterministic, ElementsMatch validates
				// values regardless of indexing sequences.
				assert.ElementsMatch(t, tt.wantEvents, got)
			}
		})
	}
}
