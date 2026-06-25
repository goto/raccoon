package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHTTPClient_DoRequest_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1beta1/search", r.URL.Path)
		assert.Equal(t, "value", r.URL.Query().Get("key"))
		assert.Equal(t, "auth-email", r.Header.Get("X-Auth-Email"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second)

	params := url.Values{}
	params.Add("key", "value")

	res, err := client.DoRequest(context.Background(), Request{
		Method:      http.MethodGet,
		BaseURL:     server.URL,
		Path:        "/v1beta1/search",
		Headers:     map[string]string{"X-Auth-Email": "auth-email"},
		QueryParams: params,
	})

	assert.NoError(t, err)
	assert.JSONEq(t, `{"status":"success"}`, string(res))
}

func TestHTTPClient_DoRequest_Non200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"something went wrong"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second)

	_, err := client.DoRequest(context.Background(), Request{
		Method:  http.MethodGet,
		BaseURL: server.URL,
		Path:    "/ping",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP request failed with status 500")
	assert.Contains(t, err.Error(), "something went wrong")
}

