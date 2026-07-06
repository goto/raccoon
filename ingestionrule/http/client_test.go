package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClient_DoRequest_SuccessFirstTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "ok"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second, 3, 1*time.Millisecond)
	resp, err := client.DoRequest(context.Background(), Request{
		Method:  http.MethodGet,
		BaseURL: server.URL,
		Path:    "/test",
	})

	require.NoError(t, err)
	assert.JSONEq(t, `{"status": "ok"}`, string(resp))
}

func TestHTTPClient_DoRequest_RetryThenSuccess(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&callCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": "internal error"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "success"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second, 4, 1*time.Millisecond)
	resp, err := client.DoRequest(context.Background(), Request{
		Method:  http.MethodGet,
		BaseURL: server.URL,
		Path:    "/retry",
	})

	require.NoError(t, err)
	assert.JSONEq(t, `{"status": "success"}`, string(resp))
	assert.Equal(t, int32(3), atomic.LoadInt32(&callCount))
}

func TestHTTPClient_DoRequest_RetryExhausted(t *testing.T) {
	var callCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error": "always fails"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second, 3, 1*time.Millisecond)
	resp, err := client.DoRequest(context.Background(), Request{
		Method:  http.MethodGet,
		BaseURL: server.URL,
		Path:    "/fail",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "HTTP request failed with status 500 Internal Server Error")
	assert.Equal(t, int32(3), atomic.LoadInt32(&callCount))
}

func TestHTTPClient_DoRequest_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error": "fail"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately so that the first retry attempts will fail due to cancelled context
	cancel()

	client := NewHTTPClient(time.Second, 3, 100*time.Millisecond)
	resp, err := client.DoRequest(ctx, Request{
		Method:  http.MethodGet,
		BaseURL: server.URL,
		Path:    "/cancel",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestHTTPClient_DoRequest_QueryParamsAndHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "custom-value", r.Header.Get("X-Custom-Header"))
		assert.Equal(t, "bar", r.URL.Query().Get("foo"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "ok"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(time.Second, 1, 1*time.Millisecond)
	q := url.Values{}
	q.Set("foo", "bar")
	resp, err := client.DoRequest(context.Background(), Request{
		Method:      http.MethodGet,
		BaseURL:     server.URL,
		Path:        "/params",
		Headers:     map[string]string{"X-Custom-Header": "custom-value"},
		QueryParams: q,
	})

	require.NoError(t, err)
	assert.JSONEq(t, `{"status": "ok"}`, string(resp))
}
