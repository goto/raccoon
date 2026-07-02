package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/goto/raccoon/logger"
)

const (
	ContentTypeJSON = "application/json"
)

const (
	errConstructURL           = "failed to construct URL"
	errSerializeRequestBody   = "failed to serialize request body"
	errCreateRequest          = "failed to create request"
	errRequestFailed          = "request failed"
	errReadResponseBody       = "failed to read response body"
	errUnsupportedContentType = "got unsupported content type of %s, with a response body of\n%s"
	errHTTPRequestFailed      = "HTTP request failed with status %s: %s"
)

// HTTPClient handles HTTP requests.
type HTTPClient struct {
	Client       *http.Client
	maxRetry     int
	retryBackoff time.Duration
}

// NewHTTPClient creates a new HTTP client with optional timeout, retry, and backoff parameters.
func NewHTTPClient(timeout time.Duration, maxRetry int, retryBackoff time.Duration) *HTTPClient {
	return &HTTPClient{
		Client:       &http.Client{Timeout: timeout},
		maxRetry:     maxRetry,
		retryBackoff: retryBackoff,
	}
}

type Request struct {
	Method      string
	BaseURL     string
	Path        string
	ContentType string
	Headers     map[string]string
	QueryParams url.Values
	Body        any
}

// DoRequest sends an HTTP request and returns the response body, retrying on failure.
func (c *HTTPClient) DoRequest(ctx context.Context, request Request) (json.RawMessage, error) {
	fullURL, err := url.JoinPath(request.BaseURL, request.Path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errConstructURL, err)
	}

	if len(request.QueryParams) > 0 {
		fullURL = fullURL + "?" + request.QueryParams.Encode()
	}

	maxRetry := c.maxRetry
	if maxRetry <= 0 {
		maxRetry = 1
	}

	backoff := c.retryBackoff
	const backoffMultiplier = 2

	var finalErr error
	var bodyData []byte

	for attempt := 0; attempt < maxRetry; attempt++ {
		req, err := createRequest(ctx, request, fullURL)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errCreateRequest, err)
		}

		bodyData, finalErr = c.doSingleRequest(req)
		if finalErr == nil {
			return bodyData, nil
		}

		if attempt < maxRetry-1 {
			logger.Infof("HTTP request attempt %d failed: %v. Retrying in %v...", attempt+1, finalErr, backoff)
			select {
			case <-ctx.Done():
				logger.Errorf("context cancelled during HTTP request retries: %v", ctx.Err())
				return nil, ctx.Err()
			case <-time.After(backoff):
			}

			backoff *= backoffMultiplier
		}
	}

	return nil, finalErr
}

// doSingleRequest handles the execution, reading, and validation of a single HTTP attempt.
func (c *HTTPClient) doSingleRequest(req *http.Request) ([]byte, error) {
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errRequestFailed, err)
	}

	defer resp.Body.Close()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errReadResponseBody, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		contentType := resp.Header.Get("Content-Type")
		if !isJsonContentType(contentType) {
			return nil, fmt.Errorf(errUnsupportedContentType, contentType, string(bodyData))
		}
		return nil, fmt.Errorf(errHTTPRequestFailed, resp.Status, string(bodyData))
	}

	return bodyData, nil
}

// isJsonContentType checks if the content type is JSON.
func isJsonContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), ContentTypeJSON)
}

// createRequest creates an HTTP request with the given parameters.
func createRequest(ctx context.Context, request Request, fullURL string) (*http.Request, error) {
	requestBody, err := createJsonBody(request.Body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errSerializeRequestBody, err)
	}

	req, err := http.NewRequestWithContext(ctx, request.Method, fullURL, requestBody)
	if err != nil {
		return nil, err
	}

	if request.ContentType != "" {
		req.Header.Set("Content-Type", request.ContentType)
	}

	for k, v := range request.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// createJsonBody creates a JSON body from the given body.
func createJsonBody(body any) (io.Reader, error) {
	if body == nil {
		return nil, nil
	}

	var requestBody io.Reader
	switch v := body.(type) {
	case string:
		if v != "" {
			requestBody = strings.NewReader(v)
		}
	default:
		jsonData, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewBuffer(jsonData)
	}

	return requestBody, nil
}
