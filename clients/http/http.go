package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPClient handles HTTP requests.
type HTTPClient struct {
	Client *http.Client
}

// NewHTTPClient creates a new HTTP client with optional timeout.
func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{Timeout: timeout},
	}
}

const (
	errConstructURL           = "failed to construct URL"
	errSerializeRequestBody   = "failed to serialize request body"
	errCreateRequest          = "failed to create request"
	errRequestFailed          = "request failed"
	errReadResponseBody       = "failed to read response body"
	errUnsupportedContentType = "got unsupported content type of %s, with a response body of\n%s"
	errHTTPRequestFailed      = "HTTP request failed with status %s: %s"
)

type Request struct {
	Method      string
	BaseURL     string
	Path        string
	ContentType string
	Body        any
}

const (
	ContentTypeJSON          = "application/json"
	ContentTypeMultipartForm = "multipart/form-data"
)

// DoRequest sends an HTTP request and returns the response body.
func (c *HTTPClient) DoRequest(ctx context.Context, request Request) (json.RawMessage, error) {
	fullURL, err := url.JoinPath(request.BaseURL, request.Path)
	if err != nil {
		return json.RawMessage{}, fmt.Errorf("%s: %w", errConstructURL, err)
	}

	req, err := createRequest(ctx, request, fullURL)
	if err != nil {
		return json.RawMessage{}, fmt.Errorf("%s: %w", errCreateRequest, err)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return json.RawMessage{}, fmt.Errorf("%s: %w", errRequestFailed, err)
	}
	defer resp.Body.Close()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return json.RawMessage{}, fmt.Errorf("%s: %w", errReadResponseBody, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		contentType := resp.Header.Get("Content-Type")
		if !isJsonContentType(contentType) {
			return json.RawMessage{}, fmt.Errorf(errUnsupportedContentType, contentType, string(bodyData))
		}

		return json.RawMessage{}, fmt.Errorf(errHTTPRequestFailed, resp.Status, string(bodyData))
	}

	return bodyData, nil
}

func isJsonContentType(contentType string) bool {
	return strings.Contains(contentType, ContentTypeJSON)
}

func createRequest(ctx context.Context, request Request, fullURL string) (*http.Request, error) {
	var requestBody io.Reader
	var err error
	contentType := request.ContentType

	switch request.ContentType {
	case ContentTypeMultipartForm:
		if formData, ok := request.Body.(map[string]any); ok {
			requestBody, contentType, err = createMultipartBody(formData)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", errSerializeRequestBody, err)
			}
		} else {
			return nil, fmt.Errorf("invalid body type for multipart form")
		}
	default:
		requestBody, err = createJsonBody(request.Body)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", errSerializeRequestBody, err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, request.Method, fullURL, requestBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", contentType)
	return req, nil
}

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

func createMultipartBody(formData map[string]any) (io.Reader, string, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for k, v := range formData {
		val, ok := v.(string)
		if !ok {
			return nil, "", fmt.Errorf("expected a string form form-data value, got %T", v)
		}

		if err := writer.WriteField(k, val); err != nil {
			return nil, "", err
		}
	}

	// Finalize the form
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return body, writer.FormDataContentType(), nil
}
