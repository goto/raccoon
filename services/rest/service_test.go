package rest

import (
	"errors"
	"github.com/goto/raccoon/health"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPingHandler(t *testing.T) {
	// mock health checkers
	health.Register("mqtt-broker", func() error { return nil })
	health.Register("kafka-broker", func() error { return errors.New("timeout") })

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()

	pingHandler(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", res.StatusCode)
	}
}

