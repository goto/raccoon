package health

import (
	"errors"
	"reflect"
	"testing"

	"github.com/goto/raccoon/constant"
)

func TestRegister(t *testing.T) {
	resetCheckers()

	mockChecker := func() error { return nil }

	Register("db", mockChecker)

	if len(checkers) != 1 {
		t.Fatalf("expected 1 checker, got %d", len(checkers))
	}

	if _, ok := checkers["db"]; !ok {
		t.Errorf("expected checker 'db' to be registered")
	}
}

func TestCheckAll(t *testing.T) {
	tests := []struct {
		name     string
		register map[string]Checker
		expected map[string]constant.HealthStatus
	}{
		{
			name: "single healthy service on register map",
			register: map[string]Checker{
				"serviceA": func() error { return nil },
			},
			expected: map[string]constant.HealthStatus{
				"serviceA": constant.HealthStatusHealthy,
			},
		},
		{
			name: "single unhealthy service on register map",
			register: map[string]Checker{
				"serviceB": func() error { return errors.New("timeout") },
			},
			expected: map[string]constant.HealthStatus{
				"serviceB": constant.HealthStatusUnhealthy,
			},
		},
		{
			name: "multiple services on register map",
			register: map[string]Checker{
				"ok":   func() error { return nil },
				"fail": func() error { return errors.New("db down") },
			},
			expected: map[string]constant.HealthStatus{
				"ok":   constant.HealthStatusHealthy,
				"fail": constant.HealthStatusUnhealthy,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// reset the register map for every test scenario
			resetCheckers()

			for name, fn := range tt.register {
				Register(name, fn)
			}

			actual := CheckAll()
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("CheckAll() = %v, want %v", actual, tt.expected)
			}
		})
	}
}

// resetCheckers clears all registered checkers to ensure isolation between tests.
func resetCheckers() {
	checkers = make(map[string]Checker)
}
