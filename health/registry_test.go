package health

import (
	"errors"
	"reflect"
	"testing"
)

func TestRegister(t *testing.T) {
	resetCheckers()

	mockChecker := func() error { return nil }

	Register("db", mockChecker)

	mu.RLock()
	defer mu.RUnlock()

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
		expected map[string]string
	}{
		{
			name: "single healthy service on register map",
			register: map[string]Checker{
				"serviceA": func() error { return nil },
			},
			expected: map[string]string{
				"serviceA": "healthy",
			},
		},
		{
			name: "single unhealthy service on register map",
			register: map[string]Checker{
				"serviceB": func() error { return errors.New("timeout") },
			},
			expected: map[string]string{
				"serviceB": "unhealthy: timeout",
			},
		},
		{
			name: "multiple services on register map",
			register: map[string]Checker{
				"ok":   func() error { return nil },
				"fail": func() error { return errors.New("db down") },
			},
			expected: map[string]string{
				"ok":   "healthy",
				"fail": "unhealthy: db down",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//rest the register map for every test scenario
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

func resetCheckers() {
	mu.Lock()
	defer mu.Unlock()
	checkers = make(map[string]Checker)
}
