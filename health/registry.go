// Package health provides a simple health check framework that allows
// different components or dependencies to register their own health check functions.
// Each checker function returns an error to indicate an unhealthy state,
// or nil to indicate that the component is healthy.
package health

import "github.com/goto/raccoon/constant"

// Checker represents a function that performs a health check for a component.
// It returns an error if the check fails (unhealthy), or nil if it passes (healthy).
type Checker func() error

var (
	// checkers stores all registered health check functions.
	// The key is the component name, and the value is its corresponding Checker function.
	checkers = make(map[string]Checker)
)

// Register adds a new health check function for a specific component.
// Each component should have a unique name; calling Register with an existing
// name will overwrite the previous checker.
func Register(name string, fn Checker) {
	checkers[name] = fn
}

// CheckAll runs all registered health check functions and returns their results.
// It returns a map where the key is the component name and the value is its health status.
// Components that return an error are marked as Unhealthy; others are marked as Healthy.
func CheckAll() map[string]constant.HealthStatus {
	results := make(map[string]constant.HealthStatus)

	for name, fn := range checkers {
		if err := fn(); err != nil {
			results[name] = constant.HealthStatusUnhealthy
		} else {
			results[name] = constant.HealthStatusHealthy
		}
	}

	return results
}
