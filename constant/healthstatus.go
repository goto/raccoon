package constant

// HealthStatus represents the possible health states of a service
// or dependency within the system.
//
// It is used to standardize how health information is communicated across
// services — for example, in HTTP health check endpoints (/ping).
// Defining these values as constants ensures consistent usage throughout the codebase.
type HealthStatus string

const (
	// HealthStatusHealthy indicates that the service is operating
	// normally, and all dependent subsystems are functioning as expected.

	HealthStatusHealthy HealthStatus = "healthy"

	// HealthStatusUnhealthy indicates that the component or service is
	// degraded or non-functional, possibly due to dependency failures,
	// connectivity issues, or internal errors.
	//
	// This status will cause readiness and liveness  probes to fail
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)
