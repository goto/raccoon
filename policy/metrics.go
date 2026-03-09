package policy

// Metric bucket names emitted during policy application.
const (
	// MetricEvalDuration tracks the latency of the full policy evaluation chain per event (milliseconds).
	MetricEvalDuration = "policy_evaluation_duration_milliseconds"

	// MetricDroppedTotal counts events dropped by DROP policies.
	MetricDroppedTotal = "policy_events_dropped_total"

	// MetricRedirectedTotal counts events forwarded to the override topic.
	MetricRedirectedTotal = "policy_events_redirected_total"

	// MetricRedirectFailedTotal counts events that failed to publish to the override topic.
	MetricRedirectFailedTotal = "policy_events_redirect_failed_total"
)
