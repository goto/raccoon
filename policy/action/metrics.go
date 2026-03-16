package action

// MetricEvalLatency is emitted by every action and by the service pipeline.
// Tag action= identifies the source: "drop", "override_timestamp", or "total".
const MetricEvalLatency = "event_policy_eval_latency_ms"

// metricEventLossCount counts events permanently dropped by the DROP action.
const metricEventLossCount = "clickstream_event_loss"

// metricEventOverrideCount counts events forwarded to the override topic.
const metricEventOverrideCount = "clickstream_event_override"
