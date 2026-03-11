package action

// MetricEvalLatency is emitted by every action and by the service pipeline.
// Tag action= identifies the source: "drop", "override_timestamp", or "total".
const MetricEvalLatency = "event_policy_eval_latency_ms"

// metricEventLoss counts events permanently dropped by the DROP action.
const metricEventLoss = "clickstream_event_loss"

// metricEventOverride counts events forwarded to the override topic.
// Tag result= indicates whether the publish succeeded ("success") or failed ("failure").
const metricEventOverride = "clickstream_event_override"
