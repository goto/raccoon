package policy

// TopicEvaluator matches topic-level policies (resource = "topic").
// Matching criteria: details.name (topic name), details.publisher.
// An empty string in a policy field is treated as a wildcard (matches anything).
type TopicEvaluator struct{}

// Evaluate iterates over topic policies and returns the appropriate eval result.
func (t *TopicEvaluator) Evaluate(meta EventMetadata, policies []PolicyConfig) EvalResult {
	for _, p := range policies {
		if p.Resource != ResourceTopic {
			continue
		}
		if !matchField(p.Details.Name, meta.TopicName) {
			continue
		}
		if !matchField(p.Details.Publisher, meta.Publisher) {
			continue
		}
		// Policy matched — evaluate timestamp threshold.
		if WithinThreshold(p.Action.EventTimestampThreshold, meta.EventTimestamp) {
			return EvalSkip
		}
		return EvalApply
	}
	return EvalNoMatch
}
