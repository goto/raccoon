package policy

// EventEvaluator matches event-level policies (resource = "event").
// Matching criteria: details.name, details.product, details.publisher.
// An empty string in a policy field is treated as a wildcard (matches anything).
type EventEvaluator struct{}

// Evaluate iterates over event policies and returns the appropriate eval result.
func (e *EventEvaluator) Evaluate(meta EventMetadata, policies []PolicyConfig) EvalResult {
	for _, p := range policies {
		if p.Resource != ResourceEvent {
			continue
		}
		if !matchField(p.Details.Name, meta.EventName) {
			continue
		}
		if !matchField(p.Details.Product, meta.Product) {
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

// matchField returns true when the policy field is empty (wildcard) or equals the actual value.
func matchField(policyField, actual string) bool {
	return policyField == "" || policyField == actual
}
