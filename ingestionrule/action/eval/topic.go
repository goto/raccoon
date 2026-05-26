package eval

import "github.com/goto/raccoon/config"

// TopicEvaluator matches topic-level policies (resource = "topic").
// Matching criteria: details.name (topic name) — must be present.
type TopicEvaluator struct{}

// Resource declares that this evaluator handles topic-scoped rules.
func (t *TopicEvaluator) Resource() config.PolicyResourceType {
	return config.PolicyResourceTopic
}

// Evaluate returns (false, false) when the topic name is absent or no rule exists
// for this topic key. Returns (condition.Breached(), true) when a rule is found —
// the chain stops here regardless of whether the condition is breached.
func (t *TopicEvaluator) Evaluate(meta EventMetadata, rules map[string]Condition) (bool, bool) {
	if meta.TopicName == "" {
		return false, false
	}
	condition, ok := rules[meta.TopicName]
	if !ok {
		return false, false
	}
	return condition.Breached(meta), true
}
