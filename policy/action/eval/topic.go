package eval

import "github.com/goto/raccoon/config"

// TopicEvaluator matches topic-level policies (resource = "topic").
// Matching criteria: details.name (topic name) — must be present.
type TopicEvaluator struct{}

// Resource declares that this evaluator handles topic-scoped rules.
func (t *TopicEvaluator) Resource() config.PolicyResourceType {
	return config.PolicyResourceTopic
}

// Evaluate returns EvalSkip when the topic name is absent in the event metadata.
// Otherwise it performs a single lookup using topicName as the key and delegates
// to the Condition to decide whether the action should be applied.
func (t *TopicEvaluator) Evaluate(meta EventMetadata, rules map[string]Condition) EvalResult {
	if meta.TopicName == "" {
		return EvalSkip
	}
	condition, ok := rules[meta.TopicName]
	if !ok {
		return EvalNoMatch
	}
	if condition.Breached(meta) {
		return EvalApply
	}
	return EvalSkip
}
