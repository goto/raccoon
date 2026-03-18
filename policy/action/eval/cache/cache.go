package cache

import (
	"github.com/goto/raccoon/config"
	eval "github.com/goto/raccoon/policy/action/eval"
)

// Cache holds policy conditions indexed by resource type and a detail key for a single action type.
// Each action (Drop, OverrideTimestamp) owns one Cache pre-populated with its rules.
//
// The detail key is the plain concatenation of name+product+publisher (event) or name (topic).
type Cache struct {
	m map[config.PolicyResourceType]map[string]eval.Condition
}

// NewCache builds a Cache from rules that are already filtered to one action type.
// Each rule's action config is wrapped in a Condition so the evaluation predicate
// can be swapped without changing the cache structure.
func NewCache(rules []config.PolicyRule) *Cache {
	c := &Cache{
		m: make(map[config.PolicyResourceType]map[string]eval.Condition),
	}
	for _, r := range rules {
		if _, ok := c.m[r.Resource]; !ok {
			c.m[r.Resource] = make(map[string]eval.Condition)
		}
		var key string
		if r.Resource == config.PolicyResourceTopic {
			key = r.Details.Name
		} else {
			key = r.Details.Name + r.Details.Product + r.Details.Publisher
		}
		switch r.Action.ConditionType {
		case config.PolicyConditionTimestampThreshold:
			c.m[r.Resource][key] = eval.NewTimestampCondition(r.Action.EventTimestampThreshold)
		}
	}
	return c
}

// Get returns the condition map for the given resource type.
// Returns nil when no rules are registered for that resource.
func (c *Cache) Get(resource config.PolicyResourceType) map[string]eval.Condition {
	if c == nil {
		return nil
	}
	return c.m[resource]
}
