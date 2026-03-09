package policy

// Cache holds policies indexed by ActionType and then by ResourceType for O(1) lookup.
//
//	policyCache:
//	  DROP
//	    event  → []PolicyConfig
//	    topic  → []PolicyConfig
//	  OVERRIDE_TIMESTAMP
//	    event  → []PolicyConfig
//	    topic  → []PolicyConfig
type Cache struct {
	m map[ActionType]map[ResourceType][]PolicyConfig
}

// NewCache builds a Cache from the provided slice of policies.
func NewCache(policies []PolicyConfig) *Cache {
	c := &Cache{
		m: make(map[ActionType]map[ResourceType][]PolicyConfig),
	}
	for _, p := range policies {
		if _, ok := c.m[p.Action.Type]; !ok {
			c.m[p.Action.Type] = make(map[ResourceType][]PolicyConfig)
		}
		c.m[p.Action.Type][p.Resource] = append(c.m[p.Action.Type][p.Resource], p)
	}
	return c
}

// Get returns the policies for the given action and resource type.
// Returns nil (empty) if no policies are found.
func (c *Cache) Get(action ActionType, resource ResourceType) []PolicyConfig {
	if c == nil {
		return nil
	}
	byResource, ok := c.m[action]
	if !ok {
		return nil
	}
	return byResource[resource]
}
