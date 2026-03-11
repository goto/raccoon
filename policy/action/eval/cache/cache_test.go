package cache_test

import (
	"testing"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
)

func TestNewCache_IndexesByResource(t *testing.T) {
	rules := []config.PolicyRule{
<<<<<<< HEAD
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e1", Product: "app", Publisher: "pub-a"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e2", Product: "app", Publisher: "pub-a"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
		{Resource: config.PolicyResourceTopic, Details: config.PolicyDetails{Name: "t1"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
=======
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e1", Product: "app", Publisher: "pub-a"}},
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e2", Product: "app", Publisher: "pub-a"}},
		{Resource: config.PolicyResourceTopic, Details: config.PolicyDetails{Name: "t1"}},
>>>>>>> 6d07be4 (chore : fix merge conflict)
	}
	c := cache.NewCache(rules)

	events := c.Get(config.PolicyResourceEvent)
	assert.Len(t, events, 2)

	topics := c.Get(config.PolicyResourceTopic)
	assert.Len(t, topics, 1)
	_, ok := topics["t1"]
	assert.True(t, ok)
}

func TestCache_GetMissingResource(t *testing.T) {
	c := cache.NewCache(nil)
	assert.Nil(t, c.Get(config.PolicyResourceEvent))
}

func TestCache_NilCache(t *testing.T) {
	var c *cache.Cache
	assert.Nil(t, c.Get(config.PolicyResourceEvent))
}
