package cache_test

import (
	"testing"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy/action/eval/cache"
	"github.com/stretchr/testify/assert"
)

func TestNewCache_IndexesByResource(t *testing.T) {
	rules := []config.PolicyRule{
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e1", Product: "app", Publisher: "pub-a"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "e2", Product: "app", Publisher: "pub-a"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
		{Resource: config.PolicyResourceTopic, Details: config.PolicyDetails{Name: "t1"}, Action: config.PolicyActionConfig{ConditionType: config.PolicyConditionTimestampThreshold}},
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

func TestNewCache_NoConditionForEventRule(t *testing.T) {
	rules := []config.PolicyRule{
		{Resource: config.PolicyResourceEvent, Details: config.PolicyDetails{Name: "click", Product: "app", Publisher: "pub-a"}, Action: config.PolicyActionConfig{Type: config.PolicyActionDeactivate}},
	}
	c := cache.NewCache(rules)

	events := c.Get(config.PolicyResourceEvent)
	assert.Len(t, events, 1)
	cond, ok := events["clickapppub-a"]
	assert.True(t, ok)
	assert.NotNil(t, cond)
}

func TestNewCache_NoConditionForTopicRule(t *testing.T) {
	rules := []config.PolicyRule{
		{Resource: config.PolicyResourceTopic, Details: config.PolicyDetails{Name: "clickstream-page-log"}, Action: config.PolicyActionConfig{Type: config.PolicyActionDeactivate}},
	}
	c := cache.NewCache(rules)

	topics := c.Get(config.PolicyResourceTopic)
	assert.Len(t, topics, 1)
	cond, ok := topics["clickstream-page-log"]
	assert.True(t, ok)
	assert.NotNil(t, cond)
}
