package policy_test

import (
	"testing"

	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
)

func buildTestPolicies() []policy.PolicyConfig {
	return []policy.PolicyConfig{
		{Resource: policy.ResourceEvent, Details: policy.Details{Name: "e1"}, Action: policy.ActionConfig{Type: policy.ActionDrop}},
		{Resource: policy.ResourceTopic, Details: policy.Details{Name: "t1"}, Action: policy.ActionConfig{Type: policy.ActionDrop}},
		{Resource: policy.ResourceEvent, Details: policy.Details{Name: "e2"}, Action: policy.ActionConfig{Type: policy.ActionOverrideTimestamp}},
	}
}

func TestNewCache_BucketsCorrectly(t *testing.T) {
	cache := policy.NewCache(buildTestPolicies())

	dropEvents := cache.Get(policy.ActionDrop, policy.ResourceEvent)
	assert.Len(t, dropEvents, 1)
	assert.Equal(t, "e1", dropEvents[0].Details.Name)

	dropTopics := cache.Get(policy.ActionDrop, policy.ResourceTopic)
	assert.Len(t, dropTopics, 1)
	assert.Equal(t, "t1", dropTopics[0].Details.Name)

	overrideEvents := cache.Get(policy.ActionOverrideTimestamp, policy.ResourceEvent)
	assert.Len(t, overrideEvents, 1)
	assert.Equal(t, "e2", overrideEvents[0].Details.Name)
}

func TestCache_GetMissingAction(t *testing.T) {
	cache := policy.NewCache(nil)
	assert.Nil(t, cache.Get(policy.ActionDrop, policy.ResourceEvent))
}

func TestCache_NilCache(t *testing.T) {
	var cache *policy.Cache
	assert.Nil(t, cache.Get(policy.ActionDrop, policy.ResourceEvent))
}
