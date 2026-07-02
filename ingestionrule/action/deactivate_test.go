package action_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/eval/cache"
	"github.com/goto/raccoon/ingestionrule/action/eventchecker"
	"github.com/goto/raccoon/model"
)

func buildHashKey(publisher, topic, product, eventName string) string {
	h := xxhash.New()
	const keySeparator = ":"
	_, _ = io.WriteString(h, publisher)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, topic)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, product)
	_, _ = io.WriteString(h, keySeparator)
	_, _ = io.WriteString(h, eventName)
	hash := h.Sum64()
	return fmt.Sprintf("%016x", hash)
}

func buildDeactivateEventCache(name, product, publisher string) *cache.Cache {
	return cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: name, Product: product, Publisher: publisher},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
}

func newDeactivate(c *cache.Cache) *action.Deactivate {
	return action.NewDeactivate(c, action.DefaultChain(), nil)
}

func TestDeactivate_DropsMatchingEvent(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{{
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_PassthroughWhenNoIngestionRuleMatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{{
		EventName:      "scroll",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsAlwaysRegardlessOfTimestamp(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now()},
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now().Add(-365 * 24 * time.Hour)},
	}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_FiltersMixedBatch(t *testing.T) {
	c := buildDeactivateEventCache("click", "app", "pub-a")
	events := []*model.EventWithMetadata{
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now()},
		{EventName: "scroll", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now()},
		{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now()},
	}
	result := newDeactivate(c).Apply(context.Background(), events, "pub-a")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].EventName)
}

func TestDeactivate_PassthroughWhenEmptyRules(t *testing.T) {
	c := cache.NewCache(nil)
	events := []*model.EventWithMetadata{{EventName: "click", Product: "app", Publisher: "pub-a", EventTimestamp: time.Now()}}
	assert.Equal(t, events, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

func TestDeactivate_DropsMatchingTopicRule(t *testing.T) {
	c := cache.NewCache([]config.PolicyRule{
		{
			Resource: config.PolicyResourceTopic,
			Details:  config.PolicyDetails{Name: "clickstream-page-log"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	})
	events := []*model.EventWithMetadata{{
		TopicName:      "clickstream-page-log",
		EventName:      "click",
		Product:        "app",
		Publisher:      "pub-a",
		EventTimestamp: time.Now(),
	}}
	assert.Empty(t, newDeactivate(c).Apply(context.Background(), events, "pub-a"))
}

type mockEventChecker struct {
	events map[string]eventchecker.EventStatus
}

func (m *mockEventChecker) GetEvents(key string) (eventchecker.EventStatus, bool) {
	status, ok := m.events[key]
	return status, ok
}

func (m *mockEventChecker) Close() {}
func (m *mockEventChecker) Start() {}
func (m *mockEventChecker) HealthCheck() error { return nil }

func TestDeactivate_WithEventChecker(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = true
	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
	}()

	mockChecker := &mockEventChecker{
		events: map[string]eventchecker.EventStatus{
			buildHashKey("pub-a", "clickstream-click-log", "app", "click"):       eventchecker.EventStatusActive,
			buildHashKey("pub-a", "clickstream-scroll-log", "app", "scroll"):     eventchecker.EventStatusInactive,
			buildHashKey("pub-a", "clickstream-pageview-log", "app", "pageview"): eventchecker.EventStatusDeprecated,
		},
	}

	c := cache.NewCache(nil)
	deactivateAction := action.NewDeactivate(c, action.DefaultChain(), mockChecker)

	events := []*model.EventWithMetadata{
		{
			EventName: "click",
			Product:   "app",
			Publisher: "pub-a",
			TopicName: "clickstream-click-log",
		},
		{
			EventName: "scroll",
			Product:   "app",
			Publisher: "pub-a",
			TopicName: "clickstream-scroll-log",
		},
		{
			EventName: "pageview",
			Product:   "app",
			Publisher: "pub-a",
			TopicName: "clickstream-pageview-log",
		},
		{
			EventName: "unknown",
			Product:   "app",
			Publisher: "pub-a",
			TopicName: "clickstream-unknown-log",
		},
	}

	result := deactivateAction.Apply(context.Background(), events, "pub-a")

	// Only "click" is active, so all others (inactive, deprecated, unregistered) should be dropped
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}
