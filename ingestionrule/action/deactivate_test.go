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
	"github.com/goto/raccoon/ingestionrule/action/eventregistry"
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
	events          map[string]eventregistry.EventStatus
	hasSynced       bool
	getEventsCalled bool
}

func (m *mockEventChecker) GetEvents(key string) (eventregistry.EventStatus, bool) {
	m.getEventsCalled = true
	status, ok := m.events[key]
	return status, ok
}

func (m *mockEventChecker) Close()             {}
func (m *mockEventChecker) Start()             {}
func (m *mockEventChecker) HealthCheck() error { return nil }
func (m *mockEventChecker) HasSynced() bool    { return m.hasSynced }
func (m *mockEventChecker) BuildCacheKey(publisher, topic, product, eventName string) string {
	return buildHashKey(publisher, topic, product, eventName)
}

func TestDeactivate_WithEventChecker(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = true
	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
	}()

	mockChecker := &mockEventChecker{
		hasSynced: true,
		events: map[string]eventregistry.EventStatus{
			buildHashKey("pub-a", "clickstream-click-log", "app", "click"):       eventregistry.EventStatusActive,
			buildHashKey("pub-a", "clickstream-scroll-log", "app", "scroll"):     eventregistry.EventStatusInactive,
			buildHashKey("pub-a", "clickstream-pageview-log", "app", "pageview"): eventregistry.EventStatusDeprecated,
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

	// Events are not dropped yet
	assert.Len(t, result, 4)
}

func TestDeactivate_WithEventChecker_NotSynced(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = true
	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
	}()

	mockChecker := &mockEventChecker{
		hasSynced: false,
		events: map[string]eventregistry.EventStatus{
			buildHashKey("pub-a", "clickstream-click-log", "app", "click"): eventregistry.EventStatusActive,
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
	}

	result := deactivateAction.Apply(context.Background(), events, "pub-a")

	assert.Len(t, result, 1)
	assert.False(t, mockChecker.getEventsCalled, "GetEvents should not be called when HasSynced is false")
}
