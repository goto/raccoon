package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache"
	"github.com/goto/raccoon/ingestionrule/action/mocks"
	"github.com/goto/raccoon/model"
)

func TestDedup_Apply_NilChecker(t *testing.T) {
	// Case 1: d is nil
	var d *action.Dedup
	events := []*model.EventWithMetadata{{Type: "click"}}
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))

	// Case 2: d is not nil, but checker is nil
	d = action.NewDedup(nil)
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))
}

func TestDedup_Apply_BypassDeduplicationWhenNotWhitelisted(t *testing.T) {
	// Configure whitelist to exclude group-1.
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"group-whitelisted": {},
	}

	mc := mocks.NewDuplicateChecker(t)
	d := action.NewDedup(mc)
	events := []*model.EventWithMetadata{{Type: "click"}}
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))
}

func TestDedup_Apply_BypassDeduplicationWhenCacheDurationNotFound(t *testing.T) {
	// Configure whitelist to include group-1, but no cache duration mapping.
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"group-1": {},
	}
	config.DedupCfg.ConnGroupCacheDuration = map[string]time.Duration{}

	mc := mocks.NewDuplicateChecker(t)
	d := action.NewDedup(mc)
	events := []*model.EventWithMetadata{{Type: "click"}}
	assert.Equal(t, events, d.Apply(context.Background(), events, "group-1"))
}

func TestDedup_Apply_DeduplicationWorkflow(t *testing.T) {
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"customer": {},
	}
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}
	config.PolicyCfg.PublisherMapping = map[string]string{
		"customer": "customer-publisher",
	}
	config.DedupCfg.ConnGroupCacheDuration = map[string]time.Duration{
		"customer": 5 * time.Minute,
	}

	// 1. Success case: event is not a duplicate.
	t.Run("EventNotDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
			},
		}, 5*time.Minute).Return([]bool{false}, nil)

		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1)
	})

	// 2. Duplicate case: event is already in cache.
	t.Run("EventDuplicate", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
			},
		}, 5*time.Minute).Return([]bool{true}, nil)

		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Empty(t, res) // Dropped
	})

	// 3. Batch with multiple duplicates within a single event slice.
	t.Run("BatchWithMultipleDuplicates", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)

		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventWithMetadata{
			{Publisher: "customer-publisher", EventGUID: "guid-1", EventName: "click", Product: "clickstream"},
			{Publisher: "customer-publisher", EventGUID: "guid-2", EventName: "click", Product: "clickstream"},
			{Publisher: "customer-publisher", EventGUID: "guid-3", EventName: "click", Product: "clickstream"},
		}, 5*time.Minute).Return([]bool{false, true, true}, nil) // 1 unique, 2 duplicates

		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher:  "customer-publisher",
				EventGUID:  "guid-1",
				EventName:  "click",
				Product:    "clickstream",
				Type:       "component",
				EventBytes: []byte("1"),
			},
			{
				Publisher:  "customer-publisher",
				EventGUID:  "guid-2",
				EventName:  "click",
				Product:    "clickstream",
				Type:       "component",
				EventBytes: []byte("2"),
			},
			{
				Publisher:  "customer-publisher",
				EventGUID:  "guid-3",
				EventName:  "click",
				Product:    "clickstream",
				Type:       "component",
				EventBytes: []byte("3"),
			},
		}

		res := d.Apply(context.Background(), events, "customer")

		// Assert that the 2 duplicates were dropped, leaving exactly 1 event
		assert.Len(t, res, 1)
		// Assert that the surviving event is the correct one (the first one)
		assert.Equal(t, []byte("1"), res[0].EventBytes)
	})

	// 4. Redis error case: fails open.
	t.Run("RedisErrorFailsOpen", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
			},
		}, 5*time.Minute).Return(nil, errors.New("redis error"))

		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1) // Bypassed and allowed through
	})

	// 5. Conversion case: identifier fields are not strings but can be converted.
	t.Run("EventWithNonStringIdentifiers", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		mc.EXPECT().AreDuplicates(mock.Anything, []cache.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "789",
			},
		}, 5*time.Minute).Return([]bool{false}, nil)

		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "789",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Len(t, res, 1)
	})
}

func TestDedup_Apply_ErrorsAndBypasses(t *testing.T) {
	config.DedupCfg.WhitelistConnGroup = map[string]struct{}{
		"customer": {},
	}
	config.DedupCfg.ProtoClassNameMapping = map[string]string{
		"component": "ClickEventProto",
	}
	config.PolicyCfg.PublisherMapping = map[string]string{
		"customer": "customer-publisher",
	}
	config.DedupCfg.ConnGroupCacheDuration = map[string]time.Duration{
		"customer": 5 * time.Minute,
	}

	// 1. Empty metadata fields (empty EventGUID)
	t.Run("EmptyEventGUID", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "customer-publisher",
				EventGUID: "",
				EventName: "click",
				Product:   "clickstream",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Should fail open and return the event
	})

	// 2. Empty publisher
	t.Run("EmptyPublisher", func(t *testing.T) {
		mc := mocks.NewDuplicateChecker(t)
		d := action.NewDedup(mc)

		events := []*model.EventWithMetadata{
			{
				Publisher: "",
				EventGUID: "guid-1",
				EventName: "click",
				Product:   "clickstream",
				Type:      "component",
			},
		}

		res := d.Apply(context.Background(), events, "customer")
		assert.Equal(t, events, res) // Should fail open and return the event
	})
}
