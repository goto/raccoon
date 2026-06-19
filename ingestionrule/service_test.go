package ingestionrule_test

import (
	"context"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule"
	checkregistration "github.com/goto/raccoon/ingestionrule/action/checkregistration"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func disableRegistrationStore(t *testing.T) {
	original := ingestionrule.NewRegistrationStore

	ingestionrule.NewRegistrationStore = func(context.Context) (*checkregistration.Store, error) {
		return nil, nil
	}

	t.Cleanup(func() {
		ingestionrule.NewRegistrationStore = original
	})
}

func timestampProto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

const testOverrideEventType = "invalid-et"

func buildRules(pastDrop, pastOverride time.Duration, withDeactivate bool) []config.PolicyRule {
	var rules []config.PolicyRule
	if withDeactivate {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		})
	}
	if pastDrop > 0 {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionDrop,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastDrop}},
			},
		})
	}
	if pastOverride > 0 {
		rules = append(rules, config.PolicyRule{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: pastOverride}},
			},
		})
	}
	return rules
}

func TestService_Apply_NilIsPassthrough(t *testing.T) {
	var svc *ingestionrule.Service
	events := []*pb.Event{{EventName: "click"}}
	assert.Equal(t, events, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DropTakesPriorityOverOverride(t *testing.T) {
	disableRegistrationStore(t)
	rules := buildRules(time.Hour, time.Hour, false)
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Empty(t, result)
}

func TestService_Apply_OverrideWhenNoDrop(t *testing.T) {
	disableRegistrationStore(t)
	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action: config.PolicyActionConfig{
				Type:                    config.PolicyActionOverrideTimestamp,
				ConditionType:           config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{Past: config.PolicyDuration{Duration: time.Hour}},
			},
		},
	}
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(context.Background(), events, "grp")
	// Event stays in batch but with Type overridden to the override event type.
	assert.Len(t, result, 1)
	assert.Equal(t, testOverrideEventType, result[0].GetType())
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	disableRegistrationStore(t)
	svc, err := ingestionrule.NewService(context.Background(), nil, testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Equal(t, events, result)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	disableRegistrationStore(t)
	rules := buildRules(time.Hour, 0, false)
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)

	clean := &pb.Event{EventName: "other", Product: "app"}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	result := svc.Apply(context.Background(), []*pb.Event{stale, clean}, "grp")
	assert.Equal(t, []*pb.Event{clean}, result)
}

func TestService_Apply_DeactivateDropsEvent(t *testing.T) {
	disableRegistrationStore(t)
	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true), testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivateTakesPriorityOverDrop(t *testing.T) {
	disableRegistrationStore(t)
	// Both DEACTIVE and DROP rules target the same event.
	// DEACTIVE runs first and removes it; DROP never sees it.
	rules := buildRules(time.Hour, 0, true)
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivatePassthroughWhenNoMatch(t *testing.T) {
	disableRegistrationStore(t)
	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true), testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "scroll", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Equal(t, events, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_UnknownActionTypeSkipped(t *testing.T) {
	disableRegistrationStore(t)
	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action:   config.PolicyActionConfig{Type: "UNKNOWN_ACTION"},
		},
	}
	// Should not panic; rule is silently skipped (error logged).
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)
	events := []*pb.Event{{EventName: "click", Product: "app"}}
	assert.Equal(t, events, svc.Apply(context.Background(), events, "grp"))
}

func TestService_WithoutRegistrationStore(t *testing.T) {
	original := ingestionrule.NewRegistrationStore
	defer func() {
		ingestionrule.NewRegistrationStore = original
	}()

	ingestionrule.NewRegistrationStore = func(context.Context) (*checkregistration.Store, error) {
		return nil, nil
	}

	svc, err := ingestionrule.NewService(
		context.Background(),
		nil,
		testOverrideEventType,
	)

	assert.NoError(t, err)

	chain := svc.Apply(
		context.Background(),
		[]*pb.Event{{EventName: "click"}},
		"grp",
	)
	// checkregistration is not in the chain, so the event should pass through unmodified.
	assert.Len(t, chain, 1)
}

func TestService_WithRegistrationStore(t *testing.T) {
	original := ingestionrule.NewRegistrationStore
	defer func() {
		ingestionrule.NewRegistrationStore = original
	}()

	store := &checkregistration.Store{}
	// pre-populate the store with a registered event for testing
	store.RegisteredEvents.Store(map[string]struct{}{
		"grp:click:app": {},
	})

	ingestionrule.NewRegistrationStore = func(context.Context) (*checkregistration.Store, error) {
		return store, nil
	}

	svc, err := ingestionrule.NewService(
		context.Background(),
		nil,
		testOverrideEventType,
	)

	assert.NoError(t, err)

	result := svc.Apply(
		context.Background(),
		[]*pb.Event{
			{
				EventName: "click",
				Product:   "app",
			},
		},
		"grp",
	)
	// checkregistration is in the chain, so the event should not be dropped and should pass through unmodified.
	assert.Len(t, result, 1)
}

func TestService_WithRegistrationStore_ActionOrder(t *testing.T) {
	originalEnable := config.PolicyCfg.EnableCheckVerification
	originalMapping := config.PolicyCfg.PublisherMapping

	config.PolicyCfg.EnableCheckVerification = true
	config.PolicyCfg.PublisherMapping = map[string]string{
		"grp": "grp",
	}

	defer func() {
		config.PolicyCfg.EnableCheckVerification = originalEnable
		config.PolicyCfg.PublisherMapping = originalMapping
	}()

	store := &checkregistration.Store{}
	// pre-populate the store with a registered event for testing
	store.RegisteredEvents.Store(map[string]struct{}{
		"grp:other:app": {},
	})

	ingestionrule.NewRegistrationStore = func(context.Context) (*checkregistration.Store, error) {
		return store, nil
	}

	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details: config.PolicyDetails{
				Name:      "click",
				Product:   "app",
				Publisher: "grp",
			},
			Action: config.PolicyActionConfig{
				Type:          config.PolicyActionOverrideTimestamp,
				ConditionType: config.PolicyConditionTimestampThreshold,
				EventTimestampThreshold: config.PolicyTimestampThreshold{
					Past: config.PolicyDuration{
						Duration: time.Hour,
					},
				},
			},
		},
	}

	svc, err := ingestionrule.NewService(
		context.Background(),
		rules,
		testOverrideEventType,
	)
	assert.NoError(t, err)

	events := []*pb.Event{
		{
			EventName:      "click",
			Product:        "app",
			EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour)),
		},
	}

	result := svc.Apply(context.Background(), events, "grp")

	// Event should be dropped by CheckRegistration because it is not registered.
	// OverrideTimestamp gets a chance to modify it.
	assert.Empty(t, result)
}
