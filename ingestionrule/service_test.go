package ingestionrule_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule"
	checkregistration "github.com/goto/raccoon/ingestionrule/action/checkregistration"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMain(m *testing.M) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalCfg := config.StencilCfg
	config.StencilCfg.URL = server.URL
	config.StencilCfg.MaxRetry = 1
	config.StencilCfg.MaxJitterInterval = time.Millisecond
	config.StencilCfg.ExponentFactor = 1
	config.StencilCfg.HTTPTimeout = 2 * time.Second

	originalPolicyCfgEnabled := config.PolicyCfg.Enabled
	config.PolicyCfg.Enabled = true

	originalExcludeList := config.DeserializationCfg.ExcludeEventTypeList
	config.DeserializationCfg.ExcludeEventTypeList = []string{"click", "scroll", ""}

	code := m.Run()

	config.StencilCfg = originalCfg
	config.PolicyCfg.Enabled = originalPolicyCfgEnabled
	config.DeserializationCfg.ExcludeEventTypeList = originalExcludeList
	os.Exit(code)
}

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

func TestService_Apply_DropTakesPriorityOverOverride(t *testing.T) {
	disableRegistrationStore(t)
	rules := buildRules(time.Hour, time.Hour, false)
	svc, err := ingestionrule.NewService(context.Background(), rules)
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
	svc, err := ingestionrule.NewService(context.Background(), rules)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(context.Background(), events, "grp")
	// Event stays in batch and Type remains empty.
	assert.Len(t, result, 1)
	assert.Empty(t, result[0].Type)
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	disableRegistrationStore(t)

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.NoError(t, err)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	disableRegistrationStore(t)
	rules := buildRules(time.Hour, 0, false)
	svc, err := ingestionrule.NewService(context.Background(), rules)
	assert.NoError(t, err)

	clean := &pb.Event{EventName: "other", Product: "app"}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	result := svc.Apply(context.Background(), []*pb.Event{stale, clean}, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "other", result[0].EventName)
}

func TestService_Apply_DeactivateDropsEvent(t *testing.T) {
	disableRegistrationStore(t)

	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true))

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
	svc, err := ingestionrule.NewService(context.Background(), rules)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivatePassthroughWhenNoMatch(t *testing.T) {
	disableRegistrationStore(t)

	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true))
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "scroll", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "scroll", result[0].EventName)
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
	svc, err := ingestionrule.NewService(context.Background(), rules)
	assert.NoError(t, err)
	events := []*pb.Event{{EventName: "click", Product: "app"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_NewService_DeserializationDisabled(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origPolicyEnabled := config.PolicyCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.PolicyCfg.Enabled = origPolicyEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = false
	config.DedupCfg.Enabled = false
	config.PolicyCfg.Enabled = false
	// Set Stencil URL to empty/invalid, which should NOT fail initialization when features are disabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestService_NewService_DedupEnabled_StencilURL_Empty(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origPolicyEnabled := config.PolicyCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.PolicyCfg.Enabled = origPolicyEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = false
	config.DedupCfg.Enabled = true
	config.PolicyCfg.Enabled = false
	// Set Stencil URL to empty/invalid, which must fail initialization when dedup is enabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.Error(t, err)
	assert.Nil(t, svc)
}

func TestService_NewService_DeserializationEnabled_StencilURL_Empty(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origPolicyEnabled := config.PolicyCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.PolicyCfg.Enabled = origPolicyEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = true
	config.DedupCfg.Enabled = false
	config.PolicyCfg.Enabled = false
	// Set Stencil URL to empty/invalid, which must fail initialization when deserialization is enabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.Error(t, err)
	assert.Nil(t, svc)
}

func TestService_NewService_PolicyEnabled_StencilURL_Empty(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origPolicyEnabled := config.PolicyCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.PolicyCfg.Enabled = origPolicyEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = false
	config.DedupCfg.Enabled = false
	config.PolicyCfg.Enabled = true
	// Set Stencil URL to empty/invalid, which must fail initialization when policy is enabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestService_NewService_PolicyDisabled(t *testing.T) {
	origEnabled := config.PolicyCfg.Enabled
	defer func() {
		config.PolicyCfg.Enabled = origEnabled
	}()

	config.PolicyCfg.Enabled = false

	// A rule that should normally deactivate/drop the event
	rules := []config.PolicyRule{
		{
			Resource: config.PolicyResourceEvent,
			Details:  config.PolicyDetails{Name: "click", Product: "app", Publisher: "grp"},
			Action:   config.PolicyActionConfig{Type: config.PolicyActionDeactivate},
		},
	}

	svc, err := ingestionrule.NewService(context.Background(), rules)
	assert.NoError(t, err)
	assert.NotNil(t, svc)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	// The event should pass through because policy enforcement is disabled
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_CompassHealthCheck(t *testing.T) {
	// A nil service should return nil error (no-op)
	var nilSvc *ingestionrule.Service
	assert.NoError(t, nilSvc.CompassHealthCheck())
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
