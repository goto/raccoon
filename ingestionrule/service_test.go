package ingestionrule_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule"
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

	code := m.Run()

	config.StencilCfg = originalCfg
	config.PolicyCfg.Enabled = originalPolicyCfgEnabled
	os.Exit(code)
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

<<<<<<< HEAD
=======
func TestService_Apply_NilIsPassthrough(t *testing.T) {
	var svc *ingestionrule.Service
	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

>>>>>>> bf970e8 (chore: pass the deserialized payload)
func TestService_Apply_DropTakesPriorityOverOverride(t *testing.T) {
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
<<<<<<< HEAD
<<<<<<< HEAD
	assert.Equal(t, testOverrideEventType, result[0].Type)
=======
	assert.Equal(t, testOverrideEventType, result[0].EventType)
>>>>>>> bf970e8 (chore: pass the deserialized payload)
=======
	assert.Equal(t, testOverrideEventType, result[0].Event.GetType())
>>>>>>> 8f1245b (chore: adjust the event type)
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	svc, err := ingestionrule.NewService(context.Background(), nil, testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	rules := buildRules(time.Hour, 0, false)
	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)

	clean := &pb.Event{EventName: "other", Product: "app"}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	result := svc.Apply(context.Background(), []*pb.Event{stale, clean}, "grp")
	assert.Len(t, result, 1)
<<<<<<< HEAD
	assert.Equal(t, "other", result[0].EventName)
=======
	assert.Equal(t, clean, result[0].Event)
>>>>>>> bf970e8 (chore: pass the deserialized payload)
}

func TestService_Apply_DeactivateDropsEvent(t *testing.T) {
	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true), testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivateTakesPriorityOverDrop(t *testing.T) {
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
	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true), testOverrideEventType)
	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "scroll", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
<<<<<<< HEAD
	assert.Equal(t, "scroll", result[0].EventName)
=======
	assert.Equal(t, events[0], result[0].Event)
>>>>>>> bf970e8 (chore: pass the deserialized payload)
}

func TestService_Apply_UnknownActionTypeSkipped(t *testing.T) {
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
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
<<<<<<< HEAD
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_NewService_DeserializationDisabled(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = false
	config.DedupCfg.Enabled = false
	// Set Stencil URL to empty/invalid, which should NOT fail initialization when both features are disabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil, testOverrideEventType)
	assert.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestService_NewService_DedupEnabled_StencilURL_Empty(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origDedupEnabled := config.DedupCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.DedupCfg.Enabled = origDedupEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = false
	config.DedupCfg.Enabled = true
	// Set Stencil URL to empty/invalid, which must fail initialization when dedup is enabled
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil, testOverrideEventType)
	assert.Error(t, err)
	assert.Nil(t, svc)
}

func TestService_NewService_DeserializationEnabled(t *testing.T) {
	origEnabled := config.DeserializationCfg.Enabled
	origStencilURL := config.StencilCfg.URL
	defer func() {
		config.DeserializationCfg.Enabled = origEnabled
		config.StencilCfg.URL = origStencilURL
	}()

	config.DeserializationCfg.Enabled = true
	// Set Stencil URL to empty/invalid, which must fail initialization
	config.StencilCfg.URL = ""

	svc, err := ingestionrule.NewService(context.Background(), nil, testOverrideEventType)
	assert.Error(t, err)
	assert.Nil(t, svc)
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

	svc, err := ingestionrule.NewService(context.Background(), rules, testOverrideEventType)
	assert.NoError(t, err)
	assert.NotNil(t, svc)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	// The event should pass through because policy enforcement is disabled
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
=======
	assert.Equal(t, events[0], result[0].Event)
>>>>>>> bf970e8 (chore: pass the deserialized payload)
}
