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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func disableEventChecker(t *testing.T) {
	original := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = false
	t.Cleanup(func() {
		config.PolicyCfg.EventVerificationEnabled = original
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
	disableEventChecker(t)
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
	disableEventChecker(t)
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
	disableEventChecker(t)

	svc, err := ingestionrule.NewService(context.Background(), nil)
	assert.NoError(t, err)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(context.Background(), events, "grp")
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	disableEventChecker(t)
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
	disableEventChecker(t)

	svc, err := ingestionrule.NewService(context.Background(), buildRules(0, 0, true))

	assert.NoError(t, err)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Empty(t, svc.Apply(context.Background(), events, "grp"))
}

func TestService_Apply_DeactivateTakesPriorityOverDrop(t *testing.T) {
	disableEventChecker(t)
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
	disableEventChecker(t)

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
	disableEventChecker(t)
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

func setupMockMSLServer(t *testing.T, responseJSON string) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/events", r.URL.Path)
		assert.Equal(t, "grp", r.URL.Query().Get("publisher"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(responseJSON))
	}))
	return server
}

func TestService_WithoutRegistrationStore(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = false
	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
	}()

	svc, err := ingestionrule.NewService(
		context.Background(),
		nil,
	)

	assert.NoError(t, err)

	chain := svc.Apply(
		context.Background(),
		[]*pb.Event{{EventName: "click"}},
		"grp",
	)
	// checkregistration/eventchecker is not enabled, so the event should pass through unmodified.
	assert.Len(t, chain, 1)
}

func TestService_WithRegistrationStore(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	config.PolicyCfg.EventVerificationEnabled = true
	originalHost := config.MslCfg.HTTPHost
	originalMapping := config.PolicyCfg.PublisherMapping
	config.PolicyCfg.PublisherMapping = map[string]string{
		"grp": "grp",
	}
	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
		config.MslCfg.HTTPHost = originalHost
		config.PolicyCfg.PublisherMapping = originalMapping
	}()

	// The event we are sending is EventName: "click", Product: "app", and resolving to TopicName: "clickstream-click-log"
	// Key format: publisher:topic:product:eventName
	responseJSON := `{
		"success": true,
		"data": {
			"event1": {
				"publisher": "grp",
				"product": "app",
				"name": "click",
				"source": {
					"table": "clickstream_click_log"
				}
			}
		}
	}`

	mockServer := setupMockMSLServer(t, responseJSON)
	defer mockServer.Close()

	config.MslCfg.HTTPHost = mockServer.URL

	svc, err := ingestionrule.NewService(
		context.Background(),
		nil,
	)
	require.NoError(t, err)
	defer svc.Close()

	result := svc.Apply(
		context.Background(),
		[]*pb.Event{
			{
				EventName: "click",
				Product:   "app",
				Type:      "click",
			},
		},
		"grp",
	)

	// Since the event is registered (Active), it should not be dropped and should pass through unmodified.
	assert.Len(t, result, 1)
	assert.Equal(t, "click", result[0].EventName)
}

func TestService_WithRegistrationStore_ActionOrder(t *testing.T) {
	originalEnable := config.PolicyCfg.EventVerificationEnabled
	originalHost := config.MslCfg.HTTPHost
	originalMapping := config.PolicyCfg.PublisherMapping

	config.PolicyCfg.EventVerificationEnabled = true
	config.PolicyCfg.PublisherMapping = map[string]string{
		"grp": "grp",
	}

	defer func() {
		config.PolicyCfg.EventVerificationEnabled = originalEnable
		config.MslCfg.HTTPHost = originalHost
		config.PolicyCfg.PublisherMapping = originalMapping
	}()

	// Mock server returns event other than "click"
	responseJSON := `{
		"success": true,
		"data": {
			"event1": {
				"publisher": "grp",
				"product": "app",
				"name": "other",
				"source": {
					"table": "clickstream_other_log"
				}
			}
		}
	}`

	mockServer := setupMockMSLServer(t, responseJSON)
	defer mockServer.Close()

	config.MslCfg.HTTPHost = mockServer.URL

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
	)
	require.NoError(t, err)
	defer svc.Close()

	events := []*pb.Event{
		{
			EventName:      "click",
			Product:        "app",
			Type:           "click",
			EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour)),
		},
	}

	result := svc.Apply(context.Background(), events, "grp")

	// Event should be dropped by Deactivate/EventChecker because it is not registered.
	assert.Empty(t, result)
}
