package policy_test

import (
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/policy"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	var svc *policy.Service
	events := []*pb.Event{{EventName: "click"}}
	assert.Equal(t, events, svc.Apply(events, "grp"))
}

func TestService_Apply_DropTakesPriorityOverOverride(t *testing.T) {
	rules := buildRules(time.Hour, time.Hour, false)
	svc := policy.NewService(rules, testOverrideEventType)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(events, "grp")
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
	svc := policy.NewService(rules, testOverrideEventType)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	result := svc.Apply(events, "grp")
	// Event stays in batch but with Type overridden to the override event type.
	assert.Len(t, result, 1)
	assert.Equal(t, testOverrideEventType, result[0].GetType())
}

func TestService_Apply_PassthroughWhenNoPolicy(t *testing.T) {
	svc := policy.NewService(nil, testOverrideEventType)

	events := []*pb.Event{{EventName: "click"}}
	result := svc.Apply(events, "grp")
	assert.Equal(t, events, result)
}

func TestService_Apply_MixedBatch(t *testing.T) {
	rules := buildRules(time.Hour, 0, false)
	svc := policy.NewService(rules, testOverrideEventType)

	clean := &pb.Event{EventName: "other", Product: "app"}
	stale := &pb.Event{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))}
	result := svc.Apply([]*pb.Event{stale, clean}, "grp")
	assert.Equal(t, []*pb.Event{clean}, result)
}

func TestService_Apply_DeactivateDropsEvent(t *testing.T) {
	svc := policy.NewService(buildRules(0, 0, true), testOverrideEventType)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Empty(t, svc.Apply(events, "grp"))
}

func TestService_Apply_DeactivateTakesPriorityOverDrop(t *testing.T) {
	// Both DEACTIVE and DROP rules target the same event.
	// DEACTIVE runs first and removes it; DROP never sees it.
	rules := buildRules(time.Hour, 0, true)
	svc := policy.NewService(rules, testOverrideEventType)

	events := []*pb.Event{
		{EventName: "click", Product: "app", EventTimestamp: timestampProto(time.Now().Add(-2 * time.Hour))},
	}
	assert.Empty(t, svc.Apply(events, "grp"))
}

func TestService_Apply_DeactivatePassthroughWhenNoMatch(t *testing.T) {
	svc := policy.NewService(buildRules(0, 0, true), testOverrideEventType)

	events := []*pb.Event{
		{EventName: "scroll", Product: "app", EventTimestamp: timestampProto(time.Now())},
	}
	assert.Equal(t, events, svc.Apply(events, "grp"))
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
	svc := policy.NewService(rules, testOverrideEventType)
	events := []*pb.Event{{EventName: "click", Product: "app"}}
	assert.Equal(t, events, svc.Apply(events, "grp"))
}
