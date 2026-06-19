package action

import (
	"context"
	"sync/atomic"
	"testing"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	registration "github.com/goto/raccoon/ingestionrule/action/checkregistration"
	"github.com/stretchr/testify/assert"
)

func TestCheckRegistration_Apply(t *testing.T) {
	// Backup global config to restore after tests finish
	oldEnableCheckVerification := config.PolicyCfg.EnableCheckVerification
	oldPublisherMapping := config.PolicyCfg.PublisherMapping
	defer func() {
		config.PolicyCfg.EnableCheckVerification = oldEnableCheckVerification
		config.PolicyCfg.PublisherMapping = oldPublisherMapping
	}()

	// Sample events for testing
	event1 := &pb.Event{EventName: "click", Product: "bi"}
	event2 := &pb.Event{EventName: "view", Product: "bi"}
	events := []*pb.Event{event1, event2}

	tests := []struct {
		name          string
		setupStore    func() *registration.Store
		setupConfig   func()
		connGroup     string
		inputEvents   []*pb.Event
		wantEventsLen int
		wantEvents    []*pb.Event
	}{
		{
			name: "Case 1: Load fails (store contains invalid type in atomic.Value)",
			setupStore: func() *registration.Store {
				store := &registration.Store{}
				store.RegisteredEvents.Store("invalid-type-not-a-map")
				return store
			},
			setupConfig: func() {
				config.PolicyCfg.EnableCheckVerification = true
			},
			connGroup:     "group-1",
			inputEvents:   events,
			wantEventsLen: 2,
			wantEvents:    events,
		},
		{
			name: "Case 2: Verification flag is disabled",
			setupStore: func() *registration.Store {
				store := &registration.Store{}
				var atomicVal atomic.Value
				atomicVal.Store(map[string]struct{}{"pub-1:click:bi": {}})
				store.RegisteredEvents = atomicVal
				return store
			},
			setupConfig: func() {
				config.PolicyCfg.EnableCheckVerification = false
			},
			connGroup:     "group-1",
			inputEvents:   events,
			wantEventsLen: 2,
			wantEvents:    events,
		},
		{
			name: "Case 3: Registered events length is 0",
			setupStore: func() *registration.Store {
				store := &registration.Store{}
				var atomicVal atomic.Value
				atomicVal.Store(map[string]struct{}{}) // empty map
				store.RegisteredEvents = atomicVal
				return store
			},
			setupConfig: func() {
				config.PolicyCfg.EnableCheckVerification = true
			},
			connGroup:     "group-1",
			inputEvents:   events,
			wantEventsLen: 2,
			wantEvents:    events,
		},
		{
			name: "Case 4: Conn group/Publisher not found in mapping (skips verification for all)",
			setupStore: func() *registration.Store {
				store := &registration.Store{}
				var atomicVal atomic.Value
				atomicVal.Store(map[string]struct{}{"pub-1:click:bi": {}})
				store.RegisteredEvents = atomicVal
				return store
			},
			setupConfig: func() {
				config.PolicyCfg.EnableCheckVerification = true
				config.PolicyCfg.PublisherMapping = map[string]string{
					"some-other-group": "pub-1",
				}
			},
			connGroup:     "unmapped-group",
			inputEvents:   events,
			wantEventsLen: 2,
			wantEvents:    events,
		},
		{
			name: "Case 5: Filter events successfully (one registered, one dropped)",
			setupStore: func() *registration.Store {
				store := &registration.Store{}
				var atomicVal atomic.Value
				// Only event1 ("pub-1:click:bi") is registered
				atomicVal.Store(map[string]struct{}{
					"pub-1:click:bi": {},
				})
				store.RegisteredEvents = atomicVal
				return store
			},
			setupConfig: func() {
				config.PolicyCfg.EnableCheckVerification = true
				config.PolicyCfg.PublisherMapping = map[string]string{
					"group-1": "pub-1",
				}
			},
			connGroup:     "group-1",
			inputEvents:   events,
			wantEventsLen: 1,
			wantEvents:    []*pb.Event{event1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test-specific configurations
			tt.setupConfig()
			store := tt.setupStore()

			checker := NewCheckRegistration(store)
			got := checker.Apply(context.Background(), tt.inputEvents, tt.connGroup)

			assert.Len(t, got, tt.wantEventsLen)
			assert.Equal(t, tt.wantEvents, got)
		})
	}
}
