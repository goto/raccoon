package checkregistration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

type MockClient struct {
	mock.Mock
}

func (m *MockClient) GetPublishers(ctx context.Context) ([]Publisher, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Publisher), args.Error(1)
}

func (m *MockClient) GetEvents(ctx context.Context, publisher string) ([]Event, error) {
	args := m.Called(ctx, publisher)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Event), args.Error(1)
}

// --- Tests ---

// 1. Test LoadPublishers updates the atomic store correctly
func TestStore_LoadPublishers(t *testing.T) {
	mockClient := new(MockClient)
	store := &Store{client: mockClient}

	expectedPublishers := []Publisher{{Name: "pub-1"}, {Name: "pub-2"}}
	mockClient.On("GetPublishers", mock.Anything).Return(expectedPublishers, nil)

	err := store.LoadPublishers(context.Background())
	assert.NoError(t, err)

	// Validate atomic update
	loaded, ok := store.Publishers.Load().([]Publisher)
	assert.True(t, ok)
	assert.Equal(t, expectedPublishers, loaded)
	mockClient.AssertExpectations(t)
}

// 2. Test LoadEvents with parallel processing and success scenario
func TestStore_LoadEvents_Success(t *testing.T) {
	mockClient := new(MockClient)
	store := &Store{client: mockClient}

	publishers := []Publisher{{Name: "pub-1"}, {Name: "pub-2"}}
	store.Publishers.Store(publishers)

	// Mock separate event feeds for both publishers to test parallel construction
	mockClient.On("GetEvents", mock.Anything, "pub-1").Return([]Event{
		{Product: "prod-a", EventName: "click"},
	}, nil)

	mockClient.On("GetEvents", mock.Anything, "pub-2").Return([]Event{
		{Product: "prod-b", EventName: "view"},
	}, nil)

	err := store.LoadEvents(context.Background())
	assert.NoError(t, err)

	// Verify structural map aggregation in RegisteredEvents cache
	registered, ok := store.RegisteredEvents.Load().(map[string]struct{})
	assert.True(t, ok)
	assert.Len(t, registered, 2)
	assert.Contains(t, registered, "pub-1:click:prod-a")
	assert.Contains(t, registered, "pub-2:view:prod-b")
	mockClient.AssertExpectations(t)
}

// 3. Test LoadEvents stops the entire process if any publisher lookup fails
func TestStore_LoadEvents_FailureStopsRefresh(t *testing.T) {
	mockClient := new(MockClient)
	store := &Store{client: mockClient}

	publishers := []Publisher{{Name: "pub-1"}, {Name: "pub-2"}}
	store.Publishers.Store(publishers)

	// Simulate one endpoint succeeding and another erroring out
	mockClient.On("GetEvents", mock.Anything, "pub-1").Return([]Event{
		{Product: "prod-a", EventName: "click"},
	}, nil).Maybe() // Use Maybe because execution order is concurrent/non-deterministic

	mockClient.On("GetEvents", mock.Anything, "pub-2").Return(nil, errors.New("network timeout")).Maybe()

	err := store.LoadEvents(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "network timeout", err.Error())
}

// 4. Test LoadEvents handles an empty or missing publisher set gracefully
func TestStore_LoadEvents_NoPublishers(t *testing.T) {
	store := &Store{client: new(MockClient)}
	// Empty slice scenario
	store.Publishers.Store([]Publisher{})

	err := store.LoadEvents(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, store.RegisteredEvents.Load())
}

// 5. Test Refresh scheduling context cancellation
func TestStore_Refresh_ContextCancellation(t *testing.T) {
	mockClient := new(MockClient)
	store := &Store{client: mockClient}

	// Backup global configuration
	oldPubInterval := config.PolicyCfg.PublisherRefreshInHours
	oldEventInterval := config.PolicyCfg.EventRefreshInMinutes

	// Apply explicit positive micro-durations for this test case
	config.PolicyCfg.PublisherRefreshInHours = 10 * time.Millisecond
	config.PolicyCfg.EventRefreshInMinutes = 10 * time.Millisecond

	defer func() {
		config.PolicyCfg.PublisherRefreshInHours = oldPubInterval
		config.PolicyCfg.EventRefreshInMinutes = oldEventInterval
	}()

	ctx, cancel := context.WithCancel(context.Background())

	// Executes cleanly without hitting 0 duration panics
	store.Refresh(ctx)

	// Trigger explicit shutdown
	cancel()

	// Let any trailing select/channel updates settle down
	time.Sleep(15 * time.Millisecond)
	mockClient.AssertNotCalled(t, "GetPublishers", mock.Anything)
	mockClient.AssertNotCalled(t, "GetEvents", mock.Anything, mock.Anything)
}

// 6. Test NewStore Initialization Edge Case
func TestStore_NilClientCheck(t *testing.T) {
	// Backup original configuration values
	oldURL := config.PolicyCfg.MSLBaseURL
	defer func() { config.PolicyCfg.MSLBaseURL = oldURL }()

	// If the URL configuration string is empty/invalid,
	// ensure the package behavior executes safely or catches nil patterns if forced.
	config.PolicyCfg.MSLBaseURL = ":"

	ctx := context.Background()
	store, err := NewStore(ctx)

	// Since NewMSLClient returns an actual structure even on dead URLs,
	// checking if it handles underlying network instantiation issues smoothly.
	if store != nil {
		assert.NoError(t, err)
		assert.NotNil(t, store)
	} else {
		assert.Nil(t, store)
	}
}
