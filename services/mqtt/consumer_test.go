package mqtt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConsumer_Init(t *testing.T) {
	tests := []struct {
		name      string
		startErr  error
		expectErr bool
	}{
		{
			name:      "successfully starts client",
			startErr:  nil,
			expectErr: false,
		},
		{
			name:      "fails to start client",
			startErr:  errors.New("connection failed"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockPubSubClient)
			mockClient.On("Start").Return(tt.startErr).Once()

			c := NewConsumer(mockClient)
			err := c.Init()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "failed to start consumer")
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestConsumer_Shutdown(t *testing.T) {
	tests := []struct {
		name      string
		stopErr   error
		expectErr bool
	}{
		{
			name:      "successfully stops client",
			stopErr:   nil,
			expectErr: false,
		},
		{
			name:      "fails to stop client",
			stopErr:   errors.New("disconnect failed"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockPubSubClient)
			mockClient.On("Stop").Return(tt.stopErr).Once()

			c := NewConsumer(mockClient)
			err := c.Shutdown()

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "failed to stop consumer")
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestConsumer_IsHealthy(t *testing.T) {
	tests := []struct {
		name        string
		isConnected bool
	}{
		{"client connected", true},
		{"client disconnected", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockPubSubClient)
			mockClient.On("IsConnected").Return(tt.isConnected).Once()

			c := NewConsumer(mockClient)
			assert.Equal(t, tt.isConnected, c.IsHealthy())

			mockClient.AssertExpectations(t)
		})
	}
}

// --- Mock Implementation ---

type mockPubSubClient struct {
	mock.Mock
}

func (m *mockPubSubClient) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockPubSubClient) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockPubSubClient) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}
