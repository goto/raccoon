package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/raccoon/cache/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestStore_IsDuplicate(t *testing.T) {
	ctx := context.Background()
	event := EventMetadata{
		UserID:    "user1",
		SessionID: "session1",
		EventGUID: "guid1",
	}
	expectedKey := "user1:session1:guid1"

	t.Run("NotDuplicate", func(t *testing.T) {
		mockClient := mocks.NewClient(t)
		boolCmd := redis.NewBoolResult(true, nil) // SETNX returns true (key did not exist)
		mockClient.On("SetNX", ctx, expectedKey, "t", DeduplicationTTL).Return(boolCmd)

		s := &Store{client: mockClient}
		isDup, err := s.IsDuplicate(ctx, event)

		assert.NoError(t, err)
		assert.False(t, isDup)
	})

	t.Run("IsDuplicate", func(t *testing.T) {
		mockClient := mocks.NewClient(t)
		boolCmd := redis.NewBoolResult(false, nil) // SETNX returns false (key already existed)
		mockClient.On("SetNX", ctx, expectedKey, "t", DeduplicationTTL).Return(boolCmd)

		s := &Store{client: mockClient}
		isDup, err := s.IsDuplicate(ctx, event)

		assert.NoError(t, err)
		assert.True(t, isDup)
	})

	t.Run("RedisError", func(t *testing.T) {
		mockClient := mocks.NewClient(t)
		boolCmd := redis.NewBoolResult(false, errors.New("connection refused"))
		mockClient.On("SetNX", ctx, expectedKey, "t", DeduplicationTTL).Return(boolCmd)

		s := &Store{client: mockClient}
		isDup, err := s.IsDuplicate(ctx, event)

		assert.Error(t, err)
		assert.False(t, isDup)
	})
}

func TestStore_Close(t *testing.T) {
	mockClient := mocks.NewClient(t)
	mockClient.On("Close").Return(nil)

	s := &Store{client: mockClient}
	err := s.Close()

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}
