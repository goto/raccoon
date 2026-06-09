package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/raccoon/ingestionrule/action/dedup/cache/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestStore_AreDuplicates(t *testing.T) {
	ctx := context.Background()

	events := []EventMetadata{
		{UserID: "user1", SessionID: "session1", EventGUID: "guid1"},
		{UserID: "user2", SessionID: "session2", EventGUID: "guid2"},
	}
	key1 := "user1:session1:guid1"
	key2 := "user2:session2:guid2"

	t.Run("Empty Events Slice", func(t *testing.T) {
		s := &Store{}
		res, err := s.AreDuplicates(ctx, nil)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Intra-batch Duplicates", func(t *testing.T) {
		batchEvents := []EventMetadata{
			{UserID: "u1", SessionID: "s1", EventGUID: "g1"},
			{UserID: "u2", SessionID: "s2", EventGUID: "g2"},
			{UserID: "u3", SessionID: "s3", EventGUID: "g3"},
			{UserID: "u3", SessionID: "s3", EventGUID: "g3"}, // Intra-batch duplicate
		}

		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		// cmd1: true (new)
		// cmd2: true (new)
		// cmd3: true (new)
		// cmd4: false (duplicate of g3 because cmd3 just set it)
		cmd1 := redis.NewBoolResult(true, nil)
		cmd2 := redis.NewBoolResult(true, nil)
		cmd3 := redis.NewBoolResult(true, nil)
		cmd4 := redis.NewBoolResult(false, nil)

		pipe.On("SetNX", ctx, "u1:s1:g1", "t", DeduplicationTTL).Return(cmd1)
		pipe.On("SetNX", ctx, "u2:s2:g2", "t", DeduplicationTTL).Return(cmd2)

		// Use .Once() to sequentially mock identical arguments
		pipe.On("SetNX", ctx, "u3:s3:g3", "t", DeduplicationTTL).Return(cmd3).Once()
		pipe.On("SetNX", ctx, "u3:s3:g3", "t", DeduplicationTTL).Return(cmd4).Once()

		pipe.On("Exec", ctx).Return([]redis.Cmder{cmd1, cmd2, cmd3, cmd4}, nil)

		mockClient.On("Pipeline").Return(pipe)

		s := &Store{client: mockClient}
		res, err := s.AreDuplicates(ctx, batchEvents)

		assert.NoError(t, err)

		// Expected Evaluation:
		// g1 -> Not Dup (false)
		// g2 -> Not Dup (false)
		// g3 (first) -> Not Dup (false)
		// g3 (second) -> Is Dup (true)
		assert.Equal(t, []bool{false, false, false, true}, res)

		pipe.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("Guaranteed Order Mapping", func(t *testing.T) {
		// In this batch, none of the events are duplicates of each other.
		// However, we will mock Redis to simulate that events 2 and 4
		// were already processed in a previous batch and are already in the cache.
		batchEvents := []EventMetadata{
			{UserID: "u1", SessionID: "s1", EventGUID: "brand_new_event_A"},
			{UserID: "u2", SessionID: "s2", EventGUID: "old_event_already_in_redis"},
			{UserID: "u3", SessionID: "s3", EventGUID: "brand_new_event_B"},
			{UserID: "u4", SessionID: "s4", EventGUID: "another_old_event"},
		}

		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		// Mock the Redis SetNX responses:
		// true  = Key didn't exist, successfully set (New Event)
		// false = Key already existed (Duplicate Event)
		cmd1 := redis.NewBoolResult(true, nil)  // Success
		cmd2 := redis.NewBoolResult(false, nil) // Failed (Already in cache)
		cmd3 := redis.NewBoolResult(true, nil)  // Success
		cmd4 := redis.NewBoolResult(false, nil) // Failed (Already in cache)

		pipe.On("SetNX", ctx, "u1:s1:brand_new_event_A", "t", DeduplicationTTL).Return(cmd1)
		pipe.On("SetNX", ctx, "u2:s2:old_event_already_in_redis", "t", DeduplicationTTL).Return(cmd2)
		pipe.On("SetNX", ctx, "u3:s3:brand_new_event_B", "t", DeduplicationTTL).Return(cmd3)
		pipe.On("SetNX", ctx, "u4:s4:another_old_event", "t", DeduplicationTTL).Return(cmd4)

		// The mock returns the responses in the exact order they were queued
		pipe.On("Exec", ctx).Return([]redis.Cmder{cmd1, cmd2, cmd3, cmd4}, nil)

		mockClient.On("Pipeline").Return(pipe)

		s := &Store{client: mockClient}
		res, err := s.AreDuplicates(ctx, batchEvents)

		assert.NoError(t, err)

		// The resulting boolean slice MUST perfectly map to the alternating states
		// index 0: true -> !true = false (Not Dup)
		// index 1: false -> !false = true (Is Dup)
		// index 2: true -> !true = false (Not Dup)
		// index 3: false -> !false = true (Is Dup)
		assert.Equal(t, []bool{false, true, false, true}, res)

		pipe.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("Mixed Pipeline Results", func(t *testing.T) {
		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		// cmd1 returns true (key did not exist -> NOT duplicate)
		cmd1 := redis.NewBoolResult(true, nil)
		// cmd2 returns false (key existed -> IS duplicate)
		cmd2 := redis.NewBoolResult(false, nil)

		pipe.On("SetNX", ctx, key1, "t", DeduplicationTTL).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", DeduplicationTTL).Return(cmd2)
		pipe.On("Exec", ctx).Return([]redis.Cmder{cmd1, cmd2}, nil)

		mockClient.On("Pipeline").Return(pipe)

		s := &Store{client: mockClient}
		res, err := s.AreDuplicates(ctx, events)

		assert.NoError(t, err)
		// Expecting false (not duplicate) for event 1, and true (is duplicate) for event 2
		assert.Equal(t, []bool{false, true}, res)

		pipe.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("Redis Exec Pipeline Error", func(t *testing.T) {
		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		cmd1 := redis.NewBoolResult(false, nil)
		cmd2 := redis.NewBoolResult(false, nil)

		pipe.On("SetNX", ctx, key1, "t", DeduplicationTTL).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", DeduplicationTTL).Return(cmd2)

		execErr := errors.New("pipeline execution failed")
		pipe.On("Exec", ctx).Return(nil, execErr)

		mockClient.On("Pipeline").Return(pipe)

		s := &Store{client: mockClient}
		res, err := s.AreDuplicates(ctx, events)

		assert.ErrorIs(t, err, execErr)
		assert.Nil(t, res)

		pipe.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("Redis Nil Error Ignored", func(t *testing.T) {
		// redis.Nil is sometimes returned by pipeline executions when keys aren't found.
		// Our logic explicitly ignores this error (err != redis.Nil).
		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		cmd1 := redis.NewBoolResult(true, nil)
		cmd2 := redis.NewBoolResult(true, nil)

		pipe.On("SetNX", ctx, key1, "t", DeduplicationTTL).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", DeduplicationTTL).Return(cmd2)
		pipe.On("Exec", ctx).Return([]redis.Cmder{cmd1, cmd2}, redis.Nil)

		mockClient.On("Pipeline").Return(pipe)

		s := &Store{client: mockClient}
		res, err := s.AreDuplicates(ctx, events)

		assert.NoError(t, err)
		assert.Equal(t, []bool{false, false}, res)

		pipe.AssertExpectations(t)
		mockClient.AssertExpectations(t)
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

func TestStore_HealthCheck(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks.NewClient(t)
	statusCmd := redis.NewStatusResult("PONG", nil)
	mockClient.On("Ping", ctx).Return(statusCmd)

	s := &Store{client: mockClient, ctx: ctx}
	err := s.HealthCheck()

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}
