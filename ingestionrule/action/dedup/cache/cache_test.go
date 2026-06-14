package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/ingestionrule/action/dedup/cache/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestStore_AreDuplicates(t *testing.T) {
	ctx := context.Background()

	s := &Store{}
	events := []EventMetadata{
		{Publisher: "pub-1", EventGUID: "guid-1"},
		{Publisher: "pub-2", EventGUID: "guid-2"},
	}
	key1 := s.buildDeduplicationKey(events[0])
	key2 := s.buildDeduplicationKey(events[1])

	t.Run("Empty Events Slice", func(t *testing.T) {
		s := &Store{}
		res, err := s.AreDuplicates(ctx, nil)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Three Elements Two Duplicates", func(t *testing.T) {
		s := &Store{}
		batchEvents := []EventMetadata{
			{Publisher: "pub-1", EventGUID: "guid-1"},
			{Publisher: "pub-2", EventGUID: "guid-2"},
			{Publisher: "pub-3", EventGUID: "guid-3"},
		}

		mockClient := mocks.NewClient(t)
		pipe := new(mocks.Pipeliner)

		// cmd1: true (not a duplicate)
		// cmd2: false (is a duplicate)
		// cmd3: false (is a duplicate)
		cmd1 := redis.NewBoolResult(true, nil)
		cmd2 := redis.NewBoolResult(false, nil)
		cmd3 := redis.NewBoolResult(false, nil)

		pipe.On("SetNX", ctx, s.buildDeduplicationKey(batchEvents[0]), "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd1)
		pipe.On("SetNX", ctx, s.buildDeduplicationKey(batchEvents[1]), "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd2)
		pipe.On("SetNX", ctx, s.buildDeduplicationKey(batchEvents[2]), "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd3)
		pipe.On("Exec", ctx).Return([]redis.Cmder{cmd1, cmd2, cmd3}, nil)

		mockClient.On("Pipeline").Return(pipe)

		s.client = mockClient
		res, err := s.AreDuplicates(ctx, batchEvents)

		assert.NoError(t, err)
		// Evaluates to: [Not Dup, Is Dup, Is Dup]
		assert.Equal(t, []bool{false, true, true}, res)

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

		pipe.On("SetNX", ctx, key1, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd2)
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

		pipe.On("SetNX", ctx, key1, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd2)

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

		pipe.On("SetNX", ctx, key1, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd1)
		pipe.On("SetNX", ctx, key2, "t", config.RedisCfg.CacheDuration.Dedup).Return(cmd2)
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
