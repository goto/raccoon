package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

const (
	SETNX            = "SETNX"
	DeduplicationTTL = 30 * time.Minute
	KeySeparator     = ":"
)

const (
	metricNameRedisError = "redis_error"
)

// Client defines the contract for the underlying database infrastructure operations.
type Client interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Pipeline() redis.Pipeliner
	Close() error
}

// Pipeliner defines the contract for the Redis pipeline operations.
type Pipeliner interface {
	redis.Pipeliner
}

// Store manages the wrapper context around the active backend storage engine.
type Store struct {
	client Client
	ctx    context.Context
}

// EventMetadata holds the unique contextual identity traits of an incoming event.
type EventMetadata struct {
	EventGUID string
}

// NewStore instantiates the unified storage framework wrapper.
func NewStore(ctx context.Context, client Client) (*Store, error) {
	return &Store{
		client: client,
		ctx:    ctx,
	}, nil
}

// AreDuplicates evaluates a batch of events in a single Redis Pipeline execution.
// It returns a slice of booleans corresponding to the input array order.
func (r *Store) AreDuplicates(ctx context.Context, events []EventMetadata) ([]bool, error) {
	if len(events) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	var cmds []*redis.BoolCmd

	// Queue all SETNX commands locally
	for _, event := range events {
		key := r.buildDeduplicationKey(event)
		cmds = append(cmds, pipe.SetNX(ctx, key, "t", DeduplicationTTL))
	}

	// Execute all queued commands in ONE network round-trip
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		logger.Errorf("failed to execute Redis pipeline for deduplication: %v", err)
		metrics.Increment(metricNameRedisError, "command=pipeline_setnx")
		return nil, err
	}

	results := make([]bool, len(events))
	for i, cmd := range cmds {
		// cmd.Val() is true if the key was set (meaning it's a NEW event).
		// Therefore, if it was NOT set (!cmd.Val()), it IS a duplicate.
		results[i] = !cmd.Val()
	}

	return results, nil
}

// HealthCheck checks the health of the cache client by pinging it.
func (r *Store) HealthCheck() error {
	return r.client.Ping(r.ctx).Err()
}

// Close invokes the underlying driver connectivity tear-down methods.
func (r *Store) Close() error {
	if err := r.client.Close(); err != nil {
		logger.Errorf("failed to close Redis client: %v", err)
		return err
	}

	return nil
}

// buildDeduplicationKey constructs a deterministic unique identifier for an event payload
// using pre-allocated memory to optimize string concatenation performance.
func (r *Store) buildDeduplicationKey(event EventMetadata) string {
	return event.EventGUID
}
