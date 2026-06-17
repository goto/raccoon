package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/xxh3"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
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
	Publisher string
	EventGUID string
	EventName string
	Product   string
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
		cmds = append(cmds, pipe.SetNX(ctx, key, "t", config.RedisCfg.CacheDuration.Dedup))
	}

	execCtx := ctx

	// Conditionally wrap the context with a timeout if the flag is enabled
	if config.RedisCfg.ContextTimeoutEnabled {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, config.RedisCfg.ContextTimeout)
		defer cancel()
	}

	// Execute all queued commands in ONE network round-trip
	_, err := pipe.Exec(execCtx)
	if err != nil && err != redis.Nil {
		logger.Errorf("failed to execute Redis pipeline for deduplication: %v", err)
		metrics.Increment(metricNameRedisError, "command=pipeline_setnx")
		return nil, err
	}

	results := make([]bool, len(events))
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			logger.Errorf("failed to execute Redis command for deduplication: %v", err)
			metrics.Increment(metricNameRedisError, "command=setnx")
		}

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

// buildDeduplicationKey constructs a deterministic, fixed-length unique identifier
// for an event payload to be stored in Redis.
//
// Algorithm & Performance:
// It utilizes the XXH3 (Extreme Hash) 128-bit non-cryptographic hashing algorithm.
// XXH3 operates near RAM speed limits, outperforming cryptographic hashes like SHA-256
// while maintaining an exceptionally low collision probability across billions of keys.
//
// Key Format:
// The generated string is always a contiguous 32-character lowercase hexadecimal string
// representing the complete 128-bit signature ($16 \text{ bytes} \times 2 \text{ hex characters/byte}$):
//   - First 16 characters: High 64 bits (`hash.Hi`) padded with leading zeros if necessary.
//   - Last 16 characters: Low 64 bits (`hash.Lo`) padded with leading zeros if necessary.
func (r *Store) buildDeduplicationKey(event EventMetadata) string {
	d := xxh3.New()

	const keySeparator = ":"

	// key: <publisher>:<event_guid>
	_, _ = d.WriteString(event.Publisher)
	_, _ = d.WriteString(keySeparator)
	_, _ = d.WriteString(event.EventGUID)

	// Sum128 returns an xxh3.Uint128 struct containing Hi and Lo uint64 values
	hash := d.Sum128()

	// Format the 128-bit hash as a contiguous 32-character hexadecimal string
	return fmt.Sprintf("%016x%016x", hash.Hi, hash.Lo)
}
