package cache

import (
	"context"
	"strings"
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
//
//go:generate  mockery --name=Client --with-expecter --output=./mocks
type Client interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// Store manages the wrapper context around the active backend storage engine.
type Store struct {
	client Client
	ctx    context.Context
}

// EventMetadata holds the unique contextual identity traits of an incoming event.
type EventMetadata struct {
	EventGUID string
	SessionID string
	UserID    string
}

// NewStore instantiates the unified storage framework wrapper.
func NewStore(ctx context.Context, client Client) (*Store, error) {
	return &Store{
		client: client,
		ctx:    ctx,
	}, nil
}

// IsDuplicate checks if an event has already been ingested within the T-minute sliding window.
// It utilizes an atomic SETNX command to guarantee thread-safe verification in a single network round-trip.
func (r *Store) IsDuplicate(ctx context.Context, event EventMetadata) (bool, error) {
	key := r.buildDeduplicationKey(event)

	ok, err := r.client.SetNX(ctx, key, "t", DeduplicationTTL).Result()
	if err != nil {
		logger.Errorf("failed to execute %q Redis command: %v", SETNX, err)
		metrics.Increment(metricNameRedisError, "command=setnx")
		return false, err
	}

	return !ok, nil
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
	var sb strings.Builder
	sb.Grow(len(event.UserID) + 1 + len(event.SessionID) + 1 + len(event.EventGUID))

	sb.WriteString(event.UserID)
	sb.WriteString(KeySeparator)
	sb.WriteString(event.SessionID)
	sb.WriteString(KeySeparator)
	sb.WriteString(event.EventGUID)

	return sb.String()
}
