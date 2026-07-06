package synccache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/goto/raccoon/logger"
)

type Cache[T any] struct {
	// value is the cached value.
	value        atomic.Value
	// ctx is the context for the cache.
	ctx          context.Context
	// cancel is the cancel function for the context.
	cancel       context.CancelFunc
	// fetchFn is a function that fetches the latest value from the source.
	fetchFn      func(ctx context.Context) (T, error)
	// syncInterval is the interval between sync operations.
	syncInterval time.Duration
	// name is the name of the cache.
	name         string
	// asyncStart if true, Start() will spawn a new goroutine to sync the cache
	// if false, Start() will sync the cache synchronously and return only after sync is completed
	asyncStart   bool
	// hasSynced tracks whether the cache has been successfully synced at least once
	// if false, it means the cache is empty or failed to sync initially
	hasSynced    atomic.Bool
}

func NewCache[T any](
	ctx context.Context,
	name string,
	fetchFn func(ctx context.Context) (T, error),
	syncInterval time.Duration,
	initialValue T,
	asyncStart bool,
) *Cache[T] {
	cCtx, cancel := context.WithCancel(ctx)
	c := &Cache[T]{
		ctx:          cCtx,
		cancel:       cancel,
		fetchFn:      fetchFn,
		syncInterval: syncInterval,
		name:         name,
		asyncStart:   asyncStart,
	}
	
	c.value.Store(initialValue)
	return c
}

// Get retrieves the cached value.
// If the cache has not been successfully synced at least once, it will return the initial value.
func (c *Cache[T]) Get() T {
	val := c.value.Load()
	return val.(T)
}

// HasSynced checks if the cache has been successfully synced at least once.
func (c *Cache[T]) HasSynced() bool {
	if c == nil {
		return false
	}

	return c.hasSynced.Load()
}

// Start starts the cache's sync worker.
// If asyncStart is true, it will spawn a new goroutine to sync the cache.
// If asyncStart is false, it will sync the cache synchronously and return only after sync is completed.
// If syncInterval is greater than 0, it will periodically sync the cache.
func (c *Cache[T]) Start() {
	if c == nil {
		return
	}

	if c.asyncStart {
		go func() {
			err := c.Sync()
			if err != nil {
				logger.Errorf("failed to fetch %s: %v. Cache will be empty initially.", c.name, err)
			}
			
			if c.syncInterval > 0 {
				c.worker()
			}
		}()

		return
	}

	err := c.Sync()
	if err == nil {
		if c.syncInterval > 0 {
			go c.worker()
		}

		return
	}

	logger.Errorf("failed to fetch %s: %v. Cache will be empty initially.", c.name, err)

	if c.syncInterval > 0 {
		go c.worker()
	}
}

// Close stops the cache's sync worker and frees resources.
func (c *Cache[T]) Close() {
	if c == nil {
		return
	}

	c.cancel()
}

// Sync fetches the latest value from the source and updates the cache.
// It returns an error if the fetch operation fails.
func (c *Cache[T]) Sync() error {
	newVal, err := c.fetchFn(c.ctx)
	if err != nil {
		return err
	}

	c.value.Store(newVal)
	c.hasSynced.Store(true)
	return nil
}

// worker is a background goroutine that periodically syncs the cache.
// It stops when the context is cancelled.
func (c *Cache[T]) worker() {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.Sync(); err != nil {
				logger.Errorf("%q sync failed: %v", c.name, err)
			}
		}
	}
}
