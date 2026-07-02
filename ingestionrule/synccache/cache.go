package synccache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/goto/raccoon/logger"
)

type Cache[T any] struct {
	value        atomic.Value
	ctx          context.Context
	cancel       context.CancelFunc
	fetchFn      func(ctx context.Context) (T, error)
	syncInterval time.Duration
	name         string
}

func NewCache[T any](
	ctx context.Context,
	name string,
	fetchFn func(ctx context.Context) (T, error),
	syncInterval time.Duration,
	initialValue T,
) *Cache[T] {
	cCtx, cancel := context.WithCancel(ctx)
	c := &Cache[T]{
		ctx:          cCtx,
		cancel:       cancel,
		fetchFn:      fetchFn,
		syncInterval: syncInterval,
		name:         name,
	}
	
	c.value.Store(initialValue)
	return c
}

func (c *Cache[T]) Get() T {
	val := c.value.Load()
	return val.(T)
}

func (c *Cache[T]) Start() {
	if c == nil {
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

func (c *Cache[T]) Close() {
	if c == nil {
		return
	}

	c.cancel()
}

func (c *Cache[T]) Sync() error {
	newVal, err := c.fetchFn(c.ctx)
	if err != nil {
		return err
	}

	c.value.Store(newVal)
	return nil
}

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
