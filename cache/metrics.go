package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goto/raccoon/metrics"
)

const (
	redisPoolHitsMetric            = "redis_pool_hits"
	redisPoolMissesMetric          = "redis_pool_misses"
	redisPoolTimeoutsMetric        = "redis_pool_timeouts"
	redisPoolTotalConnectionMetric = "redis_pool_total_conns"
	redisPoolIdleConnectionMetric  = "redis_pool_idle_conns"
	redisPoolStaleConnectionMetric = "redis_pool_stale_conns"
)

// initRedisMetricPublisher initializes a goroutine to report the redis pool stats to statsD at regular intervals.
func initRedisMetricPublisher(ctx context.Context, redisClient *redis.Client, metricPushInterval time.Duration,
) {
	reportStats(redisClient)

	tick := time.NewTicker(metricPushInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			reportStats(redisClient)
		}
	}
}

// reportStats reports the redis pool stats to statsD.
func reportStats(redisClient *redis.Client) {
	poolStats := redisClient.PoolStats()
	metrics.Gauge(redisPoolHitsMetric, uint64(poolStats.Hits), "")
	metrics.Gauge(redisPoolMissesMetric, uint64(poolStats.Misses), "")
	metrics.Gauge(redisPoolTimeoutsMetric, uint64(poolStats.Timeouts), "")
	metrics.Gauge(redisPoolTotalConnectionMetric, uint64(poolStats.TotalConns), "")
	metrics.Gauge(redisPoolIdleConnectionMetric, uint64(poolStats.IdleConns), "")
	metrics.Gauge(redisPoolStaleConnectionMetric, uint64(poolStats.StaleConns), "")
}
