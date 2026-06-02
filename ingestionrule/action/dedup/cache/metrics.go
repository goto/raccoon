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
func initRedisMetricPublisher(ctx context.Context, redisClient redis.UniversalClient, metricPushInterval time.Duration,
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
func reportStats(redisClient redis.UniversalClient) {
	poolStats := redisClient.PoolStats()
	metrics.Gauge(redisPoolHitsMetric, poolStats.Hits, "")
	metrics.Gauge(redisPoolMissesMetric, poolStats.Misses, "")
	metrics.Gauge(redisPoolTimeoutsMetric, poolStats.Timeouts, "")
	metrics.Gauge(redisPoolTotalConnectionMetric, poolStats.TotalConns, "")
	metrics.Gauge(redisPoolIdleConnectionMetric, poolStats.IdleConns, "")
	metrics.Gauge(redisPoolStaleConnectionMetric, poolStats.StaleConns, "")
}
