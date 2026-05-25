package cache

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goto/raccoon/config"
)

// NewRedisCache initializes the explicit structural configuration setups for Standalone or Sentinel environments.
func NewRedisCache(ctx context.Context, metricPushInterval time.Duration) *redis.Client {
	if config.RedisCfg.Type == config.RedisSentinel {
		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:      config.RedisCfg.SentinelMasterName,
			SentinelAddrs:   strings.Split(config.RedisCfg.Address, ","),
			MaxRetries:      config.RedisCfg.RetryProperties.MaxRetries,
			MinRetryBackoff: config.RedisCfg.RetryProperties.MinRetryBackOff,
			MaxRetryBackoff: config.RedisCfg.RetryProperties.MaxRetryBackOff,
			PoolSize:        config.RedisCfg.PoolSize,
			Username:        config.RedisCfg.Username,
			Password:        config.RedisCfg.Password,
		})

		go initRedisMetricPublisher(ctx, client, metricPushInterval)

		return client
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:            config.RedisCfg.Address,
		Username:        config.RedisCfg.Username,
		Password:        config.RedisCfg.Password,
		MaxRetries:      config.RedisCfg.RetryProperties.MaxRetries,
		MinRetryBackoff: config.RedisCfg.RetryProperties.MinRetryBackOff,
		MaxRetryBackoff: config.RedisCfg.RetryProperties.MaxRetryBackOff,
		PoolSize:        config.RedisCfg.PoolSize,
	})

	go initRedisMetricPublisher(ctx, redisClient, metricPushInterval)

	return redisClient
}
