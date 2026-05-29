package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/goto/raccoon/config"
)

// NewRedisCache initializes the explicit structural configuration setups for Standalone or Sentinel environments.
func NewRedisCache(ctx context.Context, metricPushInterval time.Duration) (redis.UniversalClient, error) {
	var client redis.UniversalClient

	switch config.RedisCfg.Type {
	case config.RedisTypeSentinel:
		rawAddrs := strings.Split(config.RedisCfg.Address, ",")
		sentinelAddrs := make([]string, 0, len(rawAddrs))
		for _, addr := range rawAddrs {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				sentinelAddrs = append(sentinelAddrs, trimmed)
			}
		}

		if len(sentinelAddrs) == 0 {
			return nil, errors.New("redis sentinel requires at least one address")
		}

		if strings.TrimSpace(config.RedisCfg.SentinelMasterName) == "" {
			return nil, errors.New("redis sentinel requires a valid SentinelMasterName")
		}

		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:      config.RedisCfg.SentinelMasterName,
			SentinelAddrs:   sentinelAddrs,
			MaxRetries:      config.RedisCfg.RetryProperties.MaxRetries,
			MinRetryBackoff: config.RedisCfg.RetryProperties.MinRetryBackOff,
			MaxRetryBackoff: config.RedisCfg.RetryProperties.MaxRetryBackOff,
			PoolSize:        config.RedisCfg.PoolSize,
			Username:        config.RedisCfg.Username,
			Password:        config.RedisCfg.Password,
		})
	case config.RedisTypeStandalone:
		client = redis.NewClient(&redis.Options{
			Addr:            config.RedisCfg.Address,
			Username:        config.RedisCfg.Username,
			Password:        config.RedisCfg.Password,
			MaxRetries:      config.RedisCfg.RetryProperties.MaxRetries,
			MinRetryBackoff: config.RedisCfg.RetryProperties.MinRetryBackOff,
			MaxRetryBackoff: config.RedisCfg.RetryProperties.MaxRetryBackOff,
			PoolSize:        config.RedisCfg.PoolSize,
		})
	default:
		return nil, fmt.Errorf("unsupported or invalid redis deployment type: %q", config.RedisCfg.Type)
	}

	go initRedisMetricPublisher(ctx, client, metricPushInterval)

	return client, nil
}
