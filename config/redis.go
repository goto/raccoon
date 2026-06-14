package config

import (
	"time"

	"github.com/spf13/viper"
)

// RedisCfg holds runtime configuration for the Redis clients.
var RedisCfg redisConfig

// RedisType represents the type of Redis deployment.
type RedisType = string

const (
	RedisTypeSentinel   RedisType = "sentinel"
	RedisTypeStandalone RedisType = "standalone"
	RedisTypeCluster    RedisType = "cluster"
)

type redisConfig struct {
	Type               RedisType
	Address            string
	Username           string
	Password           string
	PoolSize           int
	RetryProperties    retryConfig
	SentinelMasterName string
	CacheDuration      cacheConfig
}

type cacheConfig struct {
	Dedup time.Duration
}

type retryConfig struct {
	MaxRetries      int
	MinRetryBackOff time.Duration
	MaxRetryBackOff time.Duration
}

func redisConfigLoader() {
	viper.SetDefault("REDIS_TYPE", RedisTypeStandalone)
	viper.SetDefault("REDIS_ADDRESS", "localhost:6379")
	viper.SetDefault("REDIS_USERNAME", "")
	viper.SetDefault("REDIS_PASSWORD", "")
	viper.SetDefault("REDIS_POOL_SIZE", 1000)
	viper.SetDefault("REDIS_RETRY_MAX", 3)
	viper.SetDefault("REDIS_RETRY_MIN_BACKOFF", "1s")
	viper.SetDefault("REDIS_RETRY_MAX_BACKOFF", "30s")
	viper.SetDefault("REDIS_SENTINEL_MASTER_NAME", "")
	viper.SetDefault("REDIS_CACHE_DURATION_DEDUP", "30m")

	RedisCfg = redisConfig{
		Type:     viper.GetString("REDIS_TYPE"),
		Address:  viper.GetString("REDIS_ADDRESS"),
		Username: viper.GetString("REDIS_USERNAME"),
		Password: viper.GetString("REDIS_PASSWORD"),
		PoolSize: viper.GetInt("REDIS_POOL_SIZE"),
		RetryProperties: retryConfig{
			MaxRetries:      viper.GetInt("REDIS_RETRY_MAX"),
			MinRetryBackOff: viper.GetDuration("REDIS_RETRY_MIN_BACKOFF"),
			MaxRetryBackOff: viper.GetDuration("REDIS_RETRY_MAX_BACKOFF"),
		},
		SentinelMasterName: viper.GetString("REDIS_SENTINEL_MASTER_NAME"),
		CacheDuration: cacheConfig{
			Dedup: viper.GetDuration("REDIS_CACHE_DURATION_DEDUP"),
		},
	}
}
