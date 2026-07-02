package config

import (
	"time"

	"github.com/spf13/viper"
)

// MslCfg holds runtime configuration for the event registration cache feature.
var MslCfg mslConfig

// mslConfig
type mslConfig struct {
	// HTTPHost is the base URL for the MSL API.
	HTTPHost string
	// SyncInterval is the interval at which the event cache is updated.
	SyncInterval time.Duration
	// HTTPRequestTimeout is the timeout for HTTP requests to the MSL API.
	HTTPRequestTimeout time.Duration
	// HTTPMaxRetry is the maximum number of retries for every HTTP request.
	HTTPMaxRetry int
	// HTTPRetryBackoff is the backoff duration between every HTTP request retries.
	HTTPRetryBackoff time.Duration
}

func mslConfigLoader() {
	viper.SetDefault("MSL_HTTP_HOST", "")
	viper.SetDefault("MSL_SYNC_INTERVAL", "30m")
	viper.SetDefault("MSL_HTTP_REQUEST_TIMEOUT", "2m")
	viper.SetDefault("MSL_HTTP_MAX_RETRY", 3)
	viper.SetDefault("MSL_HTTP_RETRY_BACKOFF", "2s")

	MslCfg = mslConfig{
		HTTPHost:           viper.GetString("MSL_HTTP_HOST"),
		SyncInterval:       viper.GetDuration("MSL_SYNC_INTERVAL"),
		HTTPRequestTimeout: viper.GetDuration("MSL_HTTP_REQUEST_TIMEOUT"),
		HTTPMaxRetry:       viper.GetInt("MSL_HTTP_MAX_RETRY"),
		HTTPRetryBackoff:   viper.GetDuration("MSL_HTTP_RETRY_BACKOFF"),
	}
}
