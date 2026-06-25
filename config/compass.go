package config

import (
	"time"

	"github.com/spf13/viper"
)

// CompassCfg holds runtime configuration for the Compass integration.
var CompassCfg compassConfig

type compassConfig struct {
	// HTTPHost is the base URL for the Compass API.
	HTTPHost string
	// AuthEmail is the email address used for authentication with the Compass API.
	AuthEmail string
	// SyncInterval is the interval at which the schema cache is updated.
	SyncInterval time.Duration
	// HTTPRequestTimeout is the timeout for HTTP requests to the Compass API.
	HTTPRequestTimeout time.Duration
	// ProjectIDLocation is the project location for the Compass API.
	ProjectIDLocation string
	// StartupMaxRetry is the maximum number of retries for the initial schema cache sync.
	StartupMaxRetry int
	// StartupRetryBackoff is the backoff duration between schema cache sync retries.
	StartupRetryBackoff time.Duration
}

func compassConfigLoader() {
	viper.SetDefault("COMPASS_HTTP_HOST", "")
	viper.SetDefault("COMPASS_AUTH_EMAIL", "")
	viper.SetDefault("COMPASS_SYNC_INTERVAL", "1h")
	viper.SetDefault("COMPASS_HTTP_REQUEST_TIMEOUT", "5s")
	viper.SetDefault("COMPASS_PROJECT_ID_LOCATION", "")
	viper.SetDefault("COMPASS_STARTUP_MAX_RETRY", 3)
	viper.SetDefault("COMPASS_STARTUP_RETRY_BACKOFF", "200ms")

	CompassCfg = compassConfig{
		HTTPHost:            viper.GetString("COMPASS_HTTP_HOST"),
		AuthEmail:           viper.GetString("COMPASS_AUTH_EMAIL"),
		SyncInterval:        viper.GetDuration("COMPASS_SYNC_INTERVAL"),
		HTTPRequestTimeout:  viper.GetDuration("COMPASS_HTTP_REQUEST_TIMEOUT"),
		ProjectIDLocation:   viper.GetString("COMPASS_PROJECT_ID_LOCATION"),
		StartupMaxRetry:     viper.GetInt("COMPASS_STARTUP_MAX_RETRY"),
		StartupRetryBackoff: viper.GetDuration("COMPASS_STARTUP_RETRY_BACKOFF"),
	}
}
