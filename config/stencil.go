package config

import (
	"time"

	"github.com/spf13/viper"
)

// StencilCfg holds runtime configuration for the stencil clients.
var StencilCfg stencilConfig

type stencilConfig struct {
	// URL is the URL of the stencil server used for getting schema of the kafka messages.
	URL string
	// AutoRefresh is a boolean indicating whether to auto-refresh the stencil client.
	AutoRefresh bool
	// RefreshInterval is the interval at which to refresh the stencil client.
	RefreshInterval time.Duration
	// HTTPTimeout sets the HTTP request timeout duration for the stencil client.
	HTTPTimeout time.Duration
	// MaxRetry determines how many times to retry connecting on failure.
	MaxRetry int
	// MaxJitterInterval is the maximum interval base for retry backoff.
	MaxJitterInterval time.Duration
	// ExponentFactor is the factor by which the backoff duration is multiplied.
	ExponentFactor float64
}

func stencilConfigLoader() {
	viper.SetDefault("STENCIL_URL", "")
	viper.SetDefault("STENCIL_AUTO_REFRESH", "true")
	viper.SetDefault("STENCIL_REFRESH_INTERVAL", "12h")
	viper.SetDefault("STENCIL_HTTP_TIMEOUT", "15s")
	viper.SetDefault("STENCIL_MAX_RETRY", 3)
	viper.SetDefault("STENCIL_MIN_JITTER_INTERVAL", "1s")
	viper.SetDefault("STENCIL_EXPONENT_FACTOR", 1.5)

	StencilCfg = stencilConfig{
		URL:               viper.GetString("STENCIL_URL"),
		AutoRefresh:       viper.GetBool("STENCIL_AUTO_REFRESH"),
		RefreshInterval:   viper.GetDuration("STENCIL_REFRESH_INTERVAL"),
		HTTPTimeout:       viper.GetDuration("STENCIL_HTTP_TIMEOUT"),
		MaxRetry:          viper.GetInt("STENCIL_MAX_RETRY"),
		MaxJitterInterval: viper.GetDuration("STENCIL_MAX_JITTER_INTERVAL"),
		ExponentFactor:    viper.GetFloat64("STENCIL_EXPONENT_FACTOR"),
	}
}
