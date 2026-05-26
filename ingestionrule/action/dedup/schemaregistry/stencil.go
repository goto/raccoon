package schemaregistry

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"time"

	stencil "github.com/goto/stencil/clients/go"

	"github.com/goto/raccoon/config"
)

type StencilClient struct {
	Client stencil.Client
}

func NewStencilClient() (StencilClient, error) {
	opts := stencil.Options{
		AutoRefresh:     config.StencilCfg.AutoRefresh,
		RefreshInterval: config.StencilCfg.RefreshInterval,
		HTTPOptions: stencil.HTTPOptions{
			Timeout: config.StencilCfg.HTTPTimeout,
		},
	}

	var err error

	for attempt := range config.StencilCfg.MaxRetry {
		var stencilClient stencil.Client

		stencilClient, err = stencil.NewClient([]string{config.StencilCfg.URL}, opts)
		if err == nil {
			return StencilClient{
				Client: stencilClient,
			}, nil
		}

		// Don't sleep after the final attempt has failed
		if attempt == config.StencilCfg.MaxRetry-1 {
			break
		}

		backoffMultiplier := math.Pow(config.StencilCfg.ExponentFactor, float64(attempt))
		maxBackoff := time.Duration(float64(config.StencilCfg.MaxJitterInterval) * backoffMultiplier)

		// Fallback to maxBackoff if crypto/rand fails
		sleepDuration := maxBackoff

		if maxBackoff > 0 {
			if n, randErr := rand.Int(rand.Reader, big.NewInt(int64(maxBackoff))); randErr == nil {
				sleepDuration = time.Duration(n.Int64())
			}
		}

		time.Sleep(sleepDuration)
	}

	return StencilClient{
			Client: nil,
		},
		fmt.Errorf("failed to create stencil client after %d attempts: %w", config.StencilCfg.MaxRetry, err)
}
