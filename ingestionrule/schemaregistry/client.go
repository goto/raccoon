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

const headerKeyAuthorization = "Authorization"

func NewStencilClient() (StencilClient, error) {
	if config.StencilCfg.URL == "" {
		return StencilClient{}, nil
	}

	opts := stencil.Options{
		AutoRefresh:     config.StencilCfg.AutoRefresh,
		RefreshInterval: config.StencilCfg.RefreshInterval,
		HTTPOptions: stencil.HTTPOptions{
			Timeout: config.StencilCfg.HTTPTimeout,
		},
	}

	token := config.StencilCfg.BearerToken
	if token != "" {
		opts.HTTPOptions.Headers = map[string]string{
			headerKeyAuthorization: fmt.Sprintf("Bearer %s", token),
		}
	}

	maxRetry := config.StencilCfg.MaxRetry
	if maxRetry <= 0 {
		return StencilClient{}, fmt.Errorf("invalid configuration: max retry must be greater than 0 (got %d)", maxRetry)
	}

	var err error

	for attempt := range maxRetry {
		var stencilClient stencil.Client

		stencilClient, err = stencil.NewClient([]string{config.StencilCfg.URL}, opts)
		if err == nil {
			return StencilClient{
				Client: stencilClient,
			}, nil
		}

		// Don't sleep after the final attempt has failed
		if attempt == maxRetry-1 {
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

	return StencilClient{},
		fmt.Errorf("failed to create stencil client after %d attempts: %w", maxRetry, err)
}
