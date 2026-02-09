package services

import (
	"context"
	"net/http"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/services/mqtt"

	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/services/grpc"
	"github.com/goto/raccoon/services/pprof"
	"github.com/goto/raccoon/services/rest"
)

type bootstrapper interface {
	// Init initialize each HTTP based server. Return error if initialization failed. Put the Serve() function as return mostly suffice for Init process.
	Init(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Name() string
	HealthCheck() error
}

type Services struct {
	B []bootstrapper
}

func (s *Services) Start(ctx context.Context, cancel context.CancelFunc) {
	logger.Info("starting servers")
	for _, init := range s.B {
		i := init
		go func() {
			logger.Infof("%s Server --> startServers", i.Name())
			err := i.Init(ctx)
			if err != nil && err != http.ErrServerClosed {
				cancel()
			}
		}()
	}
}

func (s *Services) Shutdown(ctx context.Context) {
	for _, b := range s.B {
		logger.Infof("%s Server --> shutting down", b.Name())
		b.Shutdown(ctx)
	}
}

func Create(b chan collection.CollectRequest, ctx context.Context) Services {
	c := collection.NewChannelCollector(b)
	services := []bootstrapper{
		grpc.NewGRPCService(c),
		pprof.NewPprofService(),
		rest.NewRestService(c, ctx),
	}

	if config.ServerMQTT.Enable {
		logger.Info("MQTT Service is enabled via config, initializing...")
		services = append(services, mqtt.NewMQTTService(c, ctx))
	} else {
		logger.Info("MQTT Service is disabled via config")
	}

	return Services{
		B: services,
	}
}
