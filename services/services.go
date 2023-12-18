package services

import (
	"context"
	"github.com/goto/raccoon/config"
	"net/http"

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
}

type Services struct {
	b []bootstrapper
}

func (s *Services) Start(ctx context.Context, cancel context.CancelFunc) {
	logger.Info("starting servers")
	for _, init := range s.b {
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
	for _, b := range s.b {
		logger.Infof("%s Server --> shutting down", b.Name())
		b.Shutdown(ctx)
	}
}

func Create(b chan collection.CollectRequest) Services {
	c := collection.NewChannelCollector(b)
	if config.ServerGRPC.TLSEnabled {
		return Services{
			b: []bootstrapper{
				//running non TLS service to do health check on the probe
				grpc.NewGRPCService(c),
				grpc.NewGRPCServiceWithTLS(c),
				pprof.NewPprofService(),
				rest.NewRestService(c),
			},
		}
	}
	return Services{
		b: []bootstrapper{
			grpc.NewGRPCService(c),
			pprof.NewPprofService(),
			rest.NewRestService(c),
		},
	}
}
