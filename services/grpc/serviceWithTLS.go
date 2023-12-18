package grpc

import (
	pbgrpc "buf.build/gen/go/gotocompany/proton/grpc/go/gotocompany/raccoon/v1beta1/raccoonv1beta1grpc"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
)

type ServiceWithTLS struct {
	Collector collection.Collector
	s         *grpc.Server
}

func NewGRPCServiceWithTLS(c collection.Collector) *Service {
	server := newGRPCServerWithTLS()
	pbgrpc.RegisterEventServiceServer(server, &Handler{C: c})
	return &Service{
		s:         server,
		Collector: c,
	}
}

func (s *ServiceWithTLS) Init(context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", config.ServerGRPC.TLSPort))
	if err != nil {
		return err
	}
	return s.s.Serve(lis)
}

func (*ServiceWithTLS) Name() string {
	return "GRPC WITH TLS"
}

func (s *ServiceWithTLS) Shutdown(context.Context) error {
	s.s.GracefulStop()
	return nil
}

func newGRPCServerWithTLS() *grpc.Server {
	return grpc.NewServer(grpc.Creds(loadTLSCredentials()))
}

func loadTLSCredentials() credentials.TransportCredentials {
	serverCert, err := tls.LoadX509KeyPair(config.ServerGRPC.TLSCertPath, config.ServerGRPC.TLSPublicKey)
	if err != nil {
		panic("failed to load TLS credentials to start grpc server with TLS")
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert,
	}

	return credentials.NewTLS(config)
}
