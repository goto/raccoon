package rest

import (
	"context"
	"fmt"
	"github.com/goto/raccoon/logger"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/services/rest/websocket"
	"github.com/goto/raccoon/services/rest/websocket/connection"
)

type Service struct {
	Collector collection.Collector
	s         *http.Server
	cancel    context.CancelFunc
}

func NewRestService(c collection.Collector) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	pingChannel := make(chan connection.Conn, config.ServerWs.ServerMaxConn)
	wh := websocket.NewHandler(pingChannel, c)
	go websocket.Pinger(ctx, pingChannel, config.ServerWs.PingerSize, config.ServerWs.PingInterval, config.ServerWs.WriteWaitInterval)

	go reportConnectionMetrics(ctx, *wh.Table())

	go websocket.AckHandler(ctx, websocket.AckChan)

	restHandler := NewHandler(c)
	router := mux.NewRouter()
	router.Path("/ping").HandlerFunc(pingHandler).Methods(http.MethodGet)
	subRouter := router.PathPrefix("/api/v1").Subrouter()
	subRouter.HandleFunc("/events", wh.HandlerWSEvents).Methods(http.MethodGet).Name("events")
	subRouter.HandleFunc("/events", restHandler.RESTAPIHandler).Methods(http.MethodPost).Name("events")

	server := &http.Server{
		Handler: router,
		Addr:    ":" + config.ServerWs.AppPort,
	}
	return &Service{
		s:         server,
		Collector: c,
		cancel:    cancel,
	}
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

func reportConnectionMetrics(ctx context.Context, conn connection.Table) {
	ticker := time.NewTicker(config.MetricStatsd.FlushPeriodMs)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			conn.RangeConnectionPerGroup(func(k string, v int) {
				metrics.Gauge("connections_count_current", v, fmt.Sprintf("conn_group=%s", k))
			})
		case <-ctx.Done():
			// cleanup on shutdown
			logger.Info("[metrics.reportConnectionMetrics] - stopping metrics reporter")
			return
		}
	}
}

func (s *Service) Init(context.Context) error {
	return s.s.ListenAndServe()
}

func (*Service) Name() string {
	return "REST"
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()
	return s.s.Shutdown(ctx)
}
