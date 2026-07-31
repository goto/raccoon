package rest

import (
	"context"
	"fmt"
	"github.com/goto/raccoon/constant"
	"github.com/goto/raccoon/health"
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
}

func NewRestService(c collection.Collector, ctx context.Context) *Service {
	pingChannel := make(chan connection.Conn, config.ServerWs.ServerMaxConn)
	wh := websocket.NewHandler(pingChannel, c)
	go websocket.Pinger(ctx, pingChannel, config.ServerWs.PingerSize, config.ServerWs.PingInterval, config.ServerWs.WriteWaitInterval)

	go reportConnectionMetrics(*wh.Table())

	go websocket.AckHandler(websocket.AckChan)

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
	}
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	results := health.CheckAll()
	allHealthy := true
	for _, status := range results {
		if status != constant.HealthStatusHealthy {
			allHealthy = false
			break
		}
	}
	statusCode := http.StatusOK
	if !allHealthy {
		statusCode = http.StatusServiceUnavailable
		w.WriteHeader(statusCode)
	} else {
		w.WriteHeader(statusCode)
		w.Write([]byte("pong"))
	}
}

func reportConnectionMetrics(conn connection.Table) {
	t := time.Tick(config.MetricStatsd.FlushPeriodMs)
	for {
		<-t
		conn.RangeConnectionPerGroup(func(k string, v int) {
			metrics.Gauge("connections_count_current", v, fmt.Sprintf("conn_group=%s", k))
		})
	}
}

func (s *Service) Init(context.Context) error {
	return s.s.ListenAndServe()
}

func (*Service) Name() string {
	return "REST"
}

func (s *Service) Shutdown(ctx context.Context) error {
	return s.s.Shutdown(ctx)
}

// HealthCheck check for rest
func (s *Service) HealthCheck() error {
	return nil
}
