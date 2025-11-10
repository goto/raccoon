package app

import (
	"context"
	"fmt"
	"github.com/goto/raccoon/health"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/goto/raccoon/collection"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/publisher"
	"github.com/goto/raccoon/services"
	"github.com/goto/raccoon/worker"
)

// StartServer starts the server
func StartServer(ctx context.Context, cancel context.CancelFunc, shutdown chan bool) {
	bufferChannel := make(chan collection.CollectRequest, config.Worker.ChannelSize)
	httpServices := services.Create(bufferChannel, ctx)
	logger.Info("Start Server -->")
	httpServices.Start(ctx, cancel)
	logger.Info("Start publisher -->")
	kPublisher, err := publisher.NewKafka()
	if err != nil {
		logger.Error("Error creating kafka producer", err)
		logger.Info("Exiting server")
		os.Exit(0)
	}
	registerHealthCheck(httpServices, kPublisher)
	logger.Info("Start worker -->")
	workerPool := worker.CreateWorkerPool(config.Worker.WorkersPoolSize, bufferChannel, config.Worker.DeliveryChannelSize, kPublisher)
	workerPool.StartWorkers()
	go kPublisher.ReportStats()
	go reportProcMetrics()
	// create signal channel at startup
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go shutDownServer(ctx, cancel, httpServices, bufferChannel, workerPool, kPublisher, shutdown, signalChan)
}

func shutDownServer(ctx context.Context, cancel context.CancelFunc, httpServices services.Services, bufferChannel chan collection.CollectRequest,
	workerPool *worker.Pool, kp *publisher.Kafka, shutdown chan bool, signalChan chan os.Signal) {
	for {
		sig := <-signalChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			logger.Info(fmt.Sprintf("[App.Server] Received a signal %s", sig))
			httpServices.Shutdown(ctx)
			logger.Info("Server shutdown all the listeners")
			cancel()
			close(bufferChannel)
			timedOut := workerPool.FlushWithTimeOut(config.Worker.WorkerFlushTimeout)
			if timedOut {
				logger.Info(fmt.Sprintf("WorkerPool flush timedout %t", timedOut))
			} else {
				logger.Info("WorkerPool flushed all events")
			}
			flushInterval := config.PublisherKafka.FlushInterval
			logger.Info("Closing Kafka producer")
			logger.Info(fmt.Sprintf("Wait %d ms for all messages to be delivered", flushInterval))
			eventsInProducer := kp.Close()
			eventCountInChannel := 0
			for i := 0; i < len(bufferChannel); i++ {
				req := <-bufferChannel
				eventCountInChannel += len(req.Events)
			}
			logger.Info(fmt.Sprintf("number of events dropped during the shutdown %d", eventCountInChannel+eventsInProducer))
			metrics.Count("total_data_loss", eventCountInChannel+eventsInProducer, "reason=shutdown")
			logger.Info("Exiting server")
			shutdown <- true
		default:
			logger.Info(fmt.Sprintf("[App.Server] Received a unexpected signal %s", sig))
		}
		return
	}
}

func reportProcMetrics() {
	t := time.Tick(config.MetricStatsd.FlushPeriodMs)
	m := &runtime.MemStats{}
	for {
		<-t
		metrics.Gauge("server_go_routines_count_current", runtime.NumGoroutine(), "")

		runtime.ReadMemStats(m)
		metrics.Gauge("server_mem_heap_alloc_bytes_current", m.HeapAlloc, "")
		metrics.Gauge("server_mem_heap_inuse_bytes_current", m.HeapInuse, "")
		metrics.Gauge("server_mem_heap_objects_total_current", m.HeapObjects, "")
		metrics.Gauge("server_mem_stack_inuse_bytes_current", m.StackInuse, "")
		metrics.Gauge("server_mem_gc_triggered_current", m.LastGC/1000, "")
		metrics.Gauge("server_mem_gc_pauseNs_current", m.PauseNs[(m.NumGC+255)%256]/1000, "")
		metrics.Gauge("server_mem_gc_count_current", m.NumGC, "")
		metrics.Gauge("server_mem_gc_pauseTotalNs_current", m.PauseTotalNs, "")
	}
}

func registerHealthCheck(svcs services.Services, kafka *publisher.Kafka) {
	health.Register("kafka-broker", kafka.HealthCheck)
	for _, svc := range svcs.B {
		if svc.Name() == "MQTT" {
			health.Register("mqtt-broker", svc.HealthCheck)
		}
	}
}
