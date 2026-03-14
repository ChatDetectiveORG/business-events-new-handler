package metrics

import (
	"app/src/infrastructure/config"
	"context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registerOnce sync.Once

	queueMessages = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chatdetective_bus_enents_new_queue_updates_in_queue",
			Help: "Number of pending update messages in a shard queue (queried from RabbitMQ via passive declare).",
		},
		[]string{"queue"},
	)
	queueConsumers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "chatdetective_bus_enents_new_queue_consumers",
			Help: "Number of consumers on a shard queue (queried from RabbitMQ via passive declare).",
		},
		[]string{"queue"},
	)
)

func Start(ctx context.Context, cfg *config.Config) {
	registerOnce.Do(func() {
		prometheus.MustRegister(queueMessages, queueConsumers)
	})

	startHTTPServer(ctx)
	go startRabbitMQQueueCollector(ctx, cfg)
}

func metricsAddr() string {
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		return v
	}
	return ":9090"
}

func startHTTPServer(ctx context.Context) {
	addr := metricsAddr()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	go func() {
		log.Printf("metrics server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
}
