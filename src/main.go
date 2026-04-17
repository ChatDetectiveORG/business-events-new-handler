package main

import (
	"github.com/ChatDetectiveORG/business-events-new-handler/src/application"
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/config"
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/metrics"
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/postgresql"
	"context"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// "github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/postgresql"
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/rabbitmq"
	"log"
)

func main() {
	config, _ := config.FetchConfig()

	err := rabbitmq.InitRabbitMQ(config, rabbitmq.RequiredModels)
	if !err.IsNil() {
		log.Fatal(err.JSON())
	}

	err = postgresql.InitPostgresql()
	if !err.IsNil() {
		log.Fatal(err.JSON())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics.Start(ctx, config)

	wg := &sync.WaitGroup{}
	err = application.ListenToRabbitmq(config, ctx, wg)
	if !err.IsNil() {
		log.Fatal(err.JSON())
	}

	log.Println("Service started. Waiting for shutdown signal...")
	<-ctx.Done()
	log.Println("Shutdown signal received. Exiting...")

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		// Successfully waited for WaitGroup
	case <-time.After(30 * time.Second):
		log.Println("Timeout reached while waiting for WaitGroup, exiting forcefully")
	}

	log.Println("Service stopped")
}
