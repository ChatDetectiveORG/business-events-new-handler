package main

import (
	"app/src/application"
	"app/src/infrastructure/config"
	"app/src/infrastructure/metrics"
	"context"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// "app/src/infrastructure/postgresql"
	"app/src/infrastructure/rabbitmq"
	"log"
)

func main() {
	config, _ := config.FetchConfig()

	err := rabbitmq.InitRabbitMQ(config, rabbitmq.RequiredModels)
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
