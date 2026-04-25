// Package main is the Kafka consumer entrypoint that materializes orders
// after the synchronous /seckill API has reserved stock in Redis.
//
// Run:
//
//	go run ./cmd/consumer
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("shutdown signal received")
		cancel()
	}()

	log.Println("consumer skeleton — implement in Week 2 Day 10-12")
	<-ctx.Done()
}
