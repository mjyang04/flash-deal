// Package main is the Kafka consumer entrypoint that materializes orders
// after the synchronous /seckill API has reserved stock in Redis.
//
// Run:
//
//	go run ./cmd/consumer
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mjyangnb/flash-deal/internal/config"
	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
	"github.com/mjyangnb/flash-deal/internal/infra/logger"
	fdmysql "github.com/mjyangnb/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyangnb/flash-deal/internal/infra/redis"
	"github.com/mjyangnb/flash-deal/internal/repo"
	"github.com/mjyangnb/flash-deal/internal/service"
)

func main() {
	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if _, err := logger.Init(os.Getenv("FD_LOG_MODE")); err != nil {
		log.Fatalf("logger: %v", err)
	}

	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer db.Close()
	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	orders := repo.NewOrderRepo(db)
	mat := service.NewOrderMaterializer(orders, rdb)

	dlqProd, err := fdkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ProduceTimeout)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlqProd.Close()

	c := fdkafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.OrderTopic, cfg.Kafka.ConsumerGroup, cfg.Kafka.ConsumerBatchWait, 3)
	c.SetHandler(mat.Handle)
	c.SetDLQ(func(ctx context.Context, raw []byte, reason error) {
		log.Printf("DLQ: %v raw=%s", reason, string(raw))
		// Try to recover the queue token from the original message and mark failed.
		var om fdkafka.OrderMessage
		if jerr := json.Unmarshal(raw, &om); jerr == nil && om.QueueToken != "" {
			mat.MarkFailed(ctx, om.QueueToken, reason)
		}
		// Best-effort republish to DLQ topic.
		if om.OrderID != 0 {
			_ = dlqProd.SendOrder(ctx, cfg.Kafka.DLQTopic, om)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("shutdown signal received")
		cancel()
	}()

	log.Printf("consumer reading topic=%s group=%s", cfg.Kafka.OrderTopic, cfg.Kafka.ConsumerGroup)
	if err := c.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}
