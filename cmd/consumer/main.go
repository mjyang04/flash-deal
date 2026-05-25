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
	"net/http"
	_ "net/http/pprof" // /debug/pprof/* on :6061
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/config"
	"github.com/mjyang04/flash-deal/internal/infra/breaker"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
	"github.com/mjyang04/flash-deal/internal/infra/logger"
	"github.com/mjyang04/flash-deal/internal/infra/metrics"
	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	fdotel "github.com/mjyang04/flash-deal/internal/infra/otel"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

func main() {
	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if _, err := logger.Init(os.Getenv("FD_LOG_MODE")); err != nil {
		log.Fatalf("logger: %v", err)
	}

	if os.Getenv("FD_PROFILE") == "1" {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
	}
	if os.Getenv("FD_PROFILE") == "1" || os.Getenv("FD_PPROF") == "1" {
		go func() {
			if err := http.ListenAndServe("127.0.0.1:6061", nil); err != nil {
				log.Printf("pprof: %v", err)
			}
		}()
	}

	rootCtx := context.Background()
	if cfg.Switches.Tracing && cfg.Otel.Enabled {
		shutdown, err := fdotel.Init(rootCtx, cfg.Otel.OTLPEndpoint, "flash-deal-consumer")
		if err != nil {
			log.Printf("otel: %v", err)
		} else {
			defer shutdown(context.Background())
		}
	}

	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer db.Close()

	var shards []*sqlx.DB
	if cfg.Switches.ShardedOrder {
		for _, dsn := range cfg.Shards.DSNs {
			s, err := fdmysql.Open(config.MySQLConfig{
				DSN:             dsn,
				MaxOpenConns:    cfg.Shards.MaxOpenConns,
				MaxIdleConns:    cfg.Shards.MaxIdleConns,
				ConnMaxLifetime: cfg.MySQL.ConnMaxLifetime,
			})
			if err != nil {
				log.Fatalf("shard %s: %v", dsn, err)
			}
			defer s.Close()
			shards = append(shards, s)
		}
	}

	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	var orders repo.OrderRepo
	if cfg.Switches.ShardedOrder && len(shards) > 0 {
		orders = repo.NewShardedOrderRepo(shards)
	} else {
		orders = repo.NewOrderRepo(db)
	}

	mat := service.NewOrderMaterializer(orders, rdb)
	if cfg.Switches.CircuitBreaker {
		mat = mat.WithBreaker(breaker.New(breaker.Config{
			Name:         cfg.Breaker.Name,
			MaxRequests:  cfg.Breaker.MaxRequests,
			Interval:     cfg.Breaker.Interval,
			Timeout:      cfg.Breaker.Timeout,
			FailureRatio: cfg.Breaker.FailureRatio,
		}))
		log.Println("circuit breaker: on")
	}

	dlqProd, err := fdkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ProduceTimeout)
	if err != nil {
		log.Fatalf("dlq producer: %v", err)
	}
	defer dlqProd.Close()

	c := fdkafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.OrderTopic, cfg.Kafka.ConsumerGroup, cfg.Kafka.ConsumerBatchWait, 3)
	c.SetHandler(mat.Handle)
	c.SetDLQ(func(ctx context.Context, raw []byte, reason error) {
		log.Printf("DLQ: %v", reason)
		metrics.DLQTotal.WithLabelValues(reason.Error()).Inc()
		var om fdkafka.OrderMessage
		if jerr := json.Unmarshal(raw, &om); jerr == nil && om.QueueToken != "" {
			mat.MarkFailed(ctx, om.QueueToken, reason)
		}
		if om.OrderID != 0 {
			_ = dlqProd.SendOrder(ctx, cfg.Kafka.DLQTopic, om)
		}
	})

	// metrics HTTP server
	if cfg.Switches.Metrics {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", metrics.Handler())
			log.Println("consumer metrics on :8090")
			if err := http.ListenAndServe(":8090", mux); err != nil {
				log.Printf("metrics: %v", err)
			}
		}()
	}

	ctx, cancel := context.WithCancel(rootCtx)
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
