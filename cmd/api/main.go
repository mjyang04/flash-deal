// Package main is the HTTP API entrypoint for flash-deal.
//
// Run:
//
//	make up && make migrate-all && make kafka-topic && go run ./cmd/api
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	_ "net/http/pprof" // /debug/pprof/* on :6060
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/config"
	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/handler"
	fdkafka "github.com/mjyang04/flash-deal/internal/infra/kafka"
	"github.com/mjyang04/flash-deal/internal/infra/logger"
	"github.com/mjyang04/flash-deal/internal/infra/metrics"
	fdmysql "github.com/mjyang04/flash-deal/internal/infra/mysql"
	fdotel "github.com/mjyang04/flash-deal/internal/infra/otel"
	fdredis "github.com/mjyang04/flash-deal/internal/infra/redis"
	"github.com/mjyang04/flash-deal/internal/middleware"
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
		log.Println("mutex + block profiling enabled (FD_PROFILE=1)")
	}
	go func() {
		log.Println("pprof on :6060 (/debug/pprof/*)")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Printf("pprof: %v", err)
		}
	}()

	rootCtx := context.Background()
	if cfg.Switches.Tracing && cfg.Otel.Enabled {
		shutdown, err := fdotel.Init(rootCtx, cfg.Otel.OTLPEndpoint, cfg.Otel.ServiceName)
		if err != nil {
			log.Printf("otel init: %v (continuing without tracing)", err)
		} else {
			defer shutdown(context.Background())
			log.Println("otel: tracing enabled")
		}
	}

	// activity DB (single) + sharded DBs (4)
	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql open: %v", err)
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
				log.Fatalf("open shard %s: %v", dsn, err)
			}
			defer s.Close()
			shards = append(shards, s)
		}
		log.Printf("order: sharded × %d", len(shards))
	}

	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	ar := repo.NewActivityRepo(db)
	var sr repo.StockRepo
	if cfg.Switches.LuaStock {
		sr = repo.NewStockRedisRepo(rdb)
		log.Println("stock: Redis Lua")
	} else {
		sr = repo.NewStockRepo(db)
	}

	var or repo.OrderRepo
	if cfg.Switches.ShardedOrder && len(shards) > 0 {
		or = repo.NewShardedOrderRepo(shards)
	} else {
		or = repo.NewOrderRepo(db)
	}

	queue := service.NewQueue(rdb)

	var oc service.OrderCreator
	if cfg.Switches.KafkaOrder {
		producer, err := fdkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ProduceTimeout)
		if err != nil {
			log.Fatalf("kafka producer: %v", err)
		}
		defer producer.Close()
		oc = service.NewKafkaOrderCreator(producer, cfg.Kafka.OrderTopic)
		log.Printf("order: Kafka producer → %s", cfg.Kafka.OrderTopic)
	} else {
		oc = or
	}

	var idCounter int64
	nextID := func() int64 { return time.Now().UnixNano() + atomic.AddInt64(&idCounter, 1) }

	seckillSvc := service.New(ar, sr, oc, time.Now, nextID).WithQueue(queue)
	adminSvc := service.NewAdmin(ar, rdb)

	r := gin.New()
	r.Use(middleware.RequestID(), middleware.Recovery())
	if cfg.Switches.Metrics {
		r.Use(middleware.Metrics())
	}
	if cfg.Switches.RateLimit {
		r.Use(middleware.RateLimit(rdb, cfg.RateLimit.PerUserPerMinute, cfg.RateLimit.GlobalQPS, cfg.RateLimit.GlobalBurst))
	}

	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/metrics", gin.WrapH(metrics.Handler()))

	seckill := r.Group("/v1")
	if cfg.Switches.Idempotency {
		seckill.Use(middleware.Idempotency(rdb))
	}
	seckill.POST("/seckill", handler.Seckill(serviceHandlerAdapter{inner: seckillSvc}))

	r.GET("/v1/order/by-token/:queue_token", handler.OrderByToken(queue))
	r.POST("/admin/activities", handler.AdminCreateActivity(adminSvc))
	r.POST("/admin/activities/:id/warm", handler.AdminWarmActivity(adminSvc))

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           r,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderWait,
	}
	go func() {
		log.Printf("flash-deal api on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

type serviceHandlerAdapter struct {
	inner *service.SeckillService
}

func (a serviceHandlerAdapter) Seckill(ctx context.Context, req domain.SeckillRequest) (handler.SeckillOutput, error) {
	out, err := a.inner.Seckill(ctx, req)
	return handler.SeckillOutput{
		Outcome:    out.Outcome,
		QueueToken: out.QueueToken,
		Remaining:  out.Remaining,
	}, err
}
