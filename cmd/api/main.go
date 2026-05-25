// Package main is the HTTP API entrypoint for flash-deal.
//
// Run:
//
//	make up && make migrate && make kafka-topic && go run ./cmd/api
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/mjyangnb/flash-deal/internal/config"
	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/handler"
	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
	"github.com/mjyangnb/flash-deal/internal/infra/logger"
	fdmysql "github.com/mjyangnb/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyangnb/flash-deal/internal/infra/redis"
	"github.com/mjyangnb/flash-deal/internal/middleware"
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
		log.Fatalf("mysql open: %v", err)
	}
	defer db.Close()

	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	ar := repo.NewActivityRepo(db)
	var sr repo.StockRepo
	if cfg.Switches.LuaStock {
		sr = repo.NewStockRedisRepo(rdb)
		log.Println("stock: Redis Lua path")
	} else {
		sr = repo.NewStockRepo(db)
		log.Println("stock: SQL row-lock path (M1 fallback)")
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
		oc = repo.NewOrderRepo(db)
		log.Println("order: synchronous MySQL INSERT (M1 fallback)")
	}

	var idCounter int64
	nextID := func() int64 { return time.Now().UnixNano() + atomic.AddInt64(&idCounter, 1) }

	seckillSvc := service.New(ar, sr, oc, time.Now, nextID).WithQueue(queue)
	adminSvc := service.NewAdmin(ar, rdb)

	r := gin.New()
	r.Use(middleware.RequestID(), middleware.Recovery())

	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/v1/seckill", handler.Seckill(serviceHandlerAdapter{inner: seckillSvc}))
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
