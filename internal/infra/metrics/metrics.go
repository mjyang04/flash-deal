// Package metrics centralizes Prometheus instruments + the /metrics handler.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"route", "status"})

	SeckillRequest = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_request_total",
	}, []string{"activity_id", "outcome"})

	StockRemain = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "seckill_stock_remain",
	}, []string{"activity_id"})

	LuaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "seckill_lua_duration_seconds",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	})

	MySQLDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mysql_query_duration_seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"shard", "op"})

	KafkaProduceDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_produce_duration_seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	})

	DLQTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_dlq_total",
	}, []string{"reason"})

	RateLimitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "seckill_ratelimit_total",
	}, []string{"scope"})

	IdempotentReplayTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "seckill_idempotent_replay_total",
	})
)

func init() {
	prometheus.MustRegister(
		HTTPDuration, SeckillRequest, StockRemain, LuaDuration,
		MySQLDuration, KafkaProduceDuration, DLQTotal,
		RateLimitTotal, IdempotentReplayTotal,
	)
}

// Handler returns the Prometheus exposition handler.
func Handler() http.Handler { return promhttp.Handler() }
