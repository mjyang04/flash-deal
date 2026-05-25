// Package config loads strongly-typed runtime config from env / file via viper.
// All env vars use prefix FD_ and underscore-separated paths, e.g. FD_HTTP_ADDR.
package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type HTTPConfig struct {
	Addr            string        `mapstructure:"addr"`
	ReadHeaderWait  time.Duration `mapstructure:"read_header_wait"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type MySQLConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type KafkaConfig struct {
	Brokers           []string      `mapstructure:"brokers"`
	OrderTopic        string        `mapstructure:"order_topic"`
	DLQTopic          string        `mapstructure:"dlq_topic"`
	ConsumerGroup     string        `mapstructure:"consumer_group"`
	ProduceTimeout    time.Duration `mapstructure:"produce_timeout"`
	ConsumerBatchWait time.Duration `mapstructure:"consumer_batch_wait"`
}

type MySQLShardsConfig struct {
	DSNs         []string `mapstructure:"dsns"`
	MaxOpenConns int      `mapstructure:"max_open_conns"`
	MaxIdleConns int      `mapstructure:"max_idle_conns"`
}

type RateLimitConfig struct {
	PerUserPerMinute int `mapstructure:"per_user_per_minute"`
	GlobalQPS        int `mapstructure:"global_qps"`
	GlobalBurst      int `mapstructure:"global_burst"`
}

type BreakerConfig struct {
	Name         string        `mapstructure:"name"`
	MaxRequests  uint32        `mapstructure:"max_requests"`
	Interval     time.Duration `mapstructure:"interval"`
	Timeout      time.Duration `mapstructure:"timeout"`
	FailureRatio float64       `mapstructure:"failure_ratio"`
}

type OtelConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	ServiceName  string `mapstructure:"service_name"`
}

type Switches struct {
	LuaStock       bool `mapstructure:"lua_stock"`
	KafkaOrder     bool `mapstructure:"kafka_order"`
	ShardedOrder   bool `mapstructure:"sharded_order"`
	RateLimit      bool `mapstructure:"rate_limit"`
	Idempotency    bool `mapstructure:"idempotency"`
	CircuitBreaker bool `mapstructure:"circuit_breaker"`
	Tracing        bool `mapstructure:"tracing"`
	Metrics        bool `mapstructure:"metrics"`
}

type Config struct {
	HTTP      HTTPConfig        `mapstructure:"http"`
	MySQL     MySQLConfig       `mapstructure:"mysql"`
	Shards    MySQLShardsConfig `mapstructure:"shards"`
	Redis     RedisConfig       `mapstructure:"redis"`
	Kafka     KafkaConfig       `mapstructure:"kafka"`
	RateLimit RateLimitConfig   `mapstructure:"rate_limit"`
	Breaker   BreakerConfig     `mapstructure:"breaker"`
	Otel      OtelConfig        `mapstructure:"otel"`
	Switches  Switches          `mapstructure:"switches"`
}

// Load reads config from optional YAML at `path` and overlays env vars.
// path == "" → skip file read.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("FD")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Defaults
	v.SetDefault("http.addr", ":8080")
	v.SetDefault("http.read_header_wait", 5*time.Second)
	v.SetDefault("http.shutdown_timeout", 10*time.Second)

	v.SetDefault("mysql.dsn", "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local")
	v.SetDefault("mysql.max_open_conns", 100)
	v.SetDefault("mysql.max_idle_conns", 50)
	v.SetDefault("mysql.conn_max_lifetime", 30*time.Minute)

	v.SetDefault("redis.addr", "127.0.0.1:6380")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 200)

	v.SetDefault("kafka.brokers", []string{"127.0.0.1:9092"})
	v.SetDefault("kafka.order_topic", "seckill_orders")
	v.SetDefault("kafka.dlq_topic", "seckill_orders_dlq")
	v.SetDefault("kafka.consumer_group", "seckill-consumer")
	v.SetDefault("kafka.produce_timeout", 2*time.Second)
	v.SetDefault("kafka.consumer_batch_wait", 100*time.Millisecond)

	v.SetDefault("shards.dsns", []string{
		"flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_0?parseTime=true&loc=Local",
		"flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_1?parseTime=true&loc=Local",
		"flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_2?parseTime=true&loc=Local",
		"flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_3?parseTime=true&loc=Local",
	})
	v.SetDefault("shards.max_open_conns", 50)
	v.SetDefault("shards.max_idle_conns", 25)

	v.SetDefault("rate_limit.per_user_per_minute", 5)
	v.SetDefault("rate_limit.global_qps", 100000)
	v.SetDefault("rate_limit.global_burst", 1000)

	v.SetDefault("breaker.name", "mysql_orders")
	v.SetDefault("breaker.max_requests", 5)
	v.SetDefault("breaker.interval", 60*time.Second)
	v.SetDefault("breaker.timeout", 10*time.Second)
	v.SetDefault("breaker.failure_ratio", 0.5)

	v.SetDefault("otel.enabled", true)
	v.SetDefault("otel.otlp_endpoint", "127.0.0.1:4318")
	v.SetDefault("otel.service_name", "flash-deal-api")

	v.SetDefault("switches.lua_stock", true)
	v.SetDefault("switches.kafka_order", true)
	v.SetDefault("switches.sharded_order", true)
	v.SetDefault("switches.rate_limit", true)
	v.SetDefault("switches.idempotency", true)
	v.SetDefault("switches.circuit_breaker", true)
	v.SetDefault("switches.tracing", true)
	v.SetDefault("switches.metrics", true)

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
