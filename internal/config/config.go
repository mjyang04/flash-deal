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

type Config struct {
	HTTP  HTTPConfig  `mapstructure:"http"`
	MySQL MySQLConfig `mapstructure:"mysql"`
	Redis RedisConfig `mapstructure:"redis"`
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
