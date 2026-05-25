// Package redis wraps go-redis v9 with our config types.
package redis

import (
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyangnb/flash-deal/internal/config"
)

// New returns a configured *goredis.Client. Caller closes.
func New(cfg config.RedisConfig) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
}
