//go:build integration

package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mjyangnb/flash-deal/internal/config"
	fdredis "github.com/mjyangnb/flash-deal/internal/infra/redis"
)

func TestNew_Ping(t *testing.T) {
	addr := os.Getenv("FD_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}
	cfg := config.RedisConfig{Addr: addr, DB: 0, PoolSize: 10}
	cli := fdredis.New(cfg)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
