//go:build integration

package repo_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/repo"
)

func openTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("FD_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6380"
	}
	return goredis.NewClient(&goredis.Options{Addr: addr, DB: 0})
}

func TestStockRedis_NoOversell(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	const aid = int64(8002)
	rdb.Set(ctx, "stock:8002", 100, 0)
	t.Cleanup(func() {
		rdb.Del(ctx, "stock:8002")
		for i := 1; i <= 1000; i++ {
			rdb.Del(ctx, "user_buy:8002:"+itoa(i))
		}
	})

	sr := repo.NewStockRedisRepo(rdb)
	var succ, sold int32
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.DeductForUser(ctx, aid, int64(i+1), 1, 1)
			if err == nil {
				atomic.AddInt32(&succ, 1)
			}
			if err == repo.ErrStockNotEnough {
				atomic.AddInt32(&sold, 1)
			}
		}()
	}
	wg.Wait()
	if succ != 100 || sold != 900 {
		t.Errorf("succ=%d sold=%d (want 100/900)", succ, sold)
	}
	if v, _ := rdb.Get(ctx, "stock:8002").Int(); v != 0 {
		t.Errorf("final stock = %d, want 0", v)
	}
}

func TestStockRedis_PerUserLimit(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	ctx := context.Background()
	rdb.Set(ctx, "stock:8003", 10, 0)
	t.Cleanup(func() {
		rdb.Del(ctx, "stock:8003", "user_buy:8003:7")
	})

	sr := repo.NewStockRedisRepo(rdb)
	if _, err := sr.DeductForUser(ctx, 8003, 7, 1, 1); err != nil {
		t.Fatal(err)
	}
	_, err := sr.DeductForUser(ctx, 8003, 7, 1, 1)
	if err != repo.ErrUserLimitExceeded {
		t.Errorf("expected user-limit, got %v", err)
	}
}

func TestStockRedis_NotWarmed(t *testing.T) {
	rdb := openTestRedis(t)
	defer rdb.Close()
	rdb.Del(context.Background(), "stock:8004")
	sr := repo.NewStockRedisRepo(rdb)
	_, err := sr.DeductForUser(context.Background(), 8004, 1, 1, 1)
	if err != repo.ErrStockNotWarmed {
		t.Errorf("expected not-warmed, got %v", err)
	}
}

// tiny strconv.Itoa alias to avoid importing strconv for one-liner cleanup
func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	bp := len(b)
	for i > 0 {
		bp--
		b[bp] = digits[i%10]
		i /= 10
	}
	if neg {
		bp--
		b[bp] = '-'
	}
	return string(b[bp:])
}
