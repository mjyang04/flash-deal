//go:build integration

package service_test

import (
	"context"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/service"
)

func TestQueue_NewAndGet(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	q := service.NewQueue(rdb)
	tok, err := q.New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tok, "-") {
		t.Errorf("token shape: %q", tok)
	}
	st, err := q.Get(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	if st != "queued" {
		t.Errorf("state = %q", st)
	}
	missing, _ := q.Get(context.Background(), "no-such-token-zz")
	if missing != "not_found" {
		t.Errorf("missing state = %q", missing)
	}
}
