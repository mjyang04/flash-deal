//go:build integration

package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyangnb/flash-deal/internal/domain"
	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
	"github.com/mjyangnb/flash-deal/internal/repo"
	"github.com/mjyangnb/flash-deal/internal/service"
)

type memOrderRepo struct {
	rows map[int64]domain.Order
	keys map[string]bool
}

func newMem() *memOrderRepo {
	return &memOrderRepo{rows: map[int64]domain.Order{}, keys: map[string]bool{}}
}
func (r *memOrderRepo) Create(_ context.Context, o domain.Order) error {
	k := fmt.Sprintf("%d:%d:%s", o.ActivityID, o.UserID, o.IdempotencyToken)
	if r.keys[k] {
		return repo.ErrOrderDuplicate
	}
	r.keys[k] = true
	r.rows[o.ID] = o
	return nil
}
func (r *memOrderRepo) GetByID(_ context.Context, _ int64, id int64) (domain.Order, error) {
	if o, ok := r.rows[id]; ok {
		return o, nil
	}
	return domain.Order{}, repo.ErrOrderNotFound
}

func TestMaterializer_HappyPath_AndDuplicate(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	defer rdb.Close()
	ctx := context.Background()
	rdb.Del(ctx, "queue:tok-mat-1")

	mem := newMem()
	m := service.NewOrderMaterializer(mem, rdb)
	msg := fdkafka.OrderMessage{
		Version: 1, OrderID: 555, ActivityID: 1, UserID: 7, ProductID: 100,
		IdempotencyToken: "idem-mat-1", QueueToken: "tok-mat-1", ProducedAt: time.Now(),
	}
	if err := m.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if v, _ := rdb.Get(ctx, "queue:tok-mat-1").Result(); v != "success:555" {
		t.Errorf("queue val = %q", v)
	}
	if err := m.Handle(ctx, msg); err != nil {
		t.Fatalf("dup: %v", err)
	}
	if len(mem.rows) != 1 {
		t.Errorf("rows = %d, want 1", len(mem.rows))
	}
}
