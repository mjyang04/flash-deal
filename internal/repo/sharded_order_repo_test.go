//go:build integration

package repo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/repo"
)

func openShards(t *testing.T) []*sqlx.DB {
	t.Helper()
	var dbs []*sqlx.DB
	for i := 0; i < 4; i++ {
		dsn := fmt.Sprintf("flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal_%d?parseTime=true&loc=Local", i)
		db, err := sqlx.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		dbs = append(dbs, db)
	}
	return dbs
}

func resetShards(t *testing.T, dbs []*sqlx.DB) {
	t.Helper()
	for _, db := range dbs {
		db.MustExec("TRUNCATE TABLE orders_0")
	}
}

func TestShardedOrder_RouteAndFetch(t *testing.T) {
	dbs := openShards(t)
	defer func() {
		for _, d := range dbs {
			d.Close()
		}
	}()
	resetShards(t, dbs)

	r := repo.NewShardedOrderRepo(dbs)
	ctx := context.Background()
	for uid := int64(0); uid < 20; uid++ {
		o := domain.Order{
			ID: uid + 1000, UserID: uid, ActivityID: 9001, ProductID: 555,
			Status: domain.OrderQueued, IdempotencyToken: fmt.Sprintf("t-%d", uid),
			CreatedAt: time.Now(),
		}
		if err := r.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	for shard := 0; shard < 4; shard++ {
		var n int
		if err := dbs[shard].Get(&n, "SELECT COUNT(*) FROM orders_0"); err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Errorf("shard %d count = %d, want 5 (uniform user_id mod 4)", shard, n)
		}
	}
	got, err := r.GetByID(ctx, 7, 1007)
	if err != nil {
		t.Fatal(err)
	}
	if got.UserID != 7 {
		t.Errorf("got %+v", got)
	}
}
