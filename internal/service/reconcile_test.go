//go:build integration

package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
	"github.com/mjyang04/flash-deal/internal/service"
)

func openDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := sqlx.Open("mysql", "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func setupActivity(t *testing.T, db *sqlx.DB, rdb *goredis.Client, id int64, total int, redisRem int, ordersN int) {
	t.Helper()
	db.MustExec("DELETE FROM activities WHERE id=?", id)
	db.MustExec("DELETE FROM orders_0 WHERE activity_id=?", id)
	ar := repo.NewActivityRepo(db)
	now := time.Now()
	if err := ar.Create(context.Background(), domain.Activity{
		ID: id, ProductID: 1, TotalStock: total,
		StartAt: now.Add(-time.Hour), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}); err != nil {
		t.Fatal(err)
	}
	rdb.Set(context.Background(), fmt.Sprintf("stock:%d", id), redisRem, 0)
	for i := 0; i < ordersN; i++ {
		db.MustExec(`INSERT INTO orders_0 (id,user_id,activity_id,product_id,status,idempotency_token,created_at)
			VALUES (?,?,?,?,?,?,?)`, int64(id)*1000+int64(i), i+1, id, 1, 0, fmt.Sprintf("rt-%d-%d", id, i), now)
	}
	t.Cleanup(func() {
		db.MustExec("DELETE FROM activities WHERE id=?", id)
		db.MustExec("DELETE FROM orders_0 WHERE activity_id=?", id)
		rdb.Del(context.Background(), fmt.Sprintf("stock:%d", id))
	})
}

func TestReconcile_Consistent(t *testing.T) {
	db := openDB(t)
	t.Cleanup(func() { db.Close() })
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	t.Cleanup(func() { rdb.Close() })

	// init=100, redis_rem=80, orders=20 → drift = 100 - (80+20) = 0
	setupActivity(t, db, rdb, 9501, 100, 80, 20)
	ar := repo.NewActivityRepo(db)
	r := service.NewReconcile(ar, rdb, nil, db, time.Second)
	f, err := r.Check(context.Background(), 9501)
	if err != nil {
		t.Fatal(err)
	}
	if f.Drift != 0 {
		t.Errorf("expected drift=0, got %+v", f)
	}
}

func TestReconcile_LeakDetected(t *testing.T) {
	db := openDB(t)
	t.Cleanup(func() { db.Close() })
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	t.Cleanup(func() { rdb.Close() })

	// Simulates real stock leak:
	// init=100, redis_rem=70 (extra 5 decremented), orders=25
	//   → drift = 100 - (70+25) = 5 (5 reservations never became orders)
	setupActivity(t, db, rdb, 9502, 100, 70, 25)
	ar := repo.NewActivityRepo(db)
	r := service.NewReconcile(ar, rdb, nil, db, time.Second)
	f, err := r.Check(context.Background(), 9502)
	if err != nil {
		t.Fatal(err)
	}
	if f.Drift != 5 {
		t.Errorf("expected drift=5, got %+v", f)
	}
}

func TestReconcile_OverMaterialization(t *testing.T) {
	db := openDB(t)
	t.Cleanup(func() { db.Close() })
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6380"})
	t.Cleanup(func() { rdb.Close() })

	// Pathological: orders > what redis says was reserved (consumer wrote dup).
	// init=100, redis_rem=80, orders=22 → drift = -2
	setupActivity(t, db, rdb, 9503, 100, 80, 22)
	ar := repo.NewActivityRepo(db)
	r := service.NewReconcile(ar, rdb, nil, db, time.Second)
	f, _ := r.Check(context.Background(), 9503)
	if f.Drift != -2 {
		t.Errorf("expected drift=-2, got %+v", f)
	}
}
