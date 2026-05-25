//go:build integration

package repo_test

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("FD_TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local"
	}
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func resetActivities(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE TABLE activities"); err != nil {
		t.Fatal(err)
	}
}

func TestActivityRepo_CreateAndGet(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	r := repo.NewActivityRepo(db)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	a := domain.Activity{
		ID: 9001, ProductID: 555, TotalStock: 100,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
	if err := r.Create(ctx, a); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := r.GetByID(ctx, 9001)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ProductID != 555 || got.TotalStock != 100 || got.PerUserLimit != 1 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestActivityRepo_GetByID_NotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	r := repo.NewActivityRepo(db)
	_, err := r.GetByID(context.Background(), 404404)
	if err != repo.ErrActivityNotFound {
		t.Fatalf("got %v, want ErrActivityNotFound", err)
	}
}
