//go:build integration

package repo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/repo"
)

func resetOrders(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE TABLE orders_0"); err != nil {
		t.Fatal(err)
	}
}

func TestOrderRepo_Create_Then_GetByID(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetOrders(t, db)

	r := repo.NewOrderRepo(db)
	ctx := context.Background()
	o := domain.Order{
		ID: 7001, UserID: 42, ActivityID: 9001, ProductID: 555,
		Status: domain.OrderQueued, IdempotencyToken: "tok-1", CreatedAt: time.Now(),
	}
	if err := r.Create(ctx, o); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := r.GetByID(ctx, 42, 7001)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.IdempotencyToken != "tok-1" {
		t.Errorf("token round-trip mismatch")
	}
}

func TestOrderRepo_Create_DuplicateIdem(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetOrders(t, db)

	r := repo.NewOrderRepo(db)
	ctx := context.Background()
	o := domain.Order{
		ID: 7002, UserID: 42, ActivityID: 9001, ProductID: 555,
		Status: domain.OrderQueued, IdempotencyToken: "tok-dup", CreatedAt: time.Now(),
	}
	if err := r.Create(ctx, o); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	o2 := o
	o2.ID = 7003 // distinct PK; same (activity_id, user_id, idem) tuple
	err := r.Create(ctx, o2)
	if !errors.Is(err, repo.ErrOrderDuplicate) {
		t.Fatalf("got %v, want ErrOrderDuplicate", err)
	}
}
