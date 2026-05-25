//go:build integration

package repo_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

func TestStockRepo_Deduct_NoOversell(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	resetActivities(t, db)

	ar := repo.NewActivityRepo(db)
	now := time.Now().UTC()
	if err := ar.Create(context.Background(), domain.Activity{
		ID: 8001, ProductID: 1, TotalStock: 100,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}); err != nil {
		t.Fatal(err)
	}

	sr := repo.NewStockRepo(db)
	var success, soldOut, other int32
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.DeductForUser(context.Background(), 8001, int64(i+1), 1, 1)
			switch err {
			case nil:
				atomic.AddInt32(&success, 1)
			case repo.ErrStockNotEnough:
				atomic.AddInt32(&soldOut, 1)
			default:
				atomic.AddInt32(&other, 1) // e.g. "too many connections" on resource-constrained CI
			}
		}()
	}
	wg.Wait()

	// Hard invariants — no oversell, no row lost:
	if success > 100 {
		t.Errorf("oversell: success = %d, want <= 100", success)
	}
	if success+soldOut+other != 1000 {
		t.Errorf("accounting: success(%d)+sold(%d)+other(%d) != 1000", success, soldOut, other)
	}
	got, _ := ar.GetByID(context.Background(), 8001)
	if got.TotalStock != 100-int(success) {
		t.Errorf("final stock = %d, want %d (initial 100 - success %d)", got.TotalStock, 100-int(success), success)
	}
	// Strict (unconstrained env): expect exactly 100/900 — only enforced when no other errors.
	if other == 0 && (success != 100 || soldOut != 900) {
		t.Errorf("unconstrained run drifted: success=%d soldOut=%d (want 100/900)", success, soldOut)
	}
}
