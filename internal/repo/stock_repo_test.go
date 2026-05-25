//go:build integration

package repo_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/repo"
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
	var success, soldOut int32
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.Deduct(context.Background(), 8001, 1)
			switch err {
			case nil:
				atomic.AddInt32(&success, 1)
			case repo.ErrStockNotEnough:
				atomic.AddInt32(&soldOut, 1)
			}
		}()
	}
	wg.Wait()

	if success != 100 {
		t.Errorf("success = %d, want exactly 100", success)
	}
	if soldOut != 900 {
		t.Errorf("soldOut = %d, want exactly 900", soldOut)
	}

	// verify DB final state is 0
	got, _ := ar.GetByID(context.Background(), 8001)
	if got.TotalStock != 0 {
		t.Errorf("final stock = %d, want 0", got.TotalStock)
	}
}
