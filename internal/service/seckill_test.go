package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/repo"
	"github.com/mjyangnb/flash-deal/internal/service"
)

// ---- in-memory mocks ----

type fakeActivityRepo struct {
	store map[int64]domain.Activity
}

func (f *fakeActivityRepo) Create(_ context.Context, a domain.Activity) error {
	f.store[a.ID] = a
	return nil
}
func (f *fakeActivityRepo) GetByID(_ context.Context, id int64) (domain.Activity, error) {
	a, ok := f.store[id]
	if !ok {
		return domain.Activity{}, repo.ErrActivityNotFound
	}
	return a, nil
}

type fakeStockRepo struct {
	remaining int
	err       error
}

func (f *fakeStockRepo) DeductForUser(_ context.Context, _, _ int64, n, _ int) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.remaining -= n
	return f.remaining, nil
}

type fakeOrderRepo struct {
	created []domain.Order
	err     error
}

func (f *fakeOrderRepo) Create(_ context.Context, o domain.Order) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, o)
	return nil
}
func (f *fakeOrderRepo) GetByID(_ context.Context, userID, orderID int64) (domain.Order, error) {
	for _, o := range f.created {
		if o.UserID == userID && o.ID == orderID {
			return o, nil
		}
	}
	return domain.Order{}, repo.ErrOrderNotFound
}

// ---- helpers ----

func runningActivity() domain.Activity {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	return domain.Activity{
		ID: 9001, ProductID: 555, TotalStock: 10,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
}

func newSvc(ar service.ActivityFetcher, sr service.StockDeducter, or service.OrderCreator) *service.SeckillService {
	return service.New(ar, sr, or,
		func() time.Time { return time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC) },
		func() int64 { return 7777 },
	)
}

// ---- tests ----

func TestSeckill_Queued(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{remaining: 10}
	or := &fakeOrderRepo{}
	svc := newSvc(ar, sr, or)

	res, err := svc.Seckill(context.Background(), domain.SeckillRequest{
		ActivityID: 9001, UserID: 42, IdempotencyToken: "tok-1",
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Outcome != domain.OutcomeQueued {
		t.Errorf("outcome = %v, want Queued", res.Outcome)
	}
	if res.Remaining != 9 {
		t.Errorf("remaining = %d, want 9", res.Remaining)
	}
	if len(or.created) != 1 {
		t.Errorf("created = %d, want 1", len(or.created))
	}
}

func TestSeckill_NotFound(t *testing.T) {
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{}}, &fakeStockRepo{}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 1, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotFound {
		t.Errorf("outcome = %v, want NotFound", res.Outcome)
	}
}

func TestSeckill_NotStarted(t *testing.T) {
	a := runningActivity()
	a.StartAt = time.Date(2026, 5, 25, 13, 0, 0, 0, time.UTC) // in the future
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{9001: a}}, &fakeStockRepo{remaining: 10}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotStarted {
		t.Errorf("outcome = %v, want NotStarted", res.Outcome)
	}
}

func TestSeckill_Ended(t *testing.T) {
	a := runningActivity()
	a.EndAt = time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC)
	svc := newSvc(&fakeActivityRepo{store: map[int64]domain.Activity{9001: a}}, &fakeStockRepo{remaining: 10}, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeEnded {
		t.Errorf("outcome = %v, want Ended", res.Outcome)
	}
}

func TestSeckill_SoldOut(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrStockNotEnough}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeSoldOut {
		t.Errorf("outcome = %v, want SoldOut", res.Outcome)
	}
}

func TestSeckill_Duplicate(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{remaining: 10}
	or := &fakeOrderRepo{err: repo.ErrOrderDuplicate}
	svc := newSvc(ar, sr, or)
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeDuplicate {
		t.Errorf("outcome = %v, want Duplicate", res.Outcome)
	}
}

func TestSeckill_UserLimit(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrUserLimitExceeded}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeUserLimit {
		t.Errorf("outcome = %v, want UserLimit", res.Outcome)
	}
}

func TestSeckill_NotWarmed(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: repo.ErrStockNotWarmed}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeNotWarmed {
		t.Errorf("outcome = %v, want NotWarmed", res.Outcome)
	}
}

func TestSeckill_InternalOnUnknown(t *testing.T) {
	ar := &fakeActivityRepo{store: map[int64]domain.Activity{9001: runningActivity()}}
	sr := &fakeStockRepo{err: errors.New("boom")}
	svc := newSvc(ar, sr, &fakeOrderRepo{})
	res, _ := svc.Seckill(context.Background(), domain.SeckillRequest{ActivityID: 9001, UserID: 1, IdempotencyToken: "x"})
	if res.Outcome != domain.OutcomeInternal {
		t.Errorf("outcome = %v, want Internal", res.Outcome)
	}
}
