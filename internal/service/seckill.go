// Package service holds the seckill application service.
// The service depends on small ports (not the concrete repo structs) so that
// M2 can swap StockDeducter to Redis-Lua and OrderCreator to a Kafka producer
// without changing this file.
package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/internal/repo"
)

// ---- ports (subset of repo interfaces) ----

type ActivityFetcher interface {
	GetByID(ctx context.Context, id int64) (domain.Activity, error)
}

type StockDeducter interface {
	DeductForUser(ctx context.Context, activityID, userID int64, n, perUserLimit int) (remaining int, err error)
}

type OrderCreator interface {
	Create(ctx context.Context, o domain.Order) error
}

// ---- service ----

type SeckillService struct {
	activities ActivityFetcher
	stock      StockDeducter
	orders     OrderCreator
	now        func() time.Time
	nextID     func() int64
}

func New(
	activities ActivityFetcher, stock StockDeducter, orders OrderCreator,
	now func() time.Time, nextID func() int64,
) *SeckillService {
	return &SeckillService{
		activities: activities, stock: stock, orders: orders,
		now: now, nextID: nextID,
	}
}

// SeckillOutput is what Seckill hands back to the handler.
type SeckillOutput struct {
	Outcome    domain.SeckillOutcome
	QueueToken string
	Remaining  int
}

// Seckill is the M1 synchronous path.
// Order of checks: activity exists → window open → deduct stock → create order.
func (s *SeckillService) Seckill(ctx context.Context, req domain.SeckillRequest) (SeckillOutput, error) {
	a, err := s.activities.GetByID(ctx, req.ActivityID)
	if errors.Is(err, repo.ErrActivityNotFound) {
		return SeckillOutput{Outcome: domain.OutcomeNotFound}, nil
	}
	if err != nil {
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	now := s.now()
	switch {
	case now.Before(a.StartAt):
		return SeckillOutput{Outcome: domain.OutcomeNotStarted}, nil
	case !now.Before(a.EndAt):
		return SeckillOutput{Outcome: domain.OutcomeEnded}, nil
	}

	remaining, err := s.stock.DeductForUser(ctx, req.ActivityID, req.UserID, 1, a.PerUserLimit)
	if errors.Is(err, repo.ErrStockNotEnough) {
		return SeckillOutput{Outcome: domain.OutcomeSoldOut}, nil
	}
	if errors.Is(err, repo.ErrUserLimitExceeded) {
		return SeckillOutput{Outcome: domain.OutcomeUserLimit}, nil
	}
	if errors.Is(err, repo.ErrStockNotWarmed) {
		return SeckillOutput{Outcome: domain.OutcomeNotWarmed}, nil
	}
	if err != nil {
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	orderID := s.nextID()
	err = s.orders.Create(ctx, domain.Order{
		ID: orderID, UserID: req.UserID, ActivityID: req.ActivityID, ProductID: a.ProductID,
		Status: domain.OrderQueued, IdempotencyToken: req.IdempotencyToken, CreatedAt: now,
	})
	if errors.Is(err, repo.ErrOrderDuplicate) {
		return SeckillOutput{Outcome: domain.OutcomeDuplicate}, nil
	}
	if err != nil {
		// NOTE: stock was decremented but order failed; M3 will add a reconcile sweeper.
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	return SeckillOutput{
		Outcome:    domain.OutcomeQueued,
		QueueToken: strconv.FormatInt(orderID, 10),
		Remaining:  remaining,
	}, nil
}
