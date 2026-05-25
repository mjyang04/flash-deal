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

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
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

// OrderCreatorWithToken is an optional extension implemented by the Kafka
// path so that the queue_token can be embedded in the produced message.
// SeckillService prefers this when the impl satisfies it.
type OrderCreatorWithToken interface {
	OrderCreator
	CreateWithToken(ctx context.Context, o domain.Order, queueToken string) error
}

// ---- service ----

type SeckillService struct {
	activities ActivityFetcher
	stock      StockDeducter
	orders     OrderCreator
	now        func() time.Time
	nextID     func() int64
	queue      *QueueService // optional; when set, queue_token is generated + threaded through OrderCreatorWithToken
}

// WithQueue attaches a QueueService so Seckill returns a server-generated
// queue_token (UUIDv7) and the OrderCreator gets it via CreateWithToken.
func (s *SeckillService) WithQueue(q *QueueService) *SeckillService {
	s.queue = q
	return s
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
	o := domain.Order{
		ID: orderID, UserID: req.UserID, ActivityID: req.ActivityID, ProductID: a.ProductID,
		Status: domain.OrderQueued, IdempotencyToken: req.IdempotencyToken, CreatedAt: now,
	}
	queueToken := strconv.FormatInt(orderID, 10) // M1 fallback
	if s.queue != nil {
		if tok, terr := s.queue.New(ctx); terr == nil {
			queueToken = tok
		}
	}
	if oct, ok := s.orders.(OrderCreatorWithToken); ok {
		err = oct.CreateWithToken(ctx, o, queueToken)
	} else {
		err = s.orders.Create(ctx, o)
	}
	if errors.Is(err, repo.ErrOrderDuplicate) {
		return SeckillOutput{Outcome: domain.OutcomeDuplicate}, nil
	}
	if err != nil {
		// NOTE: stock was decremented but order failed; M3 will add a reconcile sweeper.
		return SeckillOutput{Outcome: domain.OutcomeInternal}, err
	}

	return SeckillOutput{
		Outcome:    domain.OutcomeQueued,
		QueueToken: queueToken,
		Remaining:  remaining,
	}, nil
}
