package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjyangnb/flash-deal/internal/domain"
	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
	"github.com/mjyangnb/flash-deal/internal/repo"
)

// OrderMaterializer is the consumer-side business: turn a Kafka OrderMessage
// into a row in MySQL and update the queue-token state in Redis.
type OrderMaterializer struct {
	orders repo.OrderRepo
	rdb    *goredis.Client
}

func NewOrderMaterializer(orders repo.OrderRepo, rdb *goredis.Client) *OrderMaterializer {
	return &OrderMaterializer{orders: orders, rdb: rdb}
}

// Handle is the kafka.Handler. Idempotent: duplicate insert → treat as success
// (the row already exists with the same idempotency token).
func (m *OrderMaterializer) Handle(ctx context.Context, msg fdkafka.OrderMessage) error {
	o := domain.Order{
		ID: msg.OrderID, UserID: msg.UserID, ActivityID: msg.ActivityID,
		ProductID: msg.ProductID, Status: domain.OrderQueued,
		IdempotencyToken: msg.IdempotencyToken, CreatedAt: msg.ProducedAt,
	}
	err := m.orders.Create(ctx, o)
	if err != nil && !errors.Is(err, repo.ErrOrderDuplicate) {
		return err
	}
	key := fmt.Sprintf("queue:%s", msg.QueueToken)
	val := fmt.Sprintf("success:%d", msg.OrderID)
	return m.rdb.Set(ctx, key, val, time.Hour).Err()
}

// MarkFailed is used by the DLQ handler to flip the queue token to a failure.
func (m *OrderMaterializer) MarkFailed(ctx context.Context, queueToken string, reason error) {
	key := fmt.Sprintf("queue:%s", queueToken)
	m.rdb.Set(ctx, key, fmt.Sprintf("failed:%s", reason.Error()), time.Hour)
}
