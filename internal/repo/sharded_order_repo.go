package repo

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/mjyangnb/flash-deal/internal/domain"
	"github.com/mjyangnb/flash-deal/pkg/shardkey"
)

type shardedOrderRepo struct {
	shards []*sqlx.DB
}

// NewShardedOrderRepo wraps N shards; shardkey.DBIndex(userID, N) decides routing.
func NewShardedOrderRepo(shards []*sqlx.DB) OrderRepo {
	return &shardedOrderRepo{shards: shards}
}

func (r *shardedOrderRepo) Create(ctx context.Context, o domain.Order) error {
	idx := shardkey.DBIndex(o.UserID, len(r.shards))
	const q = `
INSERT INTO orders_0 (id, user_id, activity_id, product_id, status, idempotency_token, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.shards[idx].ExecContext(ctx, q,
		o.ID, o.UserID, o.ActivityID, o.ProductID,
		int8(o.Status), o.IdempotencyToken, o.CreatedAt,
	)
	if err != nil && isDuplicateKey(err) {
		return ErrOrderDuplicate
	}
	return err
}

func (r *shardedOrderRepo) GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error) {
	idx := shardkey.DBIndex(userID, len(r.shards))
	return getOrderFromDB(ctx, r.shards[idx], userID, orderID)
}
