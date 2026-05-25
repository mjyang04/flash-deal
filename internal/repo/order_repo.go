package repo

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// ErrOrderNotFound — no order for the given (user_id, order_id).
var ErrOrderNotFound = errors.New("order not found")

// ErrOrderDuplicate — UNIQUE (activity_id, user_id, idempotency_token) hit.
var ErrOrderDuplicate = errors.New("duplicate order")

// OrderRepo is the persistence port for orders. M1 routes everything to shard 0;
// M3 will switch the impl to per-shard *sqlx.DB selected via pkg/shardkey.
type OrderRepo interface {
	Create(ctx context.Context, o domain.Order) error
	GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error)
}

type orderRepoSQL struct {
	db *sqlx.DB
}

func NewOrderRepo(db *sqlx.DB) OrderRepo {
	return &orderRepoSQL{db: db}
}

type orderRow struct {
	ID               int64     `db:"id"`
	UserID           int64     `db:"user_id"`
	ActivityID       int64     `db:"activity_id"`
	ProductID        int64     `db:"product_id"`
	Status           int8      `db:"status"`
	IdempotencyToken string    `db:"idempotency_token"`
	CreatedAt        time.Time `db:"created_at"`
}

func (r *orderRepoSQL) Create(ctx context.Context, o domain.Order) error {
	const q = `
INSERT INTO orders_0 (id, user_id, activity_id, product_id, status, idempotency_token, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q,
		o.ID, o.UserID, o.ActivityID, o.ProductID,
		int8(o.Status), o.IdempotencyToken, o.CreatedAt,
	)
	if err != nil && isDuplicateKey(err) {
		return ErrOrderDuplicate
	}
	return err
}

func (r *orderRepoSQL) GetByID(ctx context.Context, userID, orderID int64) (domain.Order, error) {
	return getOrderFromDB(ctx, r.db, userID, orderID)
}

// getOrderFromDB is shared by the single-shard and the sharded impls.
func getOrderFromDB(ctx context.Context, db *sqlx.DB, userID, orderID int64) (domain.Order, error) {
	var row orderRow
	const q = `
SELECT id, user_id, activity_id, product_id, status, idempotency_token, created_at
  FROM orders_0 WHERE user_id = ? AND id = ?`
	err := db.GetContext(ctx, &row, q, userID, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Order{}, ErrOrderNotFound
	}
	if err != nil {
		return domain.Order{}, err
	}
	return domain.Order{
		ID:               row.ID,
		UserID:           row.UserID,
		ActivityID:       row.ActivityID,
		ProductID:        row.ProductID,
		Status:           domain.OrderStatus(row.Status),
		IdempotencyToken: row.IdempotencyToken,
		CreatedAt:        row.CreatedAt,
	}, nil
}

// isDuplicateKey detects MySQL error 1062 without importing the driver type.
func isDuplicateKey(err error) bool {
	return strings.Contains(err.Error(), "Error 1062") ||
		strings.Contains(err.Error(), "Duplicate entry")
}
