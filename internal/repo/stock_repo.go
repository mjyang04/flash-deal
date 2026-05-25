package repo

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

// ErrStockNotEnough — UPDATE matched 0 rows, stock would go negative.
var ErrStockNotEnough = errors.New("stock not enough")

// ErrStockNotWarmed — Redis stock key missing (Lua only).
var ErrStockNotWarmed = errors.New("stock not warmed")

// ErrUserLimitExceeded — per-user purchased count would exceed limit (Lua only).
var ErrUserLimitExceeded = errors.New("user limit exceeded")

// StockRepo is the seckill stock port. M1 = SQL row lock; M2 = Redis Lua.
type StockRepo interface {
	// DeductForUser atomically subtracts n from activity stock.
	// userID + perUserLimit are honored by the Redis Lua impl;
	// the SQL impl ignores them (M1 didn't track per-user state).
	// Returns the post-deduction remaining stock, or one of:
	// ErrStockNotEnough / ErrStockNotWarmed / ErrUserLimitExceeded.
	DeductForUser(ctx context.Context, activityID, userID int64, n, perUserLimit int) (remaining int, err error)
}

type stockRepoSQL struct {
	db *sqlx.DB
}

func NewStockRepo(db *sqlx.DB) StockRepo {
	return &stockRepoSQL{db: db}
}

func (r *stockRepoSQL) DeductForUser(ctx context.Context, activityID, _ int64, n, _ int) (int, error) {
	const upd = `
UPDATE activities
   SET total_stock = total_stock - ?
 WHERE id = ? AND total_stock >= ?`
	res, err := r.db.ExecContext(ctx, upd, n, activityID, n)
	if err != nil {
		return 0, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if aff == 0 {
		return 0, ErrStockNotEnough
	}
	var remaining int
	if err := r.db.GetContext(ctx, &remaining,
		"SELECT total_stock FROM activities WHERE id = ?", activityID); err != nil {
		return 0, err
	}
	return remaining, nil
}
