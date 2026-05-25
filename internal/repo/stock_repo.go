package repo

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

// ErrStockNotEnough — UPDATE matched 0 rows, stock would go negative.
var ErrStockNotEnough = errors.New("stock not enough")

// StockRepo is the seckill stock port. M1 = SQL row lock; M2 = Redis Lua.
type StockRepo interface {
	// Deduct atomically subtracts n from activity's total_stock when remaining >= n.
	// Returns the post-deduction remaining stock, or ErrStockNotEnough.
	Deduct(ctx context.Context, activityID int64, n int) (remaining int, err error)
}

type stockRepoSQL struct {
	db *sqlx.DB
}

func NewStockRepo(db *sqlx.DB) StockRepo {
	return &stockRepoSQL{db: db}
}

func (r *stockRepoSQL) Deduct(ctx context.Context, activityID int64, n int) (int, error) {
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
