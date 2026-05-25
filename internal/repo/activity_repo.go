// Package repo holds DB-backed implementations of the domain repository ports.
package repo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/domain"
)

// ErrActivityNotFound is returned by ActivityRepo.GetByID when no row matches.
var ErrActivityNotFound = errors.New("activity not found")

// ActivityRepo is the persistence port for activities.
type ActivityRepo interface {
	Create(ctx context.Context, a domain.Activity) error
	GetByID(ctx context.Context, id int64) (domain.Activity, error)
}

type activityRepoSQL struct {
	db *sqlx.DB
}

// NewActivityRepo wires the sqlx-backed implementation.
func NewActivityRepo(db *sqlx.DB) ActivityRepo {
	return &activityRepoSQL{db: db}
}

type activityRow struct {
	ID           int64     `db:"id"`
	ProductID    int64     `db:"product_id"`
	TotalStock   int       `db:"total_stock"`
	StartAt      time.Time `db:"start_at"`
	EndAt        time.Time `db:"end_at"`
	PerUserLimit int       `db:"per_user_limit"`
	Status       int8      `db:"status"`
}

func (r *activityRepoSQL) Create(ctx context.Context, a domain.Activity) error {
	const q = `
INSERT INTO activities (id, product_id, total_stock, start_at, end_at, per_user_limit, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q,
		a.ID, a.ProductID, a.TotalStock,
		a.StartAt, a.EndAt, a.PerUserLimit, int8(a.Status),
	)
	return err
}

func (r *activityRepoSQL) GetByID(ctx context.Context, id int64) (domain.Activity, error) {
	var row activityRow
	const q = `
SELECT id, product_id, total_stock, start_at, end_at, per_user_limit, status
  FROM activities WHERE id = ?`
	err := r.db.GetContext(ctx, &row, q, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Activity{}, ErrActivityNotFound
	}
	if err != nil {
		return domain.Activity{}, err
	}
	return domain.Activity{
		ID:           row.ID,
		ProductID:    row.ProductID,
		TotalStock:   row.TotalStock,
		StartAt:      row.StartAt,
		EndAt:        row.EndAt,
		PerUserLimit: row.PerUserLimit,
		Status:       domain.ActivityStatus(row.Status),
	}, nil
}
