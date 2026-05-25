// Package mysql wraps sqlx.DB with pool tuning + sensible defaults.
package mysql

import (
	_ "github.com/go-sql-driver/mysql" // driver
	"github.com/jmoiron/sqlx"

	"github.com/mjyang04/flash-deal/internal/config"
)

// Open returns a tuned *sqlx.DB. Caller closes.
func Open(cfg config.MySQLConfig) (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	return db, nil
}
