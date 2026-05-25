//go:build integration

package mysql_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mjyangnb/flash-deal/internal/config"
	fdmysql "github.com/mjyangnb/flash-deal/internal/infra/mysql"
)

func TestOpen_Ping(t *testing.T) {
	dsn := os.Getenv("FD_TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "flashdeal:flashdeal@tcp(127.0.0.1:3307)/flashdeal?parseTime=true&loc=Local"
	}
	cfg := config.MySQLConfig{
		DSN:             dsn,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute,
	}
	db, err := fdmysql.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
