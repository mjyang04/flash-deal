// Package main seeds a demo activity (id=1001) and warms Redis stock for it.
//
// Run:
//
//	go run ./cmd/seed [ACTIVITY_ID] [TOTAL_STOCK]
//	# defaults: ACTIVITY_ID=1001 TOTAL_STOCK=1000
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mjyangnb/flash-deal/internal/config"
	"github.com/mjyangnb/flash-deal/internal/domain"
	fdmysql "github.com/mjyangnb/flash-deal/internal/infra/mysql"
	fdredis "github.com/mjyangnb/flash-deal/internal/infra/redis"
	"github.com/mjyangnb/flash-deal/internal/repo"
	"github.com/mjyangnb/flash-deal/internal/service"
)

func main() {
	id := int64(1001)
	stock := 1000
	if len(os.Args) > 1 {
		if v, err := strconv.ParseInt(os.Args[1], 10, 64); err == nil {
			id = v
		}
	}
	if len(os.Args) > 2 {
		if v, err := strconv.Atoi(os.Args[2]); err == nil {
			stock = v
		}
	}

	cfg, err := config.Load(os.Getenv("FD_CONFIG_FILE"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	db, err := fdmysql.Open(cfg.MySQL)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer db.Close()
	rdb := fdredis.New(cfg.Redis)
	defer rdb.Close()

	ar := repo.NewActivityRepo(db)
	adminSvc := service.NewAdmin(ar, rdb)

	now := time.Now().UTC()
	a := domain.Activity{
		ID: id, ProductID: 555, TotalStock: stock,
		StartAt: now.Add(-time.Minute), EndAt: now.Add(2 * time.Hour),
		PerUserLimit: 1, Status: domain.ActivityRunning,
	}
	ctx := context.Background()
	if err := adminSvc.Create(ctx, a); err != nil {
		if !isDuplicate(err) {
			log.Fatalf("create: %v", err)
		}
		log.Printf("activity %d already exists, continuing to warm", id)
	}
	warmed, err := adminSvc.Warm(ctx, id)
	if err != nil {
		if errors.Is(err, repo.ErrActivityNotFound) {
			log.Fatalf("warm: activity %d not found in MySQL", id)
		}
		log.Fatalf("warm: %v", err)
	}
	fmt.Printf("seeded activity=%d stock=%d warmed_stock=%d\n", id, stock, warmed)
}

func isDuplicate(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1062") || strings.Contains(msg, "Duplicate entry")
}
