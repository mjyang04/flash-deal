package service

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mjyang04/flash-deal/internal/domain"
	"github.com/mjyang04/flash-deal/internal/repo"
)

type AdminService struct {
	activities repo.ActivityRepo
	rdb        *redis.Client
}

func NewAdmin(activities repo.ActivityRepo, rdb *redis.Client) *AdminService {
	return &AdminService{activities: activities, rdb: rdb}
}

func (s *AdminService) Create(ctx context.Context, a domain.Activity) error {
	return s.activities.Create(ctx, a)
}

// Warm copies activity.total_stock into Redis under stock:{id}.
// M2's Lua deduct reads/writes the same key.
func (s *AdminService) Warm(ctx context.Context, id int64) (int, error) {
	a, err := s.activities.GetByID(ctx, id)
	if err != nil {
		return 0, err
	}
	key := fmt.Sprintf("stock:%d", id)
	if err := s.rdb.Set(ctx, key, a.TotalStock, 24*time.Hour).Err(); err != nil {
		return 0, err
	}
	return a.TotalStock, nil
}
