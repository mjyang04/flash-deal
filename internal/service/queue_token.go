package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

const queueTokenTTL = 10 * time.Minute

// QueueService issues + queries queue tokens stored in Redis.
type QueueService struct{ rdb *goredis.Client }

func NewQueue(rdb *goredis.Client) *QueueService { return &QueueService{rdb: rdb} }

// New generates a UUIDv7 (time-sortable) and marks it queued in Redis.
func (q *QueueService) New(ctx context.Context) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	tok := id.String()
	key := fmt.Sprintf("queue:%s", tok)
	if err := q.rdb.Set(ctx, key, "queued", queueTokenTTL).Err(); err != nil {
		return "", err
	}
	return tok, nil
}

// Get reads the latest known state. "not_found" for missing.
func (q *QueueService) Get(ctx context.Context, token string) (string, error) {
	key := fmt.Sprintf("queue:%s", token)
	v, err := q.rdb.Get(ctx, key).Result()
	if err == goredis.Nil {
		return "not_found", nil
	}
	return v, err
}
