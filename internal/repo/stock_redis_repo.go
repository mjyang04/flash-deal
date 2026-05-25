package repo

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// stockRedisRepo runs stock_deduct.lua via EVAL/EVALSHA caching.
type stockRedisRepo struct {
	rdb    *goredis.Client
	script *goredis.Script
}

// NewStockRedisRepo wraps a redis client with the seckill Lua script.
// The script source lives at internal/infra/redis/scripts/stock_deduct.lua;
// it is also embedded here as a string so the deployed binary needs no script file.
func NewStockRedisRepo(rdb *goredis.Client) StockRepo {
	return &stockRedisRepo{
		rdb:    rdb,
		script: goredis.NewScript(stockDeductLua),
	}
}

// DeductForUser invokes the Lua script atomically.
// KEYS[1] = stock:{activityID}
// KEYS[2] = user_buy:{activityID}:{userID}
// ARGV[1] = deduct_count
// ARGV[2] = per_user_limit
// Lua returns: ≥0 (new remaining), -1 sold_out, -2 user_limit, -3 not_warmed.
func (r *stockRedisRepo) DeductForUser(
	ctx context.Context, activityID, userID int64, n, perUserLimit int,
) (int, error) {
	stockKey := fmt.Sprintf("stock:%d", activityID)
	userKey := fmt.Sprintf("user_buy:%d:%d", activityID, userID)
	v, err := r.script.Run(ctx, r.rdb, []string{stockKey, userKey}, n, perUserLimit).Int64()
	if err != nil {
		return 0, err
	}
	switch v {
	case -1:
		return 0, ErrStockNotEnough
	case -2:
		return 0, ErrUserLimitExceeded
	case -3:
		return 0, ErrStockNotWarmed
	}
	if v < 0 {
		return 0, errors.New("unknown lua return")
	}
	return int(v), nil
}

// stockDeductLua mirrors internal/infra/redis/scripts/stock_deduct.lua.
const stockDeductLua = `
local stock_key  = KEYS[1]
local userbuy_key = KEYS[2]
local deduct      = tonumber(ARGV[1])
local user_limit  = tonumber(ARGV[2])

local raw_stock = redis.call('GET', stock_key)
if not raw_stock then
  return -3
end

local stock = tonumber(raw_stock)
if stock < deduct then
  return -1
end

local user_bought = tonumber(redis.call('GET', userbuy_key) or '0')
if user_bought + deduct > user_limit then
  return -2
end

local new_stock = redis.call('DECRBY', stock_key, deduct)
redis.call('INCRBY', userbuy_key, deduct)
redis.call('EXPIRE', userbuy_key, 86400)
return new_stock
`
