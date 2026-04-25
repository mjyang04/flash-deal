-- stock_deduct.lua
--
-- KEYS[1] = stock:{activity_id}            (string, integer)
-- KEYS[2] = user_buy:{activity_id}:{user_id} (string, integer)
-- ARGV[1] = deduct_count                   (positive integer)
-- ARGV[2] = per_user_limit                 (positive integer)
--
-- Return:
--   N  >= 0  : new remaining stock after deduction (success)
--   -1       : sold out / not enough stock
--   -2       : exceeds per-user limit
--   -3       : stock key missing (activity not warmed)

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
