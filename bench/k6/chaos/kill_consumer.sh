#!/usr/bin/env bash
# Chaos: kill consumer mid-load, restart, verify lag drains + correctness.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$ROOT"

PATH=/opt/homebrew/bin:$PATH
export PATH

cleanup() {
  pkill -f /tmp/fd-api || true
  pkill -f /tmp/fd-consumer || true
}
trap cleanup EXIT

docker exec fd-mysql mysql -uroot -prootpw -e \
  'truncate flashdeal.activities; truncate flashdeal_0.orders_0; truncate flashdeal_1.orders_0; truncate flashdeal_2.orders_0; truncate flashdeal_3.orders_0' > /dev/null 2>&1
docker exec fd-redis redis-cli flushdb > /dev/null
make seed > /dev/null

go build -o /tmp/fd-api ./cmd/api
go build -o /tmp/fd-consumer ./cmd/consumer

/tmp/fd-consumer > /tmp/chaos-consumer.log 2>&1 &
CONS_PID=$!
sleep 8

/tmp/fd-api > /tmp/chaos-api.log 2>&1 &
sleep 3

# Drive 500 reqs in 5s (single-user → 1 success then 429s; use random users)
for i in $(seq 1 500); do
  uid=$((RANDOM % 5000 + 1))
  curl -sS -X POST -H 'Content-Type: application/json' -H "X-User-Id: $uid" \
    -d "{\"activity_id\":1001,\"user_id\":$uid,\"idempotency_token\":\"chaos-$i\"}" \
    http://localhost:8080/v1/seckill > /dev/null &
  if (( i % 50 == 0 )); then wait; fi
done
wait

# Kill consumer mid-flight
echo "killing consumer (PID $CONS_PID)..."
kill "$CONS_PID" 2>/dev/null || true
sleep 5

# Drive another batch while consumer is down (should pile up in Kafka)
for i in $(seq 501 1000); do
  uid=$((RANDOM % 5000 + 1))
  curl -sS -X POST -H 'Content-Type: application/json' -H "X-User-Id: $uid" \
    -d "{\"activity_id\":1001,\"user_id\":$uid,\"idempotency_token\":\"chaos-$i\"}" \
    http://localhost:8080/v1/seckill > /dev/null &
  if (( i % 50 == 0 )); then wait; fi
done
wait

# Restart consumer
/tmp/fd-consumer > /tmp/chaos-consumer.log 2>&1 &
sleep 15

# Verify
stock=$(docker exec fd-redis redis-cli get stock:1001)
total_orders=0
for n in 0 1 2 3; do
  c=$(docker exec fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$n -sN -e 'select count(*) from orders_0' 2>/dev/null)
  echo "shard $n: $c orders"
  total_orders=$((total_orders + c))
done
echo "final redis stock: $stock"
echo "total orders: $total_orders"
echo "equation: stock $stock + orders $total_orders should equal 1000 (initial)"
[[ $((stock + total_orders)) -eq 1000 ]] && echo "CORRECTNESS: PASS" || echo "CORRECTNESS: FAIL"
