#!/usr/bin/env bash
# Chaos: kill consumer mid-load, restart, verify lag drains + correctness.
# Uses xargs -P for capped concurrency (avoids unbounded fork bomb).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$ROOT"
PATH=/opt/homebrew/bin:$PATH
export PATH

cleanup() {
  pkill -f /tmp/fd-api 2>/dev/null || true
  pkill -f /tmp/fd-consumer 2>/dev/null || true
}
trap cleanup EXIT

INITIAL_STOCK=${INITIAL_STOCK:-1000}
BATCH1=${BATCH1:-300}
BATCH2=${BATCH2:-300}
CONCURRENCY=${CONCURRENCY:-20}

echo "==> reset state (mysql + redis + kafka topic)"
docker exec fd-mysql mysql -uroot -prootpw -e \
  'truncate flashdeal.activities; truncate flashdeal_0.orders_0; truncate flashdeal_1.orders_0; truncate flashdeal_2.orders_0; truncate flashdeal_3.orders_0' > /dev/null 2>&1
docker exec fd-redis redis-cli flushdb > /dev/null
# Wipe kafka topic so historical messages don't poison the consumer group
docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic seckill_orders 2>/dev/null || true
docker exec fd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic seckill_orders --partitions 16 --replication-factor 1 > /dev/null 2>&1
FD_RECONCILE_ACTIVITY_IDS=1001 make seed > /dev/null
docker exec fd-redis redis-cli set stock:1001 "$INITIAL_STOCK" > /dev/null
docker exec -i fd-mysql mysql -uflashdeal -pflashdeal -e \
  "update activities set total_stock=$INITIAL_STOCK where id=1001" flashdeal > /dev/null 2>&1

echo "==> build"
go build -o /tmp/fd-api ./cmd/api
go build -o /tmp/fd-consumer ./cmd/consumer

# Use a unique consumer group per chaos run so historical messages don't dominate.
GROUP="chaos-$(date +%s)"
export FD_KAFKA_CONSUMER_GROUP="$GROUP"

echo "==> start consumer (group=$GROUP)"
FD_KAFKA_CONSUMER_GROUP="$GROUP" FD_RATE_LIMIT_PER_USER_PER_MINUTE=10000 \
  /tmp/fd-consumer > /tmp/chaos-consumer.log 2>&1 &
CONS_PID=$!
sleep 8

echo "==> start api"
FD_RATE_LIMIT_PER_USER_PER_MINUTE=10000 /tmp/fd-api > /tmp/chaos-api.log 2>&1 &
API_PID=$!
sleep 3

post_one() {
  local i=$1
  local uid=$((i % 1000 + 1))
  curl -sS -m 3 -o /dev/null -w "%{http_code}\n" \
    -X POST -H 'Content-Type: application/json' -H "X-User-Id: $uid" \
    -d "{\"activity_id\":1001,\"user_id\":$uid,\"idempotency_token\":\"chaos-$i\"}" \
    http://localhost:8080/v1/seckill
}
export -f post_one

echo "==> batch 1: $BATCH1 reqs (consumer ALIVE)"
seq 1 "$BATCH1" | xargs -I{} -P "$CONCURRENCY" bash -c 'post_one {}' > /tmp/chaos-b1.codes 2>/dev/null
echo "    codes: $(sort /tmp/chaos-b1.codes | uniq -c | tr '\n' ' ')"

sleep 3   # let consumer drain
EARLY_ORDERS=0
for n in 0 1 2 3; do
  c=$(docker exec fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$n -sN -e 'select count(*) from orders_0' 2>/dev/null)
  EARLY_ORDERS=$((EARLY_ORDERS + c))
done
echo "==> orders after batch1 + 3s settle: $EARLY_ORDERS"

echo "==> KILL consumer (PID $CONS_PID)"
kill "$CONS_PID" 2>/dev/null || true
wait "$CONS_PID" 2>/dev/null || true
sleep 2

echo "==> batch 2: $BATCH2 reqs (consumer DEAD — Kafka should pile up)"
seq $((BATCH1+1)) $((BATCH1+BATCH2)) | xargs -I{} -P "$CONCURRENCY" bash -c 'post_one {}' > /tmp/chaos-b2.codes 2>/dev/null
echo "    codes: $(sort /tmp/chaos-b2.codes | uniq -c | tr '\n' ' ')"

MID_ORDERS=0
for n in 0 1 2 3; do
  c=$(docker exec fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$n -sN -e 'select count(*) from orders_0' 2>/dev/null)
  MID_ORDERS=$((MID_ORDERS + c))
done
echo "==> orders while consumer DOWN: $MID_ORDERS (should == $EARLY_ORDERS, lag in kafka)"

echo "==> RESTART consumer (same group=$GROUP)"
FD_KAFKA_CONSUMER_GROUP="$GROUP" FD_RATE_LIMIT_PER_USER_PER_MINUTE=10000 \
  /tmp/fd-consumer > /tmp/chaos-consumer-2.log 2>&1 &
sleep 30

echo "==> final state"
STOCK=$(docker exec fd-redis redis-cli get stock:1001)
TOTAL=0
for n in 0 1 2 3; do
  c=$(docker exec fd-mysql mysql -uflashdeal -pflashdeal flashdeal_$n -sN -e 'select count(*) from orders_0' 2>/dev/null)
  echo "    shard $n: $c orders"
  TOTAL=$((TOTAL + c))
done
echo "    redis stock remaining: $STOCK"
echo "    total orders: $TOTAL"
EXPECTED=$((INITIAL_STOCK))
SUM=$((STOCK + TOTAL))
echo "    equation: $STOCK (redis) + $TOTAL (orders) = $SUM,  expected = $EXPECTED"
if [[ $SUM -eq $EXPECTED ]]; then
  echo "==> CORRECTNESS: PASS"
  exit 0
else
  DRIFT=$((EXPECTED - SUM))
  echo "==> CORRECTNESS: DRIFT=$DRIFT (stock leak or over-mat)"
  exit 1
fi
