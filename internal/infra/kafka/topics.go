// Package kafka holds Kafka producer/consumer primitives used by api and consumer.
package kafka

import (
	"encoding/json"
	"time"
)

// Topic constants — single source of truth used by both producer and consumer.
const (
	TopicSeckillOrders = "seckill_orders"
	TopicSeckillDLQ    = "seckill_orders_dlq"
)

// OrderMessage is the envelope produced by api after Redis Lua succeeds.
// Consumer materializes it into MySQL orders table.
type OrderMessage struct {
	Version          int       `json:"version"`
	OrderID          int64     `json:"order_id"`
	ActivityID       int64     `json:"activity_id"`
	UserID           int64     `json:"user_id"`
	ProductID        int64     `json:"product_id"`
	IdempotencyToken string    `json:"idempotency_token"`
	QueueToken       string    `json:"queue_token"`
	ProducedAt       time.Time `json:"produced_at"`
}

// Marshal returns the JSON bytes of the message.
func (m OrderMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalOrderMessage decodes JSON into OrderMessage.
func UnmarshalOrderMessage(b []byte) (OrderMessage, error) {
	var m OrderMessage
	err := json.Unmarshal(b, &m)
	return m, err
}
