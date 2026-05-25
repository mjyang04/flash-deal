package service

import (
	"context"
	"time"

	"github.com/mjyangnb/flash-deal/internal/domain"
	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
)

// KafkaOrderCreator implements OrderCreator + OrderCreatorWithToken; instead
// of writing the order row synchronously it publishes to Kafka. Consumer side
// materializes the row idempotently into MySQL.
type KafkaOrderCreator struct {
	producer *fdkafka.Producer
	topic    string
}

func NewKafkaOrderCreator(p *fdkafka.Producer, topic string) *KafkaOrderCreator {
	return &KafkaOrderCreator{producer: p, topic: topic}
}

// Create satisfies OrderCreator (no token threading — fallback path).
func (k *KafkaOrderCreator) Create(ctx context.Context, o domain.Order) error {
	return k.CreateWithToken(ctx, o, o.IdempotencyToken)
}

// CreateWithToken builds an OrderMessage and publishes it.
func (k *KafkaOrderCreator) CreateWithToken(ctx context.Context, o domain.Order, queueToken string) error {
	msg := fdkafka.OrderMessage{
		Version:          1,
		OrderID:          o.ID,
		ActivityID:       o.ActivityID,
		UserID:           o.UserID,
		ProductID:        o.ProductID,
		IdempotencyToken: o.IdempotencyToken,
		QueueToken:       queueToken,
		ProducedAt:       time.Now().UTC(),
	}
	return k.producer.SendOrder(ctx, k.topic, msg)
}
