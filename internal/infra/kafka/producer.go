package kafka

import (
	"context"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	otelpkg "go.opentelemetry.io/otel"

	fdotel "github.com/mjyang04/flash-deal/internal/infra/otel"
)

// Producer wraps a kafka-go Writer for typed sends.
type Producer struct {
	writer  *segkafka.Writer
	timeout time.Duration
}

// NewProducer builds a Writer that auto-creates the topic on first publish.
// Use Close() on shutdown.
func NewProducer(brokers []string, produceTimeout time.Duration) (*Producer, error) {
	w := &segkafka.Writer{
		Addr:                   segkafka.TCP(brokers...),
		Balancer:               &segkafka.Hash{}, // by message key
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           segkafka.RequireAll,
	}
	return &Producer{writer: w, timeout: produceTimeout}, nil
}

// SendOrder publishes a single message with key=user_id so per-user ordering is preserved.
// W3C traceparent is injected into kafka headers when an OTel propagator is registered.
func (p *Producer) SendOrder(ctx context.Context, topic string, m OrderMessage) error {
	b, err := m.Marshal()
	if err != nil {
		return err
	}
	carrier := fdotel.KafkaHeaderCarrier{}
	otelpkg.GetTextMapPropagator().Inject(ctx, &carrier)
	cctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.writer.WriteMessages(cctx, segkafka.Message{
		Topic:   topic,
		Key:     []byte(strconv.FormatInt(m.UserID, 10)),
		Value:   b,
		Time:    m.ProducedAt,
		Headers: []segkafka.Header(carrier),
	})
}

// Close flushes pending writes.
func (p *Producer) Close() error { return p.writer.Close() }
