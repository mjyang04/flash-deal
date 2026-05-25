//go:build integration

package kafka_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	fdkafka "github.com/mjyangnb/flash-deal/internal/infra/kafka"
)

func brokers(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("FD_TEST_KAFKA_BROKERS")
	if v == "" {
		v = "127.0.0.1:9092"
	}
	return strings.Split(v, ",")
}

func TestProducer_RoundTrip(t *testing.T) {
	topic := "test_producer_roundtrip"
	bs := brokers(t)
	p, err := fdkafka.NewProducer(bs, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	msg := fdkafka.OrderMessage{
		Version: 1, OrderID: 42, ActivityID: 100, UserID: 7,
		ProductID: 555, IdempotencyToken: "tok-x", QueueToken: "q-x",
		ProducedAt: time.Now().UTC(),
	}
	if err := p.SendOrder(context.Background(), topic, msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers: bs, Topic: topic, GroupID: "test_producer_roundtrip_group",
		MinBytes: 1, MaxBytes: 1e6,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rec, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	got, err := fdkafka.UnmarshalOrderMessage(rec.Value)
	if err != nil {
		t.Fatal(err)
	}
	if got.OrderID != 42 || got.IdempotencyToken != "tok-x" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}
