package kafka

import (
	"context"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

// Handler processes one message and returns nil on success.
// If it returns a non-nil error, Consumer.Run dispatches the message to dlqHandler.
type Handler func(ctx context.Context, m OrderMessage) error

// DLQHandler is called when Handler permanently fails. It should publish to a DLQ topic.
type DLQHandler func(ctx context.Context, raw []byte, reason error)

// Consumer wraps kafka-go Reader with manual commit semantics.
type Consumer struct {
	reader     *segkafka.Reader
	handler    Handler
	dlq        DLQHandler
	batchWait  time.Duration
	maxRetries int
}

// NewConsumer builds a Reader configured for manual offset commits.
func NewConsumer(brokers []string, topic, group string, batchWait time.Duration, maxRetries int) *Consumer {
	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit
		MaxWait:        batchWait,
	})
	return &Consumer{reader: r, batchWait: batchWait, maxRetries: maxRetries}
}

// SetHandler / SetDLQ — caller wires in the business callbacks before Run.
func (c *Consumer) SetHandler(h Handler) { c.handler = h }
func (c *Consumer) SetDLQ(d DLQHandler)  { c.dlq = d }

// Run blocks until ctx is canceled. Each message is retried up to maxRetries
// (simple linear backoff) before going to DLQ.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
		}
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		om, err := UnmarshalOrderMessage(msg.Value)
		if err != nil {
			if c.dlq != nil {
				c.dlq(ctx, msg.Value, err)
			}
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}
		var lastErr error
		for attempt := 0; attempt <= c.maxRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			}
			if lastErr = c.handler(ctx, om); lastErr == nil {
				break
			}
		}
		if lastErr != nil && c.dlq != nil {
			c.dlq(ctx, msg.Value, lastErr)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// Close stops the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
