// Package domain holds pure domain types for flash-deal — no infra deps.
package domain

import "time"

// Activity is a flash-sale event.
type Activity struct {
	ID           int64          `json:"id"`
	ProductID    int64          `json:"product_id"`
	TotalStock   int            `json:"total_stock"`
	StartAt      time.Time      `json:"start_at"`
	EndAt        time.Time      `json:"end_at"`
	PerUserLimit int            `json:"per_user_limit"`
	Status       ActivityStatus `json:"status"`
}

// ActivityStatus enumerates lifecycle states.
type ActivityStatus int8

const (
	ActivityDraft ActivityStatus = iota
	ActivityScheduled
	ActivityRunning
	ActivityFinished
	ActivityCanceled
)

// Order represents a materialized purchase.
type Order struct {
	ID               int64       `json:"id"`
	UserID           int64       `json:"user_id"`
	ActivityID       int64       `json:"activity_id"`
	ProductID        int64       `json:"product_id"`
	Status           OrderStatus `json:"status"`
	IdempotencyToken string      `json:"idempotency_token"`
	CreatedAt        time.Time   `json:"created_at"`
}

// OrderStatus enumerates order lifecycle.
type OrderStatus int8

const (
	OrderQueued OrderStatus = iota
	OrderPaid
	OrderCanceled
	OrderRefunded
)

// SeckillRequest is the inbound API payload.
type SeckillRequest struct {
	ActivityID       int64  `json:"activity_id" binding:"required"`
	UserID           int64  `json:"user_id"     binding:"required"`
	IdempotencyToken string `json:"idempotency_token" binding:"required"`
}

// SeckillResult is what we hand back to the client after Redis-side reservation.
type SeckillResult struct {
	Status     string `json:"status"` // queued | sold_out | exceeded | duplicate
	QueueToken string `json:"queue_token,omitempty"`
	Remaining  int    `json:"remaining,omitempty"`
}

// SeckillOutcome enumerates terminal states of a single seckill attempt.
type SeckillOutcome int

const (
	OutcomeQueued SeckillOutcome = iota
	OutcomeSoldOut
	OutcomeUserLimit
	OutcomeDuplicate
	OutcomeNotStarted
	OutcomeEnded
	OutcomeNotFound
	OutcomeInternal
)

// String returns the canonical wire-name used in the JSON response status field.
func (o SeckillOutcome) String() string {
	switch o {
	case OutcomeQueued:
		return "queued"
	case OutcomeSoldOut:
		return "sold_out"
	case OutcomeUserLimit:
		return "user_limit"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeNotStarted:
		return "not_started"
	case OutcomeEnded:
		return "ended"
	case OutcomeNotFound:
		return "not_found"
	default:
		return "internal"
	}
}
