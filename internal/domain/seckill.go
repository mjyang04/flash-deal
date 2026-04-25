// Package domain holds pure domain types for flash-deal — no infra deps.
package domain

import "time"

// Activity is a flash-sale event.
type Activity struct {
	ID            int64
	ProductID     int64
	TotalStock    int
	StartAt       time.Time
	EndAt         time.Time
	PerUserLimit  int
	Status        ActivityStatus
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
	ID                int64
	UserID            int64
	ActivityID        int64
	ProductID         int64
	Status            OrderStatus
	IdempotencyToken  string
	CreatedAt         time.Time
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
	Status     string `json:"status"`      // queued | sold_out | exceeded | duplicate
	QueueToken string `json:"queue_token,omitempty"`
	Remaining  int    `json:"remaining,omitempty"`
}
