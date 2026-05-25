// Package breaker wraps sony/gobreaker with our config types.
package breaker

import (
	"time"

	"github.com/sony/gobreaker"
)

type Config struct {
	Name         string
	MaxRequests  uint32
	Interval     time.Duration
	Timeout      time.Duration
	FailureRatio float64
}

type CB struct{ cb *gobreaker.CircuitBreaker }

func New(c Config) *CB {
	threshold := c.FailureRatio
	if threshold <= 0 {
		threshold = 0.5
	}
	return &CB{cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        c.Name,
		MaxRequests: c.MaxRequests,
		Interval:    c.Interval,
		Timeout:     c.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < 5 {
				return false
			}
			return float64(counts.TotalFailures)/float64(counts.Requests) >= threshold
		},
	})}
}

// Do runs fn through the breaker; returns gobreaker.ErrOpenState when open.
func (b *CB) Do(fn func() error) error {
	_, err := b.cb.Execute(func() (any, error) { return nil, fn() })
	return err
}
