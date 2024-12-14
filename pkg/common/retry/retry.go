package retry

import (
	"math"
	"time"
)

// RetryStrategy defines the interface for retrying failed message processing.
type RetryStrategy interface {
	NextInterval(attempt int) time.Duration
	MaxRetries() int
}

// ExponentialBackoff implements a retry strategy with exponential backoff.
type ExponentialBackoff struct {
	initialInterval time.Duration
	factor          float64
	maxInterval     time.Duration
	maxRetries      int
}

// NewExponentialBackoff creates a new ExponentialBackoff with the specified parameters.
func NewExponentialBackoff(initial time.Duration, factor float64, max time.Duration, maxRetries int) *ExponentialBackoff {
	return &ExponentialBackoff{
		initialInterval: initial,
		factor:          factor,
		maxInterval:     max,
		maxRetries:      maxRetries,
	}
}

func (e *ExponentialBackoff) NextInterval(attempt int) time.Duration {
	backoff := float64(e.initialInterval) * pow(e.factor, float64(attempt-1))
	if backoff > float64(e.maxInterval) {
		return e.maxInterval
	}
	return time.Duration(backoff)
}

func (e *ExponentialBackoff) MaxRetries() int {
	return e.maxRetries
}

func pow(a, b float64) float64 {
	return math.Pow(a, b)
}
