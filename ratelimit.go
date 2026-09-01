package eventual

import (
	"sync"
	"time"
)

// rateLimiter is a token bucket. A nil rateLimiter allows everything.
type rateLimiter struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	rate   float64
	last   time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}

	return &rateLimiter{
		tokens: float64(perSecond),
		max:    float64(perSecond),
		rate:   float64(perSecond),
		last:   time.Now(),
	}
}

func (r *rateLimiter) allow() bool {
	if r == nil {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.tokens += now.Sub(r.last).Seconds() * r.rate
	if r.tokens > r.max {
		r.tokens = r.max
	}
	r.last = now

	if r.tokens < 1 {
		return false
	}
	r.tokens--

	return true
}
