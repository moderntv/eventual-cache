package eventual

import "sync/atomic"

// rateLimiter allows at most limit events per second. It is a coarse window
// counter and not a token bucket: it sits on the miss path of Get, so it must not
// take a lock and must not call time.Now - it is handed the coarse clock instead.
//
// A nil rateLimiter allows everything.
type rateLimiter struct {
	limit int64

	// window is the current second, count the events already allowed in it.
	window atomic.Int64
	count  atomic.Int64
}

func newRateLimiter(perSecond int) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}

	return &rateLimiter{limit: int64(perSecond)}
}

// allow reports whether one more event fits into the current second.
//
// Two callers crossing a second boundary at the same time can lose or double
// count a few events, which is fine for a throttle: it is here to keep a flood of
// lookups off the source, not to meter anything.
func (r *rateLimiter) allow(nowMillis int64) bool {
	if r == nil {
		return true
	}

	window := nowMillis / 1000

	current := r.window.Load()
	if window != current {
		swapped := r.window.CompareAndSwap(current, window)
		if swapped {
			r.count.Store(0)
		}
	}

	return r.count.Add(1) <= r.limit
}
