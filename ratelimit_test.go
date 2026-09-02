package eventual

import (
	"testing"
	"time"
)

func TestRateLimiterAllowsExactlyTheLimitPerWindow(t *testing.T) {
	r := newRateLimiter(5)
	now := int64(1_700_000_000_000)

	allowed := 0
	for i := 0; i < 100; i++ {
		if r.allow(now) {
			allowed++
		}
	}

	if allowed != 5 {
		t.Fatalf("expected 5 allowed events in one window, got %d", allowed)
	}
}

func TestRateLimiterStartsANewWindowEverySecond(t *testing.T) {
	r := newRateLimiter(5)
	now := int64(1_700_000_000_000)

	for i := 0; i < 100; i++ {
		r.allow(now)
	}

	// the next second gets its own budget
	allowed := 0
	for i := 0; i < 100; i++ {
		if r.allow(now + 1000) {
			allowed++
		}
	}

	if allowed != 5 {
		t.Fatalf("expected 5 allowed events in the next window, got %d", allowed)
	}
}

// TestRateLimiterKeepsRejectingWithinAWindow guards the fast path: once the
// budget is used up every further call must be rejected, however many there are.
func TestRateLimiterKeepsRejectingWithinAWindow(t *testing.T) {
	r := newRateLimiter(2)
	now := int64(1_700_000_000_000)

	r.allow(now)
	r.allow(now)

	for i := 0; i < 1000; i++ {
		if r.allow(now) {
			t.Fatalf("call %d was allowed after the window budget ran out", i+3)
		}
	}
}

func TestNilRateLimiterAllowsEverything(t *testing.T) {
	var r *rateLimiter

	for i := 0; i < 10; i++ {
		if !r.allow(0) {
			t.Fatalf("a nil rate limiter must allow everything")
		}
	}
}

// TestMissLimiterWindowAdvancesWhileTheSourceIsSlow pins the reason the limiter
// reads the wall clock directly. Everything in the background runs in one
// goroutine, so a slow source blocks it; if the limiter's window came from a
// clock that goroutine had to tick, the window would stand still and every miss
// would be rejected until the source recovered.
func TestMissLimiterWindowAdvancesWhileTheSourceIsSlow(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MissRateLimit = 1
		p.Timeouts.RefreshInterval = 5 * time.Millisecond
	})

	// send the background goroutine into a load it will not come back from soon
	source.loadDelay.Store(int64(5 * time.Second))
	c.Invalidate(1)

	ok := waitFor(2*time.Second, func() bool { return source.inFlight.Load() > 0 })
	if !ok {
		t.Fatalf("the background goroutine never entered the slow load")
	}

	// one miss fits in this second, the second one does not
	c.Get(900001)
	c.Get(900002)

	got := c.pending.size()
	if got != 1 {
		t.Fatalf("expected 1 queued miss with MissRateLimit 1, got %d", got)
	}

	// a new second, and the background goroutine is still stuck in the load
	time.Sleep(1200 * time.Millisecond)

	c.Get(900003)

	got = c.pending.size()
	if got != 2 {
		t.Fatalf("the next second did not get its own budget, %d IDs queued in total", got)
	}

	source.loadDelay.Store(0)
}
