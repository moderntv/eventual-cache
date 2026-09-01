package eventual

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvalidateDoesNotHitTheSourceSynchronously(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = time.Hour
	})

	before := source.loadMultipleCalls.Load()

	c.Invalidate(1)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("Invalidate loaded synchronously")
	}
}

func TestInvalidationsAreLoadedInOneBatch(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	before := source.loadMultipleCalls.Load()

	source.set(1, "changed")
	source.set(7, "changed")
	source.set(13, "changed")

	c.Invalidate(1)
	c.Invalidate(7)
	c.Invalidate(13)

	ok := waitFor(5*time.Second, func() bool { return name(c, 13) == "changed" })
	if !ok {
		t.Fatalf("the invalidated items were not reloaded")
	}

	if name(c, 1) != "changed" || name(c, 7) != "changed" {
		t.Fatalf("only part of the batch was reloaded")
	}

	got := source.loadMultipleCalls.Load() - before
	if got != 1 {
		t.Fatalf("expected 1 batch load for 3 invalidations, got %d", got)
	}
}

func TestRepeatedInvalidationOfTheSameIDCollapses(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 50 * time.Millisecond
	})

	before := source.requestCount(1)

	for i := 0; i < 100; i++ {
		c.Invalidate(1)
	}

	ok := waitFor(5*time.Second, func() bool { return source.requestCount(1) > before })
	if !ok {
		t.Fatalf("item 1 was never reloaded")
	}

	// one run of the loop may ask the source for the ID exactly once
	got := source.requestCount(1) - before
	if got != 1 {
		t.Fatalf("100 invalidations asked the source %d times", got)
	}
}

func TestAFullBatchIsLoadedWithoutWaitingForTheTick(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.BatchSize = 3
		// only a full batch can trigger the load
		p.Timeouts.RefreshInterval = time.Hour
	})

	source.set(1, "changed")

	c.Invalidate(1)
	c.Invalidate(7)
	c.Invalidate(13)

	ok := waitFor(5*time.Second, func() bool { return name(c, 1) == "changed" })
	if !ok {
		t.Fatalf("a full batch did not trigger a load")
	}
}

func TestInvalidateRemovesAnItemTheSourceNoLongerHas(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	source.remove(7)
	c.Invalidate(7)

	ok := waitFor(5*time.Second, func() bool { return c.Get(7) == nil })
	if !ok {
		t.Fatalf("item 7 is still in the replica")
	}

	if c.Get(1) == nil {
		t.Fatalf("item 1 was removed too")
	}
}

func TestInvalidateLoadsAnItemTheReplicaDoesNotHaveYet(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	source.set(999, "brand new")
	c.Invalidate(999)

	ok := waitFor(5*time.Second, func() bool { return name(c, 999) == "brand new" })
	if !ok {
		t.Fatalf("the new item was not loaded")
	}
}

func TestFailedReloadKeepsTheItemMarkedAndTheValueIntact(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	setErr(&source.loadErr, errors.New("source down"))
	source.set(1, "changed")

	before := source.loadMultipleCalls.Load()
	c.Invalidate(1)

	// a failed batch is retried, so the invalidation is not lost
	ok := waitFor(5*time.Second, func() bool { return source.loadMultipleCalls.Load() >= before+2 })
	if !ok {
		t.Fatalf("the failed batch was not retried")
	}

	if name(c, 1) != "item" {
		t.Fatalf("a failed load changed the value to %q", name(c, 1))
	}

	setErr(&source.loadErr, nil)

	ok = waitFor(5*time.Second, func() bool { return name(c, 1) == "changed" })
	if !ok {
		t.Fatalf("the item was not reloaded after the source recovered")
	}
}

// TestPerItemErrorDoesNotRemoveTheItem covers a loader that answers with an error
// for one ID: the cache must not read that as "the source lost it".
func TestPerItemErrorDoesNotRemoveTheItem(t *testing.T) {
	source := newTestSource(3)

	failing := errors.New("row is locked")

	// the error starts only after the warm-up, so item 7 gets into the replica
	// first and we can watch whether the failed reload throws it out
	var failItem7 atomic.Bool

	calls := atomic.Int64{}

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
		p.LoadMultipleFunc = func(ctx context.Context, IDs []int64) ([]LoadedEntry[testItem], error) {
			calls.Add(1)

			entries := make([]LoadedEntry[testItem], 0, len(IDs))
			for _, ID := range IDs {
				if ID == 7 && failItem7.Load() {
					entries = append(entries, LoadedEntry[testItem]{ID: ID, Err: failing})

					continue
				}

				value := &testItem{ID: ID, Name: "item"}
				entries = append(entries, LoadedEntry[testItem]{ID: ID, Value: value})
			}

			return entries, nil
		}
	})

	if c.Get(7) == nil {
		t.Fatalf("item 7 was never loaded")
	}

	failItem7.Store(true)

	before := calls.Load()
	c.Invalidate(7)

	ok := waitFor(5*time.Second, func() bool { return calls.Load() > before })
	if !ok {
		t.Fatalf("the invalidated item was never reloaded")
	}

	time.Sleep(100 * time.Millisecond)

	if c.Get(7) == nil {
		t.Fatalf("a per item error removed the item")
	}
}

// TestNotFoundErrorRemovesTheItem is the other half: ErrNotFound is proof.
func TestNotFoundErrorRemovesTheItem(t *testing.T) {
	source := newTestSource(3)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
		p.LoadMultipleFunc = func(ctx context.Context, IDs []int64) ([]LoadedEntry[testItem], error) {
			entries := make([]LoadedEntry[testItem], 0, len(IDs))
			for _, ID := range IDs {
				if ID == 7 {
					entries = append(entries, LoadedEntry[testItem]{ID: ID, Err: ErrNotFound})

					continue
				}

				value := &testItem{ID: ID, Name: "item"}
				entries = append(entries, LoadedEntry[testItem]{ID: ID, Value: value})
			}

			return entries, nil
		}
	})

	if c.Get(7) != nil {
		t.Fatalf("an ErrNotFound answer must not put the item in the replica")
	}

	if c.Get(1) == nil {
		t.Fatalf("item 1 is missing")
	}
}

// TestGetQueuesUnknownIDs is the behaviour asked for explicitly: Get returns nil
// straight away, but the ID lands in the queue and the goroutine tries it.
func TestGetQueuesUnknownIDs(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	// the source got a new item and nobody told the cache
	source.set(999, "appeared")

	if c.Get(999) != nil {
		t.Fatalf("Get must return nil for an item the replica does not have")
	}

	ok := waitFor(5*time.Second, func() bool { return name(c, 999) == "appeared" })
	if !ok {
		t.Fatalf("the Get of an unknown ID did not queue a load")
	}
}

func TestGetOfANonexistentIDIsRateLimited(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MissRateLimit = 5
		p.Timeouts.RefreshInterval = time.Hour
	})

	for i := int64(0); i < 1000; i++ {
		// IDs the source does not have either
		if c.Get(100000+i) != nil {
			t.Fatalf("Get returned an item the source does not have")
		}
	}

	// the limiter starts with a full bucket of MissRateLimit tokens
	got := c.pending.size()
	if got > 6 {
		t.Fatalf("1000 lookups queued %d IDs, the limit is 5 per second", got)
	}
}

func TestInvalidateIsNotRateLimited(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MissRateLimit = 1
		p.Timeouts.RefreshInterval = time.Hour
	})

	for i := int64(0); i < 100; i++ {
		c.Invalidate(100000 + i)
	}

	got := c.pending.size()
	if got != 100 {
		t.Fatalf("expected 100 pending IDs, got %d", got)
	}
}

func TestExpiredTTLKeepsServingTheValueAndQueuesAReload(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.TTL = 10 * time.Millisecond
		p.Timeouts.RefreshInterval = time.Hour // only Get may trigger the reload
	})

	source.set(1, "changed")

	// wait for the TTL and for the coarse clock to notice
	time.Sleep(300 * time.Millisecond)

	// the value is still served even though it is stale
	got := name(c, 1)
	if got != "item" {
		t.Fatalf("an expired item must still be served, got %q", got)
	}

	ok := waitFor(5*time.Second, func() bool { return c.pending.size() > 0 })
	if !ok {
		t.Fatalf("the expired item was not queued for a reload")
	}

	e, exists := c.entryOf(1)
	if !exists {
		t.Fatalf("item 1 disappeared")
	}

	if !e.invalidated.Load() {
		t.Fatalf("the expired item is not marked")
	}
}

func TestExpiredTTLIsReloadedByTheGoroutine(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.TTL = 10 * time.Millisecond
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
	})

	source.set(1, "changed")

	// the reload needs a Get to notice the TTL first
	ok := waitFor(5*time.Second, func() bool {
		_ = c.Get(1)

		return name(c, 1) == "changed"
	})
	if !ok {
		t.Fatalf("the stale item was never reloaded")
	}
}

// TestNoTTLMeansNoAutomaticReload makes sure TTL 0 keeps the cache quiet.
func TestNoTTLMeansNoAutomaticReload(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.TTL = 0
		p.Timeouts.RefreshInterval = 5 * time.Millisecond
	})

	before := source.loadMultipleCalls.Load()

	for i := 0; i < 100; i++ {
		for _, ID := range testIDs(10) {
			_ = c.Get(ID)
		}
	}

	time.Sleep(100 * time.Millisecond)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("reads triggered %d loads with no TTL set", got-before)
	}
}
