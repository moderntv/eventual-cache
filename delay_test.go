package eventual

import (
	"testing"
	"time"
)

// storedName reads the replica without Get: a Get for an unknown ID queues it
// for a load of its own, which would bypass the delay under test.
func storedName(c *Cache[testItem], ID int64) string {
	e, exists := c.entryOf(ID)
	if !exists {
		return ""
	}

	return e.value.Load().Name
}

func TestDelayedIDsDrainOnlyWhatIsDue(t *testing.T) {
	d := newDelayedIDs()
	d.add(1, 100)
	d.add(2, 200)

	got := d.drainDue(nil, 150)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected only ID 1 to be due, got %v", got)
	}

	if d.size() != 1 {
		t.Fatalf("expected ID 2 to stay, %d IDs left", d.size())
	}
}

func TestDelayedIDsAddMovesTheDueTime(t *testing.T) {
	d := newDelayedIDs()
	d.add(1, 100)
	d.add(1, 300)

	got := d.drainDue(nil, 200)
	if len(got) != 0 {
		t.Fatalf("a repeated add did not move the due time, drained %v", got)
	}

	got = d.drainDue(nil, 300)
	if len(got) != 1 {
		t.Fatalf("expected ID 1 at its new due time, got %v", got)
	}
}

func TestInvalidateWaitsForInvalidateDelay(t *testing.T) {
	const delay = 300 * time.Millisecond

	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.InvalidateDelay = delay
	})

	source.set(1, "changed")
	source.set(999, "brand new")

	start := time.Now()
	c.Invalidate(1)
	c.Invalidate(999)

	ok := waitFor(5*time.Second, func() bool {
		return storedName(c, 1) == "changed" && storedName(c, 999) == "brand new"
	})
	if !ok {
		t.Fatalf("the invalidated items were not reloaded")
	}

	elapsed := time.Since(start)
	if elapsed < delay {
		t.Fatalf("reloaded after %v, InvalidateDelay is %v", elapsed, delay)
	}
}

func TestRepeatedInvalidationRestartsTheDelay(t *testing.T) {
	const delay = 500 * time.Millisecond

	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.InvalidateDelay = delay
	})

	before := source.requestCount(1)

	c.Invalidate(1)
	time.Sleep(100 * time.Millisecond)

	source.set(1, "changed")
	last := time.Now()
	c.Invalidate(1)

	ok := waitFor(5*time.Second, func() bool { return storedName(c, 1) == "changed" })
	if !ok {
		t.Fatalf("item 1 was never reloaded")
	}

	// with the first due time kept, the load would come 400 ms after the last one
	elapsed := time.Since(last)
	if elapsed < delay {
		t.Fatalf("reloaded %v after the last invalidation, InvalidateDelay is %v", elapsed, delay)
	}

	got := source.requestCount(1) - before
	if got != 1 {
		t.Fatalf("2 invalidations waiting at once asked the source %d times", got)
	}
}

// An item already waiting for a reload must still get a load of its own after
// the delay: the pending one may be exactly the load that comes too early.
func TestDelayedInvalidationIgnoresThePendingFlag(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.InvalidateDelay = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	})

	e, _ := c.entryOf(1)
	e.invalidated.Store(true)

	c.Invalidate(1)

	if c.delayed.size() != 1 {
		t.Fatalf("the invalidation of an item already marked was swallowed")
	}
}

func TestInvalidateDelayDoesNotDelayMisses(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.InvalidateDelay = time.Hour
	})

	source.set(999, "brand new")

	ok := waitFor(5*time.Second, func() bool { return name(c, 999) == "brand new" })
	if !ok {
		t.Fatalf("a Get for an unknown ID was held back by InvalidateDelay")
	}
}

func TestInvalidateDelayDoesNotDelayReconciliation(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.InvalidateDelay = time.Hour
		p.Timeouts.SyncInterval = 20 * time.Millisecond
	})

	// a newer version, found only by the reconciliation
	source.set(1, "changed")

	ok := waitFor(5*time.Second, func() bool { return name(c, 1) == "changed" })
	if !ok {
		t.Fatalf("an outdated item was held back by InvalidateDelay")
	}
}
