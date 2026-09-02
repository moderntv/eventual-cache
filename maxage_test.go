package eventual

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

// expire pushes the item's refresh deadline into the past, which is how these
// tests age an item without sleeping.
func expire(t *testing.T, c *Cache[testItem], IDs ...int64) {
	t.Helper()

	for _, ID := range IDs {
		e, exists := c.entryOf(ID)
		if !exists {
			t.Fatalf("item %d is missing", ID)
		}

		if e.refreshAt.Load() == 0 {
			t.Fatalf("item %d has no refresh deadline, MaxAge is off", ID)
		}

		e.refreshAt.Store(time.Now().UnixNano() - 1)
	}
}

// TestRefreshAtStaysZeroWithoutMaxAge is the regression guard for every current
// consumer: with MaxAge off the field is never written and nothing expires.
func TestRefreshAtStaysZeroWithoutMaxAge(t *testing.T) {
	source := newTestSource(3)
	c := newTestCache(t, source, nil)

	IDs := testIDs(3)

	for _, ID := range IDs {
		e, exists := c.entryOf(ID)
		if !exists {
			t.Fatalf("item %d is missing", ID)
		}

		if e.refreshAt.Load() != 0 {
			t.Fatalf("item %d got a refresh deadline even though MaxAge is off", ID)
		}
	}

	// a reload must not set one either
	c.store(IDs[0], &testItem{ID: IDs[0], Name: "reloaded"}, 99)

	e, _ := c.entryOf(IDs[0])
	if e.refreshAt.Load() != 0 {
		t.Fatalf("a reload set a refresh deadline even though MaxAge is off")
	}
}

// TestWarmUpCohortIsSpreadOverTheWholeMaxAge is the reason the first store uses a
// different rule than a reload. The warm-up stores the whole dataset within a few
// milliseconds, so a deadline of now+MaxAge for all of it would expire the whole
// replica in one window - a million reloads in a few minutes, on every instance.
func TestWarmUpCohortIsSpreadOverTheWholeMaxAge(t *testing.T) {
	const (
		items   = 500
		buckets = 10
		maxAge  = time.Hour
	)

	source := newTestSource(items)

	before := time.Now().UnixNano()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = maxAge
		p.Timeouts.Randomizer = 0.1
	})

	after := time.Now().UnixNano()

	var counts [buckets]int

	for _, ID := range testIDs(items) {
		e, exists := c.entryOf(ID)
		if !exists {
			t.Fatalf("item %d is missing", ID)
		}

		refreshAt := e.refreshAt.Load()
		if refreshAt <= before {
			t.Fatalf("item %d expires immediately: %d ns into the past", ID, before-refreshAt)
		}

		if refreshAt > after+int64(maxAge) {
			t.Fatalf("item %d expires %v past MaxAge", ID, time.Duration(refreshAt-after-int64(maxAge)))
		}

		index := (refreshAt - before) * buckets / int64(maxAge)
		counts[min(index, buckets-1)]++
	}

	// 500 items drawn uniformly leave a bucket empty with probability 0.9^500,
	// so an empty one means the deadlines are not spread at all
	for i, count := range counts {
		if count == 0 {
			t.Fatalf("no item expires in bucket %d of %d: %v", i, buckets, counts)
		}
	}
}

// TestAReloadSetsTheDeadlineAFullMaxAgeAhead is the other half of the rule: once
// the replica is phased, every further reload measures the full MaxAge from the
// load. Spreading a reload over (0, MaxAge] as well would keep pulling the whole
// replica back towards the start of the period.
func TestAReloadSetsTheDeadlineAFullMaxAgeAhead(t *testing.T) {
	const maxAge = time.Hour

	source := newTestSource(3)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = maxAge
		p.Timeouts.Randomizer = 0
	})

	ID := testIDs(3)[0]

	before := time.Now().UnixNano()
	c.store(ID, &testItem{ID: ID, Name: "reloaded"}, 99)
	after := time.Now().UnixNano()

	e, _ := c.entryOf(ID)

	got := e.refreshAt.Load()
	if got < before+int64(maxAge) || got > after+int64(maxAge) {
		t.Fatalf(
			"a reload must set the deadline a full MaxAge ahead, it is %v away",
			time.Duration(got-after),
		)
	}
}

// TestSyncReloadsAnExpiredItem is what the whole feature is for: the source
// cannot report versions, the invalidation was lost, and the only thing left that
// can notice is the age.
func TestSyncReloadsAnExpiredItem(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour // driven by hand
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
	})

	ID := testIDs(10)[0]

	// no Invalidate call anywhere - this is the message that got lost
	source.set(ID, "changed")
	expire(t, c, ID)

	l := c.newLoader()
	c.sync(c.ctx, l)
	c.reloadMarked(l)

	got := name(c, ID)
	if got != "changed" {
		t.Fatalf("the expired item was not reloaded, got %q", got)
	}
}

// TestSyncLeavesItemsWithinMaxAgeAlone is the other half: without it the age
// check would reload the whole replica on every run.
func TestSyncLeavesItemsWithinMaxAgeAlone(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
	})

	c.sync(c.ctx, c.newLoader())

	got := c.pending.size()
	if got != 0 {
		t.Fatalf("a reconciliation over a fresh replica queued %d reloads", got)
	}
}

// TestAnExpiredItemIsServedUntilTheReloadFinishes is the property the consumers
// depend on: Get returning nil means "the source does not have it", so a hole in
// a warm replica is a wrong answer, not a slow one.
func TestAnExpiredItemIsServedUntilTheReloadFinishes(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour // nothing drains the pending set
		p.Timeouts.MaxAge = time.Hour
	})

	ID := testIDs(10)[0]
	expire(t, c, ID)

	c.sync(c.ctx, c.newLoader())

	e, exists := c.entryOf(ID)
	if !exists {
		t.Fatalf("the expired item is gone")
	}

	if !e.invalidated.Load() {
		t.Fatalf("the expired item was not marked for a reload")
	}

	// marked, queued, and nothing has loaded it yet - it still has to answer
	got := name(c, ID)
	if got != "item" {
		t.Fatalf("an expired item must keep serving its old value, got %q", got)
	}
}

// TestAnExpiredItemIsNeverDeleted is the difference between this and a TTL, and
// the reason the feature is not called one.
func TestAnExpiredItemIsNeverDeleted(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
	})

	IDs := testIDs(10)

	l := c.newLoader()

	// reconciliation after reconciliation with nothing ever reloading them, so
	// the items stay expired the whole time
	for i := 0; i < 5; i++ {
		expire(t, c, IDs...)
		c.sync(c.ctx, l)
	}

	got := c.itemsCount.Load()
	if got != 10 {
		t.Fatalf("expired items were dropped: %d of 10 left", got)
	}

	for _, ID := range IDs {
		if c.Get(ID) == nil {
			t.Fatalf("expired item %d disappeared", ID)
		}

		e, _ := c.entryOf(ID)
		if e.markedForDeletion.Load() {
			t.Fatalf("expired item %d was marked for deletion", ID)
		}
	}
}

// TestMaxRefreshPerSyncCapsAgeReloads bounds what the age may cost the source in
// one run. The leftover is not dropped, the next run takes another batch.
func TestMaxRefreshPerSyncCapsAgeReloads(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Shards = 1 // one shard, so the per shard budget is the whole cap
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
		p.MaxRefreshPerSync = 4
	})

	expire(t, c, testIDs(10)...)

	l := c.newLoader()

	// a reload restarts the age clock, so each run works through the items still
	// expired: 4, 4, and the last 2
	for run, want := range []int{4, 4, 2} {
		c.sync(c.ctx, l)

		got := c.pending.size()
		if got != want {
			t.Fatalf("run %d queued %d reloads, expected %d", run+1, got, want)
		}

		c.reloadMarked(l)
	}
}

// TestMaxRefreshPerSyncDoesNotCapVersionReloads - the cap is for hygiene, and
// hygiene must never delay correctness. A version the source reports as newer
// means the replica is wrong now.
func TestMaxRefreshPerSyncDoesNotCapVersionReloads(t *testing.T) {
	source := newTestSource(10)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Shards = 1
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
		p.MaxRefreshPerSync = 1
	})

	// every item changed and every invalidation was lost
	for _, ID := range testIDs(10) {
		source.set(ID, "changed")
	}

	c.sync(c.ctx, c.newLoader())

	got := c.pending.size()
	if got != 10 {
		t.Fatalf("the cap delayed version reloads: %d of 10 queued", got)
	}
}

// TestMaxRefreshPerSyncDoesNotCapDeletions - deletions are correctness too, and
// they are not reloads at all.
func TestMaxRefreshPerSyncDoesNotCapDeletions(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Shards = 1
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
		p.MaxRefreshPerSync = 1
	})

	for _, ID := range testIDs(10) {
		source.remove(ID)
	}

	l := c.newLoader()

	// mark on the first run, delete on the second
	c.sync(c.ctx, l)
	c.sync(c.ctx, l)

	got := c.itemsCount.Load()
	if got != 0 {
		t.Fatalf("the cap delayed deletions: %d items still in the replica", got)
	}
}

// TestPerShardBudgetIsRoundedUp - splitting the cap between the shards must not
// round down to zero, which would switch MaxAge off without saying so.
func TestPerShardBudgetIsRoundedUp(t *testing.T) {
	source := newTestSource(64)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Shards = 16
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
		p.MaxRefreshPerSync = 4 // fewer than the shards
	})

	expire(t, c, testIDs(64)...)

	c.sync(c.ctx, c.newLoader())

	got := c.pending.size()
	if got == 0 {
		t.Fatalf("a cap below the shard count silently disabled the age check")
	}
}

// TestAgeMetrics - without sync_age_deferred there is no way to tell a MaxAge
// that works from a cap that is quietly never catching up.
func TestAgeMetrics(t *testing.T) {
	source := newTestSource(10)
	source.hideVersions()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Shards = 1
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
		p.MaxRefreshPerSync = 4
	})

	expire(t, c, testIDs(10)...)

	c.sync(c.ctx, c.newLoader())

	got := testutil.ToFloat64(c.metrics.SyncExpiredCount)
	if got != 4 {
		t.Fatalf("expected 4 items marked because of their age, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.SyncAgeDeferredCount)
	if got != 6 {
		t.Fatalf("expected 6 items deferred to the next run by the cap, got %v", got)
	}

	// the age never counts as an outdated version - they are different causes
	// and are tuned by different knobs
	got = testutil.ToFloat64(c.metrics.SyncOutdatedCount)
	if got != 0 {
		t.Fatalf("age reloads were counted as version reloads: %v", got)
	}
}

// TestReloadFromInvalidateResetsTheAge - the age is the time since the last
// successful load, whatever caused it, so an invalidation restarts the clock.
// Without this an item invalidated often would still be reloaded by the age.
func TestReloadFromInvalidateResetsTheAge(t *testing.T) {
	source := newTestSource(3)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
		p.Timeouts.MaxAge = time.Hour
	})

	ID := testIDs(3)[0]
	expire(t, c, ID)

	source.set(ID, "changed")
	c.Invalidate(ID)
	c.reloadMarked(c.newLoader())

	e, _ := c.entryOf(ID)

	got := e.refreshAt.Load()
	if got <= time.Now().UnixNano() {
		t.Fatalf("the reload did not restart the age clock, the item is still expired")
	}
}
