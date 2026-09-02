package eventual

import (
	"errors"
	"testing"
	"time"
)

func TestSyncRemovesItemsTheSourceNoLongerHas(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	source.remove(7)

	// deletion takes two runs: the first only marks the item
	watcher.wait(t, 3)

	if c.Get(7) != nil {
		t.Fatalf("item 7 was not removed")
	}

	if c.Get(1) == nil {
		t.Fatalf("item 1 disappeared")
	}

	got := c.itemsCount.Load()
	if got != 9 {
		t.Fatalf("expected 9 items, got %d", got)
	}
}

// TestSyncNeedsTwoRunsToRemoveAnItem is the whole point of the two-phase
// deletion: one reconciliation is never enough evidence.
func TestSyncNeedsTwoRunsToRemoveAnItem(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour // no reconciliation on its own
		p.afterSync = watcher.hook()
	})

	source.remove(7)

	l := c.newLoader()

	c.sync(c.ctx, l)

	if c.Get(7) == nil {
		t.Fatalf("the first reconciliation must only mark the item, not delete it")
	}

	e, exists := c.entryOf(7)
	if !exists {
		t.Fatalf("item 7 disappeared")
	}

	if !e.markedForDeletion.Load() {
		t.Fatalf("the first reconciliation did not mark the item")
	}

	c.sync(c.ctx, l)

	if c.Get(7) != nil {
		t.Fatalf("the second reconciliation did not delete the item")
	}
}

// TestSyncUnmarksAnItemThatCameBack covers the race the two-phase deletion is
// there for: an item loaded while a reconciliation was running is missing from
// that run's ID snapshot, so it gets marked - and the next run must clear it.
func TestSyncUnmarksAnItemThatCameBack(t *testing.T) {
	source := newTestSource(10)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
	})

	// the source has the item, its ID listing does not
	source.omitFromList(7)

	l := c.newLoader()
	c.sync(c.ctx, l)

	e, exists := c.entryOf(7)
	if !exists {
		t.Fatalf("the first reconciliation deleted the item instead of marking it")
	}

	if !e.markedForDeletion.Load() {
		t.Fatalf("item 7 is not marked")
	}

	// the listing catches up before the next run
	source.showInList(7)
	c.sync(c.ctx, l)

	if c.Get(7) == nil {
		t.Fatalf("the item was deleted even though the source reported it again")
	}

	e, exists = c.entryOf(7)
	if !exists {
		t.Fatalf("item 7 disappeared")
	}

	if e.markedForDeletion.Load() {
		t.Fatalf("the mark was not cleared")
	}
}

// TestReloadClearsTheDeletionMark - a successful load is proof the item exists,
// so it must survive the next reconciliation.
func TestReloadClearsTheDeletionMark(t *testing.T) {
	source := newTestSource(10)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	})

	source.omitFromList(7)

	l := c.newLoader()
	c.sync(c.ctx, l)

	e, exists := c.entryOf(7)
	if !exists || !e.markedForDeletion.Load() {
		t.Fatalf("item 7 is not marked for deletion")
	}

	// an invalidation reloads it, which proves the source has it
	c.Invalidate(7)
	c.reloadMarked(l)

	e, exists = c.entryOf(7)
	if !exists {
		t.Fatalf("item 7 disappeared")
	}

	if e.markedForDeletion.Load() {
		t.Fatalf("a successful reload did not clear the deletion mark")
	}

	c.sync(c.ctx, l)

	if c.Get(7) == nil {
		t.Fatalf("the item was deleted even though it had just been reloaded")
	}
}

func TestSyncLoadsItemsTheReplicaDoesNotHave(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = time.Hour
		p.afterSync = watcher.hook()
	})

	source.set(999, "appeared without an invalidation")

	watcher.wait(t, 2)

	got := name(c, 999)
	if got != "appeared without an invalidation" {
		t.Fatalf("the reconciliation did not pick the new item up, got %q", got)
	}
}

func TestSyncDoesNotRereadItemsTheReplicaAlreadyHas(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = time.Hour
		p.afterSync = watcher.hook()
	})

	before := source.loadMultipleCalls.Load()

	watcher.wait(t, 3)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("the reconciliation loaded %d batches, it had nothing to load", got-before)
	}
}

func TestSyncLeavesTheReplicaAloneWhenListIDsFails(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	setErr(&source.listIDsErr, errors.New("source down"))

	watcher.wait(t, 3)

	got := c.itemsCount.Load()
	if got != 10 {
		t.Fatalf("a failed reconciliation changed the replica: %d items", got)
	}

	for _, ID := range testIDs(10) {
		if c.Get(ID) == nil {
			t.Fatalf("item %d disappeared", ID)
		}
	}
}

func TestSyncLoadsMissingItemsInBatches(t *testing.T) {
	source := newTestSource(2)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.BatchSize = 4
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = time.Hour
		p.afterSync = watcher.hook()
	})

	source.maxBatch.Store(0)

	for i := int64(0); i < 20; i++ {
		source.set(1000+i, "appeared")
	}

	watcher.wait(t, 3)

	got := c.itemsCount.Load()
	if got != 22 {
		t.Fatalf("expected 22 items, got %d", got)
	}

	if source.maxBatch.Load() > 4 {
		t.Fatalf("the reconciliation called the loader with %d IDs, BatchSize is 4", source.maxBatch.Load())
	}
}

// TestSyncBuffersDoNotGrowWithRuns guards the reuse of the reconciliation
// buffers - the whole reason sync has them on the Cache.
func TestSyncBuffersDoNotGrowWithRuns(t *testing.T) {
	source := newTestSource(100)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	})

	l := c.newLoader()

	c.sync(c.ctx, l)
	firstCap := cap(c.syncMissing)

	for i := 0; i < 20; i++ {
		c.sync(c.ctx, l)
	}

	if cap(c.syncMissing) != firstCap {
		t.Fatalf("the missing buffer grew from %d to %d over 20 runs", firstCap, cap(c.syncMissing))
	}
}

// TestSyncReloadsItemWhoseVersionGrew is the safety net for a lost invalidation:
// the source bumped the version and never told us, so only the reconciliation
// can notice.
func TestSyncReloadsItemWhoseVersionGrew(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = 5 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	// no Invalidate call anywhere - this is the message that got lost
	source.set(7, "changed")

	ok := waitFor(5*time.Second, func() bool { return name(c, 7) == "changed" })
	if !ok {
		t.Fatalf("the reconciliation did not reload an item whose version grew")
	}
}

// TestSyncLeavesUnchangedItemsAlone is the other half: a reconciliation over a
// dataset that did not change must not reload anything. Without this the
// version check would turn every sync into a full reload of the replica.
func TestSyncLeavesUnchangedItemsAlone(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = 5 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	watcher.wait(t, 1)

	before := source.loadMultipleCalls.Load()

	watcher.wait(t, 3)
	time.Sleep(50 * time.Millisecond)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("reconciliations over an unchanged dataset triggered %d loads", got-before)
	}
}

// TestSyncIgnoresAVersionOlderThanTheStoredOne covers a stale read from a
// replica: the listing reports a version we are already past, which is not a
// reason to reload anything.
func TestSyncIgnoresAVersionOlderThanTheStoredOne(t *testing.T) {
	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour    // driven by hand
		p.Timeouts.RefreshInterval = time.Hour // nothing drains the pending set
	})

	e, exists := c.entryOf(7)
	if !exists {
		t.Fatalf("item 7 is missing")
	}

	// pretend we already hold something newer than anything the source lists
	e.version.Store(1 << 40)

	// the reconciliation only queues reloads, it does not perform them, so the
	// pending set is what says whether it decided to reload anything
	c.sync(c.ctx, c.newLoader())

	got := c.pending.size()
	if got != 0 {
		t.Fatalf("a reconciliation over an up-to-date replica queued %d reloads", got)
	}
}

// TestInvalidationDoesNotCauseASecondReloadAtTheNextSync is why LoadMultipleFunc
// reports the version of the content it returns. If it did not, the stored
// version would stay behind and every invalidated item would be loaded twice -
// once by the invalidation and once by the next reconciliation.
func TestInvalidationDoesNotCauseASecondReloadAtTheNextSync(t *testing.T) {
	source := newTestSource(10)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = time.Hour // driven by hand
		p.Timeouts.RefreshInterval = 5 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	source.set(7, "changed")
	c.Invalidate(7)

	ok := waitFor(5*time.Second, func() bool { return name(c, 7) == "changed" })
	if !ok {
		t.Fatalf("the invalidation was never applied")
	}

	before := source.loadMultipleCalls.Load()

	c.sync(c.ctx, c.newLoader())
	time.Sleep(50 * time.Millisecond)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("the reconciliation reloaded an item the invalidation had just loaded (%d loads)", got-before)
	}
}
