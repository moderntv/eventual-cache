package eventual

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	t.Run("removes_deleted_items", testSyncRemovesDeletedItems)
	t.Run("adds_new_items", testSyncAddsNewItems)
	t.Run("error_leaves_replica_untouched", testSyncErrorLeavesReplicaUntouched)
	t.Run("item_created_during_sync_survives", testSyncItemCreatedDuringSyncSurvives)
	t.Run("item_refreshed_during_sync_survives", testSyncItemRefreshedDuringSyncSurvives)
	t.Run("tombstone_is_dropped_when_item_reappears", testSyncTombstoneDroppedWhenItemReappears)
	t.Run("stats_and_hook", testSyncStatsAndHook)
	t.Run("periodic_sync_runs", testSyncPeriodicRuns)
	t.Run("no_allocations_when_nothing_changed", testSyncNoAllocationsWhenIdle)
}

func testSyncRemovesDeletedItems(t *testing.T) {
	t.Parallel()

	source := newTestSource(100)
	c := newTestCache(t, source, nil)

	assert.Equal(t, 100, c.Len())

	source.remove(7)
	source.remove(13)

	assert.NoError(t, c.Sync(context.Background()))

	assert.Equal(t, 98, c.Len())
	assert.Nil(t, c.Get(7))
	assert.NotNil(t, c.Get(1))
}

func testSyncAddsNewItems(t *testing.T) {
	t.Parallel()

	source := newTestSource(100)
	c := newTestCache(t, source, nil)

	source.set(500_001, "new one")
	source.set(500_007, "new two")

	assert.NoError(t, c.Sync(context.Background()))

	assert.Equal(t, 102, c.Len())
	if assert.NotNil(t, c.Get(500_001)) {
		assert.Equal(t, "new one", c.Get(500_001).Name)
	}
}

func testSyncErrorLeavesReplicaUntouched(t *testing.T) {
	t.Parallel()

	source := newTestSource(100)
	c := newTestCache(t, source, nil)

	setErr(&source.listIDsErr, errors.New("connection refused"))

	err := c.Sync(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 100, c.Len(), "a failed listing must never delete anything")
	assert.NotNil(t, c.Get(1))
}

// An item created while the reconciliation is running is naturally missing from
// its snapshot of IDs. It must not be deleted.
func testSyncItemCreatedDuringSyncSurvives(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	const newID = 700_001

	source.listIDsDelay.Store(int64(200 * time.Millisecond))
	source.omitFromList(newID)

	done := make(chan error, 1)
	go func() { done <- c.Sync(context.Background()) }()

	// the item appears in the source and an invalidation brings it in
	time.Sleep(50 * time.Millisecond)
	source.set(newID, "born during sync")
	c.Invalidate(newID)

	assert.True(t, waitFor(time.Second, func() bool { return c.Get(newID) != nil }))

	assert.NoError(t, <-done)

	assert.NotNil(t, c.Get(newID), "an item inserted during the sync must survive the sweep")
	assert.Equal(t, 11, c.Len())
}

// An item refreshed while the reconciliation is running must survive even when
// the snapshot of IDs does not contain it (a stale read of the ID list).
func testSyncItemRefreshedDuringSyncSurvives(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	const (
		refreshedID = int64(7)
		untouchedID = int64(13)
	)

	source.listIDsDelay.Store(int64(200 * time.Millisecond))
	// the ID listing does not see either of them, even though the source has both
	source.omitFromList(refreshedID, untouchedID)

	done := make(chan error, 1)
	go func() { done <- c.Sync(context.Background()) }()

	time.Sleep(50 * time.Millisecond)
	source.set(refreshedID, "refreshed during sync")
	c.Invalidate(refreshedID)

	assert.True(t, waitFor(time.Second, func() bool {
		value := c.Get(refreshedID)

		return value != nil && value.Name == "refreshed during sync"
	}))

	assert.NoError(t, <-done)

	assert.NotNil(t, c.Get(refreshedID), "an item refreshed during the sync must survive the sweep")
	assert.Nil(t, c.Get(untouchedID), "an item the source no longer lists must be removed")
}

func testSyncTombstoneDroppedWhenItemReappears(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	const lateID = 800_001

	// a lookup for an unknown ID leaves a tombstone
	assert.Nil(t, c.Get(lateID))
	assert.True(t, waitFor(2*time.Second, func() bool { return c.Stats().Tombstones == 1 }))

	// the item is created in the source; reconciliation has to notice
	source.set(lateID, "late arrival")
	assert.NoError(t, c.Sync(context.Background()))

	assert.True(t, waitFor(2*time.Second, func() bool { return c.Get(lateID) != nil }))
	assert.Equal(t, 0, c.Stats().Tombstones)
}

func testSyncStatsAndHook(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)

	hookStats := make(chan SyncStats, 8)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.OnSync = func(stats SyncStats) { hookStats <- stats }
	})

	// the warm-up already reports through the hook
	<-hookStats

	source.remove(7)
	source.set(900_001, "added")

	assert.NoError(t, c.Sync(context.Background()))

	stats := <-hookStats
	assert.Equal(t, 10, stats.Total)
	assert.Equal(t, 1, stats.Added)
	assert.Equal(t, 1, stats.Removed)
	assert.NoError(t, stats.Err)

	cacheStats := c.Stats()
	assert.Equal(t, 10, cacheStats.Items)
	assert.False(t, cacheStats.LastSyncAt.IsZero())
	assert.Equal(t, 1, cacheStats.LastSyncStats.Added)
}

func testSyncPeriodicRuns(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 30 * time.Millisecond
	})

	source.remove(7)

	assert.True(t, waitFor(3*time.Second, func() bool { return c.Get(7) == nil }))
	assert.GreaterOrEqual(t, source.listIDsCalls.Load(), int64(1))
}

// The reconciliation buffers are reused between runs, so a run over an unchanged
// dataset must not allocate per item.
func testSyncNoAllocationsWhenIdle(t *testing.T) {
	t.Parallel()

	source := newTestSource(5000)
	c := newTestCache(t, source, nil)

	ctx := context.Background()

	// warm the buffers up
	assert.NoError(t, c.Sync(ctx))

	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = c.Sync(ctx)
		}
	})

	perItem := float64(result.AllocedBytesPerOp()) / 5000
	t.Logf("sync over 5000 items: %d B/op, %d allocs/op (%.2f B per item)",
		result.AllocedBytesPerOp(), result.AllocsPerOp(), perItem)

	// the fake source itself allocates the ID slice on every call, so the budget
	// is not zero - but it must stay well under one allocation per item
	assert.Less(t, result.AllocsPerOp(), int64(500))
}
