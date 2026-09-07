package eventual

import (
	"context"
	"time"
)

// sweepCounts is what one sweep decided. It is a struct and not a handful of
// return values because they are all ints and all mean something different.
type sweepCounts struct {
	marked   int
	removed  int
	outdated int
	// expired counts items marked for a reload because of their age, as opposed
	// to outdated, which counts those the source reported with a newer version.
	expired int
	// ageDeferred counts items past MaxAge that MaxRefreshPerSync left for the
	// next run.
	ageDeferred int
}

// syncStats describes one reconciliation run.
type syncStats struct {
	sweepCounts

	total    int
	added    int
	duration time.Duration
}

// sync reconciles the replica with the source: items the source no longer has
// are removed, items the replica does not have yet are loaded, and items the
// source reports with a newer version are queued for a reload. It is the safety
// net for invalidations that never arrived.
//
// It only decides what to do - the reloads it queues are performed by the next
// refresh tick, like any other invalidation.
//
// Only run() calls it, so the buffers on the Cache need no lock.
func (c *Cache[T]) sync(ctx context.Context, l *loader[T]) {
	start := time.Now()

	if c.afterSync != nil {
		defer c.afterSync()
	}

	if c.metrics != nil {
		c.metrics.SyncRunsCount.Inc()
	}

	items, err := c.listIDsFunc(ctx)
	if err != nil {
		c.log.Warn().Err(err).Msg("listing source IDs failed, replica left untouched")

		if c.metrics != nil {
			c.metrics.ListIDsErrorsCount.Inc()
		}

		return
	}

	clear(c.available)
	for _, item := range items {
		c.available[item.ID] = item.Version
	}

	counts := c.sweep()
	added := c.fill(ctx, l, items)

	stats := syncStats{
		sweepCounts: counts,
		total:       len(items),
		added:       added,
		duration:    time.Since(start),
	}

	c.log.Debug().
		Int("total", stats.total).
		Int("added", stats.added).
		Int("marked", stats.marked).
		Int("removed", stats.removed).
		Int("outdated", stats.outdated).
		Int("expired", stats.expired).
		Int("age_deferred", stats.ageDeferred).
		Int64("items", c.itemsCount.Load()).
		Float64("duration_s", stats.duration.Seconds()).
		Msg("reconciliation finished")

	c.finishSync(stats)
}

// sweep walks the replica and compares it against what the source reported.
//
// An item the source did not report at all is deleted, but only on the second
// run in a row that does not find it. One run is not enough evidence: an item
// loaded while the reconciliation was running is naturally missing from the ID
// snapshot the run started with, and the ID listing itself can be a stale read
// from a replica. Keeping a deleted item for one extra interval is the cheaper
// mistake - dropping an item that really exists means serving nil for it.
//
// An item the source reports with a version newer than the stored one is marked
// for a reload, which is how a lost invalidation is eventually noticed. The
// reload itself goes through the same machinery as Invalidate.
//
// An item that has outlived Timeouts.MaxAge is marked for a reload too. That is
// the same safety net for a source which cannot report versions, and unlike the
// other two it is hygiene rather than correctness, so MaxRefreshPerSync may put
// some of it off until the next run.
func (c *Cache[T]) sweep() (counts sweepCounts) {
	// read once for the whole run: on a large replica a clock read per item would
	// cost more than the rest of the sweep
	now := time.Now().UnixNano()
	capped := c.ageBudgetPerShard > 0

	c.forEachShardParallel(func(index int, sh *shard[T]) {
		toDelete := c.shardDelete[index][:0]
		toReload := c.shardReload[index][:0]
		local := sweepCounts{}

		// the budget is per shard and lives in the worker, so the sweep stays
		// free of any shared counter the parallel workers would fight over
		budget := c.ageBudgetPerShard

		sh.mu.RLock()
		for ID, e := range sh.data {
			version, inSource := c.available[ID]
			if inSource {
				// a plain load first - storing into every entry on every run would
				// dirty every cache line in the replica
				if e.markedForDeletion.Load() {
					e.markedForDeletion.Store(false)
				}

				if version > e.version.Load() {
					toReload = append(toReload, ID)
					local.outdated++

					continue
				}

				// the backstop for a lost invalidation when the source has no
				// versions to compare. The item is only queued for a reload, never
				// dropped - Get keeps answering with the old value until the reload
				// lands.
				refreshAt := e.refreshAt.Load()
				if refreshAt == 0 || now <= refreshAt {
					continue
				}

				// out of budget: the item stays expired and the next run takes it,
				// which is what makes the cap a delay and not a loss
				if capped && budget == 0 {
					local.ageDeferred++

					continue
				}

				toReload = append(toReload, ID)
				budget--
				local.expired++

				continue
			}

			wasMarked := !e.markedForDeletion.CompareAndSwap(false, true)
			if !wasMarked {
				local.marked++

				continue
			}

			toDelete = append(toDelete, ID)
		}
		sh.mu.RUnlock()

		local.removed = c.removeMarked(sh, toDelete)

		// marking takes the pending mutex, so it happens outside the shard lock
		for _, ID := range toReload {
			e, exists := c.entryOf(ID)
			if !exists {
				continue
			}

			c.markEntry(ID, e)
		}

		c.shardDelete[index] = toDelete
		c.shardReload[index] = toReload
		c.shardCounts[index].sweepCounts = local
	})

	for i := range c.shardCounts {
		counts.marked += c.shardCounts[i].marked
		counts.removed += c.shardCounts[i].removed
		counts.outdated += c.shardCounts[i].outdated
		counts.expired += c.shardCounts[i].expired
		counts.ageDeferred += c.shardCounts[i].ageDeferred
	}

	return
}

// ageBudgetPerShard splits MaxRefreshPerSync between the shards, which is what
// lets a sweep enforce the cap without a counter the parallel workers share. The
// price is that the cap holds only roughly, and for a hygienic operation that is
// a good trade.
//
// It rounds up: a cap smaller than the shard count would otherwise leave every
// shard with a budget of zero and switch MaxAge off without saying so.
//
// 0 means no cap.
func ageBudgetPerShard(maxRefreshPerSync, shards int) int {
	if maxRefreshPerSync <= 0 {
		return 0
	}

	return (maxRefreshPerSync + shards - 1) / shards
}

// removeMarked deletes the given IDs from the shard, skipping any that were
// reloaded in the meantime. It takes the shard write lock once for the whole
// batch instead of once per ID.
func (c *Cache[T]) removeMarked(sh *shard[T], IDs []int64) (removed int) {
	if len(IDs) == 0 {
		return 0
	}

	sh.mu.Lock()
	for _, ID := range IDs {
		e, exists := sh.data[ID]
		if !exists {
			continue
		}

		// a load between the scan and here cleared the mark, so the source has the
		// item after all
		stillMarked := e.markedForDeletion.Load()
		if !stillMarked {
			continue
		}

		delete(sh.data, ID)
		removed++
	}
	sh.mu.Unlock()

	if removed > 0 {
		c.itemsCount.Add(int64(-removed))
	}

	return
}

// fill loads the items the source has and the replica does not.
func (c *Cache[T]) fill(ctx context.Context, l *loader[T], items []SourceItem) (added int) {
	missing := c.collectMissing(items)
	if len(missing) == 0 {
		return 0
	}

	added, _, err := l.loadAll(ctx, missing)
	if err != nil {
		c.log.Warn().Err(err).Int("count", len(missing)).Msg("loading missing items failed")
	}

	return
}

// collectMissing returns the IDs the source has and the replica does not. It
// walks the shards in parallel, because on a large dataset this is the most
// expensive part of a reconciliation.
func (c *Cache[T]) collectMissing(items []SourceItem) (missing []int64) {
	for i := range c.shardIDs {
		c.shardIDs[i] = c.shardIDs[i][:0]
	}

	for _, item := range items {
		index := c.shardHash(item.ID, c.shardBits)
		c.shardIDs[index] = append(c.shardIDs[index], item.ID)
	}

	c.forEachShardParallel(func(index int, sh *shard[T]) {
		shardMissing := c.shardMissing[index][:0]

		sh.mu.RLock()
		for _, ID := range c.shardIDs[index] {
			_, exists := sh.data[ID]
			if exists {
				continue
			}

			shardMissing = append(shardMissing, ID)
		}
		sh.mu.RUnlock()

		c.shardMissing[index] = shardMissing
	})

	missing = c.syncMissing[:0]
	for i := range c.shardMissing {
		missing = append(missing, c.shardMissing[i]...)
	}
	c.syncMissing = missing

	return
}

func (c *Cache[T]) finishSync(stats syncStats) {
	if c.metrics == nil {
		return
	}

	c.metrics.SyncAddedCount.Add(float64(stats.added))
	c.metrics.SyncMarkedCount.Add(float64(stats.marked))
	c.metrics.SyncRemovedCount.Add(float64(stats.removed))
	c.metrics.SyncOutdatedCount.Add(float64(stats.outdated))
	c.metrics.SyncExpiredCount.Add(float64(stats.expired))
	c.metrics.SyncAgeDeferredCount.Add(float64(stats.ageDeferred))
	c.metrics.LastSyncDuration.Set(stats.duration.Seconds())
	c.metrics.LastSyncTimestamp.Set(float64(time.Now().Unix()))
}
