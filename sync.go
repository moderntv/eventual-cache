package eventual

import (
	"context"
	"time"
)

// syncStats describes one reconciliation run.
type syncStats struct {
	Total    int
	Added    int
	Marked   int
	Removed  int
	Duration time.Duration
}

// sync reconciles the replica with the source: items the source no longer has
// are removed, items the replica does not have yet are loaded. It does not
// re-read the content of items the replica already holds - that is what
// invalidations and the TTL are for.
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

	IDs, err := c.listIDsFunc(ctx)
	if err != nil {
		c.log.Warn().Err(err).Msg("listing source IDs failed, replica left untouched")

		if c.metrics != nil {
			c.metrics.ListIDsErrorsCount.Inc()
		}

		return
	}

	clear(c.available)
	for _, ID := range IDs {
		c.available[ID] = struct{}{}
	}

	marked, removed := c.sweep()
	added := c.fill(ctx, l, IDs)

	stats := syncStats{
		Total:    len(IDs),
		Added:    added,
		Marked:   marked,
		Removed:  removed,
		Duration: time.Since(start),
	}

	c.log.Info().
		Int("total", stats.Total).
		Int("added", stats.Added).
		Int("marked", stats.Marked).
		Int("removed", stats.Removed).
		Int64("items", c.itemsCount.Load()).
		Float64("duration_s", stats.Duration.Seconds()).
		Msg("reconciliation finished")

	c.finishSync(stats)
}

// sweep deals with the items the source did not report.
//
// Deletion takes two runs. The first run that does not find an ID only marks the
// item; the next run that still does not find it deletes it. One run is not
// enough evidence: an item loaded while the reconciliation was running is
// naturally missing from the ID snapshot the run started with, and the ID listing
// itself can be a stale read from a replica. Keeping a deleted item for one extra
// interval is the cheaper mistake - dropping an item that really exists means
// serving nil for it.
func (c *Cache[T]) sweep() (marked, removed int) {
	c.forEachShardParallel(func(index int, sh *shard[T]) {
		toDelete := c.shardDelete[index][:0]
		localMarked := 0

		sh.mu.RLock()
		for ID, e := range sh.data {
			_, inSource := c.available[ID]
			if inSource {
				// a plain load first - storing into every entry on every run would
				// dirty every cache line in the replica
				if e.markedForDeletion.Load() {
					e.markedForDeletion.Store(false)
				}

				continue
			}

			wasMarked := !e.markedForDeletion.CompareAndSwap(false, true)
			if !wasMarked {
				localMarked++

				continue
			}

			toDelete = append(toDelete, ID)
		}
		sh.mu.RUnlock()

		localRemoved := c.removeMarked(sh, toDelete)

		c.shardDelete[index] = toDelete
		c.shardCounts[index].marked = localMarked
		c.shardCounts[index].removed = localRemoved
	})

	for i := range c.shardCounts {
		marked += c.shardCounts[i].marked
		removed += c.shardCounts[i].removed
	}

	return
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
func (c *Cache[T]) fill(ctx context.Context, l *loader[T], IDs []int64) (added int) {
	missing := c.collectMissing(IDs)
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
func (c *Cache[T]) collectMissing(IDs []int64) (missing []int64) {
	for i := range c.shardIDs {
		c.shardIDs[i] = c.shardIDs[i][:0]
	}

	for _, ID := range IDs {
		index := c.shardHash(ID, c.shardBits)
		c.shardIDs[index] = append(c.shardIDs[index], ID)
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

	c.metrics.SyncAddedCount.Add(float64(stats.Added))
	c.metrics.SyncMarkedCount.Add(float64(stats.Marked))
	c.metrics.SyncRemovedCount.Add(float64(stats.Removed))
	c.metrics.LastSyncDuration.Set(stats.Duration.Seconds())
	c.metrics.LastSyncTimestamp.Set(float64(time.Now().Unix()))
}
