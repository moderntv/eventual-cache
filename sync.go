package eventual

import (
	"context"
	"errors"
	"time"

	"github.com/moderntv/eventual-cache/internal/utils"
)

func (c *Cache[T]) startPeriodicSync() {
	defer c.wg.Done()

	for {
		interval := utils.RandomizeDuration(c.timeouts.SyncInterval, c.timeouts.Randomizer)
		timer := time.NewTimer(interval)

		select {
		case <-c.ctx.Done():
			timer.Stop()

			return

		case <-timer.C:
			_ = c.Sync(c.ctx)
		}
	}
}

// Sync reconciles the replica with the source: it fetches the list of all IDs,
// removes items the source no longer has and loads items the replica is missing.
// It does not re-read the content of items the source still has - that is what
// invalidations are for.
//
// It is called periodically on its own; call it directly to force a
// reconciliation. Only one reconciliation runs at a time.
func (c *Cache[T]) Sync(ctx context.Context) (err error) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()

	start := time.Now()
	// everything stored from now on is newer than this run
	startSeq := c.writeSeq.Load()

	if c.metrics != nil {
		c.metrics.SyncRunsCount.Inc()
	}

	IDs, err := c.listIDsFunc(ctx)
	if err != nil {
		c.log.Warn().Err(err).Msg("listing source IDs failed, replica left untouched")

		if c.metrics != nil {
			c.metrics.SyncErrorsCount.Inc()
		}

		c.finishSync(SyncStats{Duration: time.Since(start), Err: err}, false)

		return err
	}

	clear(c.available)
	for _, ID := range IDs {
		c.available[ID] = struct{}{}
	}

	removed, refreshed := c.sweep(startSeq)
	added := c.fill(ctx, IDs)

	stats := SyncStats{
		Total:     len(IDs),
		Added:     added,
		Removed:   removed,
		Refreshed: refreshed,
		Duration:  time.Since(start),
	}

	c.log.Info().
		Int("total", stats.Total).
		Int("added", stats.Added).
		Int("removed", stats.Removed).
		Int("refreshed", stats.Refreshed).
		Int("items", c.Len()).
		Float64("duration_s", stats.Duration.Round(time.Millisecond).Seconds()).
		Msg("reconciliation finished")

	c.finishSync(stats, true)

	return nil
}

// Reload loads the whole dataset again and reconciles the replica with it. It is
// used for the warm-up in New and by InvalidateAll. Only one reload runs at a
// time; a concurrent call returns ErrReloadInProgress.
func (c *Cache[T]) Reload(ctx context.Context) (err error) {
	if !c.reloading.CompareAndSwap(false, true) {
		return ErrReloadInProgress
	}
	defer c.reloading.Store(false)

	c.syncMu.Lock()
	defer c.syncMu.Unlock()

	start := time.Now()
	startSeq := c.writeSeq.Load()

	if c.metrics != nil {
		c.metrics.ReloadsCount.Inc()
	}

	entries, err := c.loadAllFunc(ctx)
	if err != nil {
		c.log.Warn().Err(err).Msg("full reload failed, replica left untouched")

		if c.metrics != nil {
			c.metrics.SyncErrorsCount.Inc()
		}

		return err
	}

	clear(c.available)

	nowMillis := start.UnixMilli()

	added := 0
	for _, le := range entries {
		// the ID is known to exist even when its load failed, so it is kept in
		// the replica and only its value stays as it was
		c.available[le.ID] = struct{}{}

		if le.Err != nil && !errors.Is(le.Err, ErrNotFound) {
			c.log.Warn().Err(le.Err).Int64("id", le.ID).Msg("item load failed during reload")

			continue
		}

		if c.store(le, nowMillis) {
			added++
		}
	}

	removed, refreshed := c.sweep(startSeq)

	stats := SyncStats{
		Total:     len(c.available),
		Added:     added,
		Removed:   removed,
		Refreshed: refreshed,
		Duration:  time.Since(start),
	}

	c.log.Info().
		Int("total", stats.Total).
		Int("added", stats.Added).
		Int("removed", stats.Removed).
		Int("items", c.Len()).
		Float64("duration_s", stats.Duration.Round(time.Millisecond).Seconds()).
		Msg("full reload finished")

	c.finishSync(stats, true)

	return nil
}

// sweep removes items the source does not have and queues a refresh for
// tombstones the source has again.
//
// An item is removed only when it was stored BEFORE this run started. Without
// that condition an item created or refreshed while the reconciliation was
// running (its ID is naturally missing from the snapshot) would be thrown away.
func (c *Cache[T]) sweep(startSeq uint64) (removed, refreshed int) {
	nowMillis := time.Now().UnixMilli()

	c.forEachShardParallel(func(index int, sh *shard[T]) {
		toDelete := c.shardDelete[index][:0]
		toRefresh := c.shardRefresh[index][:0]

		sh.mu.RLock()
		for ID, e := range sh.data {
			if _, inSource := c.available[ID]; !inSource {
				if e.seq.Load() <= startSeq {
					toDelete = append(toDelete, ID)
				}

				continue
			}

			// the source has the item again but we hold a tombstone
			if e.value.Load() == nil {
				toRefresh = append(toRefresh, ID)
			}
		}
		sh.mu.RUnlock()

		localRemoved := 0

		if len(toDelete) > 0 {
			sh.mu.Lock()
			for _, ID := range toDelete {
				e, exists := sh.data[ID]
				if !exists {
					continue
				}

				// written in the meantime - keep it
				if e.seq.Load() > startSeq {
					continue
				}

				value := e.value.Load()
				// a tombstone which has not expired yet is still useful
				if value == nil && e.expiresAt.Load() > nowMillis {
					continue
				}

				delete(sh.data, ID)

				if value != nil {
					c.itemsCount.Add(-1)
				} else {
					c.tombstonesCount.Add(-1)
				}
				localRemoved++
			}
			sh.mu.Unlock()
		}

		for _, ID := range toRefresh {
			c.enqueueRefresh(ID)
		}

		c.shardDelete[index] = toDelete
		c.shardRefresh[index] = toRefresh
		c.shardCounts[index].removed = localRemoved
		c.shardCounts[index].refreshed = len(toRefresh)
	})

	for i := range c.shardCounts {
		removed += c.shardCounts[i].removed
		refreshed += c.shardCounts[i].refreshed
	}

	return
}

// fill loads the items the source has and the replica does not.
func (c *Cache[T]) fill(ctx context.Context, IDs []int64) (added int) {
	for i := range c.shardIDs {
		c.shardIDs[i] = c.shardIDs[i][:0]
	}

	for _, ID := range IDs {
		index := c.shardHash(ID, c.shardBits)
		c.shardIDs[index] = append(c.shardIDs[index], ID)
	}

	c.forEachShardParallel(func(index int, sh *shard[T]) {
		missing := c.shardMissing[index][:0]

		sh.mu.RLock()
		for _, ID := range c.shardIDs[index] {
			if _, exists := sh.data[ID]; !exists {
				missing = append(missing, ID)
			}
		}
		sh.mu.RUnlock()

		c.shardMissing[index] = missing
	})

	for i := range c.shardMissing {
		if len(c.shardMissing[i]) == 0 {
			continue
		}

		added += c.loadIDs(ctx, c.shardMissing[i])

		if ctx.Err() != nil {
			break
		}
	}

	return
}

func (c *Cache[T]) finishSync(stats SyncStats, ok bool) {
	c.lastSyncStats.Store(&stats)

	if ok {
		c.lastSyncAt.Store(time.Now().UnixMilli())
	}

	if c.metrics != nil {
		c.metrics.SyncAddedCount.Add(float64(stats.Added))
		c.metrics.SyncRemovedCount.Add(float64(stats.Removed))
		c.metrics.LastSyncDuration.Set(stats.Duration.Seconds())

		if ok {
			c.metrics.LastSyncTimestamp.Set(float64(time.Now().Unix()))
		}
	}

	if c.onSync != nil {
		c.onSync(stats)
	}
}
