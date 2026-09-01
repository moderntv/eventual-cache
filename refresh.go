package eventual

import (
	"context"
	"errors"
	"time"
)

// enqueueRefresh queues an existing item for a refresh, or - when the replica
// does not know the ID at all - queues it as a new item.
func (c *Cache[T]) enqueueRefresh(ID int64) {
	sh := c.shardOf(ID)

	sh.mu.RLock()
	e, exists := sh.data[ID]
	sh.mu.RUnlock()

	if !exists {
		// an explicit invalidation of an unknown ID usually means a newly created
		// item, so it is never rate limited
		c.enqueueUnknown(ID, false)

		return
	}

	c.enqueueEntry(ID, e)
}

// enqueueEntry queues an item the caller already has the entry of.
func (c *Cache[T]) enqueueEntry(ID int64, e *entry[T]) {
	if !e.refreshing.CompareAndSwap(false, true) {
		// a refresh is already running or queued - make sure it happens once more,
		// otherwise this invalidation would be swallowed
		e.dirty.Store(true)

		return
	}

	e.dirty.Store(false)

	select {
	case c.refreshCh <- ID:
		if c.metrics != nil {
			c.metrics.RefreshEnqueuedCount.Inc()
		}

	default:
		e.refreshing.Store(false)
		c.reportDropped(ID)
	}
}

// enqueueUnknown queues an ID the replica does not hold. Dedup happens before
// the rate limiter so that duplicates do not consume tokens.
func (c *Cache[T]) enqueueUnknown(ID int64, rateLimited bool) {
	if _, loaded := c.pending.LoadOrStore(ID, struct{}{}); loaded {
		return
	}

	if rateLimited && !c.missLimiter.allow() {
		c.pending.Delete(ID)

		return
	}

	select {
	case c.refreshCh <- ID:
		if c.metrics != nil {
			c.metrics.RefreshEnqueuedCount.Inc()
		}

	default:
		c.pending.Delete(ID)
		c.reportDropped(ID)
	}
}

func (c *Cache[T]) reportDropped(ID int64) {
	if c.metrics != nil {
		c.metrics.RefreshDroppedCount.Inc()
	}

	c.log.Warn().
		Int64("id", ID).
		Int("queue_size", cap(c.refreshCh)).
		Msg("refresh queue is full, request dropped")
}

// releaseGate clears the deduplication flags of an ID and queues it again when an
// invalidation arrived while it was being loaded.
func (c *Cache[T]) releaseGate(ID int64) {
	c.pending.Delete(ID)

	sh := c.shardOf(ID)

	sh.mu.RLock()
	e, exists := sh.data[ID]
	sh.mu.RUnlock()

	if !exists {
		return
	}

	e.refreshing.Store(false)

	if e.dirty.Load() {
		c.enqueueEntry(ID, e)
	}
}

// refresher holds the per worker buffers so that a refresh does not allocate.
type refresher[T any] struct {
	cache   *Cache[T]
	batch   []int64
	entries []LoadedEntry[T]
	loaded  map[int64]struct{}
	timer   *time.Timer
}

func (c *Cache[T]) startRefreshWorker() {
	defer c.wg.Done()

	r := &refresher[T]{
		cache:   c,
		batch:   make([]int64, 0, c.refreshBatchSize),
		entries: make([]LoadedEntry[T], 0, c.refreshBatchSize),
		loaded:  make(map[int64]struct{}, c.refreshBatchSize),
		timer:   time.NewTimer(time.Hour),
	}

	if !r.timer.Stop() {
		<-r.timer.C
	}
	defer r.timer.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return

		case ID := <-c.refreshCh:
			r.batch = append(r.batch[:0], ID)
		}

		r.collect()
		r.load()

		if c.ctx.Err() != nil {
			return
		}
	}
}

// collect waits a short while for more IDs so that they can be loaded in one call.
func (r *refresher[T]) collect() {
	r.timer.Reset(r.cache.refreshBatchDelay)

collecting:
	for len(r.batch) < r.cache.refreshBatchSize {
		select {
		case ID := <-r.cache.refreshCh:
			r.batch = append(r.batch, ID)

		case <-r.timer.C:
			return

		case <-r.cache.ctx.Done():
			break collecting
		}
	}

	if !r.timer.Stop() {
		select {
		case <-r.timer.C:
		default:
		}
	}
}

func (r *refresher[T]) load() {
	c := r.cache

	if c.metrics != nil {
		c.metrics.RefreshBatchCount.Inc()
		c.metrics.RefreshItemsCount.Add(float64(len(r.batch)))
	}

	err := r.callLoader()
	if err != nil {
		c.log.Warn().Err(err).Int("count", len(r.batch)).Msg("refresh batch failed")

		if c.metrics != nil {
			c.metrics.LoadErrorsCount.Inc()
		}

		// the values stay as they are, only the deduplication flags are released
		for _, ID := range r.batch {
			c.releaseGate(ID)
		}

		r.pauseAfterError()

		return
	}

	c.applyLoaded(r.entries, r.batch, r.loaded)
}

func (r *refresher[T]) callLoader() (err error) {
	c := r.cache

	if c.loadMultipleFunc != nil {
		r.entries, err = c.loadMultipleFunc(c.ctx, r.batch)

		return
	}

	r.entries = r.entries[:0]
	for _, ID := range r.batch {
		value, version, loadErr := c.loadOneFunc(c.ctx, ID)
		r.entries = append(r.entries, LoadedEntry[T]{ID: ID, Value: value, Version: version, Err: loadErr})
	}

	return nil
}

func (r *refresher[T]) pauseAfterError() {
	if r.cache.timeouts.ErrorRetryInterval <= 0 {
		return
	}

	r.timer.Reset(r.cache.timeouts.ErrorRetryInterval)

	select {
	case <-r.timer.C:
	case <-r.cache.ctx.Done():
		if !r.timer.Stop() {
			select {
			case <-r.timer.C:
			default:
			}
		}
	}
}

// applyLoaded stores the loaded entries and releases the deduplication flags.
// IDs the loader did not return are stored as not found.
func (c *Cache[T]) applyLoaded(entries []LoadedEntry[T], requested []int64, loaded map[int64]struct{}) {
	nowMillis := time.Now().UnixMilli()

	clear(loaded)

	for _, le := range entries {
		loaded[le.ID] = struct{}{}

		if le.Err != nil && !errors.Is(le.Err, ErrNotFound) {
			c.log.Warn().Err(le.Err).Int64("id", le.ID).Msg("item load failed")

			if c.metrics != nil {
				c.metrics.LoadErrorsCount.Inc()
			}

			continue
		}

		c.store(le, nowMillis)
	}

	for _, ID := range requested {
		if _, ok := loaded[ID]; !ok {
			c.store(LoadedEntry[T]{ID: ID, Err: ErrNotFound}, nowMillis)
		}

		c.releaseGate(ID)
	}
}

// loadIDs loads the given IDs in batches. It is used by the reconciliation, which
// must not push thousands of IDs through the refresh queue.
func (c *Cache[T]) loadIDs(ctx context.Context, IDs []int64) (added int) {
	batch := make([]int64, 0, c.refreshBatchSize)
	loaded := make(map[int64]struct{}, c.refreshBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		entries, err := c.callLoaderFor(ctx, batch)
		if err != nil {
			c.log.Warn().Err(err).Int("count", len(batch)).Msg("bulk load failed")

			if c.metrics != nil {
				c.metrics.LoadErrorsCount.Inc()
			}

			for _, ID := range batch {
				c.pending.Delete(ID)
			}
			batch = batch[:0]

			return
		}

		nowMillis := time.Now().UnixMilli()
		clear(loaded)

		for _, le := range entries {
			loaded[le.ID] = struct{}{}

			if le.Err != nil && !errors.Is(le.Err, ErrNotFound) {
				continue
			}

			if c.store(le, nowMillis) {
				added++
			}
		}

		for _, ID := range batch {
			if _, ok := loaded[ID]; !ok {
				c.store(LoadedEntry[T]{ID: ID, Err: ErrNotFound}, nowMillis)
			}

			c.pending.Delete(ID)
		}

		batch = batch[:0]
	}

	for _, ID := range IDs {
		// claim the ID through the same gate the refresh workers use
		if _, alreadyPending := c.pending.LoadOrStore(ID, struct{}{}); alreadyPending {
			continue
		}

		batch = append(batch, ID)
		if len(batch) >= c.refreshBatchSize {
			flush()
		}

		if ctx.Err() != nil {
			break
		}
	}

	flush()

	return
}

func (c *Cache[T]) callLoaderFor(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error) {
	if c.loadMultipleFunc != nil {
		return c.loadMultipleFunc(ctx, IDs)
	}

	entries = make([]LoadedEntry[T], 0, len(IDs))
	for _, ID := range IDs {
		value, version, loadErr := c.loadOneFunc(ctx, ID)
		entries = append(entries, LoadedEntry[T]{ID: ID, Value: value, Version: version, Err: loadErr})
	}

	return entries, nil
}
