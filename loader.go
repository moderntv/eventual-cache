package eventual

import (
	"context"
	"errors"
)

// loader holds the buffers reused between batches, so that loading does not
// allocate per batch. The warm-up and the background goroutine each have their
// own; a loader is never used by two goroutines at once.
type loader[T any] struct {
	cache *Cache[T]

	// IDs is the working buffer for the list of IDs to load.
	IDs []int64
	// present holds the IDs the loader said something about. An ID missing from
	// the answer is an ID the source does not have.
	present map[int64]struct{}
}

func (c *Cache[T]) newLoader() *loader[T] {
	return &loader[T]{
		cache:   c,
		IDs:     make([]int64, 0, c.batchSize),
		present: make(map[int64]struct{}, c.batchSize),
	}
}

// loadAll loads the given IDs in batches of BatchSize and applies every batch to
// the replica as it arrives. The first failing batch ends the run and is returned
// as an error; the batches already applied stay in the replica.
func (l *loader[T]) loadAll(ctx context.Context, IDs []int64) (added, removed int, err error) {
	batchSize := l.cache.batchSize

	for start := 0; start < len(IDs); start += batchSize {
		err = ctx.Err()
		if err != nil {
			return
		}

		end := min(start+batchSize, len(IDs))

		batchAdded, batchRemoved, batchErr := l.loadBatch(ctx, IDs[start:end])
		added += batchAdded
		removed += batchRemoved

		if batchErr != nil {
			return added, removed, batchErr
		}
	}

	return
}

// loadBatch is the only path that writes loaded data into the replica. It removes
// an item only when the answer proves the source does not have it: a nil value,
// ErrNotFound, or no mention of the ID at all.
func (l *loader[T]) loadBatch(ctx context.Context, IDs []int64) (added, removed int, err error) {
	c := l.cache

	if c.metrics != nil {
		c.metrics.BatchLoadCount.Inc()
		c.metrics.BatchLoadItemsCount.Add(float64(len(IDs)))
	}

	entries, err := c.loadMultipleFunc(ctx, IDs)
	if err != nil {
		if c.metrics != nil {
			c.metrics.LoadErrorsCount.Inc()
		}

		return 0, 0, err
	}

	// the clock is refreshed once per batch, so that TTLs computed by store are
	// based on when the data actually arrived
	c.updateClock()

	clear(l.present)

	for _, le := range entries {
		l.present[le.ID] = struct{}{}

		notFound := errors.Is(le.Err, ErrNotFound)

		if le.Err != nil && !notFound {
			// an error about one item is not proof that the source lost it
			c.log.Warn().Err(le.Err).Int64("id", le.ID).Msg("item load failed")

			if c.metrics != nil {
				c.metrics.LoadErrorsCount.Inc()
			}

			continue
		}

		if le.Value == nil {
			if c.remove(le.ID) {
				removed++
			}

			continue
		}

		if c.store(le.ID, le.Value) {
			added++
		}
	}

	// an ID the loader did not mention at all does not exist in the source
	for _, ID := range IDs {
		_, mentioned := l.present[ID]
		if mentioned {
			continue
		}

		if c.remove(ID) {
			removed++
		}
	}

	return added, removed, nil
}
