package eventual

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// loader drives one load run. It owns the buffers reused between runs, so that
// loading does not allocate per batch. The warm-up and the background goroutine
// each have their own; a loader is never used by two goroutines at once, but the
// batchLoaders inside it are used by all of the run's workers at the same time.
type loader[T any] struct {
	cache *Cache[T]

	// IDs is the working buffer for the list of IDs to load.
	IDs []int64

	// workers is one batchLoader per possible worker, so that a run does not
	// allocate them and no two workers share their state.
	workers []*batchLoader[T]
}

// batchLoader applies single batches to the replica. Each worker of a run has
// its own, because the present map below cannot be shared.
type batchLoader[T any] struct {
	cache *Cache[T]

	// present holds the IDs the source said something about in the current
	// batch. An ID missing from the answer is an ID the source does not have.
	present map[int64]struct{}

	added   int
	removed int
}

func (c *Cache[T]) newLoader() *loader[T] {
	l := &loader[T]{
		cache:   c,
		IDs:     make([]int64, 0, c.batchSize),
		workers: make([]*batchLoader[T], c.loadConcurrency),
	}

	for i := range l.workers {
		l.workers[i] = &batchLoader[T]{
			cache:   c,
			present: make(map[int64]struct{}, c.batchSize),
		}
	}

	return l
}

// loadAll loads the given IDs in batches of BatchSize, spreading the batches
// over LoadConcurrency workers, and applies every batch to the replica as it
// arrives.
//
// The first batch that fails all of its attempts ends the run: the workers still
// in flight are cancelled so that a dead source is not handed every remaining
// batch of a run which is going to fail anyway. The batches already applied stay
// in the replica.
func (l *loader[T]) loadAll(ctx context.Context, IDs []int64) (added, removed int, err error) {
	batchSize := l.cache.batchSize

	batches := (len(IDs) + batchSize - 1) / batchSize
	if batches == 0 {
		return 0, 0, nil
	}

	// both are at least 1: LoadConcurrency is defaulted and validated, and a run
	// with no batches returned above
	workers := min(len(l.workers), batches)

	for w := 0; w < workers; w++ {
		l.workers[w].added, l.workers[w].removed = 0, 0
	}

	// the derived context is how a worker whose batch failed tells the others to
	// stop pulling further batches
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		next     atomic.Int64
		errMu    sync.Mutex
		firstErr error
	)

	run := func(worker int) {
		b := l.workers[worker]

		for {
			index := int(next.Add(1)) - 1
			if index >= batches {
				return
			}

			batchErr := ctx.Err()
			if batchErr == nil {
				start := index * batchSize
				end := min(start+batchSize, len(IDs))

				batchErr = b.loadBatch(ctx, IDs[start:end])
			}

			if batchErr == nil {
				continue
			}

			errMu.Lock()
			if firstErr == nil {
				firstErr = batchErr
			}
			errMu.Unlock()

			cancel()

			return
		}
	}

	// a single worker needs no goroutine at all, which is the common case for
	// reloads of invalidated items
	if workers == 1 {
		run(0)
	} else {
		wg := sync.WaitGroup{}
		wg.Add(workers)

		for w := 0; w < workers; w++ {
			go func(worker int) {
				defer wg.Done()

				run(worker)
			}(w)
		}

		wg.Wait()
	}

	for w := 0; w < workers; w++ {
		added += l.workers[w].added
		removed += l.workers[w].removed
	}

	return added, removed, firstErr
}

// call asks the source for one batch, retrying a few times before giving up. A
// batch that fails every attempt ends the whole run.
func (b *batchLoader[T]) call(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error) {
	c := b.cache

	backoff := c.loadRetryBackoff

	for attempt := 1; ; attempt++ {
		if c.metrics != nil {
			c.metrics.BatchLoadCount.Inc()
			c.metrics.BatchLoadItemsCount.Add(float64(len(IDs)))
		}

		entries, err = c.loadMultipleFunc(ctx, IDs)
		if err == nil {
			return entries, nil
		}

		if c.metrics != nil {
			c.metrics.LoadErrorsCount.Inc()
		}

		if attempt >= c.loadRetries {
			return nil, err
		}

		c.log.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("count", len(IDs)).
			Msg("batch load failed, retrying")

		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(backoff):
		}

		backoff *= 4
	}
}

// loadBatch is the only path that writes loaded data into the replica. It removes
// an item only when the answer proves the source does not have it: a nil value,
// ErrNotFound, or no mention of the ID at all.
func (b *batchLoader[T]) loadBatch(ctx context.Context, IDs []int64) (err error) {
	c := b.cache

	entries, err := b.call(ctx, IDs)
	if err != nil {
		return err
	}

	clear(b.present)

	for _, le := range entries {
		b.present[le.ID] = struct{}{}

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
				b.removed++
			}

			continue
		}

		if c.store(le.ID, le.Value, le.Version) {
			b.added++
		}
	}

	// an ID the loader did not mention at all does not exist in the source
	for _, ID := range IDs {
		_, mentioned := b.present[ID]
		if mentioned {
			continue
		}

		if c.remove(ID) {
			b.removed++
		}
	}

	return nil
}
