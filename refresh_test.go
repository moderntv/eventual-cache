package eventual

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRefresh(t *testing.T) {
	t.Run("concurrent_misses_are_deduplicated", testRefreshConcurrentMissesDeduplicated)
	t.Run("requests_spread_in_time_are_deduplicated", testRefreshSpreadInTimeDeduplicated)
	t.Run("requests_are_batched", testRefreshRequestsAreBatched)
	t.Run("load_error_keeps_previous_value", testRefreshLoadErrorKeepsPreviousValue)
	t.Run("full_queue_does_not_block", testRefreshFullQueueDoesNotBlock)
	t.Run("miss_rate_limit", testRefreshMissRateLimit)
	t.Run("load_one_fallback", testRefreshLoadOneFallback)
}

func testRefreshConcurrentMissesDeduplicated(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	source.loadDelay.Store(int64(30 * time.Millisecond))

	c := newTestCache(t, source, nil)

	const unknownID = 4_000_001

	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			c.Get(unknownID)
		}()
	}
	wg.Wait()

	assert.True(t, waitFor(2*time.Second, func() bool { return source.requestCount(unknownID) > 0 }))
	time.Sleep(150 * time.Millisecond)

	assert.LessOrEqual(t, source.requestCount(unknownID), 2,
		"1000 concurrent lookups of one unknown ID must not become 1000 loads")
}

// The original design deduplicated with singleflight behind the queue, which does
// nothing when the duplicates arrive one after another. The gate in front of the
// queue has to collapse those too.
func testRefreshSpreadInTimeDeduplicated(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	source.loadDelay.Store(int64(40 * time.Millisecond))

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.RefreshWorkers = 1
	})

	const ID = int64(7)

	for i := 0; i < 300; i++ {
		c.Invalidate(ID)
		time.Sleep(time.Millisecond)
	}

	assert.True(t, waitFor(3*time.Second, func() bool { return c.Stats().QueueLength == 0 }))
	time.Sleep(150 * time.Millisecond)

	requests := source.requestCount(ID)
	t.Logf("300 invalidations spread over ~300 ms produced %d loads", requests)
	assert.Less(t, requests, 30, "invalidations arriving one by one have to be collapsed as well")
	assert.Greater(t, requests, 0)
}

func testRefreshRequestsAreBatched(t *testing.T) {
	t.Parallel()

	source := newTestSource(1000)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.RefreshWorkers = 1
		p.RefreshBatchSize = 100
		p.RefreshBatchDelay = 20 * time.Millisecond
	})

	IDs := make([]int64, 0, 1000)
	for i := 0; i < 1000; i++ {
		IDs = append(IDs, int64(1+i*6))
	}

	before := source.loadMultipleCalls.Load()
	c.InvalidateMultiple(IDs)

	assert.True(t, waitFor(5*time.Second, func() bool { return source.totalRequests() >= 1000 }))

	calls := source.loadMultipleCalls.Load() - before
	t.Logf("1000 invalidations took %d loader calls", calls)
	assert.LessOrEqual(t, calls, int64(40), "invalidations have to reach the loader in batches")
}

func testRefreshLoadErrorKeepsPreviousValue(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	assert.Equal(t, "item", c.Get(7).Name)

	setErr(&source.loadErr, errors.New("connection reset"))
	source.set(7, "never seen")
	c.Invalidate(7)

	time.Sleep(200 * time.Millisecond)

	if assert.NotNil(t, c.Get(7), "a failed load must never remove an item") {
		assert.Equal(t, "item", c.Get(7).Name)
	}
	assert.Equal(t, 10, c.Len())

	// once the source recovers, the next invalidation goes through
	setErr(&source.loadErr, nil)
	c.Invalidate(7)

	assert.True(t, waitFor(3*time.Second, func() bool { return c.Get(7).Name == "never seen" }))
}

func testRefreshFullQueueDoesNotBlock(t *testing.T) {
	t.Parallel()

	source := newTestSource(500)
	source.loadDelay.Store(int64(200 * time.Millisecond))

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.RefreshWorkers = 1
		p.RefreshQueueSize = 4
		p.RefreshBatchSize = 1
	})

	IDs := make([]int64, 0, 500)
	for i := 0; i < 500; i++ {
		IDs = append(IDs, int64(1+i*6))
	}

	start := time.Now()
	c.InvalidateMultiple(IDs)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 500*time.Millisecond, "a full queue has to drop requests, not block the caller")
	assert.Equal(t, 500, c.Len())
}

func testRefreshMissRateLimit(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MissRateLimit = 2
		p.Timeouts.NotFoundTTL = 0 // do not remember the answers
	})

	for i := 0; i < 500; i++ {
		c.Get(int64(5_000_000 + i))
	}

	time.Sleep(300 * time.Millisecond)

	total := source.totalRequests()
	t.Logf("500 lookups of unknown IDs with a limit of 2/s produced %d loads", total)
	assert.LessOrEqual(t, total, 10, "lookups of unknown IDs have to be rate limited")
}

func testRefreshLoadOneFallback(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.LoadMultipleFunc = nil
	})

	source.set(7, "via load one")
	c.Invalidate(7)

	assert.True(t, waitFor(3*time.Second, func() bool { return c.Get(7).Name == "via load one" }))
	assert.Greater(t, source.loadOneCalls.Load(), int64(0))
	assert.Zero(t, source.loadMultipleCalls.Load())
}
