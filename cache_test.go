package eventual

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

func TestCache(t *testing.T) {
	t.Run("params_validation", testCacheParamsValidation)
	t.Run("blocking_warmup", testCacheBlockingWarmup)
	t.Run("warmup_error", testCacheWarmupError)
	t.Run("get_multiple", testCacheGetMultiple)
	t.Run("for_each", testCacheForEach)
	t.Run("unknown_id_is_loaded_in_background", testCacheUnknownIDLoadedInBackground)
	t.Run("tombstone_stops_repeated_loads", testCacheTombstoneStopsRepeatedLoads)
	t.Run("invalidate_picks_up_new_value", testCacheInvalidatePicksUpNewValue)
	t.Run("invalidate_during_refresh_is_not_lost", testCacheInvalidateDuringRefreshNotLost)
	t.Run("invalidate_all_reloads", testCacheInvalidateAllReloads)
	t.Run("metrics_registered", testCacheMetricsRegistered)
	t.Run("close_is_idempotent", testCacheCloseIsIdempotent)
	t.Run("parallelism", testCacheParallelism)
}

func testCacheParamsValidation(t *testing.T) {
	t.Parallel()

	source := newTestSource(1)

	valid := func() Params[testItem] {
		return Params[testItem]{
			Context:          context.Background(),
			Log:              test_utils.Logger(),
			Name:             "test",
			LoadAllFunc:      source.loadAll,
			ListIDsFunc:      source.listIDs,
			LoadMultipleFunc: source.loadMultiple,
			LoadOneFunc:      source.loadOne,
			Timeouts:         testTimeouts,
		}
	}

	cases := map[string]func(p *Params[testItem]){
		"no context":         func(p *Params[testItem]) { p.Context = nil },
		"no name":            func(p *Params[testItem]) { p.Name = "" },
		"no load all":        func(p *Params[testItem]) { p.LoadAllFunc = nil },
		"no list ids":        func(p *Params[testItem]) { p.ListIDsFunc = nil },
		"no loader":          func(p *Params[testItem]) { p.LoadOneFunc = nil; p.LoadMultipleFunc = nil },
		"shards not power 2": func(p *Params[testItem]) { p.Shards = 100 },
		"no sync interval":   func(p *Params[testItem]) { p.Timeouts.SyncInterval = 0 },
		"randomizer too big": func(p *Params[testItem]) { p.Timeouts.Randomizer = 2 },
	}

	for name, broke := range cases {
		params := valid()
		broke(&params)

		c, err := New(params)
		assert.Error(t, err, name)
		assert.Nil(t, c, name)
	}

	c, err := New(valid())
	assert.NoError(t, err)
	assert.NotNil(t, c)
	c.Close()
}

func testCacheBlockingWarmup(t *testing.T) {
	t.Parallel()

	source := newTestSource(1000)
	c := newTestCache(t, source, nil)

	// New must not return before the whole dataset is in the replica
	assert.Equal(t, 1000, c.Len())
	assert.Equal(t, int64(1), source.loadAllCalls.Load())

	value := c.Get(1)
	if assert.NotNil(t, value) {
		assert.Equal(t, int64(1), value.ID)
	}
	assert.NotNil(t, c.Get(1+999*6))
}

func testCacheWarmupError(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	setErr(&source.loadAllErr, errors.New("database is down"))

	c, err := New(Params[testItem]{
		Context:          context.Background(),
		Log:              test_utils.Logger(),
		Name:             "test",
		LoadAllFunc:      source.loadAll,
		ListIDsFunc:      source.listIDs,
		LoadMultipleFunc: source.loadMultiple,
		LoadOneFunc:      source.loadOne,
		Timeouts:         testTimeouts,
	})

	assert.Error(t, err)
	assert.Nil(t, c)
}

func testCacheGetMultiple(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	values := c.GetMultiple([]int64{1, 7, 13, 999999})

	assert.Len(t, values, 3)
	assert.NotContains(t, values, int64(999999))
	for _, value := range values {
		assert.NotNil(t, value)
	}
}

func testCacheForEach(t *testing.T) {
	t.Parallel()

	source := newTestSource(50)
	c := newTestCache(t, source, nil)

	seen := 0
	c.ForEach(func(ID int64, value *testItem) bool {
		seen++
		assert.Equal(t, ID, value.ID)

		return true
	})
	assert.Equal(t, 50, seen)

	// the callback can stop the iteration
	visited := 0
	c.ForEach(func(int64, *testItem) bool {
		visited++

		return visited < 10
	})
	assert.Equal(t, 10, visited)
}

func testCacheUnknownIDLoadedInBackground(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	const newID = 100_000_001

	source.set(newID, "created later")

	// the replica does not know about it yet
	assert.Nil(t, c.Get(newID))

	// ... but the miss queued a background load
	assert.True(t, waitFor(2*time.Second, func() bool { return c.Get(newID) != nil }))
	assert.Equal(t, "created later", c.Get(newID).Name)
}

func testCacheTombstoneStopsRepeatedLoads(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	const missingID = 999_999_999

	assert.Nil(t, c.Get(missingID))
	assert.True(t, waitFor(2*time.Second, func() bool { return c.Stats().Tombstones == 1 }))

	before := source.loadMultipleCalls.Load()

	for i := 0; i < 1000; i++ {
		assert.Nil(t, c.Get(missingID))
	}

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, before, source.loadMultipleCalls.Load(), "a tombstone must not trigger more loads")
}

func testCacheInvalidatePicksUpNewValue(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	assert.Equal(t, "item", c.Get(7).Name)

	source.set(7, "renamed")
	c.Invalidate(7)

	assert.True(t, waitFor(2*time.Second, func() bool { return c.Get(7).Name == "renamed" }))
}

// An invalidation arriving while a refresh is running must not be swallowed.
func testCacheInvalidateDuringRefreshNotLost(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	source.loadDelay.Store(int64(150 * time.Millisecond))

	c := newTestCache(t, source, nil)

	source.set(7, "second")
	c.Invalidate(7)

	// while the loader is busy with "second", the source changes again
	time.Sleep(40 * time.Millisecond)
	source.set(7, "third")
	c.Invalidate(7)

	assert.True(t, waitFor(3*time.Second, func() bool { return c.Get(7).Name == "third" }))
}

func testCacheInvalidateAllReloads(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	c := newTestCache(t, source, nil)

	source.set(1, "reloaded")
	source.set(7, "reloaded")

	c.InvalidateAll()

	assert.True(t, waitFor(2*time.Second, func() bool {
		return c.Get(1).Name == "reloaded" && c.Get(7).Name == "reloaded"
	}))
	assert.Equal(t, int64(2), source.loadAllCalls.Load())
}

func testCacheMetricsRegistered(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)
	registry := test_utils.MetricsRegistry()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = registry
	})

	for i := 0; i < 100; i++ {
		c.Get(1)
	}

	assert.NotNil(t, c.metrics)

	// the read counters are flushed by a background goroutine, not from Get
	var reads uint64
	for i := range c.shards {
		reads += c.shards[i].reads.Load()
	}
	assert.Equal(t, uint64(100), reads)
}

func testCacheCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	source := newTestSource(10)

	c, err := New(Params[testItem]{
		Context:          context.Background(),
		Log:              test_utils.Logger(),
		Name:             "test",
		LoadAllFunc:      source.loadAll,
		ListIDsFunc:      source.listIDs,
		LoadMultipleFunc: source.loadMultiple,
		LoadOneFunc:      source.loadOne,
		Timeouts:         testTimeouts,
	})
	assert.NoError(t, err)

	c.Close()
	c.Close()

	// operations after Close must not panic
	assert.NotNil(t, c.Get(1))
	c.Invalidate(1)
	c.InvalidateAll()
}

func testCacheParallelism(t *testing.T) {
	t.Parallel()

	const (
		routines   = 50
		iterations = 20_000
		items      = 500
	)

	source := newTestSource(items)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.Timeouts.SyncInterval = 50 * time.Millisecond
		p.Timeouts.Randomizer = 0.2
		p.Shards = 64
	})

	maxID := int64(1 + items*6)

	found := atomic.Int64{}
	wg := sync.WaitGroup{}

	for i := 0; i < routines; i++ {
		wg.Add(1)

		go func(seed int64) {
			defer wg.Done()

			r := rand.New(rand.NewSource(seed))

			for j := 0; j < iterations; j++ {
				ID := int64(1 + r.Intn(items)*6)

				switch r.Intn(100) {
				case 0:
					c.Invalidate(ID)
				case 1:
					c.GetMultiple([]int64{ID, ID + 6, maxID + 1})
				case 2:
					c.Len()
				default:
					if c.Get(ID) != nil {
						found.Add(1)
					}
				}
			}
		}(int64(i))
	}

	// hammer the reconciliation at the same time
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 20; i++ {
			_ = c.Sync(context.Background())
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()

	assert.Greater(t, found.Load(), int64(0))
	assert.Equal(t, items, c.Len(), "no item may be lost")
}
