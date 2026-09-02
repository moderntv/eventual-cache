package eventual

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

// TestPublicAPI pins down the whole public surface of the cache. Anything else
// belongs in an unexported helper, not in the API.
func TestPublicAPI(t *testing.T) {
	source := newTestSource(3)
	c := newTestCache(t, source, nil)

	var (
		_ func(int64) *testItem = c.Get
		_ func(int64)           = c.Invalidate
		_ func()                = c.Close
	)
}

func TestNewLoadsTheWholeDatasetInBatches(t *testing.T) {
	source := newTestSource(10)

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.BatchSize = 4
	})

	got := source.listIDsCalls.Load()
	if got != 1 {
		t.Fatalf("expected 1 ListIDs call, got %d", got)
	}

	// 10 IDs in batches of 4 = 3 calls
	got = source.loadMultipleCalls.Load()
	if got != 3 {
		t.Fatalf("expected 3 batch loads, got %d", got)
	}

	if source.maxBatch.Load() > 4 {
		t.Fatalf("the loader was called with %d IDs, BatchSize is 4", source.maxBatch.Load())
	}

	for _, ID := range testIDs(10) {
		if c.Get(ID) == nil {
			t.Fatalf("item %d is missing from the replica", ID)
		}
	}

	got = c.itemsCount.Load()
	if got != 10 {
		t.Fatalf("expected 10 items, got %d", got)
	}
}

func TestNewFailsWhenListIDsFails(t *testing.T) {
	source := newTestSource(3)
	setErr(&source.listIDsErr, errors.New("source down"))

	_, err := New(testParams(source, nil))
	if err == nil {
		t.Fatalf("New must fail when ListIDsFunc fails")
	}
}

func TestNewFailsWhenABatchFails(t *testing.T) {
	source := newTestSource(3)
	setErr(&source.loadErr, errors.New("source down"))

	_, err := New(testParams(source, nil))
	if err == nil {
		t.Fatalf("New must fail when a batch load fails")
	}
}

func TestNewValidatesParams(t *testing.T) {
	source := newTestSource(1)

	cases := []struct {
		name   string
		modify func(p *Params[testItem])
	}{
		{"no context", func(p *Params[testItem]) { p.Context = nil }},
		{"no name", func(p *Params[testItem]) { p.Name = "" }},
		{"no ListIDsFunc", func(p *Params[testItem]) { p.ListIDsFunc = nil }},
		{"no LoadMultipleFunc", func(p *Params[testItem]) { p.LoadMultipleFunc = nil }},
		{"shards not a power of two", func(p *Params[testItem]) { p.Shards = 100 }},
		{"negative batch size", func(p *Params[testItem]) { p.BatchSize = -1 }},
		{"no sync interval", func(p *Params[testItem]) { p.Timeouts.SyncInterval = 0 }},
		{"randomizer above one", func(p *Params[testItem]) { p.Timeouts.Randomizer = 1.5 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(testParams(source, tc.modify))
			if err == nil {
				t.Fatalf("New must reject params: %s", tc.name)
			}
		})
	}
}

func TestStoreAndRemove(t *testing.T) {
	source := newTestSource(3)
	c := newTestCache(t, source, nil)

	item := &testItem{ID: 999, Name: "new"}

	added := c.store(999, item, 1)
	if !added {
		t.Fatalf("store of a new ID must report it as added")
	}

	if c.Get(999) != item {
		t.Fatalf("the stored item is not readable")
	}

	replacement := &testItem{ID: 999, Name: "replaced"}

	added = c.store(999, replacement, 2)
	if added {
		t.Fatalf("store of a known ID must not report it as added")
	}

	if c.Get(999) != replacement {
		t.Fatalf("the value was not replaced")
	}

	removed := c.remove(999)
	if !removed {
		t.Fatalf("remove of a known ID must report it as removed")
	}

	if c.Get(999) != nil {
		t.Fatalf("the item is still readable after remove")
	}

	removed = c.remove(999)
	if removed {
		t.Fatalf("remove of an unknown ID must report nothing")
	}
}

// TestItemsHaveNoExpiration is the guarantee that an item is only ever dropped
// because the source stopped having it, never because it got old.
func TestItemsHaveNoExpiration(t *testing.T) {
	source := newTestSource(3)
	c := newTestCache(t, source, nil)

	time.Sleep(50 * time.Millisecond)

	for _, ID := range testIDs(3) {
		if c.Get(ID) == nil {
			t.Fatalf("item %d was dropped while the source still had it", ID)
		}
	}

	got := c.itemsCount.Load()
	if got != 3 {
		t.Fatalf("expected 3 items, got %d", got)
	}
}

func TestCloseIsIdempotentAndStopsTheGoroutine(t *testing.T) {
	source := newTestSource(3)

	c, err := New(testParams(source, nil))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	c.Close()
	c.Close()

	before := source.loadMultipleCalls.Load()

	c.Invalidate(1)
	time.Sleep(50 * time.Millisecond)

	got := source.loadMultipleCalls.Load()
	if got != before {
		t.Fatalf("the background goroutine is still running after Close")
	}
}

// TestCloseAfterContextCancel makes sure Close does not hang when the caller's
// context was cancelled first.
func TestCloseAfterContextCancel(t *testing.T) {
	source := newTestSource(3)

	ctx, cancel := context.WithCancel(context.Background())

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.Context = ctx
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cancel()

	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Close did not return")
	}
}

func TestGetIsSafeUnderConcurrentReloads(t *testing.T) {
	source := newTestSource(50)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Timeouts.RefreshInterval = time.Millisecond
		p.Timeouts.SyncInterval = 5 * time.Millisecond
		p.BatchSize = 7
	})

	IDs := testIDs(50)

	stop := make(chan struct{})
	wg := sync.WaitGroup{}

	for w := 0; w < 4; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				for _, ID := range IDs {
					_ = c.Get(ID)
					c.Invalidate(ID)
				}
			}
		}()
	}

	// let the source churn underneath the readers
	for i := 0; i < 20; i++ {
		source.set(int64(1+i*6), "changed")
		source.remove(int64(1 + (i+30)*6))
		time.Sleep(5 * time.Millisecond)
	}

	close(stop)
	wg.Wait()
}
