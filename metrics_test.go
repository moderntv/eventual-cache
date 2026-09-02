package eventual

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

// TestReadMetricsAreCollectedFromTheShards checks the read path counters. They
// are not incremented in Get directly - each shard counts its own and the
// background goroutine sums them up, so that reads never touch one shared
// Prometheus counter.
func TestReadMetricsAreCollectedFromTheShards(t *testing.T) {
	source := newTestSource(5)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.MissRateLimit = 1
		p.Timeouts.RefreshInterval = time.Hour
	})

	c.Get(1)
	c.Get(1)
	c.Get(999999) // the replica does not have it

	// the shards have counted it, Prometheus has not heard about it yet
	var reads, misses uint64
	for i := range c.shards {
		reads += c.shards[i].reads.Load()
		misses += c.shards[i].misses.Load()
	}

	if reads != 3 {
		t.Fatalf("expected 3 shard-counted reads, got %d", reads)
	}

	if misses != 1 {
		t.Fatalf("expected 1 shard-counted miss, got %d", misses)
	}

	mc := &metricsCollector{}
	c.collectMetrics(mc)

	got := testutil.ToFloat64(c.metrics.ReadsCount)
	if got != 3 {
		t.Fatalf("expected 3 reads in Prometheus, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.MissesCount)
	if got != 1 {
		t.Fatalf("expected 1 miss in Prometheus, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.ItemsCount)
	if got != 5 {
		t.Fatalf("expected 5 items, got %v", got)
	}

	// collecting twice must not double count
	c.Get(1)
	c.collectMetrics(mc)

	got = testutil.ToFloat64(c.metrics.ReadsCount)
	if got != 4 {
		t.Fatalf("expected 4 reads after one more Get, got %v", got)
	}
}

func TestLoadAndInvalidationMetricsAreCountedAtTheEvent(t *testing.T) {
	source := newTestSource(5)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Timeouts.RefreshInterval = time.Hour
	})

	// the initial load: one call with 5 IDs
	got := testutil.ToFloat64(c.metrics.BatchLoadCount)
	if got != 1 {
		t.Fatalf("expected 1 batch load, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.BatchLoadItemsCount)
	if got != 5 {
		t.Fatalf("expected 5 items sent to the loader, got %v", got)
	}

	c.Invalidate(1)
	c.Invalidate(7)

	got = testutil.ToFloat64(c.metrics.InvalidationsCount)
	if got != 2 {
		t.Fatalf("expected 2 invalidations, got %v", got)
	}
}

func TestRateLimitedMissesAreCounted(t *testing.T) {
	source := newTestSource(5)
	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.MissRateLimit = 2
		p.Timeouts.RefreshInterval = time.Hour
	})

	for i := int64(0); i < 100; i++ {
		c.Get(100000 + i)
	}

	got := testutil.ToFloat64(c.metrics.MissesRateLimitedCount)
	if got < 90 {
		t.Fatalf("expected most of the 100 lookups to be rate limited, got %v", got)
	}
}

func TestSyncMetrics(t *testing.T) {
	source := newTestSource(5)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = time.Hour
		p.afterSync = watcher.hook()
	})

	source.remove(7)

	// mark on one run, delete on the next
	watcher.wait(t, 3)

	got := testutil.ToFloat64(c.metrics.SyncRunsCount)
	if got < 2 {
		t.Fatalf("reconciliation runs were not counted, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.SyncMarkedCount)
	if got < 1 {
		t.Fatalf("marked items were not counted, got %v", got)
	}

	got = testutil.ToFloat64(c.metrics.SyncRemovedCount)
	if got != 1 {
		t.Fatalf("expected 1 removed item, got %v", got)
	}

	if testutil.ToFloat64(c.metrics.LastSyncTimestamp) == 0 {
		t.Fatalf("last_sync_timestamp was not set")
	}

	mc := &metricsCollector{}
	c.collectMetrics(mc)

	got = testutil.ToFloat64(c.metrics.ItemsCount)
	if got != 4 {
		t.Fatalf("expected 4 items, got %v", got)
	}
}

func TestErrorMetrics(t *testing.T) {
	source := newTestSource(5)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	setErr(&source.listIDsErr, errors.New("source down"))
	setErr(&source.loadErr, errors.New("source down"))

	c.Invalidate(1)
	watcher.wait(t, 2)

	got := testutil.ToFloat64(c.metrics.ListIDsErrorsCount)
	if got < 1 {
		t.Fatalf("list_ids_errors was not counted, got %v", got)
	}

	ok := waitFor(5*time.Second, func() bool { return testutil.ToFloat64(c.metrics.LoadErrorsCount) >= 1 })
	if !ok {
		t.Fatalf("error_loads was not counted")
	}
}

// TestCacheWorksWithoutMetrics - the registry is optional and every metric touch
// is behind a nil check.
func TestCacheWorksWithoutMetrics(t *testing.T) {
	source := newTestSource(5)
	watcher := newSyncWatcher()

	c := newTestCache(t, source, func(p *Params[testItem]) {
		p.MetricsRegistry = nil
		p.Timeouts.SyncInterval = 20 * time.Millisecond
		p.Timeouts.RefreshInterval = 20 * time.Millisecond
		p.afterSync = watcher.hook()
	})

	if c.metrics != nil {
		t.Fatalf("metrics must stay nil without a registry")
	}

	c.Get(1)
	c.Get(999999)
	c.Invalidate(1)

	watcher.wait(t, 2)
}
