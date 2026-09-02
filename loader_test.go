package eventual

import (
	"errors"
	"testing"
	"time"
)

// TestLoadRetriesAFailingBatch is why the warm-up survives a blip: a dataset of
// a million items is thousands of calls, and failing the whole start-up because
// one of them was unlucky is not acceptable.
func TestLoadRetriesAFailingBatch(t *testing.T) {
	source := newTestSource(10)
	source.failFirst.Store(2)

	c, err := New(testParams(source, nil))
	if err != nil {
		t.Fatalf("New must survive a batch that fails twice and then works: %v", err)
	}

	t.Cleanup(c.Close)

	for _, ID := range testIDs(10) {
		if c.Get(ID) == nil {
			t.Fatalf("item %d is missing from the replica", ID)
		}
	}
}

func TestLoadGivesUpAfterTheConfiguredAttempts(t *testing.T) {
	source := newTestSource(10)
	setErr(&source.loadErr, errors.New("source down"))

	_, err := New(testParams(source, func(p *Params[testItem]) {
		p.BatchSize = 100 // one batch, so the count is unambiguous
		p.loadRetries = 3
	}))
	if err == nil {
		t.Fatalf("New must fail when a batch keeps failing")
	}

	got := source.loadMultipleCalls.Load()
	if got != 3 {
		t.Fatalf("expected 3 attempts at the batch, got %d", got)
	}
}

func TestBatchesAreLoadedConcurrently(t *testing.T) {
	source := newTestSource(200)
	source.loadDelay.Store(int64(20 * time.Millisecond))

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.BatchSize = 10 // 20 batches
		p.LoadConcurrency = 4
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	t.Cleanup(c.Close)

	got := source.maxInFlight.Load()
	if got != 4 {
		t.Fatalf("expected 4 concurrent batch loads, got %d", got)
	}
}

func TestLoadConcurrencyOneKeepsBatchesSerial(t *testing.T) {
	source := newTestSource(200)
	source.loadDelay.Store(int64(2 * time.Millisecond))

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.BatchSize = 10
		p.LoadConcurrency = 1
	}))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	t.Cleanup(c.Close)

	got := source.maxInFlight.Load()
	if got != 1 {
		t.Fatalf("LoadConcurrency 1 must load one batch at a time, saw %d at once", got)
	}
}

// TestADeadSourceCostsOneBatchPerWorker keeps a run against a dead source from
// walking the whole dataset: every worker stops at its own first failure instead
// of pulling the next batch.
func TestADeadSourceCostsOneBatchPerWorker(t *testing.T) {
	source := newTestSource(10_000)
	setErr(&source.loadErr, errors.New("source down"))

	_, err := New(testParams(source, func(p *Params[testItem]) {
		p.BatchSize = 10 // 1000 batches
		p.LoadConcurrency = 4
		p.loadRetries = 1
	}))
	if err == nil {
		t.Fatalf("New must fail when the source is down")
	}

	got := source.loadMultipleCalls.Load()
	if got > 4 {
		t.Fatalf("a failing run made %d batch calls, expected at most one per worker", got)
	}
}

// TestAFailingBatchCancelsTheLoadsStillInFlight is what the derived context is
// for: the run is over as soon as one batch gives up, so the requests the other
// workers are still waiting on are dead weight on the source.
func TestAFailingBatchCancelsTheLoadsStillInFlight(t *testing.T) {
	source := newTestSource(200)

	// one batch fails, but only once the other three workers are inside a load,
	// so that there really is something in flight to cancel
	source.failFast.Store(int64(1), struct{}{})
	source.failFastWaitFor.Store(4)
	source.loadDelay.Store(int64(3 * time.Second))

	_, err := New(testParams(source, func(p *Params[testItem]) {
		p.BatchSize = 10 // 20 batches
		p.LoadConcurrency = 4
		p.loadRetries = 1
	}))
	if err == nil {
		t.Fatalf("New must fail when a batch keeps failing")
	}

	got := source.ctxCancelled.Load()
	if got == 0 {
		t.Fatalf("the loads still in flight were left running after the run failed")
	}
}

func TestNewRejectsNegativeLoadConcurrency(t *testing.T) {
	source := newTestSource(1)

	_, err := New(testParams(source, func(p *Params[testItem]) {
		p.LoadConcurrency = -1
	}))
	if err == nil {
		t.Fatalf("New must reject a negative LoadConcurrency")
	}
}
