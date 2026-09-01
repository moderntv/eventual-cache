package eventual

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

type testItem struct {
	ID   int64
	Name string
}

// testSource is a fake data source with call counters and hooks.
type testSource struct {
	mu    sync.Mutex
	items map[int64]testItem
	// omit holds IDs that listIDs pretends not to see, even though the source
	// still has them (a stale read from a replica).
	omit map[int64]struct{}

	listIDsCalls      atomic.Int64
	loadMultipleCalls atomic.Int64

	listIDsErr atomic.Pointer[error]
	loadErr    atomic.Pointer[error]

	loadDelay atomic.Int64 // nanoseconds

	onLoad atomic.Pointer[func(IDs []int64)]

	requestsMu sync.Mutex
	requests   map[int64]int
}

// testIDs returns the IDs newTestSource(n) fills the source with: step 6, the
// shape an auto-increment column has on a multi node cluster.
func testIDs(n int) (IDs []int64) {
	IDs = make([]int64, n)
	for i := range IDs {
		IDs[i] = int64(1 + i*6)
	}

	return
}

func newTestSource(n int) *testSource {
	s := &testSource{
		items:    make(map[int64]testItem),
		omit:     make(map[int64]struct{}),
		requests: make(map[int64]int),
	}

	for _, ID := range testIDs(n) {
		s.set(ID, "item")
	}

	return s
}

func (s *testSource) set(ID int64, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[ID] = testItem{ID: ID, Name: name}
}

func (s *testSource) remove(ID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.items, ID)
}

func (s *testSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.items)
}

func (s *testSource) omitFromList(IDs ...int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ID := range IDs {
		s.omit[ID] = struct{}{}
	}
}

func (s *testSource) showInList(IDs ...int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ID := range IDs {
		delete(s.omit, ID)
	}
}

// recordRequests counts how many times each ID was asked for.
func (s *testSource) recordRequests(IDs ...int64) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()

	for _, ID := range IDs {
		s.requests[ID]++
	}
}

func (s *testSource) requestCount(ID int64) int {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()

	return s.requests[ID]
}

func (s *testSource) err(p *atomic.Pointer[error]) error {
	e := p.Load()
	if e != nil {
		return *e
	}

	return nil
}

func setErr(p *atomic.Pointer[error], err error) {
	if err == nil {
		p.Store(nil)

		return
	}

	p.Store(&err)
}

func (s *testSource) listIDs(_ context.Context) (IDs []int64, err error) {
	s.listIDsCalls.Add(1)

	err = s.err(&s.listIDsErr)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	IDs = make([]int64, 0, len(s.items))
	for ID := range s.items {
		_, omitted := s.omit[ID]
		if omitted {
			continue
		}

		IDs = append(IDs, ID)
	}
	sort.Slice(IDs, func(i, j int) bool { return IDs[i] < IDs[j] })

	return IDs, nil
}

func (s *testSource) loadMultiple(_ context.Context, IDs []int64) (entries []LoadedEntry[testItem], err error) {
	s.loadMultipleCalls.Add(1)
	s.recordRequests(IDs...)

	hook := s.onLoad.Load()
	if hook != nil {
		(*hook)(IDs)
	}

	d := s.loadDelay.Load()
	if d > 0 {
		time.Sleep(time.Duration(d))
	}

	err = s.err(&s.loadErr)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entries = make([]LoadedEntry[testItem], 0, len(IDs))
	for _, ID := range IDs {
		item, exists := s.items[ID]
		if !exists {
			continue // an ID missing from the answer is removed by the cache
		}

		value := item
		entries = append(entries, LoadedEntry[testItem]{ID: ID, Value: &value})
	}

	return entries, nil
}

var testTimeouts = Timeouts{
	SyncInterval:    time.Hour, // tests that want a reconciliation shorten it
	RefreshInterval: 5 * time.Millisecond,
	Randomizer:      0,
}

func testParams(source *testSource, modify func(p *Params[testItem])) Params[testItem] {
	params := Params[testItem]{
		Context:          context.Background(),
		Log:              test_utils.Logger(),
		Name:             "test",
		ListIDsFunc:      source.listIDs,
		LoadMultipleFunc: source.loadMultiple,
		Timeouts:         testTimeouts,
		Shards:           16,
	}

	if modify != nil {
		modify(&params)
	}

	return params
}

func newTestCache(t *testing.T, source *testSource, modify func(p *Params[testItem])) *Cache[testItem] {
	t.Helper()

	c, err := New(testParams(source, modify))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	t.Cleanup(c.Close)

	return c
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(2 * time.Millisecond)
	}

	return cond()
}

// syncWatcher counts finished reconciliations.
type syncWatcher chan struct{}

func newSyncWatcher() syncWatcher {
	return make(syncWatcher, 256)
}

func (w syncWatcher) hook() func() {
	return func() {
		select {
		case w <- struct{}{}:
		default:
		}
	}
}

// wait waits until n reconciliations have finished.
func (w syncWatcher) wait(t *testing.T, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		select {
		case <-w:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d reconciliations finished", i, n)
		}
	}
}

// name returns the value of the item, or "" when the replica does not have it.
func name(c *Cache[testItem], ID int64) string {
	item := c.Get(ID)
	if item == nil {
		return ""
	}

	return item.Name
}
