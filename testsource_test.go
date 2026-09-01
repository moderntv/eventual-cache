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
	mu       sync.Mutex
	items    map[int64]testItem
	versions map[int64]int64
	// omit holds IDs that listIDs pretends not to see, even though the source
	// still has them (a stale read from a replica).
	omit map[int64]struct{}

	loadAllCalls      atomic.Int64
	listIDsCalls      atomic.Int64
	loadOneCalls      atomic.Int64
	loadMultipleCalls atomic.Int64
	loadedItems       atomic.Int64

	loadAllErr atomic.Pointer[error]
	listIDsErr atomic.Pointer[error]
	loadErr    atomic.Pointer[error]

	loadDelay    atomic.Int64 // nanoseconds
	listIDsDelay atomic.Int64 // nanoseconds

	onListIDs atomic.Pointer[func()]
	onLoad    atomic.Pointer[func(IDs []int64)]

	requestsMu sync.Mutex
	requests   map[int64]int
}

func newTestSource(n int) *testSource {
	s := &testSource{
		items:    make(map[int64]testItem),
		versions: make(map[int64]int64),
		omit:     make(map[int64]struct{}),
		requests: make(map[int64]int),
	}

	// IDs with step 6, like a 6 node MariaDB Galera cluster
	for i := 0; i < n; i++ {
		ID := int64(1 + i*6)
		s.set(ID, "item")
	}

	return s
}

func (s *testSource) set(ID int64, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[ID] = testItem{ID: ID, Name: name}
	s.versions[ID]++
}

func (s *testSource) remove(ID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.items, ID)
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

func (s *testSource) totalRequests() (total int) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()

	for _, count := range s.requests {
		total += count
	}

	return
}

func (s *testSource) omitFromList(IDs ...int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ID := range IDs {
		s.omit[ID] = struct{}{}
	}
}

func (s *testSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.items)
}

func (s *testSource) err(p *atomic.Pointer[error]) error {
	if e := p.Load(); e != nil {
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

func (s *testSource) loadAll(_ context.Context) (entries []LoadedEntry[testItem], err error) {
	s.loadAllCalls.Add(1)

	if err = s.err(&s.loadAllErr); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entries = make([]LoadedEntry[testItem], 0, len(s.items))
	for ID, item := range s.items {
		value := item
		entries = append(entries, LoadedEntry[testItem]{ID: ID, Value: &value, Version: s.versions[ID]})
	}

	s.loadedItems.Add(int64(len(entries)))

	return entries, nil
}

func (s *testSource) listIDs(_ context.Context) (IDs []int64, err error) {
	s.listIDsCalls.Add(1)

	if d := s.listIDsDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	if hook := s.onListIDs.Load(); hook != nil {
		(*hook)()
	}

	if err = s.err(&s.listIDsErr); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	IDs = make([]int64, 0, len(s.items))
	for ID := range s.items {
		if _, omitted := s.omit[ID]; omitted {
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

	if hook := s.onLoad.Load(); hook != nil {
		(*hook)(IDs)
	}

	if d := s.loadDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	if err = s.err(&s.loadErr); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entries = make([]LoadedEntry[testItem], 0, len(IDs))
	for _, ID := range IDs {
		item, exists := s.items[ID]
		if !exists {
			continue // the cache turns a missing ID into a tombstone on its own
		}

		value := item
		entries = append(entries, LoadedEntry[testItem]{ID: ID, Value: &value, Version: s.versions[ID]})
		s.loadedItems.Add(1)
	}

	return entries, nil
}

func (s *testSource) loadOne(_ context.Context, ID int64) (value *testItem, version int64, err error) {
	s.loadOneCalls.Add(1)
	s.recordRequests(ID)

	if d := s.loadDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	if err = s.err(&s.loadErr); err != nil {
		return nil, 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[ID]
	if !exists {
		return nil, 0, ErrNotFound
	}

	s.loadedItems.Add(1)
	copied := item

	return &copied, s.versions[ID], nil
}

var testTimeouts = Timeouts{
	SyncInterval:       time.Hour, // tests drive Sync explicitly unless stated otherwise
	NotFoundTTL:        time.Minute,
	ErrorRetryInterval: 0,
	Randomizer:         0,
}

func newTestCache(t *testing.T, source *testSource, modify func(p *Params[testItem])) *Cache[testItem] {
	t.Helper()

	params := Params[testItem]{
		Context:           context.Background(),
		Log:               test_utils.Logger(),
		Name:              "test",
		LoadAllFunc:       source.loadAll,
		ListIDsFunc:       source.listIDs,
		LoadMultipleFunc:  source.loadMultiple,
		LoadOneFunc:       source.loadOne,
		Timeouts:          testTimeouts,
		Shards:            16,
		RefreshBatchDelay: 5 * time.Millisecond,
	}

	if modify != nil {
		modify(&params)
	}

	c, err := New(params)
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
