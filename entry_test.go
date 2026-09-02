package eventual

import "testing"

func TestEntryHoldsTheValueAndItsFlags(t *testing.T) {
	first := &testItem{ID: 1, Name: "first"}

	e := newEntry(first, 1234, 5678)

	if e.value.Load() != first {
		t.Fatalf("newEntry did not store the value")
	}

	if e.version.Load() != 1234 {
		t.Fatalf("newEntry did not store the version")
	}

	if e.refreshAt.Load() != 5678 {
		t.Fatalf("newEntry did not store the refresh deadline")
	}

	if e.invalidated.Load() {
		t.Fatalf("a fresh entry must not be marked for a reload")
	}

	if e.markedForDeletion.Load() {
		t.Fatalf("a fresh entry must not be marked for deletion")
	}

	second := &testItem{ID: 1, Name: "second"}
	e.value.Store(second)

	if e.value.Load() != second {
		t.Fatalf("the value was not replaced")
	}
}

func TestPendingIDsDeduplicatesAndDrains(t *testing.T) {
	p := newPendingIDs()

	got := p.add(1)
	if got != 1 {
		t.Fatalf("expected size 1, got %d", got)
	}

	got = p.add(1)
	if got != 1 {
		t.Fatalf("adding the same ID twice must not grow the set, got %d", got)
	}

	got = p.add(2)
	if got != 2 {
		t.Fatalf("expected size 2, got %d", got)
	}

	drained := p.drain(nil)
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained IDs, got %d", len(drained))
	}

	if p.size() != 0 {
		t.Fatalf("drain must empty the set")
	}

	// drain appends to the given buffer and reuses its capacity
	buf := make([]int64, 0, 4)
	p.add(7)

	drained = p.drain(buf)
	if len(drained) != 1 || drained[0] != 7 {
		t.Fatalf("unexpected drain result: %v", drained)
	}
}
