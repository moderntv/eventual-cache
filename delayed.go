package eventual

import "sync"

// delayedIDs holds invalidated IDs until Timeouts.InvalidateDelay has passed.
// Only then does the background goroutine move them to the pending set, so that
// a source which heard the same message has processed it before it is asked.
//
// It deliberately ignores the invalidated flag of the entry. An item that is
// already pending must still get a load of its own after the delay - the
// pending load may be exactly the one that comes too early.
type delayedIDs struct {
	mu sync.Mutex
	// dueAt maps an ID to the unix nanoseconds from which it may be marked.
	dueAt map[int64]int64
}

func newDelayedIDs() *delayedIDs {
	return &delayedIDs{dueAt: make(map[int64]int64)}
}

// add sets the time from which the ID may be marked. Adding an ID that is
// already waiting overwrites its time, so every invalidation waits the whole
// delay.
func (d *delayedIDs) add(ID int64, dueAt int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.dueAt[ID] = dueAt
}

// drainDue appends the IDs whose time has come to dst and removes them from the
// set. It walks the whole set: it is empty or small almost all the time, and a
// heap would complicate the overwrite in add for no measurable gain.
func (d *delayedIDs) drainDue(dst []int64, now int64) []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	for ID, dueAt := range d.dueAt {
		if dueAt > now {
			continue
		}

		dst = append(dst, ID)
		delete(d.dueAt, ID)
	}

	return dst
}

func (d *delayedIDs) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.dueAt)
}
