package eventual

import (
	"sync"
	"sync/atomic"
)

// update is one new state of an item, handed to entry.apply. It is passed by
// value, so applying an update does not allocate.
type update[T any] struct {
	// value is the item. Nil means a tombstone - the item does not exist in the
	// source.
	value *T
	// version is the source version of the value (typically updated_at in
	// milliseconds). Zero means the source does not provide versions and the
	// last write wins.
	version int64
	// seq is the value of the cache write counter at the moment the update was
	// built. Reconciliation uses it to never delete an item written while the
	// reconciliation was running. A counter and not a timestamp, so that two
	// writes within the same millisecond - or a clock step - cannot confuse it.
	seq uint64
	// expiresAt is when a tombstone should be re-checked, in milliseconds. Zero
	// for regular values.
	expiresAt int64
}

// entry is the value stored in a shard map. The pointer stays stable for the
// whole lifetime of the item, so refreshing a value never needs the shard write
// lock - only creating and removing an item does.
//
// The value is stored directly in the entry and not behind a snapshot struct. It
// costs one pointer hop less on every read, which is worth a lot: with a large
// dataset and scattered heap the extra hop nearly doubles the cost of Get
// (measured 226 ns vs 120 ns over 200k items).
type entry[T any] struct {
	// mu serializes writers, so that value, version and seq can never end up in
	// an inconsistent combination. Readers never take it - they only load value.
	mu sync.Mutex

	value     atomic.Pointer[T]
	version   atomic.Int64
	seq       atomic.Uint64
	expiresAt atomic.Int64

	// refreshing is true while the item is queued for a refresh. It deduplicates
	// refresh requests before they enter the queue.
	refreshing atomic.Bool
	// dirty is set when an invalidation arrives while a refresh is already
	// running. The finished refresh then queues the item once more, so an
	// invalidation is never swallowed.
	dirty atomic.Bool
}

func newEntry[T any](u update[T]) (e *entry[T]) {
	e = &entry[T]{}
	e.version.Store(u.version)
	e.seq.Store(u.seq)
	e.expiresAt.Store(u.expiresAt)
	e.value.Store(u.value)

	return
}

// apply stores the update unless the entry already holds a newer version. It
// returns the replaced value and whether the update was stored.
//
// Value and version have to be replaced together. Were they written without the
// lock, a slow writer could overwrite a value already stored by a writer with a
// higher version - a lost update that `go test -race` cannot detect, because
// each operation on its own is atomic and only their order would be wrong.
func (e *entry[T]) apply(u update[T]) (previousValue *T, applied bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	previousValue = e.value.Load()

	if u.version != 0 && e.version.Load() > u.version {
		return previousValue, false
	}

	e.version.Store(u.version)
	e.seq.Store(u.seq)
	e.expiresAt.Store(u.expiresAt)
	// the value is stored last, so a reader which already sees it also sees the
	// metadata that belongs to it
	e.value.Store(u.value)

	return previousValue, true
}
