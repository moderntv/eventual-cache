package eventual

import "sync/atomic"

// entry is the value stored in a shard map. The pointer to an entry stays stable
// for the whole lifetime of the item, so replacing a value never needs the shard
// write lock - only adding and removing an item does.
//
// The value is stored directly in the entry and not behind a snapshot struct. It
// costs one pointer hop less on every read, which is worth a lot: with a large
// dataset and a scattered heap the extra hop nearly doubles the cost of Get.
type entry[T any] struct {
	// value is the item. It is never nil for an entry that is in a shard map -
	// an item either is in the replica with a value, or it is not there at all.
	value atomic.Pointer[T]

	// version is the version of the value above, as reported by the loader. The
	// reconciliation compares it against the version the source lists and marks
	// the item for a reload when the source is ahead.
	version atomic.Int64

	// invalidated is set when the item is waiting for a reload. It is cleared
	// just before the reload starts, so an invalidation arriving during the load
	// marks the item again instead of being swallowed.
	invalidated atomic.Bool

	// markedForDeletion is set by a reconciliation that did not find the ID in
	// the source. The item is deleted by the *next* reconciliation that still
	// does not find it. One reconciliation is not enough evidence: an item loaded
	// while the reconciliation was running is naturally missing from the ID
	// snapshot it started with, and deleting it right away would throw away a
	// perfectly good item.
	markedForDeletion atomic.Bool
}

func newEntry[T any](value *T, version int64) (e *entry[T]) {
	e = &entry[T]{}
	e.version.Store(version)
	// the value is stored last, so a reader which already sees it also sees the
	// metadata that belongs to it
	e.value.Store(value)

	return
}
