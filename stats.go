package eventual

import "time"

// Stats is a snapshot of the cache state, useful for health checks and
// debugging.
type Stats struct {
	// Items is the number of items held in the replica (tombstones excluded).
	Items int
	// Tombstones is the number of remembered "does not exist" answers.
	Tombstones int
	// QueueLength is the number of IDs waiting for a refresh.
	QueueLength int
	// LastSyncAt is when the last reconciliation finished.
	LastSyncAt time.Time
	// LastSyncStats is the result of the last reconciliation.
	LastSyncStats SyncStats
}

// SyncStats describes one reconciliation run.
type SyncStats struct {
	// Total is the number of IDs the source reported.
	Total int
	// Added is the number of items loaded because the replica did not have them.
	Added int
	// Removed is the number of items dropped because the source no longer has
	// them (expired tombstones included).
	Removed int
	// Refreshed is the number of items queued for a refresh because the replica
	// held a tombstone for an ID the source has again.
	Refreshed int
	Duration  time.Duration
	Err       error
}
