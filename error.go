package eventual

import "errors"

var (
	// ErrNotFound is returned by a loader when the item does not exist in the
	// source. The cache remembers it as a tombstone for Timeouts.NotFoundTTL.
	ErrNotFound = errors.New("not found")

	// ErrReloadInProgress is returned by Reload when another reload is already
	// running.
	ErrReloadInProgress = errors.New("reload already in progress")
)
