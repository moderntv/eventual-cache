package eventual

import "errors"

// ErrNotFound is returned by a loader when the item does not exist in the source.
// The cache removes such an item from the replica.
var ErrNotFound = errors.New("not found")
