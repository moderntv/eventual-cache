package eventual

import "sync"

// pendingIDs is the set of IDs waiting to be loaded. Invalidate, an expired TTL
// and a Get for an unknown ID all only put an ID in here; the background
// goroutine then drains the whole set and loads it in batches.
//
// The set deduplicates on its own. The flag on the entry is a shortcut in front
// of it, which saves taking this mutex for repeated marks of the same ID.
type pendingIDs struct {
	mu  sync.Mutex
	IDs map[int64]struct{}
}

func newPendingIDs() *pendingIDs {
	return &pendingIDs{IDs: make(map[int64]struct{})}
}

// add puts the ID in the set and returns the size of the set afterwards.
func (p *pendingIDs) add(ID int64) (size int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.IDs[ID] = struct{}{}

	return len(p.IDs)
}

// drain appends the whole set to dst and empties it. The map keeps its capacity,
// so a steady stream of invalidations does not keep reallocating it.
func (p *pendingIDs) drain(dst []int64) []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	for ID := range p.IDs {
		dst = append(dst, ID)
		delete(p.IDs, ID)
	}

	return dst
}

func (p *pendingIDs) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.IDs)
}
