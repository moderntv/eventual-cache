package eventual

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// cacheLinePadBytes is the size a shard is padded to. It is 128 and not 64
// because x86 prefetches cache lines in pairs.
const cacheLinePadBytes = 128

type shardCore[T any] struct {
	mu   sync.RWMutex
	data map[int64]*entry[T]

	// Read counters live in the shard so that incrementing them does not create
	// one globally contended cache line. They share the line with the mutex,
	// which every reader dirties anyway. The background goroutine sums them up
	// into Prometheus once a second.
	reads  atomic.Uint64
	misses atomic.Uint64
}

// shard is padded to a whole number of cache lines. Shards are kept in one
// contiguous slice ([]shard, not []*shard), so without the padding two shards
// would share a cache line and their mutexes would false-share.
type shard[T any] struct {
	shardCore[T]

	_ [cacheLinePadBytes - unsafe.Sizeof(shardCore[struct{}]{})]byte
}

// compile time check that a shard really is a whole number of cache lines
var _ = [1]struct{}{}[unsafe.Sizeof(shard[struct{}]{})%cacheLinePadBytes]

// syncShardCounts is where a parallel shard worker leaves its counts. Padded,
// because the workers write to neighbouring elements of one slice.
type syncShardCounts struct {
	sweepCounts

	_ [cacheLinePadBytes - unsafe.Sizeof(sweepCounts{})]byte
}

// compile time check that the counts are a whole cache line pair
var _ = [1]struct{}{}[unsafe.Sizeof(syncShardCounts{})%cacheLinePadBytes]

func (c *Cache[T]) shardOf(ID int64) *shard[T] {
	return &c.shards[c.shardHash(ID, c.shardBits)]
}

// forEachShardParallel runs fn for every shard using GOMAXPROCS goroutines.
// Spawning one goroutine per shard would be wasteful - there can be thousands.
func (c *Cache[T]) forEachShardParallel(fn func(index int, sh *shard[T])) {
	workers := min(runtime.GOMAXPROCS(0), len(c.shards))
	if workers < 1 {
		workers = 1
	}

	next := atomic.Int64{}
	wg := sync.WaitGroup{}
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()

			for {
				index := int(next.Add(1)) - 1
				if index >= len(c.shards) {
					return
				}

				fn(index, &c.shards[index])
			}
		}()
	}

	wg.Wait()
}
