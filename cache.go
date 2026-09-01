package eventual

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	metrics_pkg "github.com/moderntv/eventual-cache/internal/metrics"
	"github.com/moderntv/eventual-cache/internal/utils"
)

// metricsFlushInterval is how often the per shard read counters are flushed into
// Prometheus. Incrementing a Prometheus counter directly in Get would create one
// globally contended cache line, which is exactly what the sharding avoids.
const metricsFlushInterval = time.Second

// Cache is an eventually consistent in-memory replica of a whole dataset keyed
// by int64. Reads never block on I/O; the replica is kept up to date by
// invalidations from the caller and by periodic reconciliation with the source.
type Cache[T any] struct {
	// static attributes (does not change its value after initialization)
	ctx      context.Context
	cancel   context.CancelFunc
	log      zerolog.Logger
	metrics  *metrics_pkg.Metrics
	name     string
	timeouts Timeouts

	loadAllFunc      LoadAllFunc[T]
	listIDsFunc      ListIDsFunc
	loadMultipleFunc LoadMultipleFunc[T]
	loadOneFunc      LoadOneFunc[T]
	onSync           func(stats SyncStats)

	shards    []shard[T]
	shardHash ShardHashFunc
	shardBits uint

	refreshCh         chan int64
	refreshWorkers    int
	refreshBatchSize  int
	refreshBatchDelay time.Duration
	missLimiter       *rateLimiter

	// dynamic attributes (not using mutex)
	pending         sync.Map // int64 -> struct{}; IDs missing from the replica which are being loaded
	itemsCount      atomic.Int64
	tombstonesCount atomic.Int64
	writeSeq        atomic.Uint64
	lastSyncAt      atomic.Int64
	lastSyncStats   atomic.Pointer[SyncStats]
	reloading       atomic.Bool

	// attributes protected by syncMu (buffers reused between reconciliation runs)
	syncMu       sync.Mutex
	available    map[int64]struct{}
	shardIDs     [][]int64
	shardDelete  [][]int64
	shardRefresh [][]int64
	shardMissing [][]int64
	shardCounts  []syncShardCounts

	// spawnMu guards spawning new goroutines against Close
	spawnMu   sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type syncShardCounts struct {
	removed   int
	refreshed int
	_         [cacheLinePadBytes - 16]byte // written from parallel shard workers
}

// New creates the cache and performs a blocking warm-up - it returns only after
// the whole dataset has been loaded, or with an error when the load failed.
func New[T any](params Params[T]) (c *Cache[T], err error) {
	err = params.check()
	if err != nil {
		return
	}

	params.withDefaults()

	var metrics *metrics_pkg.Metrics
	if params.MetricsRegistry != nil {
		metrics, err = metrics_pkg.New(params.Name, params.MetricsRegistry)
		if err != nil {
			return
		}
	}

	log := params.Log.With().Str("cache", params.Name).Logger()

	ctx, cancel := context.WithCancel(params.Context)

	c = &Cache[T]{
		ctx:      ctx,
		cancel:   cancel,
		log:      log,
		metrics:  metrics,
		name:     params.Name,
		timeouts: params.Timeouts,

		loadAllFunc:      params.LoadAllFunc,
		listIDsFunc:      params.ListIDsFunc,
		loadMultipleFunc: params.LoadMultipleFunc,
		loadOneFunc:      params.LoadOneFunc,
		onSync:           params.OnSync,

		shards:    make([]shard[T], params.Shards),
		shardHash: params.ShardHash,
		shardBits: shardBits(params.Shards),

		refreshCh:         make(chan int64, params.RefreshQueueSize),
		refreshWorkers:    params.RefreshWorkers,
		refreshBatchSize:  params.RefreshBatchSize,
		refreshBatchDelay: params.RefreshBatchDelay,
		missLimiter:       newRateLimiter(params.MissRateLimit),

		available:    make(map[int64]struct{}),
		shardIDs:     make([][]int64, params.Shards),
		shardDelete:  make([][]int64, params.Shards),
		shardRefresh: make([][]int64, params.Shards),
		shardMissing: make([][]int64, params.Shards),
		shardCounts:  make([]syncShardCounts, params.Shards),
	}

	for i := range c.shards {
		c.shards[i].data = make(map[int64]*entry[T])
	}

	// blocking warm-up - the replica must be complete before anybody reads it
	err = c.Reload(ctx)
	if err != nil {
		cancel()

		return nil, err
	}

	c.wg.Add(c.refreshWorkers + 1)
	for i := 0; i < c.refreshWorkers; i++ {
		go c.startRefreshWorker()
	}
	go c.startPeriodicSync()

	if c.metrics != nil {
		c.wg.Add(1)
		go c.startMetricsFlusher()
	}

	c.log.Info().
		Int("items", c.Len()).
		Int("shards", len(c.shards)).
		Msg("cache started")

	return c, nil
}

// Get returns the item or nil when it does not exist. It never blocks on I/O.
//
// A nil result means one of two things: the source does not have the item, or
// the replica does not know about it yet (a very recently created item whose
// invalidation was lost). The second case queues a background load, so a
// following Get can already succeed.
func (c *Cache[T]) Get(ID int64) *T {
	sh := c.shardOf(ID)
	sh.reads.Add(1)

	sh.mu.RLock()
	e, exists := sh.data[ID]
	sh.mu.RUnlock()

	if !exists {
		sh.misses.Add(1)
		c.enqueueUnknown(ID, true)

		return nil
	}

	value := e.value.Load()
	if value == nil {
		// a tombstone - ask the source again once its TTL has passed.
		// time.Now stays out of the hot path, this branch is the rare one.
		if expiresAt := e.expiresAt.Load(); expiresAt != 0 && expiresAt <= time.Now().UnixMilli() {
			c.enqueueEntry(ID, e)
		}

		return nil
	}

	return value
}

// GetMultiple returns the items for the given IDs. A key missing from the result
// means the item does not exist; the map never contains a nil value.
func (c *Cache[T]) GetMultiple(IDs []int64) (values map[int64]*T) {
	values = make(map[int64]*T, len(IDs))

	for _, ID := range IDs {
		value := c.Get(ID)
		if value != nil {
			values[ID] = value
		}
	}

	return
}

// ForEach calls fn for every item in the replica until fn returns false. The
// callback runs under the shard read lock, so it must not call back into the
// cache and should be quick.
func (c *Cache[T]) ForEach(fn func(ID int64, value *T) bool) {
	for i := range c.shards {
		sh := &c.shards[i]

		sh.mu.RLock()
		for ID, e := range sh.data {
			value := e.value.Load()
			if value == nil {
				continue
			}

			if !fn(ID, value) {
				sh.mu.RUnlock()

				return
			}
		}
		sh.mu.RUnlock()
	}
}

// Len returns the number of items in the replica (tombstones excluded).
func (c *Cache[T]) Len() int {
	return int(c.itemsCount.Load())
}

// Invalidate queues the item for a background refresh. The caller is expected to
// call it when the source announces a change (typically from a NATS handler) -
// the cache itself does not subscribe to anything.
func (c *Cache[T]) Invalidate(ID int64) {
	if c.metrics != nil {
		c.metrics.InvalidationsCount.Inc()
	}

	c.enqueueRefresh(ID)
}

// InvalidateMultiple queues several items for a background refresh.
func (c *Cache[T]) InvalidateMultiple(IDs []int64) {
	if c.metrics != nil {
		c.metrics.InvalidationsCount.Add(float64(len(IDs)))
	}

	for _, ID := range IDs {
		c.enqueueRefresh(ID)
	}
}

// InvalidateAll starts a full reload in the background. When a reload is already
// running, it does nothing.
func (c *Cache[T]) InvalidateAll() {
	c.spawnMu.Lock()
	if c.closed {
		c.spawnMu.Unlock()

		return
	}
	c.wg.Add(1)
	c.spawnMu.Unlock()

	go func() {
		defer c.wg.Done()

		err := c.Reload(c.ctx)
		if err != nil && !errors.Is(err, ErrReloadInProgress) {
			c.log.Warn().Err(err).Msg("background reload failed")
		}
	}()
}

// Stats returns a snapshot of the cache state.
func (c *Cache[T]) Stats() (stats Stats) {
	stats = Stats{
		Items:       int(c.itemsCount.Load()),
		Tombstones:  int(c.tombstonesCount.Load()),
		QueueLength: len(c.refreshCh),
	}

	if at := c.lastSyncAt.Load(); at > 0 {
		stats.LastSyncAt = time.UnixMilli(at)
	}

	if last := c.lastSyncStats.Load(); last != nil {
		stats.LastSyncStats = *last
	}

	return
}

// Close stops all background goroutines and waits for them to finish. It is safe
// to call it more than once.
func (c *Cache[T]) Close() {
	c.closeOnce.Do(func() {
		c.spawnMu.Lock()
		c.closed = true
		c.spawnMu.Unlock()

		c.cancel()
		c.wg.Wait()
	})
}

// store inserts or updates an item. It never removes an item and never
// overwrites a value with a failed load.
func (c *Cache[T]) store(le LoadedEntry[T], nowMillis int64) (added bool) {
	sh := c.shardOf(le.ID)

	sh.mu.RLock()
	e, exists := sh.data[le.ID]
	sh.mu.RUnlock()

	next := c.newUpdate(le, nowMillis)

	if exists {
		previousValue, applied := e.apply(next)
		if applied {
			c.countTransition(true, previousValue, next.value)
		}

		return false
	}

	// not-found answers are not stored when there is no TTL for them
	if next.value == nil && c.timeouts.NotFoundTTL <= 0 {
		return false
	}

	sh.mu.Lock()
	e, exists = sh.data[le.ID]
	if exists {
		sh.mu.Unlock()

		previousValue, applied := e.apply(next)
		if applied {
			c.countTransition(true, previousValue, next.value)
		}

		return false
	}

	sh.data[le.ID] = newEntry(next)
	sh.mu.Unlock()

	c.countTransition(false, nil, next.value)

	return true
}

func (c *Cache[T]) newUpdate(le LoadedEntry[T], nowMillis int64) (u update[T]) {
	u = update[T]{
		value:   le.Value,
		version: le.Version,
		seq:     c.writeSeq.Add(1),
	}

	if le.Err != nil {
		u.value = nil
	}

	if u.value == nil && c.timeouts.NotFoundTTL > 0 {
		u.expiresAt = nowMillis + utils.RandomizeDuration(c.timeouts.NotFoundTTL, c.timeouts.Randomizer).Milliseconds()
	}

	return
}

// countTransition keeps the item and tombstone counters in sync with what was
// actually stored. A nil value means a tombstone.
func (c *Cache[T]) countTransition(hadPrevious bool, previousValue, nextValue *T) {
	if hadPrevious {
		if previousValue != nil {
			c.itemsCount.Add(-1)
		} else {
			c.tombstonesCount.Add(-1)
		}
	}

	if nextValue != nil {
		c.itemsCount.Add(1)
	} else {
		c.tombstonesCount.Add(1)
	}
}

func (c *Cache[T]) startMetricsFlusher() {
	defer c.wg.Done()

	ticker := time.NewTicker(metricsFlushInterval)
	defer ticker.Stop()

	var lastReads, lastMisses uint64

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-ticker.C:
			var reads, misses uint64
			for i := range c.shards {
				reads += c.shards[i].reads.Load()
				misses += c.shards[i].misses.Load()
			}

			c.metrics.ReadsCount.Add(float64(reads - lastReads))
			c.metrics.MissesCount.Add(float64(misses - lastMisses))
			lastReads, lastMisses = reads, misses

			c.metrics.ItemsCount.Set(float64(c.itemsCount.Load()))
			c.metrics.TombstonesCount.Set(float64(c.tombstonesCount.Load()))
			c.metrics.QueueLength.Set(float64(len(c.refreshCh)))
		}
	}
}
