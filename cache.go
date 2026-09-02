package eventual

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	metrics_pkg "github.com/moderntv/eventual-cache/internal/metrics"
	"github.com/moderntv/eventual-cache/internal/utils"
)

// Cache is an eventually consistent in-memory replica of a whole dataset keyed
// by int64. Reads never block on I/O; the replica is kept up to date by
// invalidations from the caller and by periodic reconciliation with the source,
// which compares item versions and so also catches invalidations that were lost
// on the way.
type Cache[T any] struct {
	// static attributes (do not change their value after initialization)
	ctx       context.Context
	cancel    context.CancelFunc
	log       zerolog.Logger
	metrics   *metrics_pkg.Metrics
	name      string
	timeouts  Timeouts
	batchSize int
	afterSync func()

	loadConcurrency  int
	loadRetries      int
	loadRetryBackoff time.Duration

	listIDsFunc      ListIDsFunc
	loadMultipleFunc LoadMultipleFunc[T]

	shards    []shard[T]
	shardHash ShardHashFunc
	shardBits uint

	// ageBudgetPerShard is MaxRefreshPerSync split between the shards, 0 for no
	// cap. See ageBudgetPerShard().
	ageBudgetPerShard int

	missLimiter *rateLimiter

	// dynamic attributes
	pending   *pendingIDs
	batchFull chan struct{}

	itemsCount atomic.Int64

	// buffers reused between reconciliation runs. Only the background goroutine
	// touches them, so they need no lock.
	available    map[int64]int64 // ID -> version the source lists
	shardIDs     [][]int64
	shardDelete  [][]int64
	shardReload  [][]int64
	shardMissing [][]int64
	shardCounts  []syncShardCounts
	syncMissing  []int64

	wg        sync.WaitGroup
	closeOnce sync.Once
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
		ctx:       ctx,
		cancel:    cancel,
		log:       log,
		metrics:   metrics,
		name:      params.Name,
		timeouts:  params.Timeouts,
		batchSize: params.BatchSize,
		afterSync: params.afterSync,

		loadConcurrency:  params.LoadConcurrency,
		loadRetries:      params.loadRetries,
		loadRetryBackoff: params.loadRetryBackoff,

		listIDsFunc:      params.ListIDsFunc,
		loadMultipleFunc: params.LoadMultipleFunc,

		shards:    make([]shard[T], params.Shards),
		shardHash: params.ShardHash,
		shardBits: shardBits(params.Shards),

		ageBudgetPerShard: ageBudgetPerShard(params.MaxRefreshPerSync, params.Shards),

		missLimiter: newRateLimiter(params.MissRateLimit),

		pending:   newPendingIDs(),
		batchFull: make(chan struct{}, 1),

		available:    make(map[int64]int64),
		shardIDs:     make([][]int64, params.Shards),
		shardDelete:  make([][]int64, params.Shards),
		shardReload:  make([][]int64, params.Shards),
		shardMissing: make([][]int64, params.Shards),
		shardCounts:  make([]syncShardCounts, params.Shards),
	}

	for i := range c.shards {
		c.shards[i].data = make(map[int64]*entry[T])
	}

	// blocking warm-up - the replica must be complete before anybody reads it
	err = c.warmUp(ctx)
	if err != nil {
		cancel()

		return nil, err
	}

	c.wg.Add(1)
	go c.run()

	c.log.Info().
		Int64("items", c.itemsCount.Load()).
		Int("shards", len(c.shards)).
		Msg("cache started")

	return c, nil
}

// Get returns the item, or nil when the replica does not have it. It never
// blocks on I/O and never loads anything itself.
//
// A nil result means either that the source does not have the item, or that the
// replica does not know about it yet. The ID is queued for a background load, so
// a later Get can already succeed.
func (c *Cache[T]) Get(ID int64) *T {
	sh := c.shardOf(ID)

	// the read counters live in the shard, so counting does not create one
	// globally contended cache line. They are collected into Prometheus by the
	// background goroutine.
	sh.reads.Add(1)

	sh.mu.RLock()
	e, exists := sh.data[ID]
	sh.mu.RUnlock()

	if !exists {
		sh.misses.Add(1)
		c.markUnknown(ID)

		return nil
	}

	return e.value.Load()
}

// Invalidate marks the item for a reload. It does not load anything itself: the
// background goroutine reloads all marked items together, either on its next
// tick or as soon as there are enough of them for a whole batch.
//
// The caller is expected to call it when the source announces a change (typically
// from a NATS handler) - the cache itself does not subscribe to anything.
func (c *Cache[T]) Invalidate(ID int64) {
	if c.metrics != nil {
		c.metrics.InvalidationsCount.Inc()
	}

	e, exists := c.entryOf(ID)
	if exists {
		c.markEntry(ID, e)

		return
	}

	// an explicit invalidation of an unknown ID usually means a newly created
	// item, so it is never rate limited
	c.enqueue(ID)
}

// Close stops the background goroutine and waits for it to finish. It is safe to
// call it more than once.
func (c *Cache[T]) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
	})
}

// markEntry marks an item the caller already has the entry of.
func (c *Cache[T]) markEntry(ID int64, e *entry[T]) {
	// a plain load first: a CompareAndSwap is a locked instruction, and repeated
	// marks of the same item are common - a burst of invalidations for one ID, or
	// a reconciliation marking something Invalidate already did
	if e.invalidated.Load() {
		return
	}

	alreadyMarked := !e.invalidated.CompareAndSwap(false, true)
	if alreadyMarked {
		return
	}

	c.enqueue(ID)
}

// markUnknown queues an ID the replica does not hold. This is the one path a
// caller can trigger at will with arbitrary IDs, so it is rate limited.
//
// The clock is read here and not kept in a field the background goroutine ticks:
// that goroutine also does the I/O, so a slow source would stop the clock, and
// with it the limiter's window - every miss would then be rejected until the
// source recovered. Only a miss pays for the call, never a hit.
func (c *Cache[T]) markUnknown(ID int64) {
	allowed := c.missLimiter.allow(time.Now().UnixMilli())
	if !allowed {
		if c.metrics != nil {
			c.metrics.MissesRateLimitedCount.Inc()
		}

		return
	}

	c.enqueue(ID)
}

// enqueue puts the ID into the pending set and wakes the background goroutine
// when there is enough for a whole batch.
func (c *Cache[T]) enqueue(ID int64) {
	size := c.pending.add(ID)
	if size < c.batchSize {
		return
	}

	select {
	case c.batchFull <- struct{}{}:
	default:
	}
}

func (c *Cache[T]) entryOf(ID int64) (e *entry[T], exists bool) {
	sh := c.shardOf(ID)

	sh.mu.RLock()
	e, exists = sh.data[ID]
	sh.mu.RUnlock()

	return
}

// store inserts or replaces an item. Replacing the value of an existing item
// only needs the shard read lock, because the pointer to the entry stays the
// same.
func (c *Cache[T]) store(ID int64, value *T, version int64) (added bool) {
	e, exists := c.entryOf(ID)
	if exists {
		c.updateEntry(e, value, version)

		return false
	}

	sh := c.shardOf(ID)

	refreshAt := c.nextRefreshAt(true)

	sh.mu.Lock()
	e, exists = sh.data[ID]
	if !exists {
		sh.data[ID] = newEntry(value, version, refreshAt)
	}
	sh.mu.Unlock()

	// somebody inserted it between the two locks
	if exists {
		c.updateEntry(e, value, version)

		return false
	}

	c.itemsCount.Add(1)

	return true
}

// updateEntry puts a freshly loaded value into an existing entry.
func (c *Cache[T]) updateEntry(e *entry[T], value *T, version int64) {
	e.version.Store(version)

	// any successful load restarts the age clock, whatever triggered it, so the
	// age really is the time since the last load
	e.refreshAt.Store(c.nextRefreshAt(false))

	// a successful load is proof the source has the item
	if e.markedForDeletion.Load() {
		e.markedForDeletion.Store(false)
	}

	// the value is stored last, so a reader which already sees it also sees the
	// metadata that belongs to it
	e.value.Store(value)
}

// nextRefreshAt returns the deadline for an item that has just been loaded, or 0
// when MaxAge is off.
//
// The first store of an item draws uniformly from the whole period, every later
// reload takes a full MaxAge. That difference is not cosmetic: the warm-up stores
// the whole dataset within seconds, so with one rule for both the entire replica
// would fall due in the same window and reload at once. The uniform first draw
// phases the replica, and once phased it stays phased.
func (c *Cache[T]) nextRefreshAt(first bool) int64 {
	maxAge := int64(c.timeouts.MaxAge)
	if maxAge <= 0 {
		return 0
	}

	now := time.Now().UnixNano()

	// Int63n gives [0, maxAge), so this is (0, maxAge] - never the current
	// instant, which would expire the item the moment it is stored
	if first {
		return now + 1 + rand.Int63n(maxAge)
	}

	return now + int64(utils.RandomizeDuration(c.timeouts.MaxAge, c.timeouts.Randomizer))
}

// remove drops the item from the replica. Only a load which proves the source
// does not have the item removes anything - a failed load never does.
func (c *Cache[T]) remove(ID int64) (removed bool) {
	sh := c.shardOf(ID)

	sh.mu.Lock()
	_, exists := sh.data[ID]
	if exists {
		delete(sh.data, ID)
	}
	sh.mu.Unlock()

	if !exists {
		return false
	}

	c.itemsCount.Add(-1)

	return true
}

// warmUp fills the replica: it lists every ID the source has and loads them in
// batches. New blocks on it and fails when it fails, so that a service never
// starts serving from an incomplete replica.
func (c *Cache[T]) warmUp(ctx context.Context) (err error) {
	start := time.Now()

	items, err := c.listIDsFunc(ctx)
	if err != nil {
		if c.metrics != nil {
			c.metrics.ListIDsErrorsCount.Inc()
		}

		return fmt.Errorf("listing source IDs failed: %w", err)
	}

	IDs := make([]int64, len(items))
	for i := range items {
		IDs[i] = items[i].ID
	}

	added, _, err := c.newLoader().loadAll(ctx, IDs)
	if err != nil {
		return fmt.Errorf("initial load failed: %w", err)
	}

	c.log.Info().
		Int("total", len(IDs)).
		Int("loaded", added).
		Float64("duration_s", time.Since(start).Seconds()).
		Msg("initial load finished")

	return nil
}

// metricsCollector remembers what has already been pushed into Prometheus, so
// that the shard counters can keep growing monotonically.
type metricsCollector struct {
	lastReads  uint64
	lastMisses uint64
}

// collectMetrics pushes the shard read counters into Prometheus and refreshes the
// gauges. Incrementing a Prometheus counter directly in Get would create one
// globally contended cache line, which is exactly what the sharding avoids.
func (c *Cache[T]) collectMetrics(mc *metricsCollector) {
	var reads, misses uint64
	for i := range c.shards {
		reads += c.shards[i].reads.Load()
		misses += c.shards[i].misses.Load()
	}

	c.metrics.ReadsCount.Add(float64(reads - mc.lastReads))
	c.metrics.MissesCount.Add(float64(misses - mc.lastMisses))
	mc.lastReads, mc.lastMisses = reads, misses

	c.metrics.ItemsCount.Set(float64(c.itemsCount.Load()))
	c.metrics.PendingCount.Set(float64(c.pending.size()))
}
