package eventual

import (
	"time"

	"github.com/moderntv/eventual-cache/internal/utils"
)

// metricsInterval is how often the shard read counters are collected into
// Prometheus.
const metricsInterval = time.Second

// run is the only background goroutine of the cache. It reloads marked items,
// periodically reconciles the replica with the source and collects the shard
// counters into Prometheus.
//
// Reloading and reconciliation share one goroutine on purpose: a reconciliation
// can then never interleave with a reload, which is one less race to reason
// about and costs nothing - both are background work with no latency budget.
func (c *Cache[T]) run() {
	defer c.wg.Done()

	l := c.newLoader()
	mc := &metricsCollector{}

	refreshTicker := time.NewTicker(c.timeouts.RefreshInterval)
	defer refreshTicker.Stop()

	syncTimer := time.NewTimer(utils.RandomizeDuration(c.timeouts.SyncInterval, c.timeouts.Randomizer))
	defer syncTimer.Stop()

	// receiving from a nil channel blocks forever, which is how the metrics ticker
	// stays switched off when there is no registry
	var metricsCh <-chan time.Time

	if c.metrics != nil {
		metricsTicker := time.NewTicker(metricsInterval)
		defer metricsTicker.Stop()

		metricsCh = metricsTicker.C
	}

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-refreshTicker.C:
			c.reloadMarked(l)

		case <-c.batchFull:
			c.reloadMarked(l)

		case <-syncTimer.C:
			c.sync(c.ctx, l)
			syncTimer.Reset(utils.RandomizeDuration(c.timeouts.SyncInterval, c.timeouts.Randomizer))

		case <-metricsCh:
			c.collectMetrics(mc)
		}
	}
}

// reloadMarked loads every item currently in the pending set: items marked by
// Invalidate, items a reconciliation found outdated, and IDs a Get asked for and
// the replica did not have. IDs whose load failed are marked again, so the next
// run tries them.
func (c *Cache[T]) reloadMarked(l *loader[T]) {
	l.IDs = c.pending.drain(l.IDs[:0])
	if len(l.IDs) == 0 {
		return
	}

	// the flags are cleared before the load, so that an invalidation arriving
	// during the load marks the item again instead of being swallowed
	for _, ID := range l.IDs {
		e, exists := c.entryOf(ID)
		if !exists {
			continue
		}

		e.invalidated.Store(false)
	}

	added, removed, err := l.loadAll(c.ctx, l.IDs)
	if err != nil {
		c.log.Warn().
			Err(err).
			Int("count", len(l.IDs)).
			Msg("reloading marked items failed, they stay marked")

		for _, ID := range l.IDs {
			c.remark(ID)
		}

		return
	}

	c.log.Trace().
		Int("count", len(l.IDs)).
		Int("added", added).
		Int("removed", removed).
		Msg("marked items reloaded")
}

// remark puts an ID back into the pending set after a failed load. It does not
// go through the rate limiter - the ID has already been let through once.
func (c *Cache[T]) remark(ID int64) {
	e, exists := c.entryOf(ID)
	if exists {
		alreadyMarked := !e.invalidated.CompareAndSwap(false, true)
		if alreadyMarked {
			return
		}
	}

	c.enqueue(ID)
}
