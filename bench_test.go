package eventual

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	cadre_metrics "github.com/moderntv/cadre/metrics"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

func benchmarkCache(b *testing.B, items, shards int, registry *cadre_metrics.Registry) (c *Cache[testItem], IDs []int64) {
	b.Helper()

	source := newTestSource(items)

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.MetricsRegistry = registry
		p.Shards = shards
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	}))
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(c.Close)

	return c, testIDs(items)
}

var benchSink *testItem

// benchmarkOrder returns the indices 0..n-1 in a fixed pseudo-random order, so a
// reader can walk the dataset in random order without paying for a random number
// generator inside the measured loop.
func benchmarkOrder(n int) []int32 {
	order := make([]int32, n)
	for i := range order {
		order[i] = int32(i)
	}

	r := rand.New(rand.NewSource(1))
	r.Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })

	return order
}

// BenchmarkGetThroughput answers "how many reads per second". It runs the cache
// in a realistic configuration - metrics registered, so the read path pays for
// the shard counter - and reads 200 000 items in random order.
//
// RunParallel spawns GOMAXPROCS goroutines, so the scaling curve comes from
// -cpu=1,2,4,...
func BenchmarkGetThroughput(b *testing.B) {
	source := newTestSource(200_000)

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Shards = 256
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	}))
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(c.Close)

	IDs := testIDs(200_000)
	order := benchmarkOrder(len(IDs))

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// a different starting offset per goroutine, so they do not walk the same
		// cache lines in lockstep
		i := int(atomicOffset.Add(4096))

		var value *testItem
		for pb.Next() {
			value = c.Get(IDs[order[i%len(order)]])
			i++
		}

		benchSink = value
	})

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/elapsed/1e6, "Mreads/s")
}

// BenchmarkGetThroughputHot is the same measurement over a working set of 1000
// items, which fits in the CPU caches. Real traffic is usually skewed like this,
// so the two benchmarks bracket what to expect: the full random walk pays a
// memory miss on every lookup, this one does not.
func BenchmarkGetThroughputHot(b *testing.B) {
	source := newTestSource(200_000)

	c, err := New(testParams(source, func(p *Params[testItem]) {
		p.MetricsRegistry = test_utils.MetricsRegistry()
		p.Shards = 256
		p.Timeouts.SyncInterval = time.Hour
		p.Timeouts.RefreshInterval = time.Hour
	}))
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(c.Close)

	const hot = 1000

	IDs := testIDs(200_000)[:hot]
	order := benchmarkOrder(hot)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := int(atomicOffset.Add(97))

		var value *testItem
		for pb.Next() {
			value = c.Get(IDs[order[i%len(order)]])
			i++
		}

		benchSink = value
	})

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/elapsed/1e6, "Mreads/s")
}

var atomicOffset atomic.Int64

func BenchmarkGet(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	var value *testItem
	for i := 0; i < b.N; i++ {
		value = c.Get(IDs[i%len(IDs)])
	}

	benchSink = value
}

func BenchmarkGetParallel(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		var value *testItem
		for pb.Next() {
			value = c.Get(IDs[i%len(IDs)])
			i++
		}

		benchSink = value
	})
}

// BenchmarkGetParallelWithMetrics shows what the metrics cost on the read path.
// It should be within noise of BenchmarkGetParallel - that is the whole reason
// the read counters live in the shards instead of in Prometheus.
func BenchmarkGetParallelWithMetrics(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256, test_utils.MetricsRegistry())

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		var value *testItem
		for pb.Next() {
			value = c.Get(IDs[i%len(IDs)])
			i++
		}

		benchSink = value
	})
}

// BenchmarkGetParallelOneShard shows what a badly distributing shard hash costs -
// every lookup lands in the same shard.
func BenchmarkGetParallelOneShard(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 1, nil)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		var value *testItem
		for pb.Next() {
			value = c.Get(IDs[i%len(IDs)])
			i++
		}

		benchSink = value
	})
}

func BenchmarkGetMiss(b *testing.B) {
	c, _ := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	var value *testItem
	for i := 0; i < b.N; i++ {
		value = c.Get(int64(10_000_000 + i))
	}

	benchSink = value
}

// BenchmarkGetMissParallel is the shape a flood of lookups for IDs that do not
// exist has. It is the one read path a caller can point at the cache on purpose,
// so it has to scale with the cores like a hit does - which is what the shared
// read in the rate limiter is for.
func BenchmarkGetMissParallel(b *testing.B) {
	c, _ := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ID := 10_000_000 + atomicOffset.Add(1<<20)

		var value *testItem
		for pb.Next() {
			ID++
			value = c.Get(ID)
		}

		benchSink = value
	})
}

func BenchmarkInvalidateDeduplicated(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	// the same ID over and over - the flag on the entry collapses it before the
	// pending set mutex
	for i := 0; i < b.N; i++ {
		c.Invalidate(IDs[0])
	}
}

func BenchmarkInvalidateDistinct(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Invalidate(IDs[i%len(IDs)])
	}
}

func BenchmarkSync(b *testing.B) {
	c, _ := benchmarkCache(b, 200_000, 256, nil)

	l := c.newLoader()
	c.sync(c.ctx, l)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.sync(c.ctx, l)
	}
}
