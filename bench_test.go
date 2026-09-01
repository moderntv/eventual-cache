package eventual

import (
	"context"
	"testing"
	"time"

	"github.com/moderntv/eventual-cache/internal/test_utils"
)

func benchmarkCache(b *testing.B, items, shards int) (c *Cache[testItem], IDs []int64) {
	b.Helper()

	source := newTestSource(items)

	IDs = make([]int64, items)
	for i := range IDs {
		IDs[i] = int64(1 + i*6)
	}

	c, err := New(Params[testItem]{
		Context:          context.Background(),
		Log:              test_utils.Logger(),
		Name:             "bench",
		LoadAllFunc:      source.loadAll,
		ListIDsFunc:      source.listIDs,
		LoadMultipleFunc: source.loadMultiple,
		LoadOneFunc:      source.loadOne,
		Timeouts:         Timeouts{SyncInterval: time.Hour, NotFoundTTL: time.Minute},
		Shards:           shards,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(c.Close)

	return c, IDs
}

var benchSink *testItem

func BenchmarkGet(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256)

	b.ReportAllocs()
	b.ResetTimer()

	var value *testItem
	for i := 0; i < b.N; i++ {
		value = c.Get(IDs[i%len(IDs)])
	}

	benchSink = value
}

func BenchmarkGetParallel(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256)

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
	c, IDs := benchmarkCache(b, 200_000, 1)

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

func BenchmarkGetMultiple100(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.GetMultiple(IDs[i%1000 : i%1000+100])
	}
}

func BenchmarkInvalidateDeduplicated(b *testing.B) {
	c, IDs := benchmarkCache(b, 200_000, 256)

	b.ReportAllocs()
	b.ResetTimer()

	// the same ID over and over - the dedup gate collapses it into one queue item
	for i := 0; i < b.N; i++ {
		c.Invalidate(IDs[0])
	}
}

func BenchmarkSync(b *testing.B) {
	c, _ := benchmarkCache(b, 200_000, 256)

	ctx := context.Background()
	_ = c.Sync(ctx)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = c.Sync(ctx)
	}
}
