package eventual

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	t.Run("shard_bits", testShardBits)
	t.Run("power_of_two", testPowerOfTwo)
	t.Run("distribution_arithmetic_ids", testHashDistributionArithmeticIDs)
	t.Run("distribution_random_ids", testHashDistributionRandomIDs)
	t.Run("mask_would_fail", testMaskWouldFail)
	t.Run("shard_size", testShardSize)
}

func testShardBits(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint(0), shardBits(1))
	assert.Equal(t, uint(1), shardBits(2))
	assert.Equal(t, uint(8), shardBits(256))
	assert.Equal(t, uint(10), shardBits(1024))
}

func testPowerOfTwo(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 2, 4, 256, 1024, 4096} {
		assert.True(t, isPowerOfTwo(n), n)
	}
	for _, n := range []int{0, -8, 3, 6, 100, 1000} {
		assert.False(t, isPowerOfTwo(n), n)
	}
}

// distribution measures how evenly a hash spreads IDs over the shards.
func distribution(hash ShardHashFunc, IDs []int64, shards int) (empty int, maxOverAvg float64) {
	bits := shardBits(shards)
	buckets := make([]int, shards)

	for _, ID := range IDs {
		buckets[hash(ID, bits)]++
	}

	maxLoad := 0
	for _, b := range buckets {
		if b == 0 {
			empty++
		}
		if b > maxLoad {
			maxLoad = b
		}
	}

	return empty, float64(maxLoad) / (float64(len(IDs)) / float64(shards))
}

func arithmeticIDs(start, step int64, n int) (IDs []int64) {
	IDs = make([]int64, n)
	for i := range IDs {
		IDs[i] = start + int64(i)*step
	}

	return
}

// IDs of a MariaDB Galera node grow by auto_increment_increment, which is the
// number of nodes. The shard hash has to stay uniform for any such step.
func testHashDistributionArithmeticIDs(t *testing.T) {
	t.Parallel()

	hashes := map[string]ShardHashFunc{
		"multiply_shift": MultiplyShiftHash,
		"splitmix64":     SplitMix64Hash,
	}

	for name, hash := range hashes {
		for _, shards := range []int{256, 512, 1024} {
			for _, step := range []int64{1, 2, 3, 4, 6, 8, 10, 16, 100} {
				for _, start := range []int64{1, 1_000_000, 9_000_000_000_000} {
					IDs := arithmeticIDs(start, step, 100_000)

					empty, maxOverAvg := distribution(hash, IDs, shards)

					assert.Zero(t, empty, "%s shards=%d step=%d start=%d: %d empty shards", name, shards, step, start, empty)
					assert.Less(t, maxOverAvg, 1.5, "%s shards=%d step=%d start=%d: max/avg %.2f", name, shards, step, start, maxOverAvg)
				}
			}
		}
	}
}

func testHashDistributionRandomIDs(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewSource(42))
	IDs := make([]int64, 100_000)
	for i := range IDs {
		IDs[i] = r.Int63()
	}

	empty, maxOverAvg := distribution(MultiplyShiftHash, IDs, 256)

	assert.Zero(t, empty)
	assert.Less(t, maxOverAvg, 1.5)
}

// testMaskWouldFail documents why the low bits of the ID are not used directly.
func testMaskWouldFail(t *testing.T) {
	t.Parallel()

	mask := func(ID int64, bits uint) uint32 { return uint32(uint64(ID) & (1<<bits - 1)) }

	// step 6 (six Galera nodes) leaves exactly half of the shards empty
	empty, maxOverAvg := distribution(mask, arithmeticIDs(1, 6, 100_000), 256)
	assert.Equal(t, 128, empty)
	assert.InDelta(t, 2.0, maxOverAvg, 0.01)

	// the same IDs with the real hash use everything
	empty, maxOverAvg = distribution(MultiplyShiftHash, arithmeticIDs(1, 6, 100_000), 256)
	assert.Zero(t, empty)
	assert.Less(t, maxOverAvg, 1.1)
}

func testShardSize(t *testing.T) {
	t.Parallel()

	// the padding must hold whatever the value type is
	assert.Zero(t, unsafe.Sizeof(shard[struct{}]{})%cacheLinePadBytes)
	assert.Zero(t, unsafe.Sizeof(shard[testItem]{})%cacheLinePadBytes)
	assert.Zero(t, unsafe.Sizeof(shard[[512]byte]{})%cacheLinePadBytes)

	t.Logf("shard=%d B entry=%d B",
		unsafe.Sizeof(shard[testItem]{}),
		unsafe.Sizeof(entry[testItem]{}))
}

var hashSink uint32

func benchmarkHash(b *testing.B, hash ShardHashFunc) {
	b.ReportAllocs()

	var (
		sum uint32
		ID  = int64(1_000_000)
	)

	for i := 0; i < b.N; i++ {
		sum += hash(ID, 8)
		ID += 6
	}

	hashSink = sum
}

func BenchmarkMultiplyShiftHash(b *testing.B) { benchmarkHash(b, MultiplyShiftHash) }
func BenchmarkSplitMix64Hash(b *testing.B)    { benchmarkHash(b, SplitMix64Hash) }
