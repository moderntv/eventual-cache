package eventual

// fibonacci64 is 2^64/phi rounded to an odd integer (Knuth's multiplicative
// hashing constant).
const fibonacci64 = 0x9E3779B97F4A7C15

// ShardHashFunc maps an ID to a shard index in range [0, 1<<bits).
type ShardHashFunc func(ID int64, bits uint) uint32

// MultiplyShiftHash is the default shard hash: one multiplication and one shift.
//
// It uses the TOP bits of the product, which keeps it uniform even for IDs
// forming an arithmetic progression. That matters for MariaDB Galera, which sets
// auto_increment_increment to the number of nodes, so IDs grow by a constant
// step. Masking the low bits instead (ID & (shards-1)) would leave
// shards - shards/gcd(step, shards) of the shards completely empty for any even
// step - with step 6 and 256 shards exactly half of them.
func MultiplyShiftHash(ID int64, bits uint) uint32 {
	return uint32((uint64(ID) * fibonacci64) >> (64 - bits))
}

// SplitMix64Hash has full avalanche. It is roughly twice as slow as
// MultiplyShiftHash and is only needed when the IDs may be chosen by an
// untrusted party - multiply-shift is not resistant to a deliberately crafted
// set of IDs.
func SplitMix64Hash(ID int64, bits uint) uint32 {
	x := uint64(ID) + fibonacci64
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	x ^= x >> 31

	return uint32(x >> (64 - bits))
}

// shardBits returns log2(shards). Shards must be a power of two.
func shardBits(shards int) (bits uint) {
	for 1<<bits < shards {
		bits++
	}

	return
}

func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}
