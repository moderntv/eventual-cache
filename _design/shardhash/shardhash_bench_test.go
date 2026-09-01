package main

import "testing"

var sink uint32

func benchHash(b *testing.B, f func(int64) uint32) {
	b.ReportAllocs()

	var (
		s  uint32
		id = int64(1_000_000)
	)
	for i := 0; i < b.N; i++ {
		s += f(id)
		id += 6 // Galera se 6 nody
	}

	sink = s
}

func BenchmarkMask(b *testing.B)     { benchHash(b, hMask) }
func BenchmarkFib(b *testing.B)      { benchHash(b, hFib) }
func BenchmarkSplitMix(b *testing.B) { benchHash(b, hSplitMix) }
func BenchmarkXXMix(b *testing.B)    { benchHash(b, hXXMix) }
