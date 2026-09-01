package eventual

import (
	"sync"
	"testing"
	"unsafe"
)

type payload struct{ N int }

// starsi verze nikdy neprepise novejsi, ani kdyz dobehne pozdeji
func TestApplyVersionOrdering(t *testing.T) {
	e := &entry[payload]{}
	e.state.Store(&state[payload]{value: &payload{N: 100}, version: 100})

	if !e.apply(&state[payload]{value: &payload{N: 102}, version: 102}) {
		t.Fatal("verze 102 se mela aplikovat")
	}
	if e.apply(&state[payload]{value: &payload{N: 101}, version: 101}) {
		t.Fatal("verze 101 se NEmela aplikovat po 102")
	}
	if got := e.state.Load().value.N; got != 102 {
		t.Fatalf("ocekavano 102, ziskano %d", got)
	}
}

// souběžné zápisy ruznych verzi: vyhrava vzdy nejvyssi
func TestApplyConcurrentHighestWins(t *testing.T) {
	for round := 0; round < 200; round++ {
		e := &entry[payload]{}
		e.state.Store(&state[payload]{value: &payload{N: 0}, version: 0})

		wg := sync.WaitGroup{}
		for v := 1; v <= 32; v++ {
			wg.Add(1)
			go func(v int) {
				defer wg.Done()
				e.apply(&state[payload]{value: &payload{N: v}, version: int64(v)})
			}(v)
		}
		wg.Wait()

		got := e.state.Load()
		if got.version != 32 || got.value.N != 32 {
			t.Fatalf("kolo %d: ocekavana verze 32, ziskano verze=%d hodnota=%d", round, got.version, got.value.N)
		}
	}
}

func TestShardSizeIsCacheLineMultiple(t *testing.T) {
	sizes := map[string]uintptr{
		"shard[struct{}]":  unsafe.Sizeof(shard[struct{}]{}),
		"shard[payload]":   unsafe.Sizeof(shard[payload]{}),
		"shard[[512]byte]": unsafe.Sizeof(shard[[512]byte]{}),
	}
	for name, size := range sizes {
		if size%cacheLinePadBytes != 0 {
			t.Errorf("%s = %d B, neni nasobek %d", name, size, cacheLinePadBytes)
		}
		t.Logf("%s = %d B", name, size)
	}
	t.Logf("entry[payload] = %d B, state[payload] = %d B",
		unsafe.Sizeof(entry[payload]{}), unsafe.Sizeof(state[payload]{}))
	t.Logf("1024 shardu = %d kB", 1024*unsafe.Sizeof(shard[payload]{})/1024)
}

// rovnomernost hashe pro Galera ID s ruznym krokem a ruznym poctem shardu
func TestShardDistribution(t *testing.T) {
	for _, shards := range []int{256, 512, 1024} {
		bits := uint(0)
		for 1<<bits < shards {
			bits++
		}
		for _, step := range []int64{1, 2, 3, 4, 6, 8, 10, 16} {
			buckets := make([]int, shards)
			const n = 100_000
			for i := int64(0); i < n; i++ {
				buckets[MultiplyShiftHash(1+i*step, bits)]++
			}
			exp := float64(n) / float64(shards)
			maxLoad, empty := 0, 0
			for _, b := range buckets {
				if b > maxLoad {
					maxLoad = b
				}
				if b == 0 {
					empty++
				}
			}
			ratio := float64(maxLoad) / exp
			if empty != 0 || ratio > 1.5 {
				t.Errorf("shards=%d krok=%d: prazdnych=%d max/avg=%.2f", shards, step, empty, ratio)
			}
		}
	}
}
