// Package main je jednorázový měřicí harness pro výběr hashovací funkce shardu
// v eventualcache. Doplň reálný vzorek ID do loadRealIDs() a spusť `go run .`.
//
//	go run .                       # tabulka distribuce
//	go test -bench=. -run='^$' .   # rychlost jednotlivých hashů
package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

const shards = 256 // musí být mocnina dvou

const shardBits = 8 // log2(shards)

// ---------------------------------------------------------------- kandidáti

// hMask je varianta z původního návrhu: spodní bity ID.
func hMask(id int64) uint32 { return uint32(uint64(id) & (shards - 1)) }

const fibonacci64 = 0x9E3779B97F4A7C15 // 2^64 / φ, liché

// hFib je multiply-shift (Fibonacci) hash - doporučený default.
func hFib(id int64) uint32 { return uint32((uint64(id) * fibonacci64) >> (64 - shardBits)) }

func splitmix64(x uint64) uint64 {
	x += fibonacci64
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB

	return x ^ (x >> 31)
}

// hSplitMix má plný avalanche efekt - použít, pokud by ID pocházela z venku.
func hSplitMix(id int64) uint32 { return uint32(splitmix64(uint64(id)) >> (64 - shardBits)) }

// hXXMix je finalizér ve stylu xxhash pro 8bajtový vstup.
func hXXMix(id int64) uint32 {
	h := uint64(id) * 0x9E3779B185EBCA87
	h ^= h >> 29
	h *= 0xC2B2AE3D27D4EB4F
	h ^= h >> 32

	return uint32(h >> (64 - shardBits))
}

type hashFunc struct {
	name string
	f    func(int64) uint32
}

var hashFuncs = []hashFunc{
	{"mask id&255", hMask},
	{"fib >>56", hFib},
	{"splitmix64", hSplitMix},
	{"xxmix", hXXMix},
}

// ------------------------------------------------------------- generátory ID

// arith vrací aritmetickou posloupnost - odpovídá jednomu Galera nodu
// s auto_increment_increment = step.
func arith(start, step int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = start + int64(i)*step
	}

	return out
}

// galera simuluje cluster s `nodes` nody, z nichž `active` skutečně zapisuje.
func galera(start int64, nodes, active, n int) []int64 {
	out := make([]int64, 0, n)
	for i := 0; len(out) < n; i++ {
		for a := 1; a <= active && len(out) < n; a++ {
			out = append(out, start+int64(i)*int64(nodes)+int64(a))
		}
	}

	return out
}

func randomIDs(n int) []int64 {
	r := rand.New(rand.NewSource(42))
	out := make([]int64, n)
	for i := range out {
		out[i] = r.Int63()
	}

	return out
}

// loadRealIDs načte ID z textového souboru (jedno ID na řádek), pokud existuje.
// Vyrob ho třeba přes: mysql -N -e 'SELECT id FROM tabulka' > real_ids.txt
func loadRealIDs(path string) []int64 {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var out []int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		id, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, id)
	}

	return out
}

// ------------------------------------------------------------------ měření

type stat struct {
	empty    int
	maxLoad  int
	ratio    float64 // nejplnější shard / průměr
	chi2norm float64 // 1.0 == ideální náhodné rozložení, 0.0 == dokonale rovnoměrné
}

func measure(ids []int64, f func(int64) uint32) stat {
	buckets := make([]int, shards)
	for _, id := range ids {
		buckets[f(id)]++
	}

	exp := float64(len(ids)) / shards

	var (
		s    stat
		chi2 float64
	)
	for _, b := range buckets {
		if b == 0 {
			s.empty++
		}
		if b > s.maxLoad {
			s.maxLoad = b
		}
		d := float64(b) - exp
		chi2 += d * d / exp
	}

	s.ratio = float64(s.maxLoad) / exp
	s.chi2norm = chi2 / float64(shards-1)

	return s
}

func main() {
	const n = 100_000

	cases := []struct {
		name string
		ids  []int64
	}{
		{"krok 1 (1 node)", arith(1, 1, n)},
		{"krok 2 (2 nody)", arith(1, 2, n)},
		{"krok 3 (3 nody)", arith(1, 3, n)},
		{"krok 4 (4 nody)", arith(1, 4, n)},
		{"krok 6 (6 nodu)", arith(1, 6, n)},
		{"krok 8 (8 nodu)", arith(1, 8, n)},
		{"krok 10 (10 nodu)", arith(1, 10, n)},
		{"krok 16", arith(1, 16, n)},
		{"galera 6 nodu / 6 aktivnich", galera(1_000_000, 6, 6, n)},
		{"galera 6 nodu / 3 aktivni", galera(1_000_000, 6, 3, n)},
		{"krok 6, velky offset", arith(9_000_000_000_000, 6, n)},
		{"nahodna ID", randomIDs(n)},
	}

	if real := loadRealIDs("real_ids.txt"); len(real) > 0 {
		cases = append(cases, struct {
			name string
			ids  []int64
		}{fmt.Sprintf("REALNA ID (%d)", len(real)), real})
	} else {
		fmt.Fprintln(os.Stderr, "poznamka: real_ids.txt nenalezen, meri se jen synteticka data")
	}

	fmt.Printf("%-30s | %-12s | %7s | %8s | %8s\n", "vzor ID", "hash", "prazdne", "max/avg", "chi2n")
	fmt.Println(strings.Repeat("-", 80))

	for _, c := range cases {
		for _, h := range hashFuncs {
			s := measure(c.ids, h.f)
			fmt.Printf("%-30s | %-12s | %7d | %8.2f | %8.2f\n",
				c.name, h.name, s.empty, s.ratio, s.chi2norm)
		}
		fmt.Println(strings.Repeat("-", 80))
	}
}
