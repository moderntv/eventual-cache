package eventual

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var ErrNotFound = errors.New("not found")

const (
	defaultShards     = 256
	fibonacci64       = 0x9E3779B97F4A7C15
	cacheLinePadBytes = 128
)

// ShardHashFunc mapuje ID na index shardu v rozsahu [0, 1<<bits).
type ShardHashFunc func(id int64, bits uint) uint32

// MultiplyShiftHash je vychozi hash shardu: jedno nasobeni a jeden posun.
// Bere HORNI bity soucinu, takze je rovnomerny i pro ID tvorici aritmetickou
// posloupnost (MariaDB Galera s auto_increment_increment > 1).
func MultiplyShiftHash(id int64, bits uint) uint32 {
	return uint32((uint64(id) * fibonacci64) >> (64 - bits))
}

// SplitMix64Hash ma plny avalanche efekt. Pouzit, kdyby cache nekdy klicovala
// podle hodnot prichazejicich zvenci (multiply-shift neni odolny proti utoku).
func SplitMix64Hash(id int64, bits uint) uint32 {
	x := uint64(id) + fibonacci64
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	x ^= x >> 31

	return uint32(x >> (64 - bits))
}

// state je nemenny snapshot polozky. Nikdy se nemutuje, jen nahrazuje pres CAS.
type state[T any] struct {
	value    *T    // nil == tombstone (ve zdroji neexistuje)
	version  int64 // volitelne; 0 == verzovani se nepouziva
	loadedAt int64 // ms, kdy byl tento stav nacten ze zdroje (ochrana sweepu)
	// deadline v ms. Pro zivou polozku: kdy ji znovu nacist (ReloadInterval).
	// Pro tombstone: kdy ho zahodit (NotFoundTTL). 0 == bez deadline.
	deadline int64
}

type entry[T any] struct {
	state      atomic.Pointer[state[T]]
	refreshing atomic.Bool
}

// apply zapise novy stav, pokud neni starsi nez ten soucasny.
func (e *entry[T]) apply(next *state[T]) (applied bool) {
	for {
		cur := e.state.Load()
		if cur != nil && next.version != 0 && cur.version > next.version {
			return false
		}
		if e.state.CompareAndSwap(cur, next) {
			return true
		}
	}
}

type shardCore[T any] struct {
	mu   sync.RWMutex
	data map[int64]*entry[T]
}

type shard[T any] struct {
	shardCore[T]
	_ [cacheLinePadBytes - unsafe.Sizeof(shardCore[struct{}]{})]byte
}

// kontrola pri prekladu, ze shard je presne nasobek cache line
var _ = [1]struct{}{}[unsafe.Sizeof(shard[struct{}]{})%cacheLinePadBytes]

type LoadedEntry[T any] struct {
	ID      int64
	Value   *T
	Version int64
	Err     error
}

// randomizeDuration je v hotove knihovne v internal/utils (prevzato z lazy-cache).
func randomizeDuration(d time.Duration, randomizer float64) time.Duration {
	if randomizer == 0 {
		return d
	}

	add := time.Duration(float64(d) * rand.Float64() * randomizer)
	if rand.Intn(2) == 0 {
		add = -add
	}

	return d + add
}

type Timeouts struct {
	SyncInterval       time.Duration `mapstructure:"sync_interval"`
	ReloadInterval     time.Duration `mapstructure:"reload_interval"`
	NotFoundTTL        time.Duration `mapstructure:"not_found_ttl"`
	ErrorRetryInterval time.Duration `mapstructure:"error_retry_interval"`
	Randomizer         float64       `mapstructure:"randomizer"`
}

type Cache[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	timeouts Timeouts
	shards   []shard[T]
	hash     ShardHashFunc
	bits     uint

	refreshCh chan int64
	pending   sync.Map

	wg sync.WaitGroup

	itemsCount atomic.Int64
}

func (c *Cache[T]) shardOf(id int64) *shard[T] {
	return &c.shards[c.hash(id, c.bits)]
}

// Get vraci hodnotu z lokalni repliky, nebo nil kdyz polozka neexistuje.
// Nikdy neblokuje na I/O.
func (c *Cache[T]) Get(id int64) *T {
	sh := c.shardOf(id)

	sh.mu.RLock()
	e, exists := sh.data[id]
	sh.mu.RUnlock()

	if !exists {
		c.onMiss(id)

		return nil
	}

	return e.state.Load().value
}

// GetMultiple vraci hodnoty pro zadana ID. Chybejici klic znamena "neexistuje".
func (c *Cache[T]) GetMultiple(ids []int64) (values map[int64]*T) {
	values = make(map[int64]*T, len(ids))

	for _, id := range ids {
		value := c.Get(id)
		if value != nil {
			values[id] = value
		}
	}

	return
}

func (c *Cache[T]) onMiss(id int64) {
	if _, loaded := c.pending.LoadOrStore(id, struct{}{}); loaded {
		return
	}

	select {
	case c.refreshCh <- id:
	default:
		c.pending.Delete(id)
	}
}

// Invalidate oznaci polozku k obnove na pozadi. Volajici ji zavola napr. po
// prijeti NATS zpravy - eventual-cache sama zadne zpravy neposlouchá.
func (c *Cache[T]) Invalidate(id int64) {
	sh := c.shardOf(id)

	sh.mu.RLock()
	e, exists := sh.data[id]
	sh.mu.RUnlock()

	if !exists {
		c.onMiss(id)

		return
	}

	if !e.refreshing.CompareAndSwap(false, true) {
		return
	}

	select {
	case c.refreshCh <- id:
	default:
		e.refreshing.Store(false)
	}
}

// applyLoaded zapise vysledek loaderu do repliky.
func (c *Cache[T]) applyLoaded(le LoadedEntry[T], nowMillis int64) {
	sh := c.shardOf(le.ID)

	sh.mu.RLock()
	e, exists := sh.data[le.ID]
	sh.mu.RUnlock()

	loadFailed := le.Err != nil && !errors.Is(le.Err, ErrNotFound)

	if exists {
		defer e.refreshing.Store(false)

		// chyba nikdy neprepisuje ani nemaze uz nactenou hodnotu
		if loadFailed {
			return
		}

		e.apply(c.newState(le, nowMillis))

		return
	}

	defer c.pending.Delete(le.ID)

	if loadFailed {
		return
	}

	ne := &entry[T]{}
	ne.state.Store(c.newState(le, nowMillis))

	sh.mu.Lock()
	if _, raced := sh.data[le.ID]; !raced {
		sh.data[le.ID] = ne
		c.itemsCount.Add(1)
	}
	sh.mu.Unlock()
}

func (c *Cache[T]) newState(le LoadedEntry[T], nowMillis int64) *state[T] {
	s := &state[T]{
		value:    le.Value,
		version:  le.Version,
		loadedAt: nowMillis,
	}

	ttl := c.timeouts.ReloadInterval
	if le.Value == nil {
		ttl = c.timeouts.NotFoundTTL
	}
	if ttl > 0 {
		s.deadline = nowMillis + randomizeDuration(ttl, c.timeouts.Randomizer).Milliseconds()
	}

	return s
}

// ForEach projde celou repliku. Callback nesmi volat metody cache.
func (c *Cache[T]) ForEach(fn func(id int64, value *T) bool) {
	for i := range c.shards {
		sh := &c.shards[i]

		sh.mu.RLock()
		for id, e := range sh.data {
			value := e.state.Load().value
			if value == nil {
				continue
			}
			if !fn(id, value) {
				sh.mu.RUnlock()

				return
			}
		}
		sh.mu.RUnlock()
	}
}

func (c *Cache[T]) Len() int { return int(c.itemsCount.Load()) }

// Close zastavi vsechny goroutiny a pocka na jejich ukonceni.
func (c *Cache[T]) Close() {
	c.cancel()
	c.wg.Wait()
}
