# Eventual cache

Eventual cache is an **eventually consistent, in-memory replica of a whole
dataset** keyed by `int64`. The replica is filled once at start-up and then kept
up to date in the background; reads are served from local memory and **never
block on I/O**.

The cache knows nothing about where the data comes from. It only needs two
functions: one that lists the IDs the source currently has, and one that loads a
batch of items by ID. Anything that can answer those two questions can back the
cache.

The value type `T` is generic, the key is always `int64`.

## Public API

-   **`New(params)`** — creates the cache and blocks until the whole dataset is
    in memory, or fails. A service never starts serving from an incomplete
    replica.
-   **`Get(ID) *T`** — returns the item from the local replica, or `nil` when the
    replica does not have it. Never blocks on I/O.
-   **`Invalidate(ID)`** — marks the item for a reload. It does not load
    anything; the background goroutine reloads all marked items together.
-   **`Close()`** — stops the background goroutine and waits for it.

That is the whole surface. There is deliberately no `GetMultiple`, no `ForEach`,
no `Len` and no `InvalidateAll` — call `Get` in a loop, and let the
reconciliation do the bulk work.

## How the replica is filled and kept up to date

Everything below happens in **one** background goroutine. Reloads and
reconciliation therefore never interleave, which is one less race to reason about
and costs nothing — both are background work with no latency budget.

**1. Initial load.** `New` calls `ListIDsFunc` to get every ID the source has and
then loads them through `LoadMultipleFunc` in batches of `BatchSize`. It returns
only once all batches are stored, or with an error.

**2. Marking.** Four things put an ID on the list of items to load:

| Trigger | Rate limited |
|---|---|
| `Invalidate(ID)` from the caller | no |
| The `TTL` of an item passed and somebody called `Get` on it | no |
| `Get(ID)` for an ID the replica does not have | **yes**, `MissRateLimit`/s |
| A reconciliation found an ID the replica does not have | no |

Marking is cheap and never touches the source: a flag on the item plus the ID in
a set. Repeated marks of the same ID between two runs collapse into one load.

**3. Batched reloading.** Every `RefreshInterval` (1 s by default) the goroutine
drains the whole set and loads it through `LoadMultipleFunc` in batches of
`BatchSize`. Reaching `BatchSize` marked IDs wakes it earlier, so a burst of
invalidations does not wait for the tick.

The cache itself does not subscribe to anything — the transport (NATS, a webhook,
polling, anything) stays in the service:

```go
natsConn.Subscribe("cache.item.updated", func(msg *nats.Msg) {
	var event pb.ItemUpdated
	err := proto.Unmarshal(msg.Data, &event)
	if err != nil {
		log.Warn().Err(err).Msg("invalid invalidation message")

		return
	}

	for _, ID := range event.GetIds() {
		cache.Invalidate(ID)
	}
})
```

**4. Periodic reconciliation.** Every `SyncInterval` the cache fetches the list of
all IDs again, removes items the source no longer has and loads items the replica
does not have yet. It does **not** re-read the content of items the replica
already holds — that is what invalidations and the TTL are for. It is the safety
net for lost invalidations.

## TTL — a refresh interval, not an expiration

`Timeouts.TTL` is how long a value is considered fresh. Once it passes, the next
`Get` **still returns the value** and only marks the item for a reload on its way
out. The reload then happens in the background like any other.

An item is therefore never dropped because it is old — only because the source
stopped having it. A stale value is always better than no value here: the caller
asked a cache, not the database.

The TTL of each item is randomized by at least ±10 % (more with a bigger
`Randomizer`), so that the items stored together by the initial load do not all
come due in the same instant.

With `TTL` left at 0 values are only reloaded when somebody invalidates them or a
reconciliation notices they are missing.

The TTL check in `Get` reads a coarse clock the background goroutine updates every
100 ms. A `time.Now()` call on the read path would cost more than the whole rest
of `Get`; 100 ms of imprecision on a TTL measured in minutes costs nothing.

## What the source has to provide

```go
// ListIDsFunc returns the IDs of all items the source currently has. Called by
// the initial load and by every reconciliation, so it should be cheap.
type ListIDsFunc func(ctx context.Context) (IDs []int64, err error)

// LoadMultipleFunc loads a batch of items in one call.
type LoadMultipleFunc[T any] func(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error)

type LoadedEntry[T any] struct {
	ID    int64
	Value *T
	Err   error
}
```

Both are required. `LoadMultipleFunc` is always called with at most `BatchSize`
IDs.

## When an item is removed

An item is removed when a `LoadMultipleFunc` call that asked for its ID:

-   returned a `LoadedEntry` with a `nil` `Value`, or
-   returned a `LoadedEntry` with `Err` set to `ErrNotFound`, or
-   did not mention the ID at all.

Anything else leaves the item alone. A batch that fails as a whole (`err != nil`)
and a per item `Err` other than `ErrNotFound` are treated as "we do not know" —
the replica keeps what it has, and the IDs stay marked so the next run tries
again. A broken source degrades the freshness of the replica, never its content.

### Deletion by reconciliation takes two runs

The reconciliation works from a snapshot of IDs, so it cannot delete on the spot.
The first run that does not find an ID only **marks** the item; the next run that
still does not find it deletes it.

One run is not enough evidence. An item loaded while the reconciliation was
running is naturally missing from the snapshot it started with, and the ID listing
itself can be a stale read from a replica. Keeping a deleted item for one extra
`SyncInterval` is the cheaper mistake — dropping an item that really exists means
serving `nil` for something the caller can see in the database.

A successful load clears the mark, so an item that gets invalidated between two
reconciliations is never deleted.

## Caveats

-   **Do not mutate an item after it has been handed to the cache.** The cache
    stores pointers; changing the pointed-to data affects every reader. Treat the
    values returned by `Get` as read-only.
-   Data may lag behind the source — that is the whole point of the library.
-   `Get` never waits for a load. For an item the source created a moment ago it
    returns `nil` and queues the ID; a later `Get` can already succeed. If your
    code needs a consistent read, this cache is the wrong tool.
-   Lookups of IDs the replica does not have are rate limited (`MissRateLimit`,
    100/s by default). Without it a caller iterating random IDs would turn every
    read into a query. Explicit invalidations are never limited, so a genuinely
    new item always gets through.
-   The whole dataset is held in memory by every instance of the service.

## Sharding and the shard hash

The map is split into `Shards` independent segments, each with its own
`RWMutex`. A shard costs 128 B, so the number of shards is chosen by concurrency,
not by memory: roughly **8 to 16 times `GOMAXPROCS`**, rounded up to a power of
two (so 1024 for a 64 core machine). Default is 256.

The default `ShardHash` is `MultiplyShiftHash` — one multiplication by the
Fibonacci constant and one shift, taking the **top** bits of the product:

```go
func MultiplyShiftHash(ID int64, bits uint) uint32 {
	return uint32((uint64(ID) * 0x9E3779B97F4A7C15) >> (64 - bits))
}
```

It costs about 0.7 ns, and because it takes the top bits it stays uniform even
when the IDs form an arithmetic progression — which is the common shape of
auto-increment IDs, and the case where masking the low bits (`ID & (shards-1)`)
leaves most shards permanently empty.

Multiply-shift is not resistant to a deliberately crafted set of IDs. When the
IDs can be chosen by an untrusted party, use `SplitMix64Hash` (full avalanche,
roughly twice as slow). `Params.ShardHash` takes any function with the
`ShardHashFunc` signature.

## Params

-   **Context** — shutdown of the background goroutine. Required.
-   **Log** — `zerolog.Logger`. Required.
-   **Name** — used in logs and metrics. Required.
-   **MetricsRegistry** — `*cadre_metrics.Registry`; when set, Prometheus metrics
    are registered. Optional.
-   **ListIDsFunc** — lists all IDs the source has. Required.
-   **LoadMultipleFunc** — loads a batch of items by ID. Required.
-   **BatchSize** — maximum number of IDs per `LoadMultipleFunc` call. Applies to
    the initial load, to reloads of marked items and to the reconciliation.
    Reaching this many marked IDs also wakes the background goroutine early.
    Default 200.
-   **MissRateLimit** — how many IDs unknown to the replica `Get` may queue per
    second. Default 100, zero or less means no limit.
-   **Shards** — power of two, default 256.
-   **ShardHash** — default `MultiplyShiftHash`.
-   **Timeouts** — see below.

### Timeouts

-   **SyncInterval** — how often the reconciliation runs. Randomized by
    `Randomizer`. Required, must be > 0.
-   **RefreshInterval** — how often marked items are reloaded. Default 1s.
-   **TTL** — how long a value is considered fresh, see above. Default 0 (off).
-   **Randomizer** — `[0, 1]`. 0 = no jitter, 0.1 = ±10 %. Without it every
    instance of the service would hit the source in the same second.

## Metrics

When `MetricsRegistry` is set, these are registered (subsystem `eventual_cache`,
label `name`):

| Metric | Type | Description |
|--------|------|-------------|
| `items_count` | Gauge | Items in the replica |
| `pending_count` | Gauge | IDs waiting to be loaded |
| `last_sync_timestamp` | Gauge | Unix time of the last successful reconciliation |
| `last_sync_duration_seconds` | Gauge | Duration of the last reconciliation |
| `reads_count` | Counter | `Get` calls |
| `misses_count` | Counter | `Get` calls for an ID the replica does not have |
| `misses_rate_limited` | Counter | Lookups of unknown IDs dropped by the rate limit |
| `invalidations_count` | Counter | `Invalidate` calls |
| `batch_loads` | Counter | `LoadMultipleFunc` calls |
| `batch_load_items` | Counter | IDs passed to `LoadMultipleFunc` |
| `error_loads` | Counter | Loads that failed with something other than not found |
| `list_ids_errors` | Counter | `ListIDsFunc` calls that failed |
| `sync_runs` | Counter | Reconciliation runs |
| `sync_added` / `sync_marked` / `sync_removed` | Counter | What the reconciliations did |

`reads_count` and `misses_count` are counted **per shard** and collected into
Prometheus once a second by the background goroutine. Incrementing a Prometheus
counter directly in `Get` would create one globally contended cache line —
exactly what the sharding is there to avoid. Everything else is counted where the
event happens.

Worth alerting on: `time() - last_sync_timestamp` above a few sync intervals,
`list_ids_errors` growing, `misses_rate_limited` growing, and a `pending_count`
that does not come back down.

---

## Usage

```go
package items

import (
	"context"
	"time"

	cadre_metrics "github.com/moderntv/cadre/metrics"
	eventual "github.com/moderntv/eventual-cache"
	"github.com/rs/zerolog"
)

type Item struct {
	ID   int64
	Name string
}

// Source is whatever holds the data - a database, an HTTP API, a file.
type Source interface {
	ItemIDs(ctx context.Context) ([]int64, error)
	Items(ctx context.Context, IDs []int64) ([]Item, error)
}

type Repository struct {
	cache *eventual.Cache[Item]
}

func NewRepository(
	ctx context.Context,
	log zerolog.Logger,
	source Source,
	metrics *cadre_metrics.Registry,
) (*Repository, error) {
	c, err := eventual.New(eventual.Params[Item]{
		Context:         ctx,
		Log:             log,
		MetricsRegistry: metrics,
		Name:            "item",

		ListIDsFunc: source.ItemIDs,

		LoadMultipleFunc: func(ctx context.Context, IDs []int64) ([]eventual.LoadedEntry[Item], error) {
			items, err := source.Items(ctx, IDs)
			if err != nil {
				return nil, err
			}

			// IDs missing from the result are removed from the replica
			entries := make([]eventual.LoadedEntry[Item], 0, len(items))
			for i := range items {
				entries = append(entries, eventual.LoadedEntry[Item]{
					ID:    items[i].ID,
					Value: &items[i],
				})
			}

			return entries, nil
		},

		Timeouts: eventual.Timeouts{
			SyncInterval:    5 * time.Minute,
			RefreshInterval: time.Second,
			TTL:             10 * time.Minute,
			Randomizer:      0.1,
		},

		BatchSize: 200,
		Shards:    1024, // 64 core machine
	})
	if err != nil {
		return nil, err
	}

	return &Repository{cache: c}, nil
}

func (r *Repository) Item(ID int64) *Item { return r.cache.Get(ID) }

// Invalidate is what the invalidation transport calls.
func (r *Repository) Invalidate(IDs []int64) {
	for _, ID := range IDs {
		r.cache.Invalidate(ID)
	}
}

func (r *Repository) Close() { r.cache.Close() }
```

## Benchmarks

All numbers from an i5-11500H (6 cores / 12 threads, 2.9 GHz), 200 000 items,
256 shards. No allocations anywhere on the read path.

### How many reads per second

Measured with metrics registered and a TTL set, so the read path pays for the
shard counter and the coarse clock check. Two access patterns, because that is
what decides the answer: a random walk over the whole dataset misses the CPU
caches on every lookup, a skewed workload with a small hot set does not. Real
traffic sits between them.

| Readers | Random over 200 000 items | Hot set of 1 000 items |
|---|---|---|
| 1 | **9.6 M reads/s** (105 ns) | **49 M reads/s** (20 ns) |
| 4 | 25 M reads/s (40 ns) | 99 M reads/s (10 ns) |
| 8 | 53 M reads/s (19 ns) | – |
| 12 | **62 M reads/s** (16 ns) | **141 M reads/s** (7.1 ns) |

The random walk is bound by memory latency: a lookup is a chain of dependent
cache misses (shard → map bucket → entry → value), so nothing can be prefetched.
That is also why it keeps scaling to 12 threads on 6 cores — hyperthreading buys
more outstanding misses, not more arithmetic.

The hot set is bound by the `RLock`/`RUnlock` pair instead. It is a
read-modify-write on the shard's cache line, so the line bounces between cores.
Raising the shard count does **not** help: measured over the hot set, 256 → 16 384
shards takes throughput *down* from 144 to 117 M reads/s, because the shard array
itself stops fitting in cache. Around 7 ns per read is the floor of a sharded
`RWMutex` map on this machine.

```bash
go test -run='^$' -bench=Throughput -benchtime=3s -cpu=1,2,4,8,12 .
```

### Everything else

| Benchmark | ns/op | allocs/op |
|---|---|---|
| `Get` (serial, sequential walk) | 34.9 | 0 |
| `Get` (parallel, sequential walk) | 6.3 | 0 |
| `Get` (parallel, metrics registered) | 6.4 | 0 |
| `Get` (parallel, TTL set) | 6.5 | 0 |
| `Get` (parallel, **1 shard** — what a bad hash does) | 52.3 | 0 |
| `Get` (miss, rate limiter on the path) | 43.5 | 0 |
| `Invalidate` (same ID, collapsed by the flag) | 16.4 | 0 |
| `Invalidate` (distinct IDs) | 63.1 | 0 |
| Reconciliation over 200 000 items | 34.5 ms | 37 |

Metrics and TTL are both free on the read path — that is the point of the per
shard counters and of the coarse clock. A single shard is 8× slower than 256,
which is what the shard hash is for.

The reconciliation reuses its buffers between runs, so its allocation count does
not grow with the size of the dataset. Its 1.6 MB per run is the `[]int64` of
200 000 IDs that `ListIDsFunc` returns — the test source allocates it fresh every
time, the cache itself does not.

```bash
go test -run='^$' -bench=. -benchmem .
```
