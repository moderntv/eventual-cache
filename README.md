# Eventual cache

Eventual cache is an **eventually consistent, in-memory replica of a whole
dataset** keyed by `int64`. The replica is filled once at start-up and then kept
up to date in the background; reads are served from local memory and **never
block on I/O**.

The cache knows nothing about where the data comes from. It only needs two
functions: one that lists the IDs the source currently has together with their
versions, and one that loads a batch of items by ID. Anything that can answer
those two questions can back the cache.

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
and costs nothing — both are background work with no latency budget. Only the
batches inside a single load run are parallel, see `LoadConcurrency`.

**1. Initial load.** `New` calls `ListIDsFunc` to get every ID the source has and
then loads them through `LoadMultipleFunc` in batches of `BatchSize`. It returns
only once all batches are stored, or with an error.

**2. Marking.** Three things put an ID on the list of items to load:

| Trigger | Rate limited |
|---|---|
| `Invalidate(ID)` from the caller | no |
| A reconciliation found a newer version, or an ID the replica does not have | no |
| `Get(ID)` for an ID the replica does not have | **yes**, `MissRateLimit`/s |

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
all IDs and versions again and compares it against the replica. It removes items
the source no longer has, loads items the replica does not have yet, and marks
items whose version in the source is newer than the stored one. It is the safety
net for invalidations that never arrived.

The reconciliation only *decides*; the reloads it queues are performed by the
next refresh tick, like any other invalidation.

## Versions — how a lost invalidation is caught

Invalidation over a message bus is best-effort: a message can be dropped, and
nothing in the replica would ever notice. That is what `Version` is for.

`ListIDsFunc` reports a version along with every ID, and `LoadMultipleFunc`
reports the version of the content it returns. When a reconciliation sees a
version greater than the one stored with the value, it reloads the item.
**`SyncInterval` is therefore the upper bound on how long a lost invalidation can
leave a stale value in the replica** — not some multiple of an item's age.

The version is typically the unix microseconds of an `updated_at` column, but any
value that grows with every change will do. It is only ever compared against
another version **from the same source**, never against a clock of ours, so
neither the scale nor the clock of the machine running the cache matters:

```sql
SELECT id, UNIX_TIMESTAMP(updated_at) * 1000000 FROM items
```

Two things worth getting right:

-   **Use the same scale in both functions.** If `ListIDsFunc` reports real
    versions and `LoadMultipleFunc` leaves `Version` at 0, every reconciliation
    finds every item outdated and reloads the whole replica. Watch `sync_outdated`.
-   **Give the timestamp sub-second precision** (`DATETIME(6)`), or two changes
    within the same second collapse into one version and the second one is missed.

Reporting the version from `LoadMultipleFunc` is also what keeps an invalidation
from being done twice: without it the stored version would stay behind, and the
next reconciliation would reload everything the invalidations had just loaded.

Leaving `Version` at 0 everywhere switches change detection off — items are then
only reloaded by an explicit `Invalidate`, and `MaxAge` takes over as the safety
net. See the next section.

An item is never dropped because it is old, only because the source stopped
having it. There is no TTL and no expiration: a stale value is always better than
no value here, because the caller asked a cache, not the database.

## `MaxAge` — when the source cannot report versions

Not every source has a version to report. A payload assembled from a dozen tables
has no single `updated_at` to select, and a version invented by the service that
loads the item is worse than none at all: behind a load balancer the instance
that answers `ListIDsFunc` and the instance that answered `LoadMultipleFunc` are
different processes with different values, so every reconciliation would find
every item outdated and reload the whole replica, in a loop, forever.

For that case leave `Version` at 0 everywhere and set `Timeouts.MaxAge` instead.
An item that has not been loaded for that long is marked for a reload by the next
reconciliation, whether or not anything said it changed.

```go
Timeouts: eventual.Timeouts{
	SyncInterval: 15 * time.Minute,
	MaxAge:       6 * time.Hour,
	Randomizer:   0.1,
},
MaxRefreshPerSync: 42000,
```

With versions off, this is how each kind of change reaches the replica:

| Change | How it propagates | Within |
|---|---|---|
| the source gains an item | new in the ID set → loaded | 1× `SyncInterval` |
| the source loses an item | gone from the ID set → removed | 2× `SyncInterval` |
| the content of an item changes | `Invalidate` from the message bus | `RefreshInterval` |
| **the invalidation was lost** | **`MaxAge`** | `MaxAge` + `SyncInterval` |

Note that the first two rows need no versions at all — they are decided by
membership in the ID set, which the reconciliation diffs either way.

**The bound is `MaxAge` + `SyncInterval`, not `MaxAge`.** The age is only checked
during a reconciliation; walking every item on every `RefreshInterval` tick is not
affordable on a large replica. For the same reason a `MaxAge` shorter than
`SyncInterval` is rejected by `New` — it could never be honoured.

**Nothing is ever dropped because of its age.** An expired item is only *marked
for a reload*, and `Get` keeps returning the old value until the reload lands.
This is not a detail: `Get` → `nil` means "the source does not have this", so a
hole in a warm replica is a wrong answer, not a slow one. That is also why this is
not called a TTL.

### The cost, and how to cap it

A full replica reloading itself every `MaxAge` is real traffic: a million items
with `MaxAge` 1 h is ~278 loads/s **per instance**, all aimed at the source. With
6 h it is ~46/s. Measure before choosing.

Two things keep it from arriving all at once:

-   **The deadline of an item is drawn when it is stored.** The *first* store
    draws uniformly from `(0, MaxAge]`, every later reload takes a full `MaxAge`
    ± `Randomizer`. Without that first uniform draw the warm-up cohort — the whole
    dataset, stored within seconds — would fall due in the same window and reload
    together. After one period the replica has phased itself and stays phased.
-   **`MaxRefreshPerSync`** caps how many items one reconciliation may mark
    *because of their age*. It does not apply to reloads caused by a newer version
    or to deletions: those are correctness and must not be delayed. When the cap
    bites, the rest is taken by the next run — the degradation is "recovery takes
    longer", not "the source fell over".

The cap has to be able to get through the whole dataset within one `MaxAge`,
otherwise `MaxAge` is a wish rather than a bound:

```
MaxRefreshPerSync >= items * SyncInterval / MaxAge
```

A million items with `SyncInterval` 15 min and `MaxAge` 6 h needs
`1e6 * 15/360 ≈ 42 000` per run. Below that the items keep ageing past the limit,
which is exactly what `sync_age_deferred` shows: permanently non-zero means the
cap is too low.

The budget is split evenly between the shards (rounded up, so a cap smaller than
the shard count still does something), which is what lets the parallel sweep
enforce it without a counter the workers share. The cap therefore holds only
roughly — fine for a hygienic operation.

`MaxAge` is off by default: the deadline stays 0, the sweep skips the check on
the first comparison, and nothing ever expires. The 8 B per item it needs are
part of every entry either way — see [Sizing](#sizing-a-large-replica).

## What the source has to provide

```go
// ListIDsFunc returns every item the source currently has, as an ID and a
// version. Called by the initial load and by every reconciliation, so it should
// be cheap.
type ListIDsFunc func(ctx context.Context) (items []SourceItem, err error)

type SourceItem struct {
	ID      int64
	Version int64
}

// LoadMultipleFunc loads a batch of items in one call.
type LoadMultipleFunc[T any] func(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error)

type LoadedEntry[T any] struct {
	ID      int64
	Value   *T
	Version int64
	Err     error
}
```

Both are required. `LoadMultipleFunc` is always called with at most `BatchSize`
IDs, and **from several goroutines at once** — see `LoadConcurrency`.

### Timeouts are yours to set

The cache passes its own context to both functions and never adds a deadline. It
cannot pick one for you: only you know what a reasonable duration is for your
source.

Do set one. Everything in the background — reloads, reconciliation, the metrics —
runs in a single goroutine, so **one call that hangs stops all of it** for as
long as it hangs. Reads keep being served from the replica the whole time, but it
stops being updated.

```go
ListIDsFunc: func(ctx context.Context) ([]eventual.SourceItem, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return source.ItemIDs(ctx)
},
```

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
still does not find it deletes it. A deleted item therefore disappears from the
replica within two `SyncInterval`s.

One run is not enough evidence. An item loaded while the reconciliation was
running is naturally missing from the snapshot it started with, and the ID listing
itself can be a stale read from a replica. Keeping a deleted item for one extra
`SyncInterval` is the cheaper mistake — dropping an item that really exists means
serving `nil` for something the caller can see in the database.

A successful load clears the mark, so an item that gets invalidated between two
reconciliations is never deleted.

## Failed loads and retries

A batch that fails is retried three times with a growing backoff before the run
gives up. That matters for the warm-up: a large dataset is hundreds of calls, and
failing the start-up because one of them was unlucky is not acceptable.

Steady state barely needs it — a run that gives up leaves its IDs marked and the
next `RefreshInterval` tick tries them again anyway.

The first batch that fails all of its attempts ends the run, and the batches
still in flight are cancelled through their context. A source that is down
therefore costs one failed batch per worker per run, not one per batch.

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

It costs about 0.5 ns, and because it takes the top bits it stays uniform even
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
-   **ListIDsFunc** — lists all IDs and versions the source has. Required.
-   **LoadMultipleFunc** — loads a batch of items by ID. Required.
-   **BatchSize** — maximum number of IDs per `LoadMultipleFunc` call. Applies to
    the initial load, to reloads of marked items and to the reconciliation.
    Reaching this many marked IDs also wakes the background goroutine early.
    Default 1000.
-   **LoadConcurrency** — how many batches of one load run are in flight at once.
    Default 4; 1 loads them one after another. A run with a single batch never
    starts a goroutine.
-   **MissRateLimit** — how many IDs unknown to the replica `Get` may queue per
    second. Default 100; a negative value means no limit.
-   **MaxRefreshPerSync** — cap on how many items one reconciliation may mark for
    a reload *because of their age*. Never applies to version reloads or
    deletions. Must be at least `items * SyncInterval / MaxAge` to make `MaxAge`
    a real bound. Default 0 = no cap.
-   **Shards** — power of two, default 256.
-   **ShardHash** — default `MultiplyShiftHash`.
-   **Timeouts** — see below.

### Timeouts

-   **SyncInterval** — how often the reconciliation runs, and therefore the upper
    bound on how long a lost invalidation can go unnoticed. Randomized by
    `Randomizer`. Required, must be > 0.
-   **RefreshInterval** — how often marked items are reloaded. Default 1s.
-   **MaxAge** — how long an item may go without a reload before a reconciliation
    marks it for one. The backstop for a lost invalidation when the source cannot
    report versions; leave it off when it can. Checked only during a
    reconciliation, so the real bound is `MaxAge + SyncInterval` and a `MaxAge`
    below `SyncInterval` is rejected. Default 0 = off.
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
| `batch_loads` | Counter | `LoadMultipleFunc` calls, retries included |
| `batch_load_items` | Counter | IDs passed to `LoadMultipleFunc` |
| `error_loads` | Counter | Loads that failed with something other than not found |
| `list_ids_errors` | Counter | `ListIDsFunc` calls that failed |
| `sync_runs` | Counter | Reconciliation runs |
| `sync_added` / `sync_marked` / `sync_removed` / `sync_outdated` | Counter | What the reconciliations did |
| `sync_expired` | Counter | Items marked for a reload because they reached `MaxAge` |
| `sync_age_deferred` | Counter | Items past `MaxAge` left for the next run by `MaxRefreshPerSync` |

`reads_count` and `misses_count` are counted **per shard** and collected into
Prometheus once a second by the background goroutine. Incrementing a Prometheus
counter directly in `Get` would create one globally contended cache line —
exactly what the sharding is there to avoid. Everything else is counted where the
event happens.

Worth alerting on: `time() - last_sync_timestamp` above a few sync intervals,
`list_ids_errors` growing, `misses_rate_limited` growing, a `pending_count` that
does not come back down, a `sync_outdated` in the order of the whole dataset
(which means the two `Version`s are not on the same scale), and a
`sync_age_deferred` that is never zero (which means `MaxRefreshPerSync` is too low
for `MaxAge` to hold).

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
	ID        int64
	Name      string
	UpdatedAt time.Time
}

// Source is whatever holds the data - a database, an HTTP API, a file.
type Source interface {
	ItemIDs(ctx context.Context) ([]eventual.SourceItem, error)
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

		// SELECT id, UNIX_TIMESTAMP(updated_at) * 1000000 FROM items
		ListIDsFunc: func(ctx context.Context) ([]eventual.SourceItem, error) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			return source.ItemIDs(ctx)
		},

		LoadMultipleFunc: func(ctx context.Context, IDs []int64) ([]eventual.LoadedEntry[Item], error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

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
					// the same scale as SourceItem.Version above
					Version: items[i].UpdatedAt.UnixMicro(),
				})
			}

			return entries, nil
		},

		Timeouts: eventual.Timeouts{
			SyncInterval:    5 * time.Minute,
			RefreshInterval: time.Second,
			Randomizer:      0.1,
		},

		BatchSize:       1000,
		LoadConcurrency: 4,
		Shards:          1024, // 64 core machine
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

## Sizing a large replica

Measured on an i5-11500H with **1 000 000 items**, 1024 shards:

| | |
|---|---|
| The replica itself | 70 MB, i.e. **70 B per item** on top of your values |
| Reconciliation buffers | 46 MB, allocated on the first run and kept |
| One reconciliation | ~170 ms of CPU, spread over `GOMAXPROCS` |

The buffers are reused between runs on purpose, so that a reconciliation does not
allocate proportionally to the dataset — the 46 MB never comes back. Budget
around **116 MB of overhead per million items** plus the values themselves, and
set `GOMEMLIMIT`: a million entries is a million pointers for the GC to mark.

The per item figure includes the 8 B of `refreshAt` whether or not `MaxAge` is
set — the field is part of every entry, which pushes it from the 24 B size class
into the 32 B one.

`ListIDsFunc` returns the whole ID list on every run, so `SyncInterval` is also
how often the source is asked for all of it. Ten to fifteen minutes is a
reasonable starting point for a dataset this size; keep `Randomizer` non-zero so
that the instances do not all ask at the same moment.

### Warm-up

`New` does not return until the whole dataset is in memory, so this is start-up
time for every instance on every deploy. Loading 1 000 000 items from a source
that takes 5 ms per call:

| BatchSize | LoadConcurrency | Warm-up |
|---|---|---|
| 200 | 1 | 27.5 s |
| 1000 | 1 | 6.2 s |
| **1000** | **4** | **1.6 s** ← defaults |
| 1000 | 8 | 0.84 s |
| 5000 | 4 | 0.43 s |

Bigger batches do most of the work — they cut the number of round trips — and
concurrency overlaps what is left. Past that the floor is your source: streaming
a million rows takes what it takes.

## Benchmarks

All numbers from an i5-11500H (6 cores / 12 threads, 2.9 GHz), 200 000 items,
256 shards. No allocations anywhere on the read path.

### How many reads per second

Measured with metrics registered, so the read path pays for the shard counter.
Two access patterns, because that is what decides the answer: a random walk over
the whole dataset misses the CPU caches on every lookup, a skewed workload with a
small hot set does not. Real traffic sits between them.

| Readers | Random over 200 000 items | Hot set of 1 000 items |
|---|---|---|
| 1 | **9.4 M reads/s** (106 ns) | **50 M reads/s** (20 ns) |
| 4 | 25 M reads/s (39 ns) | 104 M reads/s (9.6 ns) |
| 8 | 53 M reads/s (19 ns) | 130 M reads/s (7.7 ns) |
| 12 | **63 M reads/s** (16 ns) | **133 M reads/s** (7.5 ns) |

The random walk is bound by memory latency: a lookup is a chain of dependent
cache misses (shard → map bucket → entry → value), so nothing can be prefetched.
That is also why it keeps scaling to 12 threads on 6 cores — hyperthreading buys
more outstanding misses, not more arithmetic.

The hot set is bound by the `RLock`/`RUnlock` pair instead. It is a
read-modify-write on the shard's cache line, so the line bounces between cores.
Raising the shard count does **not** help: measured over the hot set, 256 → 16 384
shards takes throughput *down*, because the shard array itself stops fitting in
cache. Around 7 ns per read is the floor of a sharded `RWMutex` map on this
machine.

```bash
go test -run='^$' -bench=Throughput -benchtime=3s -cpu=1,4,8,12 .
```

### The miss path

A flood of lookups for IDs that do not exist is the one read path a caller can
point at the cache on purpose, so it has to behave under concurrency:

| Readers | Hit | Miss |
|---|---|---|
| 1 | 37.2 ns | 84.7 ns |
| 4 | 11.6 ns | 28.4 ns |
| 12 | **6.6 ns** | **12.8 ns** |

Two things shape that column. The rate limiter answers from a plain shared read
once the second's budget is used up, instead of an atomic read-modify-write on
one globally shared cache line — that is the difference between a miss path that
scales with the cores and one that gets *slower* the more of them you use, which
is what it did before (36.4 ns at 12 threads, and heading the wrong way).

The rest is the `time.Now()` the limiter's window needs. It is deliberately not
cached in a field the background goroutine ticks: that goroutine also does the
I/O, so a slow source would stop the clock and with it the window, and every miss
would be rejected until the source recovered. A hit never pays for it.

Note that the cost of `time.Now()` belongs to the host, not to this code. These
numbers are from a machine whose clocksource is `tsc`; where the kernel falls
back to `acpi_pm` or `hpet`, a clock read costs hundreds of nanoseconds and this
column grows accordingly.

### Everything else

| Benchmark | ns/op | allocs/op |
|---|---|---|
| `Get` (serial, sequential walk) | 36.8 | 0 |
| `Get` (parallel, sequential walk) | 6.8 | 0 |
| `Get` (parallel, metrics registered) | 7.2 | 0 |
| `Get` (parallel, **1 shard** — what a bad hash does) | 55.1 | 0 |
| `Invalidate` (same ID, collapsed by the flag) | 15.7 | 0 |
| `Invalidate` (distinct IDs) | 117.8 | 0 |
| Reconciliation over 200 000 items | 45.0 ms | 37 |
| `MultiplyShiftHash` / `SplitMix64Hash` | 0.49 / 1.12 | 0 |

Metrics are free on the read path — that is the point of the per shard counters.
A single shard is 9× slower than 256, which is what the shard hash is for.

The reconciliation reuses its buffers between runs, so its allocation count does
not grow with the size of the dataset. Its 3.2 MB per run is the `[]SourceItem`
of 200 000 IDs and versions that `ListIDsFunc` returns — the test source
allocates it fresh every time, the cache itself does not.

```bash
go test -run='^$' -bench=. -benchmem .
```
