# Eventual cache

Eventual cache is an **eventually consistent, in-memory replica of a whole
dataset** keyed by `int64`. The replica is loaded once at start-up and then kept
up to date in the background; reads are served from local memory and **never
block on I/O**.

It sits between the two sibling libraries:

| | [lazy-cache](https://github.com/moderntv/lazy-cache) | [codebook-cache](https://github.com/moderntv/codebook-cache) | **eventual-cache** |
|---|---|---|---|
| What it holds | only what somebody asked for | the whole dataset | the whole dataset |
| Filling | lazy, per item | full reload of the whole map | blocking warm-up + per item refresh |
| Structure | one map, one `RWMutex` | `atomic.Value` with the whole map | N shards, each with its own `RWMutex` |
| `Get` can block on I/O | **yes** | no | **no, never** |
| Invalidation | `Invalidate(ID)` | NATS inside, always "reload everything" | `Invalidate(ID)`, caller owns the transport |
| Eviction | TTL watcher | – | reconciliation against the source only |

Provided functions:

-   **Get(ID)** — returns `*T` from the local replica, or `nil` when the item does
    not exist. Never blocks on I/O. A lookup for an ID the replica does not know
    queues a background load, so a following `Get` can already succeed.
-   **GetMultiple(IDs)** — returns `map[int64]*T`. Keys missing from the result
    mean "does not exist"; the map never contains a `nil` value.
-   **ForEach(fn)** — iterates the whole replica. The callback runs under the
    shard read lock, so it must not call back into the cache.
-   **Len()** — number of items in the replica (tombstones excluded).
-   **Invalidate(ID) / InvalidateMultiple(IDs)** — queue items for a background
    refresh. Call them from wherever the source announces changes.
-   **InvalidateAll()** — starts a full reload in the background.
-   **Sync(ctx)** — forces a reconciliation with the source.
-   **Reload(ctx)** — forces a full reload (what `New` does).
-   **Stats()** — snapshot of the cache state.
-   **Close()** — stops the background goroutines and waits for them.

The value type `T` is generic, the key is always `int64`.

## How the replica stays up to date

Three independent mechanisms:

1. **Blocking warm-up.** `New` calls `LoadAllFunc` and returns only when the
   whole dataset is in memory - or with an error, so a service never starts
   serving from an empty replica.
2. **Invalidations from the caller.** When the source announces a change (a NATS
   message, a webhook, anything), the caller calls `Invalidate(ID)`. The cache
   itself does not subscribe to anything - the transport stays in the service, the
   same way lazy-cache does it.
3. **Periodic reconciliation.** Every `SyncInterval` the cache fetches the list of
   all IDs from `ListIDsFunc`, removes items the source no longer has and loads
   items the replica does not have yet. It is the safety net for lost
   invalidations - it does not re-read the content of items the source still has.

```go
// wiring an invalidation transport in the service
natsConn.Subscribe("cache.channel.updated", func(msg *nats.Msg) {
	var event pb.ChannelUpdated
	if err := proto.Unmarshal(msg.Data, &event); err != nil {
		log.Warn().Err(err).Msg("invalid invalidation message")

		return
	}

	cache.InvalidateMultiple(event.GetIds())
})
```

## Caveats

-   **Do not mutate an item after it has been handed to the cache.** The cache
    stores pointers; changing the pointed-to data affects every reader. Treat the
    values returned by `Get` as read-only.
-   Data may lag behind the source - that is the whole point of the library.
-   A `nil` from `Get` means either "the source does not have it" or "the replica
    does not know about it yet". If your code has to tell the two apart, it is
    asking for a consistent read and this cache is the wrong tool.
-   A failed load never removes an item and never overwrites it with an error.
-   The whole dataset is held in memory by every instance of the service.

## Sharding and the shard hash

The map is split into `Shards` independent segments, each with its own
`RWMutex`. A shard costs 128 B, so the number of shards is chosen by concurrency,
not by memory: roughly **8 to 16 times `GOMAXPROCS`**, rounded up to a power of
two (so 1024 for a 64 core machine). Default is 256.

The default `MultiplyShiftHash` is one multiplication and one shift and takes the
**top** bits of the product:

```go
func MultiplyShiftHash(ID int64, bits uint) uint32 {
	return uint32((uint64(ID) * 0x9E3779B97F4A7C15) >> (64 - bits))
}
```

Masking the low bits (`ID & (shards-1)`) would be the obvious choice, and it is
wrong for our IDs. MariaDB Galera sets `auto_increment_increment` to the number
of nodes, so IDs grow by a constant step, and `ID % shards` then only ever hits
`shards / gcd(step, shards)` of the shards. Measured over 100 000 IDs and 256
shards:

| Step between IDs | `ID & 255`: empty shards | `ID & 255`: max/avg | `MultiplyShiftHash`: empty | max/avg |
|---|---|---|---|---|
| 1 | 0 | 1.00 | 0 | 1.01 |
| 2 | **128** | **2.00** | 0 | 1.01 |
| 4 | **192** | **4.00** | 0 | 1.01 |
| 6 | **128** | **2.00** | 0 | 1.00 |
| 8 | **224** | **8.00** | 0 | 1.01 |
| 16 | **240** | **16.00** | 0 | 1.01 |

Multiply-shift is as fast as masking (0.70 vs 0.69 ns) and, on arithmetic
progressions, more uniform than a strong hash. `SplitMix64Hash` (full avalanche,
1.6 ns) is available for the case where IDs could be chosen by an untrusted
party - multiply-shift is not resistant to a deliberately crafted set of IDs.
`Params.ShardHash` takes any function.

## Params

-   **Context** — shutdown of all background goroutines. Required.
-   **Log** — `zerolog.Logger`. Required.
-   **MetricsRegistry** — `*cadre_metrics.Registry`; when set, Prometheus metrics
    are registered. Optional.
-   **Name** — used in logs and metrics. Required.
-   **LoadAllFunc** — `func(ctx) ([]LoadedEntry[T], error)`. Loads the whole
    dataset, used by the warm-up and by `Reload`. Required.
-   **ListIDsFunc** — `func(ctx) ([]int64, error)`. Lists all IDs the source has;
    should be as cheap as possible (`SELECT id FROM ...`). Required.
-   **LoadMultipleFunc** — `func(ctx, []int64) ([]LoadedEntry[T], error)`. Loads a
    batch in one call. Optional but strongly recommended.
-   **LoadOneFunc** — `func(ctx, int64) (*T, int64, error)`. Required when
    `LoadMultipleFunc` is not set.
-   **Timeouts** — see below.
-   **Shards** — power of two, default 256.
-   **ShardHash** — default `MultiplyShiftHash`.
-   **RefreshWorkers** — goroutines loading invalidated items, default 4.
-   **RefreshQueueSize** — default 10000. A full queue drops requests (they are
    counted, and reconciliation picks the items up later).
-   **RefreshBatchSize** — maximum IDs per `LoadMultipleFunc` call, default 200.
-   **RefreshBatchDelay** — how long a worker waits for more IDs before sending an
    incomplete batch, default 20ms.
-   **MissRateLimit** — lookups per second for IDs unknown to the replica, default
    100. Explicit invalidations are never rate limited.
-   **OnSync** — called after every reconciliation. Optional.

### Timeouts

-   **SyncInterval** — how often the reconciliation runs. Randomized by
    `Randomizer`. Required, must be > 0.
-   **NotFoundTTL** — how long a "does not exist" answer is remembered, so that
    repeated lookups of an unknown ID do not hit the source. If 0, such answers
    are not stored. Default in the examples: a minute.
-   **ErrorRetryInterval** — how long a refresh worker waits after a failed load
    before taking another batch. If 0, it does not wait.
-   **Randomizer** — `[0, 1]`. 0 = no jitter, 0.1 = ±10 %. Without it every
    instance of the service would hit the source in the same second.

### Versions

`LoadedEntry.Version` is optional. When the source can provide one (typically
`updated_at` in milliseconds), an older version can never overwrite a newer one -
which matters when an invalidation and a bulk load of the same ID race. With
`Version` left at 0 the last write wins.

## Metrics

When `MetricsRegistry` is set, these are registered (subsystem `eventual_cache`,
label `name`):

| Metric | Type | Description |
|--------|------|-------------|
| `items_count` | Gauge | Items in the replica |
| `tombstones_count` | Gauge | Remembered not-found answers |
| `queue_length` | Gauge | IDs waiting for a refresh |
| `last_sync_timestamp` | Gauge | Unix time of the last successful reconciliation |
| `last_sync_duration_seconds` | Gauge | Duration of the last reconciliation |
| `reads_count` | Counter | `Get` / `GetMultiple` lookups |
| `misses_count` | Counter | Lookups for an ID the replica does not know at all |
| `refresh_enqueued_count` | Counter | Items queued for a refresh |
| `refresh_dropped_count` | Counter | Requests dropped because the queue was full |
| `refresh_batch_count` | Counter | Loader calls made by refresh workers |
| `refresh_items_count` | Counter | Items sent to the loader by refresh workers |
| `invalidations_count` | Counter | `Invalidate` / `InvalidateMultiple` calls |
| `load_errors_count` | Counter | Loads that failed with something other than not found |
| `sync_runs_count` / `sync_errors_count` | Counter | Reconciliation runs and failures |
| `sync_added_count` / `sync_removed_count` | Counter | What the reconciliation fixed |
| `reloads_count` | Counter | Full reloads |

Worth alerting on: `refresh_dropped_count` growing, `time() - last_sync_timestamp`
above a few sync intervals, and `sync_errors_count` growing.

`reads_count` and `misses_count` are counted per shard and flushed into
Prometheus once a second. Incrementing a Prometheus counter directly in `Get`
would create one globally contended cache line - exactly what the sharding is
there to avoid.

---

## Usage

```go
package channel

import (
	"context"
	"errors"
	"time"

	cadre_metrics "github.com/moderntv/cadre/metrics"
	eventual "github.com/moderntv/eventual-cache"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

type Channel struct {
	ID        int64
	Name      string
	UpdatedAt time.Time
}

type Repository struct {
	cache *eventual.Cache[Channel]
}

func NewRepository(
	ctx context.Context,
	log zerolog.Logger,
	db *gorm.DB,
	metrics *cadre_metrics.Registry,
) (*Repository, error) {
	toEntries := func(channels []Channel) (entries []eventual.LoadedEntry[Channel]) {
		entries = make([]eventual.LoadedEntry[Channel], 0, len(channels))
		for i := range channels {
			channel := channels[i]
			entries = append(entries, eventual.LoadedEntry[Channel]{
				ID:      channel.ID,
				Value:   &channel,
				Version: channel.UpdatedAt.UnixMilli(),
			})
		}

		return
	}

	c, err := eventual.New(eventual.Params[Channel]{
		Context:         ctx,
		Log:             log,
		MetricsRegistry: metrics,
		Name:            "channel",

		LoadAllFunc: func(ctx context.Context) ([]eventual.LoadedEntry[Channel], error) {
			var channels []Channel
			if err := db.WithContext(ctx).Find(&channels).Error; err != nil {
				return nil, err
			}

			return toEntries(channels), nil
		},

		ListIDsFunc: func(ctx context.Context) (IDs []int64, err error) {
			err = db.WithContext(ctx).Model(&Channel{}).Pluck("id", &IDs).Error

			return
		},

		LoadMultipleFunc: func(ctx context.Context, IDs []int64) ([]eventual.LoadedEntry[Channel], error) {
			var channels []Channel
			if err := db.WithContext(ctx).Where("id IN ?", IDs).Find(&channels).Error; err != nil {
				return nil, err
			}
			// IDs missing from the result are turned into tombstones by the cache

			return toEntries(channels), nil
		},

		LoadOneFunc: func(ctx context.Context, ID int64) (*Channel, int64, error) {
			var channel Channel
			err := db.WithContext(ctx).Where("id = ?", ID).First(&channel).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, 0, eventual.ErrNotFound
				}

				return nil, 0, err
			}

			return &channel, channel.UpdatedAt.UnixMilli(), nil
		},

		Timeouts: eventual.Timeouts{
			SyncInterval:       5 * time.Minute,
			NotFoundTTL:        time.Minute,
			ErrorRetryInterval: 10 * time.Second,
			Randomizer:         0.1,
		},

		Shards: 1024, // 64 core machine
	})
	if err != nil {
		return nil, err
	}

	return &Repository{cache: c}, nil
}

func (r *Repository) Channel(ID int64) *Channel { return r.cache.Get(ID) }

func (r *Repository) Channels(IDs []int64) map[int64]*Channel { return r.cache.GetMultiple(IDs) }

// Invalidate is what the NATS handler calls.
func (r *Repository) Invalidate(IDs []int64) { r.cache.InvalidateMultiple(IDs) }

func (r *Repository) Close() { r.cache.Close() }
```

## Benchmarks

Measured on a 2 vCPU Xeon 2.8 GHz with 200 000 items and 256 shards, so the
absolute numbers on real hardware will be better - the ratios are what matters.

| Benchmark | ns/op | allocs/op |
|---|---|---|
| `Get` (serial, cold working set) | 114 | 0 |
| `Get` (parallel) | 67 | 0 |
| `Get` (parallel, **1 shard** - what a bad hash does) | 100 | 0 |
| `Invalidate` (deduplicated) | 35 | 0 |
| `MultiplyShiftHash` | 0.70 | 0 |
| `SplitMix64Hash` | 1.59 | 0 |
| `Sync` over 200 000 items | 57 ms | 14 |

The reconciliation reuses its buffers between runs, so its allocation count does
not grow with the size of the dataset.
