package eventual

import (
	"context"
	"errors"
	"time"

	cadre_metrics "github.com/moderntv/cadre/metrics"
	"github.com/rs/zerolog"
)

const (
	defaultShards            = 256
	defaultRefreshWorkers    = 4
	defaultRefreshQueueSize  = 10000
	defaultRefreshBatchSize  = 200
	defaultRefreshBatchDelay = 20 * time.Millisecond
	defaultMissRateLimit     = 100
)

// LoadedEntry is one item returned by a loader.
type LoadedEntry[T any] struct {
	ID int64
	// Value is the loaded item. Nil (or Err set to ErrNotFound) means the item
	// does not exist in the source.
	Value *T
	// Version is the source version of the value, typically updated_at in
	// milliseconds. When it is set, an older version can never overwrite a newer
	// one. Zero means versioning is not used and the last write wins.
	Version int64
	Err     error
}

type (
	// LoadOneFunc loads a single item. It is used only as a fallback when
	// LoadMultipleFunc is not set. Return ErrNotFound for a missing item.
	LoadOneFunc[T any] func(ctx context.Context, ID int64) (value *T, version int64, err error)

	// LoadMultipleFunc loads a batch of items in one call. The loader does not
	// have to return an entry for every requested ID - IDs missing from the
	// result are stored as not-found by the cache itself. A non-nil error means
	// the whole batch failed; per item errors belong into LoadedEntry.Err.
	LoadMultipleFunc[T any] func(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error)

	// LoadAllFunc loads the complete dataset. It is called by New (blocking
	// warm-up) and by Reload / InvalidateAll.
	LoadAllFunc[T any] func(ctx context.Context) (entries []LoadedEntry[T], err error)

	// ListIDsFunc returns the IDs of all items that currently exist in the
	// source. It is called by the periodic reconciliation, so it should be as
	// cheap as possible (typically SELECT id FROM ...).
	ListIDsFunc func(ctx context.Context) (IDs []int64, err error)
)

type Params[T any] struct {
	Context         context.Context
	Log             zerolog.Logger
	MetricsRegistry *cadre_metrics.Registry
	Name            string

	// LoadAllFunc loads the whole dataset. Required - New blocks on it.
	LoadAllFunc LoadAllFunc[T]
	// ListIDsFunc lists all IDs the source has. Required - the periodic
	// reconciliation is built on it.
	ListIDsFunc ListIDsFunc
	// LoadMultipleFunc loads a batch of items at once. Optional but strongly
	// recommended; without it the cache falls back to LoadOneFunc per item.
	LoadMultipleFunc LoadMultipleFunc[T]
	// LoadOneFunc loads a single item. Required when LoadMultipleFunc is not set.
	LoadOneFunc LoadOneFunc[T]

	Timeouts Timeouts

	// Shards is the number of independent segments of the cache. Must be a power
	// of two. A shard costs 128 B, so the number is chosen by concurrency, not
	// by memory - about 8 to 16 times GOMAXPROCS rounded up to a power of two
	// (1024 for a 64 core machine).
	// Default 256.
	Shards int
	// ShardHash maps an ID to a shard. Default MultiplyShiftHash.
	ShardHash ShardHashFunc

	// RefreshWorkers is the number of goroutines loading invalidated items.
	// Default 4.
	RefreshWorkers int
	// RefreshQueueSize is the capacity of the refresh queue. When it is full,
	// further refresh requests are dropped (and counted) - the reconciliation
	// picks the items up later.
	// Default 10000.
	RefreshQueueSize int
	// RefreshBatchSize is the maximum number of IDs passed to LoadMultipleFunc
	// in one call. Default 200.
	RefreshBatchSize int
	// RefreshBatchDelay is how long a worker waits for more IDs before it sends
	// an incomplete batch to the loader. Default 20ms.
	RefreshBatchDelay time.Duration
	// MissRateLimit caps how many IDs unknown to the replica may be looked up in
	// the source per second, so that requests for random IDs cannot overload it.
	// Explicit invalidations are never rate limited.
	// Default 100, zero or less means no limit.
	MissRateLimit int

	// OnSync is called after every reconciliation run. Optional.
	OnSync func(stats SyncStats)
}

func (p *Params[T]) check() error {
	if p.Context == nil {
		return errors.New("context must be set")
	}

	if p.Name == "" {
		return errors.New("name must be set")
	}

	if p.LoadAllFunc == nil {
		return errors.New("LoadAllFunc must be provided")
	}

	if p.ListIDsFunc == nil {
		return errors.New("ListIDsFunc must be provided")
	}

	if p.LoadOneFunc == nil && p.LoadMultipleFunc == nil {
		return errors.New("LoadOneFunc or LoadMultipleFunc must be provided")
	}

	if p.Shards != 0 && !isPowerOfTwo(p.Shards) {
		return errors.New("shards must be a power of two")
	}

	if p.RefreshWorkers < 0 {
		return errors.New("refreshWorkers cannot be negative")
	}

	if p.RefreshQueueSize < 0 {
		return errors.New("refreshQueueSize cannot be negative")
	}

	if p.RefreshBatchSize < 0 {
		return errors.New("refreshBatchSize cannot be negative")
	}

	if p.RefreshBatchDelay < 0 {
		return errors.New("refreshBatchDelay cannot be negative")
	}

	return p.Timeouts.check()
}

func (p *Params[T]) withDefaults() {
	if p.Shards == 0 {
		p.Shards = defaultShards
	}

	if p.ShardHash == nil {
		p.ShardHash = MultiplyShiftHash
	}

	if p.RefreshWorkers == 0 {
		p.RefreshWorkers = defaultRefreshWorkers
	}

	if p.RefreshQueueSize == 0 {
		p.RefreshQueueSize = defaultRefreshQueueSize
	}

	if p.RefreshBatchSize == 0 {
		p.RefreshBatchSize = defaultRefreshBatchSize
	}

	if p.RefreshBatchDelay == 0 {
		p.RefreshBatchDelay = defaultRefreshBatchDelay
	}

	if p.MissRateLimit == 0 {
		p.MissRateLimit = defaultMissRateLimit
	}
}
