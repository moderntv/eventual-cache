package eventual

import (
	"context"
	"errors"

	cadre_metrics "github.com/moderntv/cadre/metrics"
	"github.com/rs/zerolog"
)

const (
	defaultShards        = 256
	defaultBatchSize     = 200
	defaultMissRateLimit = 100
)

// LoadedEntry is one item returned by a loader.
type LoadedEntry[T any] struct {
	ID int64
	// Value is the loaded item. A nil Value - or Err set to ErrNotFound - means
	// the source does not have the item any more and the cache removes it.
	Value *T
	// Err is a per item error. Anything other than ErrNotFound leaves the item in
	// the replica untouched, because a failed load is not proof of anything.
	Err error
}

type (
	// LoadMultipleFunc loads a batch of items in one call. The loader does not
	// have to return an entry for every requested ID - an ID missing from the
	// result is treated the same as ErrNotFound and the item is removed from the
	// replica. A non-nil error means the whole batch failed and the replica is
	// left untouched.
	LoadMultipleFunc[T any] func(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error)

	// ListIDsFunc returns the IDs of all items the source currently has. It is
	// called by the initial load and by every reconciliation, so it should be as
	// cheap as possible (typically SELECT id FROM ...).
	ListIDsFunc func(ctx context.Context) (IDs []int64, err error)
)

type Params[T any] struct {
	Context         context.Context
	Log             zerolog.Logger
	MetricsRegistry *cadre_metrics.Registry
	Name            string

	// ListIDsFunc lists all IDs the source has. Required.
	ListIDsFunc ListIDsFunc
	// LoadMultipleFunc loads a batch of items by ID. Required.
	LoadMultipleFunc LoadMultipleFunc[T]

	Timeouts Timeouts

	// Shards is the number of independent segments of the cache. Must be a power
	// of two. A shard costs 128 B, so the number is chosen by concurrency, not by
	// memory - about 8 to 16 times GOMAXPROCS rounded up to a power of two (1024
	// for a 64 core machine).
	// Default 256.
	Shards int
	// ShardHash maps an ID to a shard. Default MultiplyShiftHash.
	ShardHash ShardHashFunc

	// BatchSize is the maximum number of IDs passed to LoadMultipleFunc in one
	// call. It applies to the initial load, to reloads of marked items and to the
	// reconciliation. Reaching BatchSize marked items also wakes the background
	// goroutine before its next tick.
	// Default 200.
	BatchSize int

	// MissRateLimit caps how many IDs unknown to the replica may be queued for a
	// load per second by Get, so that lookups for random IDs cannot overload the
	// source. Invalidations, expired TTLs and the reconciliation are never rate
	// limited.
	// Default 100, zero or less means no limit.
	MissRateLimit int

	// afterSync is called at the end of every reconciliation. It exists for tests
	// which need to know that a reconciliation has finished, which is why it is
	// not exported.
	afterSync func()
}

func (p *Params[T]) check() error {
	if p.Context == nil {
		return errors.New("context must be set")
	}

	if p.Name == "" {
		return errors.New("name must be set")
	}

	if p.ListIDsFunc == nil {
		return errors.New("listIDsFunc must be provided")
	}

	if p.LoadMultipleFunc == nil {
		return errors.New("loadMultipleFunc must be provided")
	}

	if p.Shards != 0 && !isPowerOfTwo(p.Shards) {
		return errors.New("shards must be a power of two")
	}

	if p.BatchSize < 0 {
		return errors.New("batchSize cannot be negative")
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

	if p.BatchSize == 0 {
		p.BatchSize = defaultBatchSize
	}

	if p.MissRateLimit == 0 {
		p.MissRateLimit = defaultMissRateLimit
	}

	p.Timeouts.withDefaults()
}
