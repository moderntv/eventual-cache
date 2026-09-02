package eventual

import (
	"context"
	"errors"
	"time"

	cadre_metrics "github.com/moderntv/cadre/metrics"
	"github.com/rs/zerolog"
)

const (
	defaultShards          = 256
	defaultBatchSize       = 1000
	defaultMissRateLimit   = 100
	defaultLoadConcurrency = 4

	// A batch is retried a few times before the run gives up. It is what keeps a
	// warm-up of a large dataset - thousands of calls - from failing because one
	// of them was unlucky. Steady state does not really need it: the IDs stay
	// marked and the next refresh tick tries them again.
	defaultLoadRetries      = 3
	defaultLoadRetryBackoff = 100 * time.Millisecond
)

// SourceItem is one item as reported by ListIDsFunc: its ID and the version of
// the content the source currently holds for it.
type SourceItem struct {
	ID int64
	// Version identifies the content of the item. The reconciliation reloads an
	// item when the version reported here is greater than the version of the
	// value the replica holds, which is what catches an invalidation that never
	// arrived.
	//
	// Typically the unix microseconds of an updated_at column, but any value that
	// grows with every change will do - it is only ever compared against another
	// version from the same source, never against a clock of ours. Use the same
	// scale in LoadedEntry.Version.
	//
	// Leave it at 0 everywhere to switch change detection off; items are then
	// only reloaded by Invalidate.
	Version int64
}

// LoadedEntry is one item returned by a loader.
type LoadedEntry[T any] struct {
	ID int64
	// Value is the loaded item. A nil Value - or Err set to ErrNotFound - means
	// the source does not have the item any more and the cache removes it.
	Value *T
	// Version is the version of the returned content, on the same scale as
	// SourceItem.Version. Reporting it here is what keeps a reload triggered by
	// Invalidate from being repeated by the next reconciliation.
	Version int64
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
	//
	// It is called from several goroutines at once, see Params.LoadConcurrency.
	LoadMultipleFunc[T any] func(ctx context.Context, IDs []int64) (entries []LoadedEntry[T], err error)

	// ListIDsFunc returns every item the source currently has, as an ID and a
	// version. It is called by the initial load and by every reconciliation, so
	// it should be as cheap as possible (typically
	// SELECT id, updated_at FROM ...).
	ListIDsFunc func(ctx context.Context) (items []SourceItem, err error)
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
	// Default 1000.
	BatchSize int

	// LoadConcurrency is how many batches of one load run are in flight at once.
	// It matters most for the warm-up, which is the only run big enough for the
	// round trips to add up; a run with a single batch never starts a goroutine.
	// Note that LoadMultipleFunc is therefore called from several goroutines.
	// Default 4, 1 loads batches one after another.
	LoadConcurrency int

	// MaxRefreshPerSync caps how many items may be marked for a reload because of
	// their age in one reconciliation. It does not apply to reloads caused by a
	// newer version or to deletions - those are correctness and must not be
	// delayed.
	//
	// The budget is split evenly between the shards, rounded up, so a run may
	// mark a little more than this. To make Timeouts.MaxAge a real bound rather
	// than a wish, the cap has to get through the whole dataset within one MaxAge:
	//
	//	MaxRefreshPerSync >= items * SyncInterval / MaxAge
	//
	// Below that the items keep ageing past the limit, which is what the
	// sync_age_deferred metric shows: permanently non-zero means the cap is too
	// low.
	//
	// Default 0, which means no cap.
	MaxRefreshPerSync int `mapstructure:"max_refresh_per_sync"`

	// MissRateLimit caps how many IDs unknown to the replica may be queued for a
	// load per second by Get, so that lookups for random IDs cannot overload the
	// source. Invalidations and the reconciliation are never rate limited.
	// Default 100; a negative value means no limit.
	MissRateLimit int

	// afterSync is called at the end of every reconciliation. It exists for tests
	// which need to know that a reconciliation has finished, which is why it is
	// not exported.
	afterSync func()

	// loadRetries and loadRetryBackoff are how a failing batch is retried. They
	// are not exported because there is no good reason to tune them from the
	// outside; tests set them to keep their runtime down.
	loadRetries      int
	loadRetryBackoff time.Duration
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

	if p.LoadConcurrency < 0 {
		return errors.New("loadConcurrency cannot be negative")
	}

	if p.MaxRefreshPerSync < 0 {
		return errors.New("maxRefreshPerSync cannot be negative")
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

	if p.LoadConcurrency == 0 {
		p.LoadConcurrency = defaultLoadConcurrency
	}

	if p.loadRetries == 0 {
		p.loadRetries = defaultLoadRetries
	}

	if p.loadRetryBackoff == 0 {
		p.loadRetryBackoff = defaultLoadRetryBackoff
	}

	p.Timeouts.withDefaults()
}
