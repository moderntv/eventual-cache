package eventual

import (
	"errors"
	"time"
)

type Timeouts struct {
	// SyncInterval is how often the whole list of IDs is fetched from the source
	// and the replica reconciled against it: items the source no longer has are
	// removed, items the replica does not have yet are loaded.
	// The duration is randomized by Randomizer.
	// Required, must be greater than 0.
	SyncInterval time.Duration `mapstructure:"sync_interval"`

	// NotFoundTTL is how long a "does not exist" answer is remembered, so that
	// repeated Get calls for an unknown ID do not query the source over and
	// over. The duration is randomized by Randomizer.
	// If set to 0, not-found answers are not stored at all.
	NotFoundTTL time.Duration `mapstructure:"not_found_ttl"`

	// ErrorRetryInterval is how long a refresh worker waits after a failed load
	// before it picks up another batch. It keeps a struggling source from being
	// hammered.
	// If set to 0, the worker does not wait.
	ErrorRetryInterval time.Duration `mapstructure:"error_retry_interval"`

	// Randomizer specifies how much the durations should be randomized. Value 0
	// means no randomization, 0.1 means 10 %, etc. Any value above 1 is
	// rejected.
	// e.g. the real sync interval is `SyncInterval` +/- `SyncInterval` *
	// `Randomizer`. All durations are randomized every time they are set.
	Randomizer float64 `mapstructure:"randomizer"`
}

func (t *Timeouts) check() error {
	if t.SyncInterval <= 0 {
		return errors.New("syncInterval must be greater than 0")
	}

	if t.NotFoundTTL < 0 {
		return errors.New("notFoundTTL cannot be negative")
	}

	if t.ErrorRetryInterval < 0 {
		return errors.New("errorRetryInterval cannot be negative")
	}

	if t.Randomizer < 0 {
		return errors.New("randomizer cannot be negative")
	}
	if t.Randomizer > 1 {
		return errors.New("randomizer cannot be greater than 1")
	}

	return nil
}
