package eventual

import (
	"errors"
	"time"
)

const defaultRefreshInterval = time.Second

type Timeouts struct {
	// SyncInterval is how often the whole list of IDs and versions is fetched
	// from the source and the replica reconciled against it: items the source no
	// longer has are removed, items the replica does not have yet are loaded, and
	// items whose version in the source is newer than the stored one are reloaded.
	// It is therefore also the upper bound on how long a lost invalidation can
	// leave a stale value in the replica.
	// The duration is randomized by Randomizer.
	// Required, must be greater than 0.
	SyncInterval time.Duration `mapstructure:"sync_interval"`

	// RefreshInterval is how often the items marked for a reload are loaded.
	// They are also loaded earlier, as soon as there are BatchSize of them.
	// Default 1s.
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`

	// Randomizer specifies how much the durations should be randomized. Value 0
	// means no randomization, 0.1 means 10 %, etc. Any value above 1 is rejected.
	// e.g. the real sync interval is `SyncInterval` +/- `SyncInterval` *
	// `Randomizer`. All durations are randomized every time they are set.
	// Without it every instance of the service would hit the source in the same
	// second.
	Randomizer float64 `mapstructure:"randomizer"`
}

func (t *Timeouts) check() error {
	if t.SyncInterval <= 0 {
		return errors.New("syncInterval must be greater than 0")
	}

	if t.RefreshInterval < 0 {
		return errors.New("refreshInterval cannot be negative")
	}

	if t.Randomizer < 0 {
		return errors.New("randomizer cannot be negative")
	}

	if t.Randomizer > 1 {
		return errors.New("randomizer cannot be greater than 1")
	}

	return nil
}

func (t *Timeouts) withDefaults() {
	if t.RefreshInterval == 0 {
		t.RefreshInterval = defaultRefreshInterval
	}
}
