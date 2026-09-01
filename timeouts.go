package eventual

import (
	"errors"
	"time"
)

const (
	defaultRefreshInterval = time.Second

	// minTTLRandomizer is the smallest randomization applied to TTL, whatever
	// Randomizer says. Without it every item stored by the initial load would
	// expire in the very same millisecond and the whole dataset would be queued
	// for a reload at once.
	minTTLRandomizer = 0.1
)

type Timeouts struct {
	// SyncInterval is how often the whole list of IDs is fetched from the source
	// and the replica reconciled against it: items the source no longer has are
	// removed, items the replica does not have yet are loaded.
	// The duration is randomized by Randomizer.
	// Required, must be greater than 0.
	SyncInterval time.Duration `mapstructure:"sync_interval"`

	// RefreshInterval is how often the items marked for a reload are loaded.
	// They are also loaded earlier, as soon as there are BatchSize of them.
	// Default 1s.
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`

	// TTL is how long a value is considered fresh. Once it passes, the next Get
	// still returns the value, but it also marks the item for a reload - the same
	// thing Invalidate does. It is a refresh interval, not an expiration: an item
	// is never dropped because it is old, only because the source stopped having
	// it.
	// The duration is randomized by Randomizer, at least by 10 %, so that items
	// stored together do not all expire together.
	// If set to 0, values are only reloaded on an explicit Invalidate.
	TTL time.Duration `mapstructure:"ttl"`

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

	if t.TTL < 0 {
		return errors.New("ttl cannot be negative")
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

// ttlRandomizer is the randomization used for TTL. It never goes below
// minTTLRandomizer, see the constant.
func (t *Timeouts) ttlRandomizer() float64 {
	return max(t.Randomizer, minTTLRandomizer)
}
