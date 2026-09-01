package utils

import (
	"math/rand"
	"time"
)

// RandomizeDuration returns d changed by up to +/- randomizer * d.
func RandomizeDuration(d time.Duration, randomizer float64) time.Duration {
	if randomizer == 0 {
		return d
	}

	add := time.Duration(float64(d) * rand.Float64() * randomizer)
	if rand.Intn(2) == 0 {
		add = -add
	}

	return d + add
}
