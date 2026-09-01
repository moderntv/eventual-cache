package test_utils

import (
	"os"

	"github.com/rs/zerolog"
)

// Logger returns a logger for tests. Set CACHE_TEST_LOG=1 to see the output.
func Logger() zerolog.Logger {
	if os.Getenv("CACHE_TEST_LOG") == "" {
		return zerolog.Nop()
	}

	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.TraceLevel)
}
