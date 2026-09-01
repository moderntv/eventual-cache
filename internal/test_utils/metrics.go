package test_utils

import (
	cadre_metrics "github.com/moderntv/cadre/metrics"
)

// MetricsRegistry returns a fresh registry for tests.
func MetricsRegistry() *cadre_metrics.Registry {
	registry, err := cadre_metrics.NewRegistry("test", nil)
	if err != nil {
		panic(err)
	}

	return registry
}
