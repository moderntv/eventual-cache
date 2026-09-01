package metrics

import (
	cadre_metrics "github.com/moderntv/cadre/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsPrefix = "cache_"
	subSystem     = "eventual_cache"
	labelName     = "name"
)

type Metrics struct {
	ItemsCount        prometheus.Gauge
	PendingCount      prometheus.Gauge
	LastSyncTimestamp prometheus.Gauge
	LastSyncDuration  prometheus.Gauge

	// ReadsCount and MissesCount are fed from the per shard counters by the
	// background goroutine, everything else is counted where the event happens.
	ReadsCount             prometheus.Counter
	MissesCount            prometheus.Counter
	MissesRateLimitedCount prometheus.Counter
	InvalidationsCount     prometheus.Counter

	BatchLoadCount      prometheus.Counter
	BatchLoadItemsCount prometheus.Counter
	LoadErrorsCount     prometheus.Counter
	ListIDsErrorsCount  prometheus.Counter

	SyncRunsCount    prometheus.Counter
	SyncAddedCount   prometheus.Counter
	SyncMarkedCount  prometheus.Counter
	SyncRemovedCount prometheus.Counter
}

func New(
	name string,
	registry *cadre_metrics.Registry,
) (m *Metrics, err error) {
	m = &Metrics{}

	gauges := []struct {
		target *prometheus.Gauge
		name   string
		help   string
	}{
		{&m.ItemsCount, "items_count", "Current number of cached items"},
		{&m.PendingCount, "pending_count", "Number of IDs waiting to be loaded"},
		{&m.LastSyncTimestamp, "last_sync_timestamp", "Unix timestamp of the last successful reconciliation"},
		{&m.LastSyncDuration, "last_sync_duration_seconds", "Duration of the last reconciliation in seconds"},
	}

	for _, g := range gauges {
		gauge := registry.NewGauge(prometheus.GaugeOpts{
			Subsystem:   subSystem,
			Name:        g.name,
			Help:        g.help,
			ConstLabels: prometheus.Labels{labelName: name},
		})

		err = registry.Register(metricsPrefix+name+"_"+g.name, gauge)
		if err != nil {
			return nil, err
		}

		*g.target = gauge
	}

	counters := []struct {
		target *prometheus.Counter
		name   string
		help   string
	}{
		{&m.ReadsCount, "reads_count", "Total number of Get calls"},
		{&m.MissesCount, "misses_count", "Get calls for an ID the replica does not have"},
		{&m.MissesRateLimitedCount, "misses_rate_limited", "Lookups of unknown IDs not queued because of the rate limit"},
		{&m.InvalidationsCount, "invalidations_count", "Total number of Invalidate calls"},
		{&m.BatchLoadCount, "batch_loads", "Total number of LoadMultipleFunc calls"},
		{&m.BatchLoadItemsCount, "batch_load_items", "Total number of IDs passed to LoadMultipleFunc"},
		{&m.LoadErrorsCount, "error_loads", "Loads which ended with an error other than not found"},
		{&m.ListIDsErrorsCount, "list_ids_errors", "ListIDsFunc calls which failed"},
		{&m.SyncRunsCount, "sync_runs", "Total number of reconciliation runs"},
		{&m.SyncAddedCount, "sync_added", "Items loaded by reconciliation because the replica did not have them"},
		{&m.SyncMarkedCount, "sync_marked", "Items marked for deletion by reconciliation"},
		{&m.SyncRemovedCount, "sync_removed", "Items removed by reconciliation"},
	}

	for _, c := range counters {
		counter := registry.NewCounter(prometheus.CounterOpts{
			Subsystem:   subSystem,
			Name:        c.name,
			Help:        c.help,
			ConstLabels: prometheus.Labels{labelName: name},
		})

		err = registry.Register(metricsPrefix+name+"_"+c.name, counter)
		if err != nil {
			return nil, err
		}

		*c.target = counter
	}

	return m, nil
}
