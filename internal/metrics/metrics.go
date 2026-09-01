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
	TombstonesCount   prometheus.Gauge
	QueueLength       prometheus.Gauge
	LastSyncTimestamp prometheus.Gauge
	LastSyncDuration  prometheus.Gauge

	ReadsCount  prometheus.Counter
	MissesCount prometheus.Counter

	RefreshEnqueuedCount prometheus.Counter
	RefreshDroppedCount  prometheus.Counter
	RefreshBatchCount    prometheus.Counter
	RefreshItemsCount    prometheus.Counter
	InvalidationsCount   prometheus.Counter
	LoadErrorsCount      prometheus.Counter

	SyncRunsCount    prometheus.Counter
	SyncErrorsCount  prometheus.Counter
	SyncAddedCount   prometheus.Counter
	SyncRemovedCount prometheus.Counter
	ReloadsCount     prometheus.Counter
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
		{&m.TombstonesCount, "tombstones_count", "Current number of remembered not-found answers"},
		{&m.QueueLength, "queue_length", "Number of IDs waiting for a refresh"},
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
		{&m.ReadsCount, "reads_count", "Total number of Get / GetMultiple lookups"},
		{&m.MissesCount, "misses_count", "Lookups for an ID the replica does not know at all"},
		{&m.RefreshEnqueuedCount, "refresh_enqueued_count", "Total number of items queued for a refresh"},
		{&m.RefreshDroppedCount, "refresh_dropped_count", "Refresh requests dropped because the queue was full"},
		{&m.RefreshBatchCount, "refresh_batch_count", "Total number of loader calls made by refresh workers"},
		{&m.RefreshItemsCount, "refresh_items_count", "Total number of items sent to the loader by refresh workers"},
		{&m.InvalidationsCount, "invalidations_count", "Total number of Invalidate / InvalidateMultiple requests"},
		{&m.LoadErrorsCount, "load_errors_count", "Loads which ended with an error other than not found"},
		{&m.SyncRunsCount, "sync_runs_count", "Total number of reconciliation runs"},
		{&m.SyncErrorsCount, "sync_errors_count", "Reconciliation runs which failed"},
		{&m.SyncAddedCount, "sync_added_count", "Items added by reconciliation"},
		{&m.SyncRemovedCount, "sync_removed_count", "Items removed by reconciliation"},
		{&m.ReloadsCount, "reloads_count", "Total number of full reloads"},
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
