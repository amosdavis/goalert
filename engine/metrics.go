package engine

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricCycleTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "goalert",
		Subsystem: "engine",
		Name:      "cycle_total",
		Help:      "Total number of engine cycles.",
	})

	metricLease = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "goalert",
		Subsystem: "engine",
		Name:      "lease_held",
		Help:      "Whether this instance runs the engine (1) or another one does (0).",
	})

	metricCycleErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "goalert",
		Subsystem: "engine",
		Name:      "cycle_errors_total",
		Help:      "Total number of engine cycles, or modules in one, that failed.",
	})

	metricRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "goalert",
		Subsystem: "engine",
		Name:      "message_retries_total",
		Help:      "Total number of messages that were queued again, because sending them failed or may have.",
	})

	metricModuleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "goalert",
		Subsystem: "engine",
		Name:      "cycle_duration_seconds",
		Help:      "Engine cycle duration in seconds by module.",
	}, []string{"module"})
)
