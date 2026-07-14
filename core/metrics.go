package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsDispatched counts jobs claimed and turned into execution runs.
	JobsDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_scheduler_jobs_dispatched_total",
		Help: "Total jobs dispatched (execution run created) by the scheduler.",
	})

	// TickDuration measures the wall time of one full scheduler tick.
	TickDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "praetor_scheduler_tick_duration_seconds",
		Help:    "Duration of one scheduler tick (all sub-passes).",
		Buckets: prometheus.DefBuckets,
	})

	// TickTaskErrors counts errors per tick task, so a single failing pass is
	// visible instead of being lost in the log.
	TickTaskErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "praetor_scheduler_tick_task_errors_total",
		Help: "Errors returned by a scheduler tick task, labeled by task name.",
	}, []string{"task"})

	// RunsReconciling counts stale remote runs handed to the reconciler.
	RunsReconciling = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_scheduler_runs_reconciling_total",
		Help: "Total stale remote runs moved to the reconciling state.",
	})

	// RunsLost counts local runs declared lost (no SSH path to recover).
	RunsLost = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_scheduler_runs_lost_total",
		Help: "Total local runs marked lost by the heartbeat check.",
	})

	// QueueDepth is the number of jobs waiting to be dispatched.
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "praetor_scheduler_queue_depth",
		Help: "Jobs currently pending or queued (not yet running).",
	})
)
