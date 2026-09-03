package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/maxider/job-queue/queue"
)

// Metrics instrumentation lives here rather than in the queue package on
// purpose: queue is the library, this cmd is the demo harness, and the
// harness is what should own the Prometheus dependency.
var (
	jobsEnqueuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Total number of jobs enqueued.",
	})
	jobsCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_completed_total",
		Help: "Total number of jobs completed successfully.",
	})
	jobsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of job attempts that failed (including retries and dead-letters).",
	})
	jobsDeadLetteredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_dead_lettered_total",
		Help: "Total number of jobs moved to the dead-letter set after exhausting retries.",
	})

	queuePendingJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_pending_jobs",
		Help: "Current number of jobs waiting to be claimed.",
	})
	queueRunningJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_running_jobs",
		Help: "Current number of jobs claimed and in flight.",
	})

	jobProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_processing_duration_seconds",
		Help:    "Time spent processing a job from claim to completion or failure.",
		Buckets: prometheus.DefBuckets,
	})
)

// runMetricsSampler periodically samples queue depth gauges via Counts,
// since JobQueue deliberately doesn't expose a Prometheus-flavored API.
func runMetricsSampler(ctx context.Context, jq *queue.JobQueue, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, running := jq.Counts()
			queuePendingJobs.Set(float64(pending))
			queueRunningJobs.Set(float64(running))
		}
	}
}
