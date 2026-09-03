package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/maxider/job-queue/queue"
)

// Depth gauges live here, sampled straight off the queue, rather than in
// rpc.Server: unlike the counters in rpc/metrics.go, pending/running depth
// isn't tied to a single RPC call, and JobQueue deliberately doesn't expose
// a Prometheus-flavored API itself.
var (
	queuePendingJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_pending_jobs",
		Help: "Current number of jobs waiting to be claimed.",
	})
	queueRunningJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_running_jobs",
		Help: "Current number of jobs claimed and in flight.",
	})
)

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
