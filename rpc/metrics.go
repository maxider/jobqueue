package rpc

import (
	"sync"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics instrumentation lives here, on the RPC boundary, rather than in
// the queue package: Server is the thing that observes every Enqueue,
// Claim, Complete, and Fail as it happens over the network, so it's the
// natural place to record what happened without the queue library itself
// taking on a Prometheus dependency.
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

	jobProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_processing_duration_seconds",
		Help:    "Time spent processing a job from claim to completion or failure.",
		Buckets: prometheus.DefBuckets,
	})
)

// claimClock tracks when each in-flight job was claimed, so Complete/Fail
// can report job_processing_duration_seconds. It's keyed by job ID rather
// than carried on the RPC request because the client only echoes job_id
// and worker_id back, not claim time.
type claimClock struct {
	mu      sync.Mutex
	claimed map[uuid.UUID]time.Time
}

func newClaimClock() *claimClock {
	return &claimClock{claimed: make(map[uuid.UUID]time.Time)}
}

func (c *claimClock) start(id uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.claimed[id] = time.Now()
}

// stop returns the elapsed time since start(id) and forgets it. ok is false
// if no claim was recorded (e.g. server restarted mid-lease).
func (c *claimClock) stop(id uuid.UUID) (elapsed time.Duration, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	startedAt, ok := c.claimed[id]
	if !ok {
		return 0, false
	}
	delete(c.claimed, id)
	return time.Since(startedAt), true
}
