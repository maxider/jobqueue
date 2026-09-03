// Package queue implements a concurrent, in-memory job queue with
// lease-based claiming, automatic retries, and dead-lettering.
package queue

import (
	"container/heap"
	"errors"
	"log/slog"
	"sync"
	"time"
	"uuid"
)

type JobHeap []*Job
type JobMap map[uuid.UUID]*Job

// JobQueue is a min-heap of jobs ordered by UpdatedAt (oldest first). Use
// NewJobQueue to construct one, and Claim/Complete/Fail/Peek to interact with
// it; Push/Pop/Len/Less/Swap only exist to satisfy heap.Interface, and
// enqueue/dequeue are internal helpers that assume the caller holds jq.mu.
type JobQueue struct {
	Pending   JobHeap
	DeadJobs  JobMap
	Running   JobMap
	MaxJobs   uint16
	LeaseTime time.Duration
	mu        sync.Mutex
}

func NewJobQueue(maxJobs uint16, leaseTime time.Duration) *JobQueue {
	jq := &JobQueue{
		MaxJobs:   maxJobs,
		LeaseTime: leaseTime,
		Running:   make(JobMap),
		DeadJobs:  make(JobMap),
	}
	heap.Init(&jq.Pending)
	return jq
}

func (jh *JobHeap) Len() int { return len(*jh) }
func (jh *JobHeap) Less(i, j int) bool {
	return (*jh)[i].UpdatedAt.Before((*jh)[j].UpdatedAt)
}

func (jh *JobHeap) Swap(i, j int) {
	(*jh)[i], (*jh)[j] = (*jh)[j], (*jh)[i]
}

func (jh *JobHeap) Push(x any) {
	*jh = append(*jh, x.(*Job))
}

func (jh *JobHeap) Pop() any {
	old := *jh
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	*jh = old[:n-1]
	return job
}

func (jq *JobQueue) Enqueue(j *Job) bool {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	return jq.enqueueLocked(j)
}

// enqueueLocked adds a job to the heap, returning false if the queue is already at MaxJobs capacity.
// Caller must hold jq.mu.
func (jq *JobQueue) enqueueLocked(j *Job) bool {
	if jq.MaxJobs > 0 && uint16(len(jq.Pending)+len(jq.Running)) >= jq.MaxJobs {
		return false
	}
	j.JobStatus = StatusPending
	j.UpdatedAt = time.Now()
	heap.Push(&jq.Pending, j)
	return true
}

// Peek returns the job with the oldest UpdatedAt without removing it.
func (jq *JobQueue) Peek() (*Job, bool) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.Pending) == 0 {
		return nil, false
	}
	return jq.Pending[0], true
}

// Counts returns the current number of pending and running jobs.
func (jq *JobQueue) Counts() (pending int, running int) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	return len(jq.Pending), len(jq.Running)
}

func (jq *JobQueue) Claim(workerId uuid.UUID) *Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.Pending) == 0 {
		return nil
	}

	j := heap.Pop(&jq.Pending).(*Job)
	j.LeaseExpiration = time.Now().Add(jq.LeaseTime)
	j.JobStatus = StatusRunning
	j.UpdatedAt = time.Now()
	j.LastWorkerId = workerId
	jq.Running[j.ID] = j
	return j
}

var (
	ErrJobNotRunning     = errors.New("job not running")
	ErrWorkerIdMissmatch = errors.New("job is running for another worker")
	ErrLeaseExpired      = errors.New("lease has expired")
	ErrEnqueueFailed     = errors.New("failed to put task back in queue")
)

func jobCheck(j *Job, wid uuid.UUID) error {
	if j == nil {
		return ErrJobNotRunning
	}
	if j.LastWorkerId != wid {
		return ErrWorkerIdMissmatch
	}
	return nil
}

func (jq *JobQueue) Complete(id uuid.UUID, workerId uuid.UUID) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	j := jq.Running[id]
	if err := jobCheck(j, workerId); err != nil {
		return err
	}
	defer delete(jq.Running, id)
	j.JobStatus = StatusComplete
	j.UpdatedAt = time.Now()
	return nil
}

func (jq *JobQueue) Fail(id uuid.UUID, wid uuid.UUID, jobError error) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	return jq.failLocked(id, wid, jobError)
}

func (jq *JobQueue) failLocked(id uuid.UUID, wid uuid.UUID, jobError error) error {
	j := jq.Running[id]
	if err := jobCheck(j, wid); err != nil {
		return err
	}
	delete(jq.Running, id)

	j.Attempts++
	j.LastError = jobError.Error()
	if j.Attempts > j.MaxAttempts {
		markDead(jq, j, nil)
		return nil
	}

	if ok := jq.enqueueLocked(j); !ok {
		//This should not happen as the job gets taken from the Running list while locked and put back into the pending list while still locked.
		markDead(jq, j, ErrEnqueueFailed)
	}

	return nil
}

func markDead(jq *JobQueue, j *Job, err error) {
	j.JobStatus = StatusDead
	j.UpdatedAt = time.Now()
	jq.DeadJobs[j.ID] = j
	if err != nil {
		j.LastError = err.Error()
	}
}

func (jq *JobQueue) Sweep() {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	for _, j := range jq.Running {
		if time.Now().After(j.LeaseExpiration) {
			slog.Debug("sweeper reclaimed expired lease", "job_id", j.ID, "worker_id", j.LastWorkerId)
			// j came straight out of jq.Running keyed by its own LastWorkerId,
			// so jobCheck inside failLocked can never reject it.
			_ = jq.failLocked(j.ID, j.LastWorkerId, ErrLeaseExpired)
		}
	}
}
