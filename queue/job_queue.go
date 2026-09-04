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

// NewJobQueue constructs an empty JobQueue. maxJobs caps the number of jobs
// counted across Pending+Running at once (0 means unlimited); leaseTime is
// how long a worker has to Complete or Fail a claimed job before Sweep
// reclaims it.
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

// Enqueue adds j to the queue as Pending, stamping its status and
// UpdatedAt. It returns false without modifying j if the queue is already
// at MaxJobs capacity.
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

// IsDead reports whether id was moved to the dead-letter set, e.g. to tell
// a caller of Fail whether that call was the one that exhausted retries.
func (jq *JobQueue) IsDead(id uuid.UUID) bool {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	_, dead := jq.DeadJobs[id]
	return dead
}

// Claim pops the oldest Pending job (by UpdatedAt), moves it to Running
// under a new lease owned by workerID, and returns it. It returns nil if
// nothing is Pending.
func (jq *JobQueue) Claim(workerID uuid.UUID) *Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.Pending) == 0 {
		return nil
	}

	j := heap.Pop(&jq.Pending).(*Job)
	j.LeaseExpiration = time.Now().Add(jq.LeaseTime)
	j.JobStatus = StatusRunning
	j.UpdatedAt = time.Now()
	j.LastWorkerID = workerID
	jq.Running[j.ID] = j
	return j
}

var (
	// ErrJobNotRunning is returned by Complete/Fail when id isn't currently
	// in Running (already completed/dead-lettered, or never claimed).
	ErrJobNotRunning = errors.New("job not running")
	// ErrWorkerIDMismatch is returned by Complete/Fail when the caller's
	// worker ID doesn't own the job's current lease — e.g. its lease
	// already expired and a different worker claimed it. See the
	// "At-least-once delivery" section in the README.
	ErrWorkerIDMismatch = errors.New("job is running for another worker")
	// ErrLeaseExpired is the internal failure reason Sweep passes to Fail
	// when it reclaims a job whose lease expired.
	ErrLeaseExpired = errors.New("lease has expired")
	// ErrEnqueueFailed marks a job dead when Fail can't re-enqueue it for
	// retry; see the comment at its call site for why this shouldn't happen.
	ErrEnqueueFailed = errors.New("failed to put task back in queue")
)

func jobCheck(j *Job, wid uuid.UUID) error {
	if j == nil {
		return ErrJobNotRunning
	}
	if j.LastWorkerID != wid {
		return ErrWorkerIDMismatch
	}
	return nil
}

// Complete marks id as done, provided workerID matches the worker that
// currently holds its lease (see ErrWorkerIDMismatch).
func (jq *JobQueue) Complete(id uuid.UUID, workerID uuid.UUID) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	j := jq.Running[id]
	if err := jobCheck(j, workerID); err != nil {
		return err
	}
	defer delete(jq.Running, id)
	j.JobStatus = StatusComplete
	j.UpdatedAt = time.Now()
	return nil
}

// Fail records a failed attempt at id by worker wid: it re-enqueues the job
// for another attempt if it hasn't exceeded MaxAttempts yet, or moves it to
// DeadJobs otherwise. jobError is stored on the job as LastError.
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

// Sweep reclaims every Running job whose lease has expired, treating each
// as a failed attempt (see Fail) so it either goes back to Pending for
// another worker or is dead-lettered if out of attempts. Intended to be
// called periodically by the caller (e.g. cmd/server runs it on a ticker).
func (jq *JobQueue) Sweep() {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	for _, j := range jq.Running {
		if time.Now().After(j.LeaseExpiration) {
			slog.Debug("sweeper reclaimed expired lease", "job_id", j.ID, "worker_id", j.LastWorkerID)
			// j came straight out of jq.Running keyed by its own LastWorkerID,
			// so jobCheck inside failLocked can never reject it.
			_ = jq.failLocked(j.ID, j.LastWorkerID, ErrLeaseExpired)
		}
	}
}
