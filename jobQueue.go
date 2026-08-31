package main

import (
	"container/heap"
	"errors"
	"sync"
	"time"
	"uuid"
)

type JobMap map[uuid.UUID]*Job

// JobQueue is a min-heap of jobs ordered by UpdatedAt (oldest first). Use
// NewJobQueue to construct one, and Claim/Complete/Fail/Peek to interact with
// it; Push/Pop/Len/Less/Swap only exist to satisfy heap.Interface, and
// enqueue/dequeue are internal helpers that assume the caller holds jq.mu.
type JobQueue struct {
	Pending   []*Job
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
	heap.Init(jq)
	return jq
}

func (jq *JobQueue) Len() int { return len(jq.Pending) }
func (jq *JobQueue) Less(i, j int) bool {
	if jq.Pending[i].UpdatedAt.Compare(jq.Pending[j].UpdatedAt) == -1 {
		return true
	}
	return false
}

func (jq *JobQueue) Swap(i, j int) {
	jq.Pending[i], jq.Pending[j] = jq.Pending[j], jq.Pending[i]
}

func (jq *JobQueue) Push(x any) {
	jq.Pending = append(jq.Pending, x.(*Job))
}

func (jq *JobQueue) Pop() any {
	old := jq.Pending
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	jq.Pending = old[:n-1]
	return job
}

// enqueue adds a job to the heap, returning false if the queue is already at MaxJobs capacity.
// Caller must hold jq.mu.
func (jq *JobQueue) enqueue(j *Job) bool {
	if jq.MaxJobs > 0 && uint16(len(jq.Pending)+len(jq.Running)) >= jq.MaxJobs {
		return false
	}
	j.JobStatus = StatusPending
	j.UpdatedAt = time.Now()
	heap.Push(jq, j)
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

func (jq *JobQueue) Claim(workerId uuid.UUID) *Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if len(jq.Pending) == 0 {
		return nil
	}

	j := heap.Pop(jq).(*Job)
	j.LeaseExpiration = time.Now().Add(jq.LeaseTime)
	j.JobStatus = StatusRunning
	j.UpdatedAt = time.Now()
	j.LastWorkerId = workerId
	jq.Running[j.ID] = j
	return j
}

var (
	ErrJobNotRunning     = errors.New("Job not running")
	ErrWorkerIdMissmatch = errors.New("Job is running for another worker")
	ErrLeaseExpired      = errors.New("Lease has expired")
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
		j.JobStatus = StatusDead
		j.UpdatedAt = time.Now()
		jq.DeadJobs[j.ID] = j
		return nil
	}

	jq.enqueue(j) // we discard bool since j was already removed from Running above, freeing its capacity slot

	return nil
}

func (jq *JobQueue) Sweep() {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	for _, j := range jq.Running {
		if time.Now().After(j.LeaseExpiration) {
			jq.failLocked(j.ID, j.LastWorkerId, ErrLeaseExpired)
		}
	}
}
