package main

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"uuid"
)

var _ sort.Interface = (*JobHeap)(nil)

var errBoom = errors.New("boom")

func jobAt(t time.Time) *Job {
	return &Job{ID: uuid.New(), UpdatedAt: t, JobStatus: StatusPending}
}

func TestJobQueueLen(t *testing.T) {
	now := time.Now()
	jq := &JobQueue{Pending: []*Job{jobAt(now), jobAt(now), jobAt(now)}}

	if got := jq.Pending.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3", got)
	}

	jq = &JobQueue{}
	if got := jq.Pending.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
}

func TestJobQueueLess(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-time.Hour)

	jq := &JobQueue{Pending: []*Job{jobAt(earlier), jobAt(now)}}

	if !jq.Pending.Less(0, 1) {
		t.Error("Less(0, 1) = false, want true when Jobs[0] is older than Jobs[1]")
	}
	if jq.Pending.Less(1, 0) {
		t.Error("Less(1, 0) = true, want false when Jobs[1] is newer than Jobs[0]")
	}
	if jq.Pending.Less(0, 0) {
		t.Error("Less(0, 0) = true, want false for equal timestamps")
	}
}

func TestJobQueueSwap(t *testing.T) {
	a, b := jobAt(time.Now()), jobAt(time.Now().Add(time.Hour))
	jq := &JobQueue{Pending: []*Job{a, b}}

	jq.Pending.Swap(0, 1)

	if jq.Pending[0] != b || jq.Pending[1] != a {
		t.Error("Swap(0, 1) did not swap the elements")
	}
}

func TestJobQueueSort(t *testing.T) {
	now := time.Now()
	jq := &JobQueue{Pending: []*Job{
		jobAt(now.Add(2 * time.Hour)),
		jobAt(now),
		jobAt(now.Add(time.Hour)),
		jobAt(now.Add(-time.Hour)),
	}}

	sort.Sort(&jq.Pending)

	if !sort.IsSorted(&jq.Pending) {
		t.Fatal("queue is not sorted after sort.Sort")
	}
	for i := 1; i < jq.Pending.Len(); i++ {
		if jq.Pending[i-1].UpdatedAt.After(jq.Pending[i].UpdatedAt) {
			t.Errorf("Jobs[%d].UpdatedAt (%v) is after Jobs[%d].UpdatedAt (%v)",
				i-1, jq.Pending[i-1].UpdatedAt, i, jq.Pending[i].UpdatedAt)
		}
	}
}

func TestJobQueueClaimOrder(t *testing.T) {
	// enqueue always stamps UpdatedAt = time.Now() at call time, so the only
	// way to control relative order is via enqueue order itself (with a
	// small delay to guarantee strictly increasing timestamps). Claim also
	// mutates UpdatedAt, so we track expected order by job identity (ID).
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()

	var want []uuid.UUID
	for i := 0; i < 4; i++ {
		j := jobAt(time.Now())
		jq.enqueueLocked(j)
		want = append(want, j.ID)
		time.Sleep(time.Millisecond)
	}

	var got []uuid.UUID
	for jq.Pending.Len() > 0 {
		j := jq.Claim(worker)
		if j == nil {
			t.Fatal("Claim() = nil while queue is non-empty")
		}
		got = append(got, j.ID)
	}

	if len(got) != len(want) {
		t.Fatalf("claimed %d jobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("claim order[%d] = %v, want %v: jobs must be claimed in enqueue (oldest-first) order", i, got[i], want[i])
		}
	}
}

func TestJobQueuePeek(t *testing.T) {
	// enqueue always stamps UpdatedAt = time.Now() at call time, so the
	// first job enqueued is necessarily the oldest.
	jq := NewJobQueue(0, time.Minute)

	if _, ok := jq.Peek(); ok {
		t.Error("Peek() on empty queue = true, want false")
	}

	oldest := jobAt(time.Now())
	jq.enqueueLocked(oldest)
	time.Sleep(time.Millisecond)
	jq.enqueueLocked(jobAt(time.Now()))
	time.Sleep(time.Millisecond)
	jq.enqueueLocked(jobAt(time.Now()))

	peeked, ok := jq.Peek()
	if !ok || peeked != oldest {
		t.Errorf("Peek() = (%v, %v), want the oldest job", peeked, ok)
	}
	if jq.Pending.Len() != 3 {
		t.Errorf("Peek() changed queue length to %d, want 3", jq.Pending.Len())
	}
}

func TestJobQueueEnqueueRespectsMaxJobs(t *testing.T) {
	jq := NewJobQueue(2, time.Minute)

	if !jq.enqueueLocked(jobAt(time.Now())) {
		t.Fatal("enqueue() = false, want true for first job under capacity")
	}
	if !jq.enqueueLocked(jobAt(time.Now())) {
		t.Fatal("enqueue() = false, want true for second job at capacity limit")
	}
	if jq.enqueueLocked(jobAt(time.Now())) {
		t.Error("enqueue() = true, want false once MaxJobs is reached")
	}
	if jq.Pending.Len() != 2 {
		t.Errorf("Len() = %d, want 2 after rejected enqueue", jq.Pending.Len())
	}
}

func TestJobQueueEnqueueMaxJobsCountsRunning(t *testing.T) {
	jq := NewJobQueue(2, time.Minute)

	jq.enqueueLocked(jobAt(time.Now()))
	jq.enqueueLocked(jobAt(time.Now()))
	jq.Claim(uuid.New()) // moves one job from Pending into Running; total in-flight is still 2

	if jq.enqueueLocked(jobAt(time.Now())) {
		t.Error("enqueue() = true, want false: MaxJobs should count Pending+Running, not Pending alone")
	}
}

func TestJobQueueClaim(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()

	if got := jq.Claim(worker); got != nil {
		t.Fatalf("Claim() on empty queue = %v, want nil", got)
	}

	// enqueue always stamps UpdatedAt = time.Now() at call time, so the
	// first job enqueued is necessarily the oldest.
	oldest := jobAt(time.Now())
	jq.enqueueLocked(oldest)
	time.Sleep(time.Millisecond)
	jq.enqueueLocked(jobAt(time.Now()))

	before := time.Now()
	claimed := jq.Claim(worker)
	if claimed != oldest {
		t.Fatalf("Claim() returned %v, want the oldest job %v", claimed, oldest)
	}
	if claimed.JobStatus != StatusRunning {
		t.Errorf("claimed job JobStatus = %v, want %v", claimed.JobStatus, StatusRunning)
	}
	if claimed.LeaseExpiration.Before(before.Add(time.Minute)) {
		t.Errorf("LeaseExpiration = %v, want at least %v after claim", claimed.LeaseExpiration, before.Add(time.Minute))
	}
	if claimed.LastWorkerId != worker {
		t.Errorf("LastWorkerId = %v, want %v", claimed.LastWorkerId, worker)
	}
	if jq.Running[claimed.ID] != claimed {
		t.Error("claimed job was not recorded in Running")
	}
	if jq.Pending.Len() != 1 {
		t.Errorf("Len() = %d, want 1 after claiming one of two jobs", jq.Pending.Len())
	}
}

func TestJobQueueClaimRecordsDifferentWorkerPerJob(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	jq.enqueueLocked(jobAt(time.Now()))
	jq.enqueueLocked(jobAt(time.Now().Add(time.Hour)))

	w1, w2 := uuid.New(), uuid.New()
	j1 := jq.Claim(w1)
	j2 := jq.Claim(w2)

	if j1.LastWorkerId != w1 {
		t.Errorf("j1.LastWorkerId = %v, want %v", j1.LastWorkerId, w1)
	}
	if j2.LastWorkerId != w2 {
		t.Errorf("j2.LastWorkerId = %v, want %v", j2.LastWorkerId, w2)
	}
}

func TestJobQueueComplete(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()
	jq.enqueueLocked(jobAt(time.Now()))
	claimed := jq.Claim(worker)

	if err := jq.Complete(claimed.ID, worker); err != nil {
		t.Fatalf("Complete() error = %v, want nil", err)
	}
	if claimed.JobStatus != StatusComplete {
		t.Errorf("JobStatus = %v, want %v", claimed.JobStatus, StatusComplete)
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("completed job is still present in Running")
	}
}

func TestJobQueueCompleteUnknownID(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)

	if err := jq.Complete(uuid.New(), uuid.New()); err != ErrJobNotRunning {
		t.Errorf("Complete() error = %v, want %v", err, ErrJobNotRunning)
	}
}

func TestJobQueueCompleteWrongWorker(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	owner, other := uuid.New(), uuid.New()
	jq.enqueueLocked(jobAt(time.Now()))
	claimed := jq.Claim(owner)
	if err := jq.Complete(claimed.ID, other); err != ErrWorkerIdMissmatch {
		t.Fatalf("Complete() error = %v, want %v", err, ErrWorkerIdMissmatch)
	}
	if claimed.JobStatus != StatusRunning {
		t.Errorf("JobStatus = %v, want %v: job must not be completed by a non-owning worker", claimed.JobStatus, StatusRunning)
	}
	if _, ok := jq.Running[claimed.ID]; !ok {
		t.Error("job was removed from Running despite the ownership check failing")
	}
}

func TestJobQueueFailRetriesUntilMaxAttempts(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 1
	jq.enqueueLocked(j)

	// First failure: Attempts (1) is not > MaxAttempts (1), should go back to Pending.
	claimed := jq.Claim(worker)
	if err := jq.Fail(claimed.ID, worker, errBoom); err != nil {
		t.Fatalf("Fail() error = %v, want nil", err)
	}
	if claimed.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", claimed.Attempts)
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("failed job is still present in Running")
	}
	if jq.Pending.Len() != 1 {
		t.Fatalf("Len() = %d, want 1: job should have been re-enqueued into Pending", jq.Pending.Len())
	}
	if len(jq.DeadJobs) != 0 {
		t.Errorf("DeadJobs = %d, want 0: job has not exceeded MaxAttempts yet", len(jq.DeadJobs))
	}

	// Second failure: Attempts (2) exceeds MaxAttempts (1), should move to
	// DeadJobs and NOT also be re-enqueued into Pending.
	claimed = jq.Claim(worker)
	if err := jq.Fail(claimed.ID, worker, errBoom); err != nil {
		t.Fatalf("Fail() error = %v, want nil", err)
	}
	if claimed.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", claimed.Attempts)
	}
	if claimed.JobStatus != StatusDead {
		t.Errorf("JobStatus = %v, want %v", claimed.JobStatus, StatusDead)
	}
	if claimed.LastError != errBoom.Error() {
		t.Errorf("LastError = %q, want %q", claimed.LastError, errBoom.Error())
	}
	if jq.DeadJobs[claimed.ID] != claimed {
		t.Error("job exceeding MaxAttempts was not recorded in DeadJobs")
	}
	if jq.Pending.Len() != 0 {
		t.Errorf("Len() = %d, want 0: dead job must not also be re-enqueued into Pending", jq.Pending.Len())
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("dead job is still present in Running")
	}
}

func TestJobQueueFailRetryRecordsLastError(t *testing.T) {
	// LastError is recorded on every failure, including a retry that goes
	// back to Pending, not just once a job is moved to DeadJobs.
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)

	claimed := jq.Claim(worker)
	if err := jq.Fail(claimed.ID, worker, errBoom); err != nil {
		t.Fatalf("Fail() error = %v, want nil", err)
	}
	if claimed.LastError != errBoom.Error() {
		t.Errorf("LastError = %q, want %q after a retry", claimed.LastError, errBoom.Error())
	}
	if claimed.JobStatus != StatusPending {
		t.Errorf("JobStatus = %v, want %v after a retry", claimed.JobStatus, StatusPending)
	}
}

func TestJobQueueFailUnknownID(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)

	if err := jq.Fail(uuid.New(), uuid.New(), errBoom); err != ErrJobNotRunning {
		t.Errorf("Fail() error = %v, want %v", err, ErrJobNotRunning)
	}
}

func TestJobQueueFailWrongWorker(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	owner, other := uuid.New(), uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)
	claimed := jq.Claim(owner)

	if err := jq.Fail(claimed.ID, other, errBoom); err != ErrWorkerIdMissmatch {
		t.Fatalf("Fail() error = %v, want %v", err, ErrWorkerIdMissmatch)
	}
	if claimed.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0: a non-owning worker must not be able to record a failed attempt", claimed.Attempts)
	}
	if _, ok := jq.Running[claimed.ID]; !ok {
		t.Error("job was removed from Running despite the ownership check failing")
	}
}

func TestJobQueueFailRetryAtCapacityIsNotLost(t *testing.T) {
	// Regression test: MaxJobs counts Pending+Running, so without removing a
	// failed job from Running before re-enqueueing it, retrying at capacity
	// would make enqueue reject the job and it would vanish (not in Pending,
	// not in Running, not in DeadJobs).
	jq := NewJobQueue(1, time.Minute)
	worker := uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)

	claimed := jq.Claim(worker)
	if err := jq.Fail(claimed.ID, worker, errBoom); err != nil {
		t.Fatalf("Fail() error = %v, want nil", err)
	}

	if jq.Pending.Len() != 1 {
		t.Fatalf("Len() = %d, want 1: retried job must not be dropped at capacity", jq.Pending.Len())
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("retried job is still present in Running")
	}
}

func TestJobQueueRetriedJobCanBeClaimedByDifferentWorker(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	w1, w2 := uuid.New(), uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)

	claimed := jq.Claim(w1)
	if err := jq.Fail(claimed.ID, w1, errBoom); err != nil {
		t.Fatalf("Fail() error = %v, want nil", err)
	}

	reclaimed := jq.Claim(w2)
	if reclaimed != claimed {
		t.Fatalf("Claim() returned %v, want the retried job %v", reclaimed, claimed)
	}
	if reclaimed.LastWorkerId != w2 {
		t.Errorf("LastWorkerId = %v, want %v after reclaiming", reclaimed.LastWorkerId, w2)
	}
	// w1 no longer owns the job, so it must not be able to complete it.
	if err := jq.Complete(reclaimed.ID, w1); err != ErrWorkerIdMissmatch {
		t.Errorf("Complete() by original worker error = %v, want %v", err, ErrWorkerIdMissmatch)
	}
	if err := jq.Complete(reclaimed.ID, w2); err != nil {
		t.Errorf("Complete() by new owning worker error = %v, want nil", err)
	}
}

// runSweep calls jq.Sweep() with a timeout guard: Sweep locks jq.mu and, for
// each expired job, must go through the already-locked failLocked path
// rather than Fail (which would try to re-lock jq.mu and deadlock, since
// sync.Mutex is not reentrant).
func runSweep(t *testing.T, jq *JobQueue) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		jq.Sweep()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sweep() did not return within 2s, likely deadlocked on jq.mu")
	}
}

func TestJobQueueSweepReclaimsExpiredLease(t *testing.T) {
	jq := NewJobQueue(0, time.Millisecond)
	worker := uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)
	claimed := jq.Claim(worker)

	time.Sleep(5 * time.Millisecond) // let the lease expire

	runSweep(t, jq)

	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("expired job is still present in Running after Sweep")
	}
	if jq.Pending.Len() != 1 {
		t.Fatalf("Len() = %d, want 1: expired job should have been re-enqueued into Pending", jq.Pending.Len())
	}
	if claimed.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", claimed.Attempts)
	}
	if claimed.LastError != ErrLeaseExpired.Error() {
		t.Errorf("LastError = %q, want %q", claimed.LastError, ErrLeaseExpired.Error())
	}
}

func TestJobQueueSweepMovesExhaustedJobToDeadJobs(t *testing.T) {
	jq := NewJobQueue(0, time.Millisecond)
	worker := uuid.New()
	j := jobAt(time.Now())
	j.MaxAttempts = 0 // any failure exceeds MaxAttempts
	jq.enqueueLocked(j)
	claimed := jq.Claim(worker)

	time.Sleep(5 * time.Millisecond) // let the lease expire

	runSweep(t, jq)

	if claimed.JobStatus != StatusDead {
		t.Errorf("JobStatus = %v, want %v", claimed.JobStatus, StatusDead)
	}
	if jq.DeadJobs[claimed.ID] != claimed {
		t.Error("exhausted job was not moved to DeadJobs by Sweep")
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("dead job is still present in Running after Sweep")
	}
	if jq.Pending.Len() != 0 {
		t.Errorf("Len() = %d, want 0: dead job must not also be re-enqueued into Pending", jq.Pending.Len())
	}
}

func TestJobQueueSweepIgnoresUnexpiredLease(t *testing.T) {
	jq := NewJobQueue(0, time.Hour)
	worker := uuid.New()
	jq.enqueueLocked(jobAt(time.Now()))
	claimed := jq.Claim(worker)

	runSweep(t, jq)

	if _, ok := jq.Running[claimed.ID]; !ok {
		t.Error("Sweep() removed a job whose lease has not expired")
	}
	if claimed.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0: Sweep must not touch jobs with an unexpired lease", claimed.Attempts)
	}
}

func TestJobQueueConcurrentClaimCompleteFail(t *testing.T) {
	const numJobs = 200
	jq := NewJobQueue(0, time.Minute)
	for i := 0; i < numJobs; i++ {
		j := jobAt(time.Now())
		j.MaxAttempts = 10
		jq.enqueueLocked(j)
	}

	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := uuid.New()
			j := jq.Claim(worker)
			if j == nil {
				return
			}
			if j.Attempts%2 == 0 {
				jq.Complete(j.ID, worker)
			} else {
				jq.Fail(j.ID, worker, errBoom)
			}
		}()
	}
	wg.Wait()
}
