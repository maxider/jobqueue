package queue

import (
	"testing"
	"time"
	"uuid"
)

// Scenario 1: happy path — claim, complete, and a job can't be completed twice.
func TestScenarioClaimCompleteHappyPath(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()

	j := jobAt(time.Now())
	jq.enqueueLocked(j)

	claimed := jq.Claim(worker)
	if claimed == nil {
		t.Fatal("Claim() = nil, want the enqueued job")
	}
	if claimed.JobStatus != StatusRunning {
		t.Errorf("JobStatus = %v, want %v", claimed.JobStatus, StatusRunning)
	}
	if claimed.LastWorkerID != worker {
		t.Errorf("LastWorkerID = %v, want %v", claimed.LastWorkerID, worker)
	}

	if err := jq.Complete(claimed.ID, worker); err != nil {
		t.Fatalf("Complete() error = %v, want nil", err)
	}
	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("completed job is still present in Running")
	}
	if claimed.JobStatus != StatusComplete {
		t.Errorf("JobStatus = %v, want %v", claimed.JobStatus, StatusComplete)
	}

	// Bonus: a job can't be completed twice.
	if err := jq.Complete(claimed.ID, worker); err != ErrJobNotRunning {
		t.Errorf("second Complete() error = %v, want %v", err, ErrJobNotRunning)
	}
}

// Scenario 2: a worker crashes without completing or failing the job; Sweep
// reclaims it after the lease expires, and a different worker can then claim it.
func TestScenarioWorkerCrashJobReclaimedBySweep(t *testing.T) {
	jq := NewJobQueue(0, 5*time.Millisecond)
	workerA := uuid.New()

	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)

	claimed := jq.Claim(workerA)
	if claimed == nil {
		t.Fatal("Claim() = nil, want the enqueued job")
	}
	// Simulated crash: neither Complete nor Fail is ever called by workerA.

	time.Sleep(10 * time.Millisecond) // let the lease expire

	runSweep(t, jq)

	if _, ok := jq.Running[claimed.ID]; ok {
		t.Error("expired job is still present in Running after Sweep")
	}
	if claimed.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", claimed.Attempts)
	}
	if claimed.LastError != ErrLeaseExpired.Error() {
		t.Errorf("LastError = %q, want %q", claimed.LastError, ErrLeaseExpired.Error())
	}
	if jq.Pending.Len() != 1 {
		t.Fatalf("Len() = %d, want 1: job should be back in Pending", jq.Pending.Len())
	}

	workerB := uuid.New()
	reclaimed := jq.Claim(workerB)
	if reclaimed == nil || reclaimed.ID != claimed.ID {
		t.Fatalf("Claim() = %v, want the reclaimed job %v", reclaimed, claimed)
	}
	if reclaimed.LastWorkerID != workerB {
		t.Errorf("LastWorkerID = %v, want %v after reclaiming", reclaimed.LastWorkerID, workerB)
	}
}

// Scenario 3: repeated failures dead-letter a job once MaxAttempts is exceeded.
func TestScenarioRepeatedFailureDeadLetters(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	worker := uuid.New()

	j := jobAt(time.Now())
	j.MaxAttempts = 2
	jq.enqueueLocked(j)

	const totalFailures = 3 // MaxAttempts (2) retries, then the 3rd failure dead-letters it
	for i := 1; i <= totalFailures; i++ {
		claimed := jq.Claim(worker)
		if claimed == nil {
			t.Fatalf("attempt %d: Claim() = nil, want the job", i)
		}

		if err := jq.Fail(claimed.ID, worker, errBoom); err != nil {
			t.Fatalf("attempt %d: Fail() error = %v, want nil", i, err)
		}
		if claimed.Attempts != uint16(i) {
			t.Errorf("attempt %d: Attempts = %d, want %d", i, claimed.Attempts, i)
		}

		if i < totalFailures {
			// Still under MaxAttempts: back in Pending, claimable again.
			if claimed.JobStatus != StatusPending {
				t.Errorf("attempt %d: JobStatus = %v, want %v", i, claimed.JobStatus, StatusPending)
			}
			if jq.Pending.Len() != 1 {
				t.Errorf("attempt %d: Len() = %d, want 1", i, jq.Pending.Len())
			}
			if len(jq.DeadJobs) != 0 {
				t.Errorf("attempt %d: DeadJobs = %d, want 0", i, len(jq.DeadJobs))
			}
		} else {
			// Attempts (3) now exceeds MaxAttempts (2): dead-lettered.
			if claimed.JobStatus != StatusDead {
				t.Errorf("attempt %d: JobStatus = %v, want %v", i, claimed.JobStatus, StatusDead)
			}
			if jq.DeadJobs[claimed.ID] != claimed {
				t.Errorf("attempt %d: job not recorded in DeadJobs", i)
			}
			if _, ok := jq.Peek(); ok {
				t.Errorf("attempt %d: Peek() found a job, want the queue empty", i)
			}
			if got := jq.Claim(worker); got != nil {
				t.Errorf("attempt %d: Claim() = %v, want nil: dead job must not be claimable", i, got)
			}
		}
	}
}

// Scenario 4: the worker-fencing check directly. A stale worker can't
// complete a job it no longer owns, and the job's bookkeeping is untouched.
func TestScenarioFencingPreventsStaleComplete(t *testing.T) {
	jq := NewJobQueue(0, time.Minute)
	workerA, workerB := uuid.New(), uuid.New()

	j := jobAt(time.Now())
	jq.enqueueLocked(j)
	claimed := jq.Claim(workerA)

	if err := jq.Complete(claimed.ID, workerB); err != ErrWorkerIDMismatch {
		t.Fatalf("Complete() by non-owning worker error = %v, want %v", err, ErrWorkerIDMismatch)
	}

	if claimed.JobStatus != StatusRunning {
		t.Errorf("JobStatus = %v, want %v: stale Complete must not change job state", claimed.JobStatus, StatusRunning)
	}
	if _, ok := jq.Running[claimed.ID]; !ok {
		t.Error("job was removed from Running despite the stale Complete being rejected")
	}
	if claimed.LastWorkerID != workerA {
		t.Errorf("LastWorkerID = %v, want %v: ownership must not change on a rejected Complete", claimed.LastWorkerID, workerA)
	}
}
