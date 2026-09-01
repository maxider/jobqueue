package main

import (
	"sync"
	"testing"
	"time"

	"uuid"
)

// This file is a worked example of Scenario 5: proving a job *handler's*
// idempotency strategy, not JobQueue's own correctness. It's kept separate
// from jobQueue_test.go / jobQueue_scenarios_test.go on purpose — once a
// real handler exists (e.g. an actual "charge card" job), this pattern
// should move to live next to that handler instead of here.

// fakePaymentProcessor stands in for a real payment gateway that supports
// idempotency keys: charging twice with the same key is a no-op server-side,
// same as e.g. Stripe's Idempotency-Key header.
type fakePaymentProcessor struct {
	mu      sync.Mutex
	charges map[string]int // idempotency key -> times actually applied; must never exceed 1
}

func newFakePaymentProcessor() *fakePaymentProcessor {
	return &fakePaymentProcessor{charges: make(map[string]int)}
}

func (p *fakePaymentProcessor) Charge(idempotencyKey string, amountCents int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.charges[idempotencyKey] > 0 {
		return
	}
	p.charges[idempotencyKey] = 1
}

func (p *fakePaymentProcessor) ChargeCount(idempotencyKey string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.charges[idempotencyKey]
}

// chargeCardHandler is an example job handler built on top of JobQueue. It
// derives its idempotency key from the job's ID, so re-running the same job
// (e.g. after a crash between the charge succeeding and Complete being
// called) never charges the card twice.
func chargeCardHandler(p *fakePaymentProcessor, j *Job) {
	p.Charge(j.ID.String(), 999)
}

// Scenario 5: a worker's handler runs successfully but crashes before
// calling Complete. The lease expires, Sweep puts the job back in Pending,
// and a second worker claims and runs the handler again. The handler ran
// twice, but the idempotency key means the card was only ever charged once.
func TestChargeCardHandlerIsIdempotentAcrossReclaim(t *testing.T) {
	jq := NewJobQueue(0, 5*time.Millisecond)
	processor := newFakePaymentProcessor()

	j := jobAt(time.Now())
	j.MaxAttempts = 5
	jq.enqueueLocked(j)

	workerA := uuid.New()
	claimed := jq.Claim(workerA)
	if claimed == nil {
		t.Fatal("Claim() = nil, want the enqueued job")
	}

	chargeCardHandler(processor, claimed)
	// Simulated crash: the handler succeeded, but Complete is never called.

	time.Sleep(10 * time.Millisecond) // let the lease expire
	runSweep(t, jq)

	workerB := uuid.New()
	reclaimed := jq.Claim(workerB)
	if reclaimed == nil || reclaimed.ID != claimed.ID {
		t.Fatalf("Claim() = %v, want the reclaimed job %v", reclaimed, claimed)
	}

	chargeCardHandler(processor, reclaimed)

	if got := processor.ChargeCount(claimed.ID.String()); got != 1 {
		t.Errorf("charge count for job %s = %d, want 1: handler ran twice but must only charge once", claimed.ID, got)
	}

	if err := jq.Complete(reclaimed.ID, workerB); err != nil {
		t.Fatalf("Complete() error = %v, want nil", err)
	}
}
