package queue

import (
	"encoding/json"
	"time"
	"uuid"
)

// Status is a Job's position in its lifecycle: Pending -> Running, then
// either Complete or, after MaxAttempts is exhausted, Dead. A failed attempt
// that still has retries left goes back to Pending rather than Dead.
type Status string

const (
	StatusPending  Status = "pending"
	StatusRunning  Status = "running"
	StatusComplete Status = "complete"
	StatusDead     Status = "dead"
)

// Job is a unit of work tracked by a JobQueue. Callers construct one with an
// ID, MaxAttempts, and Payload and pass it to JobQueue.Enqueue; the queue
// owns every other field from that point on.
type Job struct {
	ID              uuid.UUID
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Attempts        uint16
	MaxAttempts     uint16
	JobStatus       Status
	LeaseExpiration time.Time
	Payload         json.RawMessage
	LastError       string
	// LastWorkerID is the worker that currently holds (or most recently
	// held) this job's lease; Complete/Fail reject calls from any other
	// worker ID for a Running job.
	LastWorkerID uuid.UUID
}
