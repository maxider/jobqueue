package queue

import (
	"encoding/json"
	"time"
	"uuid"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusRunning  Status = "running"
	StatusComplete Status = "complete"
	StatusDead     Status = "dead"
)

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
	LastWorkerId    uuid.UUID
}
