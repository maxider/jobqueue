// Package rpc adapts queue.JobQueue to a gRPC service, so producers and
// workers can reach it over the network instead of holding a direct
// in-process reference to it.
package rpc

import (
	"context"
	"errors"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
	"github.com/maxider/job-queue/queue"
)

// Server implements jobqueuev1.JobQueueServiceServer on top of a
// *queue.JobQueue.
type Server struct {
	jobqueuev1.UnimplementedJobQueueServiceServer
	jq     *queue.JobQueue
	claims *claimClock
}

func NewServer(jq *queue.JobQueue) *Server {
	return &Server{jq: jq, claims: newClaimClock()}
}

func (s *Server) Enqueue(_ context.Context, req *jobqueuev1.EnqueueRequest) (*jobqueuev1.EnqueueResponse, error) {
	j := &queue.Job{
		ID:          uuid.New(),
		MaxAttempts: uint16(req.GetMaxAttempts()),
		Payload:     req.GetPayload(),
	}

	accepted := s.jq.Enqueue(j)
	if accepted {
		jobsEnqueuedTotal.Inc()
	}
	return &jobqueuev1.EnqueueResponse{
		JobId:    j.ID.String(),
		Accepted: accepted,
	}, nil
}

func (s *Server) Claim(_ context.Context, req *jobqueuev1.ClaimRequest) (*jobqueuev1.ClaimResponse, error) {
	workerID, err := uuid.Parse(req.GetWorkerId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker_id: %v", err)
	}

	j := s.jq.Claim(workerID)
	if j == nil {
		return &jobqueuev1.ClaimResponse{Found: false}, nil
	}

	s.claims.start(j.ID)
	return &jobqueuev1.ClaimResponse{Found: true, Job: toProto(j)}, nil
}

func (s *Server) Complete(_ context.Context, req *jobqueuev1.CompleteRequest) (*jobqueuev1.CompleteResponse, error) {
	jobID, workerID, err := parseIDs(req.GetJobId(), req.GetWorkerId())
	if err != nil {
		return nil, err
	}

	if err := s.jq.Complete(jobID, workerID); err != nil {
		return nil, toStatusError(err)
	}
	if elapsed, ok := s.claims.stop(jobID); ok {
		jobProcessingDuration.Observe(elapsed.Seconds())
	}
	jobsCompletedTotal.Inc()
	return &jobqueuev1.CompleteResponse{}, nil
}

func (s *Server) Fail(_ context.Context, req *jobqueuev1.FailRequest) (*jobqueuev1.FailResponse, error) {
	jobID, workerID, err := parseIDs(req.GetJobId(), req.GetWorkerId())
	if err != nil {
		return nil, err
	}

	if err := s.jq.Fail(jobID, workerID, errors.New(req.GetError())); err != nil {
		return nil, toStatusError(err)
	}
	if elapsed, ok := s.claims.stop(jobID); ok {
		jobProcessingDuration.Observe(elapsed.Seconds())
	}
	jobsFailedTotal.Inc()
	if s.jq.IsDead(jobID) {
		jobsDeadLetteredTotal.Inc()
	}
	return &jobqueuev1.FailResponse{}, nil
}

func parseIDs(rawJobID, rawWorkerID string) (jobID, workerID uuid.UUID, err error) {
	jobID, err = uuid.Parse(rawJobID)
	if err != nil {
		return uuid.UUID{}, uuid.UUID{}, status.Errorf(codes.InvalidArgument, "invalid job_id: %v", err)
	}
	workerID, err = uuid.Parse(rawWorkerID)
	if err != nil {
		return uuid.UUID{}, uuid.UUID{}, status.Errorf(codes.InvalidArgument, "invalid worker_id: %v", err)
	}
	return jobID, workerID, nil
}

func toStatusError(err error) error {
	switch {
	case errors.Is(err, queue.ErrJobNotRunning):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, queue.ErrWorkerIDMismatch):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func toProto(j *queue.Job) *jobqueuev1.Job {
	return &jobqueuev1.Job{
		Id:              j.ID.String(),
		CreatedAt:       timestamppb.New(j.CreatedAt),
		UpdatedAt:       timestamppb.New(j.UpdatedAt),
		Attempts:        uint32(j.Attempts),
		MaxAttempts:     uint32(j.MaxAttempts),
		Status:          toProtoStatus(j.JobStatus),
		LeaseExpiration: timestamppb.New(j.LeaseExpiration),
		Payload:         j.Payload,
		LastError:       j.LastError,
		LastWorkerId:    j.LastWorkerID.String(),
	}
}

func toProtoStatus(s queue.Status) jobqueuev1.JobStatus {
	switch s {
	case queue.StatusPending:
		return jobqueuev1.JobStatus_JOB_STATUS_PENDING
	case queue.StatusRunning:
		return jobqueuev1.JobStatus_JOB_STATUS_RUNNING
	case queue.StatusComplete:
		return jobqueuev1.JobStatus_JOB_STATUS_COMPLETE
	case queue.StatusDead:
		return jobqueuev1.JobStatus_JOB_STATUS_DEAD
	default:
		return jobqueuev1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}
