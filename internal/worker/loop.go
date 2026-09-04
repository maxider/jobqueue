// Package worker holds the claim/process/complete-or-fail loop shared by
// every worker binary (cmd/worker's in-process pool and cmd/worker-single's
// one-consumer-per-process model), so the two don't drift apart.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
	"uuid"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
)

// Run claims jobs from client as workerID until ctx is done, "processing"
// each one (simulated work with a random failure/stall chance) and
// reporting back Complete or Fail.
func Run(ctx context.Context, client jobqueuev1.JobQueueServiceClient, workerID uuid.UUID, leaseTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default: //leave select
		}

		resp, err := client.Claim(ctx, &jobqueuev1.ClaimRequest{WorkerId: workerID.String()})
		if err != nil {
			slog.Warn("claim failed", "worker_id", workerID, "error", err)
			time.Sleep(400 * time.Millisecond)
			continue
		}
		if !resp.GetFound() {
			//no job ready so wait and try again
			time.Sleep(400 * time.Millisecond)
			continue
		}
		j := resp.GetJob()

		slog.Debug("processing job", "worker_id", workerID, "job_id", j.GetId(), "attempt", j.GetAttempts())
		time.Sleep(200*time.Millisecond + time.Duration(50-rand.Intn(100))*time.Millisecond)

		if stallChance := rand.Float32(); stallChance > .9 {
			time.Sleep(leaseTime)
		}

		if errChance := rand.Float32(); errChance > .8 {
			failReq := &jobqueuev1.FailRequest{
				JobId:    j.GetId(),
				WorkerId: workerID.String(),
				Error:    fmt.Sprintf("worker %s failed", workerID),
			}
			if _, err := client.Fail(ctx, failReq); err != nil {
				slog.Warn("fail rejected", "worker_id", workerID, "job_id", j.GetId(), "error", err)
			}
			continue
		}

		completeReq := &jobqueuev1.CompleteRequest{JobId: j.GetId(), WorkerId: workerID.String()}
		if _, err := client.Complete(ctx, completeReq); err != nil {
			slog.Warn("complete rejected", "worker_id", workerID, "job_id", j.GetId(), "error", err)
		}
	}
}
