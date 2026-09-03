// Command worker is a demo consumer: it claims jobs from a server's
// JobQueueService over gRPC, "processes" them (simulated work with a
// random failure/stall chance), and reports back Complete or Fail.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
)

func main() {
	serverAddr := flag.String("server-addr", "localhost:50051", "address of the JobQueueService gRPC server")
	numWorkers := flag.Uint("workers", 4, "number of concurrent worker goroutines to run in this process")
	// Should match the server's -lease-time; used only to occasionally
	// simulate a worker that stalls past its lease.
	leaseTime := flag.Duration("lease-time", time.Second, "lease duration configured on the server, used to simulate the occasional stalled job")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to dial server", "server_addr", *serverAddr, "error", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()
	client := jobqueuev1.NewJobQueueServiceClient(conn)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for range *numWorkers {
		wg.Go(func() {
			workerId := uuid.New()
			runWorker(ctx, client, workerId, *leaseTime)
		})
	}

	slog.Info("running", "server_addr", *serverAddr, "workers", *numWorkers, "msg", "press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for in-flight work to finish...")

	wg.Wait()
	slog.Info("clean shutdown complete")
}

func runWorker(ctx context.Context, client jobqueuev1.JobQueueServiceClient, workerId uuid.UUID, leaseTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default: //leave select
		}

		resp, err := client.Claim(ctx, &jobqueuev1.ClaimRequest{WorkerId: workerId.String()})
		if err != nil {
			slog.Warn("claim failed", "worker_id", workerId, "error", err)
			time.Sleep(400 * time.Millisecond)
			continue
		}
		if !resp.GetFound() {
			//no job ready so wait and try again
			time.Sleep(400 * time.Millisecond)
			continue
		}
		j := resp.GetJob()

		slog.Debug("processing job", "worker_id", workerId, "job_id", j.GetId(), "attempt", j.GetAttempts())
		time.Sleep(200*time.Millisecond + time.Duration(50-rand.Intn(100))*time.Millisecond)

		if stallChance := rand.Float32(); stallChance > .9 {
			time.Sleep(leaseTime)
		}

		if errChance := rand.Float32(); errChance > .8 {
			failReq := &jobqueuev1.FailRequest{
				JobId:    j.GetId(),
				WorkerId: workerId.String(),
				Error:    fmt.Sprintf("worker %s failed", workerId),
			}
			if _, err := client.Fail(ctx, failReq); err != nil {
				slog.Warn("fail rejected", "worker_id", workerId, "job_id", j.GetId(), "error", err)
			}
			continue
		}

		completeReq := &jobqueuev1.CompleteRequest{JobId: j.GetId(), WorkerId: workerId.String()}
		if _, err := client.Complete(ctx, completeReq); err != nil {
			slog.Warn("complete rejected", "worker_id", workerId, "job_id", j.GetId(), "error", err)
		}
	}
}
