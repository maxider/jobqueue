// Command worker-single runs exactly one worker/consumer per process: it
// claims jobs from a server's JobQueueService over gRPC, "processes" them,
// and reports back Complete or Fail. Unlike cmd/worker, which runs a fixed
// pool of goroutines sized by -workers, this binary has no pool at all —
// you adjust how many consumers are running by starting or stopping
// additional worker-single processes (or, in docker-compose, by scaling the
// worker-single service) instead of restarting a process with a new flag
// value.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
	"github.com/maxider/job-queue/internal/worker"
)

func main() {
	serverAddr := flag.String("server-addr", "localhost:50051", "address of the JobQueueService gRPC server")
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

	workerID := uuid.New()
	slog.Info("running", "server_addr", *serverAddr, "worker_id", workerID, "msg", "press Ctrl+C to stop")

	worker.Run(ctx, client, workerID, *leaseTime)

	slog.Info("clean shutdown complete")
}
