// Command worker runs a fixed pool of worker goroutines in one process: it
// claims jobs from a server's JobQueueService over gRPC, "processes" them,
// and reports back Complete or Fail. To add or remove consumers you restart
// this process with a different -workers value; see cmd/worker-single for a
// one-consumer-per-process alternative that can be scaled by starting or
// stopping additional processes/containers instead.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
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
			workerID := uuid.New()
			worker.Run(ctx, client, workerID, *leaseTime)
		})
	}

	slog.Info("running", "server_addr", *serverAddr, "workers", *numWorkers, "msg", "press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for in-flight work to finish...")

	wg.Wait()
	slog.Info("clean shutdown complete")
}
