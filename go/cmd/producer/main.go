// Command producer is a demo producer: it periodically enqueues synthetic
// jobs against a server's JobQueueService over gRPC.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
)

func main() {
	serverAddr := flag.String("server-addr", "localhost:50051", "address of the JobQueueService gRPC server")
	interval := flag.Duration("interval", 50*time.Millisecond, "how often to enqueue a job")
	maxAttempts := flag.Uint("max-attempts", 3, "max attempts before a job is dead-lettered")
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

	slog.Info("running", "server_addr", *serverAddr, "interval", *interval, "msg", "press Ctrl+C to stop")
	runProducer(ctx, client, *interval, uint32(*maxAttempts))
	slog.Info("clean shutdown complete")
}

func runProducer(ctx context.Context, client jobqueuev1.JobQueueServiceClient, interval time.Duration, maxAttempts uint32) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req := &jobqueuev1.EnqueueRequest{
				Payload:     json.RawMessage(fmt.Sprintf(`{"n": %d}`, n)),
				MaxAttempts: maxAttempts,
			}
			resp, err := client.Enqueue(ctx, req)
			if err != nil {
				slog.Warn("enqueue failed", "n", n, "error", err)
				continue
			}
			slog.Debug("enqueued job", "n", n, "job_id", resp.GetJobId(), "accepted", resp.GetAccepted())
			n++
		}
	}
}
