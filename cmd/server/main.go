// Command server runs the JobQueue as a standalone gRPC service: producers
// and workers reach it over the network instead of linking against the
// queue package directly.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	jobqueuev1 "github.com/maxider/job-queue/gen/jobqueue/v1"
	"github.com/maxider/job-queue/queue"
	"github.com/maxider/job-queue/rpc"
)

func main() {
	grpcAddr := flag.String("grpc-addr", ":50051", "address to serve the JobQueueService gRPC API on")
	metricsAddr := flag.String("metrics-addr", ":2112", "address to serve Prometheus metrics on")
	leaseTime := flag.Duration("lease-time", time.Second, "how long a worker has to Complete/Fail a claimed job before the sweeper reclaims it")
	maxJobs := flag.Uint("max-jobs", 0, "maximum number of pending+running jobs (0 = unbounded)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	jq := queue.NewJobQueue(uint16(*maxJobs), *leaseTime)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		runMetricsServer(ctx, *metricsAddr)
	})

	wg.Go(func() {
		runMetricsSampler(ctx, jq, 500*time.Millisecond)
	})

	wg.Go(func() {
		runSweeper(ctx, jq, 500*time.Millisecond)
	})

	wg.Go(func() {
		runGRPCServer(ctx, jq, *grpcAddr)
	})

	slog.Info("running", "grpc_addr", *grpcAddr, "metrics_addr", *metricsAddr, "msg", "press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for in-flight work to finish...")

	wg.Wait()
	slog.Info("clean shutdown complete")
}

func runGRPCServer(ctx context.Context, jq *queue.JobQueue, addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("grpc listen failed", "error", err)
		return
	}

	srv := grpc.NewServer()
	jobqueuev1.RegisterJobQueueServiceServer(srv, rpc.NewServer(jq))

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	if err := srv.Serve(lis); err != nil {
		slog.Error("grpc server failed", "error", err)
	}
}

func runMetricsServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("metrics server shutdown", "error", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server failed", "error", err)
	}
}

func runSweeper(ctx context.Context, jq *queue.JobQueue, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jq.Sweep()
		}
	}
}
