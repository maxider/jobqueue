package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/maxider/job-queue/queue"
)

var (
	leaseTime   = time.Second
	metricsAddr = ":2112"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	jq := queue.NewJobQueue(0, leaseTime)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		runMetricsServer(ctx, metricsAddr)
	})

	wg.Go(func() {
		runMetricsSampler(ctx, jq, 500*time.Millisecond)
	})

	wg.Go(func() {
		runSweeper(ctx, jq, 500*time.Millisecond)
	})

	wg.Go(func() {
		runProducer(ctx, jq, 50*time.Millisecond)
	})

	const NumWorkers = 4
	for range NumWorkers {
		wg.Go(func() {
			workerId := uuid.New()
			runWorker(ctx, jq, workerId)
		})
	}

	slog.Info("running", "metrics_addr", metricsAddr, "msg", "press Ctrl+C to stop")
	<-ctx.Done()
	slog.Info("shutdown signal received, waiting for in-flight work to finish...")

	wg.Wait()
	slog.Info("clean shutdown complete")

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

func runWorker(ctx context.Context, jq *queue.JobQueue, workerId uuid.UUID) {
	for {
		select {
		case <-ctx.Done():
			return
		default: //leave select
		}

		j := jq.Claim(workerId)
		if j == nil {
			//no job ready so wait and try again
			time.Sleep(400 * time.Millisecond)
			continue
		}

		slog.Debug("processing job", "worker_id", workerId, "job_id", j.ID, "attempt", j.Attempts)
		start := time.Now()
		time.Sleep(200*time.Millisecond + time.Duration(50-rand.Intn(100))*time.Millisecond)

		if stallChance := rand.Float32(); stallChance > .9 {
			time.Sleep(leaseTime)
		}
		jobProcessingDuration.Observe(time.Since(start).Seconds())

		if errChance := rand.Float32(); errChance > .8 {
			jobsFailedTotal.Inc()
			if err := jq.Fail(j.ID, workerId, fmt.Errorf("worker %s failed", workerId)); err != nil {
				slog.Warn("fail rejected", "worker_id", workerId, "job_id", j.ID, "error", err)
			} else if j.JobStatus == queue.StatusDead {
				jobsDeadLetteredTotal.Inc()
			}
			continue
		}

		if err := jq.Complete(j.ID, workerId); err != nil {
			slog.Warn("complete rejected", "worker_id", workerId, "job_id", j.ID, "error", err)
		} else {
			jobsCompletedTotal.Inc()
		}
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

func newJob(payload string) *queue.Job {
	return &queue.Job{
		ID:              uuid.New(),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		Attempts:        0,
		MaxAttempts:     3,
		JobStatus:       queue.StatusPending,
		LeaseExpiration: time.Time{},
		Payload:         json.RawMessage(fmt.Sprintf(`{"n": %s}`, payload)),
		LastError:       "",
		LastWorkerId:    uuid.UUID{},
	}
}

func runProducer(ctx context.Context, jq *queue.JobQueue, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			{
				j := newJob(strconv.Itoa(n))
				if jq.Enqueue(j) {
					jobsEnqueuedTotal.Inc()
				}
				slog.Debug("enqueued job", "n", n, "job_id", j.ID)
				n++
			}
		}
	}
}
