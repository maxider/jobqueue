package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"uuid"
)

var (
	leaseTime = time.Second
)

func main() {
	jq := NewJobQueue(0, leaseTime)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
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

	fmt.Println("running — press Ctrl+C to stop")
	<-ctx.Done()
	fmt.Println("shutdown signal received, waiting for in-flight work to finish...")

	wg.Wait()
	fmt.Println("clean shutdown complete")

}

func runWorker(ctx context.Context, jq *JobQueue, workerId uuid.UUID) {
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

		fmt.Printf("worker %s: processing job %s (attempt %d)\n", workerId, j.ID, j.Attempts)
		time.Sleep(200*time.Millisecond + time.Duration(50-rand.Intn(100))*time.Millisecond)

		if stallChance := rand.Float32(); stallChance > .9 {
			time.Sleep(leaseTime)
		}

		if errChance := rand.Float32(); errChance > .8 {
			if err := jq.Fail(j.ID, workerId, fmt.Errorf("worker %s failed", workerId)); err != nil {
				fmt.Printf("worker %s: fail rejected: %v\n", workerId, err)
			}
			continue
		}

		if err := jq.Complete(j.ID, workerId); err != nil {
			fmt.Printf("worker %s: complete rejected: %v\n", workerId, err)
		}
	}
}

func runSweeper(ctx context.Context, jq *JobQueue, interval time.Duration) {
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

func newJob(payload string) *Job {
	return &Job{
		ID:              uuid.New(),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		Attempts:        0,
		MaxAttempts:     3,
		JobStatus:       StatusPending,
		LeaseExpiration: time.Time{},
		Payload:         json.RawMessage(fmt.Sprintf(`{"n": %s}`, payload)),
		LastError:       "",
		LastWorkerId:    uuid.UUID{},
	}
}

func runProducer(ctx context.Context, jq *JobQueue, interval time.Duration) {
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
				jq.Enqueue(j)
				fmt.Printf("producer: Enqued Job %d ID: %s\n", n, j.ID)
				n++
			}
		}
	}
}
