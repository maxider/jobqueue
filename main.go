package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"uuid"
)

const (
	numWorkers = 4
	numJobs    = 10
)

func main() {
	jobs := make(chan Job, numJobs)
	results := make(chan Job, numJobs)

	var wg sync.WaitGroup
	for id := 1; id <= numWorkers; id++ {
		wg.Go(func() {
			worker(id, jobs, results)
		})
	}

	for i := 0; i < numJobs; i++ {
		jobs <- newJob(fmt.Sprintf(`{"n": %d}`, i))
	}
	close(jobs)

	// Close results only after every worker has stopped writing to it,
	// otherwise the range below would block forever waiting for more values.
	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		fmt.Printf("job %s finished with status %s (attempt %d)\n", r.ID, r.JobStatus, r.Attempts)
	}
}

// worker pulls jobs off the shared channel until it's closed and drained,
// so any number of workers can run concurrently without double-processing a job.
func worker(id int, jobs <-chan Job, results chan<- Job) {
	for j := range jobs {
		j.Attempts++
		j.JobStatus = StatusRunning
		fmt.Printf("worker %d: starting job %s\n", id, j.ID)

		time.Sleep(100 * time.Millisecond) // simulate work

		j.JobStatus = StatusComplete
		j.UpdatedAt = time.Now()
		fmt.Printf("worker %d: finished job %s\n", id, j.ID)

		results <- j
	}
}

func newJob(payload string) Job {
	now := time.Now()
	return Job{
		ID:          uuid.New(),
		CreatedAt:   now,
		UpdatedAt:   now,
		MaxAttempts: 3,
		JobStatus:   StatusPending,
		Payload:     json.RawMessage(payload),
	}
}
