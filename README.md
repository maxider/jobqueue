# job-queue

A concurrent, in-memory job queue in Go with lease-based claiming, automatic
retries, dead-lettering, and Prometheus/Grafana observability.

## Running it

```sh
go run .
```

This starts a demo harness: a producer enqueuing synthetic jobs, four
workers claiming/processing/completing (or randomly failing) them, and a
sweeper reclaiming jobs whose lease expired without being completed or
failed (e.g. a crashed worker). Metrics are exposed at
`http://localhost:2112/metrics`.

For the full observability stack (app + Prometheus + a pre-provisioned
Grafana dashboard):

```sh
docker compose up --build
```

Then open `http://localhost:3000` for Grafana (anonymous viewer access is
enabled) — the "Job Queue" dashboard shows queue depth, throughput,
failure rate, and processing duration out of the box. Raw Prometheus is at
`http://localhost:9090`.

## Design

**Min-heap over a sorted slice.** `Claim` needs to repeatedly pull the
oldest pending job while `Enqueue`/`Fail` insert jobs (including retries)
at arbitrary points in time-order. A `container/heap`-backed min-heap
ordered by `UpdatedAt` gives O(log n) push/pop instead of O(n) insertion
into a sorted slice, and jobs put back into the queue by `Fail` or `Sweep`
naturally re-sort by their new `UpdatedAt`.

**A single mutex.** `JobQueue` is a small amount of tightly-coupled state
(the pending heap plus the running/dead-lettered maps) that has to move
between them atomically — e.g. `Claim` must remove a job from `Pending`
and add it to `Running` as one operation, or a second worker could claim
the same job. A single `sync.Mutex` guarding all of it is simpler and
easier to reason about than fine-grained locking, and the queue isn't a
throughput bottleneck at the scale this is built for.

**Lease-based reclamation + dead-lettering.** A worker that claims a job
gets a time-limited lease, not permanent ownership. If it doesn't call
`Complete` or `Fail` before the lease expires — because it crashed, hung,
or lost its connection — a periodic `Sweep` treats the expired lease as a
failure and puts the job back in `Pending` for another worker to pick up.
Jobs that exceed `MaxAttempts` (via explicit `Fail` or repeated
lease expiry) move to `DeadJobs` instead of retrying forever.

**At-least-once delivery.** Because of lease-based reclamation, the same
job can be claimed and processed by two different workers if the first
one is merely slow rather than actually dead (its lease expires, a second
worker claims and starts processing, and *then* the first worker finishes
and calls `Complete`). Job handlers built on top of this queue must be
idempotent — e.g. keying external side effects (like a payment charge) by
job ID. See `chargecard_example_test.go` for a worked example.

## Metrics

Exposed via `/metrics` in Prometheus format:

| Metric | Type | Meaning |
|---|---|---|
| `jobs_enqueued_total` | counter | Jobs successfully enqueued |
| `jobs_completed_total` | counter | Jobs completed successfully |
| `jobs_failed_total` | counter | Job attempts that failed (retries + dead-letters) |
| `jobs_dead_lettered_total` | counter | Jobs moved to the dead-letter set |
| `queue_pending_jobs` | gauge | Jobs currently waiting to be claimed |
| `queue_running_jobs` | gauge | Jobs currently claimed and in flight |
| `job_processing_duration_seconds` | histogram | Time from claim to completion/failure |

Metrics are recorded in `main.go` (the demo harness), not inside
`jobQueue.go` — the queue itself has no Prometheus dependency, which keeps
it usable as a plain library regardless of how a caller wants to observe
it.

## Testing

```sh
go test ./... -race
```

`main.go` (the demo harness) is intentionally uncovered; `jobQueue.go` and
`job.go` are the tested surface.

## Known limitations

- Fully in-memory — state doesn't survive a restart. Pluggable persistence
  is a natural next step but isn't implemented here.
- `Claim`/`Complete`/`Fail` don't take a `context.Context`, so there's no
  built-in way to time out or cancel an individual call.
