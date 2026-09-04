# TODO

## Repo hygiene
- [x] Remove `job-queue.exe` from git and add a real `.gitignore`
- [x] Commit the scenario/example test files
- [x] Add a `.gitattributes` to normalize line endings
- [x] Split into `queue/` (library), `cmd/job-queue/` (demo binary), and `deploy/` (Docker/Prometheus/Grafana config) instead of one flat `package main`

## Documentation
- [x] Write a `README.md`: what this is, how to run it, and a design write-up (min-heap vs. sorted slice, single mutex, lease-based reclamation + dead-lettering, at-least-once delivery and consumer idempotency)

## Code quality / idiom cleanup
- [x] Lowercase error strings (`staticcheck` ST1005)
- [x] Simplify `JobHeap.Less` to use `.Before(...)`
- [x] Replace `fmt.Printf`/`fmt.Println` logging with `log/slog`
- [x] Resolve the `//TODO: Add debug level logging` comment in `queue/job_queue.go`
- [ ] Consider unexporting `JobQueue.Pending` / `.Running` / `.DeadJobs` (or documenting more forcefully) — they're only safe to touch under `jq.mu`, but nothing stops external callers from mutating them directly today

## Observability
- [x] Prometheus metrics (`cmd/job-queue/metrics.go`): enqueue/complete/fail/dead-letter counters, pending/running gauges, processing-duration histogram, exposed via `/metrics`
- [x] Docker Compose stack (app + Prometheus + Grafana) with a provisioned dashboard

## Engineering maturity signals
- [x] Add a GitHub Actions workflow: `go build ./...`, `go vet ./...`, `go test ./... -race`, `golangci-lint`
- [x] Add a `golangci-lint` config and fix what it flags

## Network layer
- [x] Define a `JobQueueService` gRPC contract (`api/jobqueue/v1/jobqueue.proto`) and check in generated code (`gen/jobqueue/v1/`)
- [x] Add `rpc/` adapting `queue.JobQueue` to `JobQueueService`, owning event-driven Prometheus metrics (enqueue/complete/fail/dead-letter counters + processing-duration histogram)
- [x] Split the single demo binary into `cmd/server` (queue + sweeper + gRPC API + `/metrics`), `cmd/worker`, and `cmd/producer` (network gRPC clients)
- [x] Update `deploy/` (Dockerfile, docker-compose, prometheus.yml) for the three-service split
- [x] Add `cmd/worker-single`, a one-consumer-per-process worker (sharing its claim/process/fail loop with `cmd/worker` via `internal/worker`), so consumer count can be adjusted by starting/stopping processes instead of restarting `cmd/worker` with a different `-workers` value
- [ ] Long-poll or server-streaming `Claim` instead of unary polling, to cut idle-worker RPC chatter
- [ ] TLS + auth on the gRPC connection instead of insecure credentials
- [x] A `Makefile` (`make proto`) wrapping the `protoc` invocation instead of only documenting it by hand in the README

## Stretch (only if turning this into more than a portfolio demo)
- [ ] Thread `context.Context` through `Claim`/`Complete`/`Fail` for cancellation/timeouts
- [ ] Pluggable persistence (currently fully in-memory — noted as a known limitation in the README)
- [ ] gRPC health checking (`grpc.health.v1.Health`) on `cmd/server`, so `deploy/docker-compose.yml` can gate `worker`/`worker-single`/`producer` on `depends_on: condition: service_healthy` instead of just `server` having started
