# TODO

## Repo hygiene
- [x] Remove `job-queue.exe` from git and add a real `.gitignore`
- [x] Commit the scenario/example test files
- [x] Add a `.gitattributes` to normalize line endings

## Documentation
- [x] Write a `README.md`: what this is, how to run it, and a design write-up (min-heap vs. sorted slice, single mutex, lease-based reclamation + dead-lettering, at-least-once delivery and consumer idempotency)

## Code quality / idiom cleanup
- [x] Lowercase error strings (`staticcheck` ST1005)
- [x] Simplify `JobHeap.Less` to use `.Before(...)`
- [x] Replace `fmt.Printf`/`fmt.Println` logging with `log/slog`
- [x] Resolve the `//TODO: Add debug level logging` comment in `jobQueue.go`
- [ ] Consider unexporting `JobQueue.Pending` / `.Running` / `.DeadJobs` (or documenting more forcefully) — they're only safe to touch under `jq.mu`, but nothing stops external callers from mutating them directly today

## Observability
- [x] Prometheus metrics (`metrics.go`): enqueue/complete/fail/dead-letter counters, pending/running gauges, processing-duration histogram, exposed via `/metrics`
- [x] Docker Compose stack (app + Prometheus + Grafana) with a provisioned dashboard

## Engineering maturity signals
- [x] Add a GitHub Actions workflow: `go build ./...`, `go vet ./...`, `go test ./... -race`, `golangci-lint`
- [x] Add a `golangci-lint` config and fix what it flags

## Stretch (only if turning this into more than a portfolio demo)
- [ ] Thread `context.Context` through `Claim`/`Complete`/`Fail` for cancellation/timeouts
- [ ] Pluggable persistence (currently fully in-memory — noted as a known limitation in the README)
