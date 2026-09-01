# TODO

## Repo hygiene (do these first — quick, high visibility)
- [x] Remove `job-queue.exe` from git (`git rm --cached job-queue.exe`) and add a real `.gitignore` (at minimum: `*.exe`, `/bin`, `*.test`, `*.out`)
- [ ] Commit the currently untracked test files (`jobQueue_scenarios_test.go`, `chargecard_example_test.go`) or fold them into the existing test files
- [ ] Fix the `uuid` import: change `"uuid"` to `"github.com/google/uuid"` in `job.go` and `jobQueue.go`, then run `go mod tidy` so `go.mod` actually declares the dependency (currently only builds here because of a local `GOROOT` shim — will fail `go build` on any other machine)
- [ ] Add a `.gitattributes` (or normalize line endings) — git currently warns about LF→CRLF on `main.go` / `jobQueue_test.go`

## Documentation
- [ ] Write a `README.md`: what this is, how to run it (`go run .`), and a short design write-up — why a min-heap over a sorted slice, why a single mutex, why lease-based reclamation + dead-lettering, what a consumer needs to do (idempotency) given at-least-once delivery

## Code quality / idiom cleanup
- [ ] Lowercase error strings per Go convention (`errors.New("Job not running")` → `"job not running"`, etc. — `staticcheck` ST1005)
- [ ] Simplify `JobHeap.Less`: `return (*jh)[i].UpdatedAt.Before((*jh)[j].UpdatedAt)` instead of the `.Compare(...) == -1` branch
- [ ] Replace `fmt.Printf`/`fmt.Println` logging in `Sweep`/`main.go` with `log/slog`
- [ ] Resolve the `//TODO: Add debug level logging` comment in `jobQueue.go` (goes away once slog is in)
- [ ] Consider unexporting `JobQueue.Pending` / `.Running` / `.DeadJobs` (or documenting more forcefully) — they're only safe to touch under `jq.mu`, but nothing stops external callers from mutating them directly today

## Engineering maturity signals (nice-to-have, cheap, high signal)
- [ ] Add a GitHub Actions workflow: `go build ./...`, `go vet ./...`, `go test ./... -race`
- [ ] Add a `golangci-lint` config and fix what it flags
- [ ] Note current test coverage (54.9%) in the README and call out `main.go` (demo harness) as intentionally uncovered

## Stretch (only if turning this into more than a portfolio demo)
- [ ] Thread `context.Context` through `Claim`/`Complete`/`Fail` for cancellation/timeouts
- [ ] Pluggable persistence (currently fully in-memory — note this as a known limitation if not fixing it)
