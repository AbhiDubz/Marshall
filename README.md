# marshal

A distributed batch job scheduler modeled on SLURM, in Go: a Postgres-
backed control plane, node agents, and a deterministic in-memory
cluster simulator where the scheduling algorithms are developed and
measured.

Six functions are deliberately **stubbed** (`panic("not implemented")`)
with complete failing test suites — they are the owner's to implement:

| # | Function | Suite |
|---|---|---|
| 1 | `alloc.BinPackAllocator.Fit` | `pkg/alloc/binpack_test.go` |
| 2 | `sched.BackfillScheduler.Schedule` | `pkg/sched/backfill_test.go` |
| 3 | `sched.BackfillScheduler.computeReservation` | `pkg/sched/backfill_test.go` |
| 4 | `sched.GangScheduler.accumulate` | `pkg/sched/gang_test.go` |
| 5 | `preempt.Policy.SelectVictims` | `pkg/preempt/policy_test.go` |
| 6 | `dispatch.Dispatcher.dispatchExactlyOnce` | `pkg/dispatch/dispatch_test.go` |

Each stub's doc comment specifies the algorithm and the invariants its
tests check. `go test ./...` fails exactly these suites; everything
else passes.

## Architecture

```
                 ┌────────────┐   submit/status/cancel   ┌────────────┐
     users ───▶  │ marshalctl │ ───────gRPC─────────────▶│  marshald   │
                 └────────────┘                          │ control     │
                                                         │ plane       │
                        ┌────────────────────────────────│ (leader)    │
                        │        dispatch (exactly-once) └──────┬──────┘
                        ▼                                       │
                 ┌──────────────┐    heartbeats / status        │
                 │ marshal-agent│ ◀─────────────────────────────┘
                 │  (per node)  │                        ┌──────▼──────┐
                 └──────────────┘                        │  Postgres   │
                     runs jobs                           │ jobs, WAL,  │
                     as containers                       │ event log   │
                                                         └─────────────┘

     marshal-sim: the same schedulers/allocators against a virtual
     clock and simulated nodes — no cluster, fully deterministic.
```

- `pkg/types` — Job, Node, ResourceSpec, Allocation, the job state machine
- `pkg/store` — Postgres store + append-only event log (+ in-memory twin)
- `pkg/alloc` — firstfit, bestfit (working); binpack (stub #1)
- `pkg/sched` — fifo (working); backfill (stubs #2, #3); gang (stub #4)
- `pkg/preempt` — preemption policy (stub #5) + fair-share accounting
- `pkg/dispatch` — exactly-once dispatch protocol (stub #6)
- `pkg/sim` — deterministic simulator, trace generator, replay
- `pkg/chaos` — seeded fault injection + invariant checker
- `pkg/metrics` — Prometheus instrumentation

## Quickstart

```bash
# toolchain: Go 1.22+ (see docs/setup-notes.md for this machine's setup)
go test ./...          # passes except the six stub suites

# replay a committed trace deterministically
go build -o bin/marshal-sim ./cmd/marshal-sim
./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42

# same seed → byte-identical output
./bin/marshal-sim --trace traces/uniform.json --sched fifo --alloc firstfit --seed 42 | sha256sum

# compare every scheduler × allocator over the committed traces
./bin/marshal-sim --compare --chart docs/compare.svg

# regenerate traces (committed ones use seed 42, 200 jobs)
./bin/marshal-sim --gen --kind bursty --seed 42 --jobs 200 --out traces/bursty.json
```

## Results

Numbers are produced by real runs of `marshal-sim --compare` on the
committed traces. Rows for stubbed policies are intentionally blank —
fill them in after implementing the stubs; nothing here is ever
estimated.

| Trace | Scheduler | Allocator | Utilization (CPU) | Mean wait | P95 wait | Makespan |
|---|---|---|---|---|---|---|
| uniform | fifo | firstfit | 0.5318 | 3m10.07s | 10m2.358s | 1h4m57.205s |
| uniform | fifo | bestfit | 0.5337 | 3m27.246s | 11m6.387s | 1h4m43.4s |
| bimodal | fifo | firstfit | 0.5711 | 21m30.296s | 1h1m58.377s | 2h0m28.621s |
| bimodal | fifo | bestfit | 0.6040 | 24m16.374s | 1h4m21.103s | 1h53m54.69s |
| bursty | fifo | firstfit | 0.5218 | 38.607s | 3m54.352s | 45m1.005s |
| bursty | fifo | bestfit | 0.5218 | 43.2s | 4m27.536s | 45m1.005s |
| * | backfill | * | *(stubs #2/#3 pending)* | | | |
| * | gang | * | *(stub #4 pending on gang traces)* | | | |
| * | * | binpack | *(stub #1 pending)* | | | |

Regenerate with: `./bin/marshal-sim --compare`.

## Docs

- [docs/design.md](docs/design.md) — control plane, agent protocol, state machine
- [docs/decisions.md](docs/decisions.md) — ADRs
- [docs/chaos.md](docs/chaos.md) — fault model, invariants, replay
- [docs/setup-notes.md](docs/setup-notes.md) — environment deviations
